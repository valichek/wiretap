// Package proxy implements TCP shadow proxies for rpcx connections.
// Each Proxy listens on a configured port, dials the upstream on accept,
// and forwards both directions of the rpcx wire protocol while emitting
// paired request/response Events to a Sink.
package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/smallnest/rpcx/protocol"

	"wiretap/codec"
	"wiretap/config"
	"wiretap/event"
)

// Dialer dials the upstream. Replaceable in tests.
type Dialer func(network, addr string) (net.Conn, error)

// drainReason describes why pending entries were flushed at connection close.
type drainReason string

const (
	reasonUpstreamClosed drainReason = "upstream_closed"
	reasonShutdown       drainReason = "shutdown"
)

// Proxy is one shadow listener forwarding rpcx traffic between a client and
// an upstream server. Run is the entry point.
type Proxy struct {
	listen        string
	upstream      string
	src           string
	dst           string
	truncateBytes int
	sink          event.Sink
	log           zerolog.Logger
	dialer        Dialer
}

// New constructs a Proxy from a config entry.
func New(cfg config.ProxyConfig, sink event.Sink, log zerolog.Logger, truncateBytes int) *Proxy {
	return &Proxy{
		listen:        cfg.Listen,
		upstream:      cfg.Upstream,
		src:           cfg.Src,
		dst:           cfg.Dst,
		truncateBytes: truncateBytes,
		sink:          sink,
		log:           log.With().Str("proxy", cfg.Dst).Str("listen", cfg.Listen).Logger(),
		dialer:        net.Dial,
	}
}

// Run accepts connections on the configured listen port until ctx is canceled.
// Each accepted connection is handled in its own goroutine.
func (p *Proxy) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", p.listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", p.listen, err)
	}
	p.log.Info().Str("upstream", p.upstream).Str("src", p.src).Str("dst", p.dst).Msg("proxy listening")

	// close listener when context cancels — Accept returns an error and the loop exits.
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	var wg sync.WaitGroup
	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				wg.Wait()
				return nil
			}
			return fmt.Errorf("accept: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.handleConn(ctx, clientConn)
		}()
	}
}

// handleConn dials the upstream, forwards both streams, and drains any
// unmatched pending requests when either side closes.
func (p *Proxy) handleConn(ctx context.Context, clientConn net.Conn) {
	defer clientConn.Close()

	upstreamConn, err := p.dialer("tcp", p.upstream)
	if err != nil {
		p.log.Warn().Err(err).Msg("upstream dial failed")
		p.sink.Emit(event.Event{
			Ts:      time.Now().UnixNano(),
			Channel: "rpcx",
			Src:     p.src,
			Dst:     p.dst,
			Method:  "",
			Status:  "dial_error",
			Error:   err.Error(),
		})
		return
	}
	defer upstreamConn.Close()

	pending := newPendingMap()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// closing upstream signals the response forwarder to exit cleanly.
		defer upstreamConn.Close()
		p.forwardRequests(clientConn, upstreamConn, pending)
	}()
	go func() {
		defer wg.Done()
		defer clientConn.Close()
		p.forwardResponses(upstreamConn, clientConn, pending)
	}()

	// closing both conns when ctx cancels causes the forward goroutines to error
	// out of their reads and return.
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = clientConn.Close()
			_ = upstreamConn.Close()
		case <-stop:
		}
	}()

	wg.Wait()
	close(stop)

	reason := reasonUpstreamClosed
	if ctx.Err() != nil {
		reason = reasonShutdown
	}
	for _, c := range pending.drain() {
		p.sink.Emit(p.buildEventFromPending(c, reason))
	}
}

// forwardRequests reads request frames from src, registers them in pending,
// and writes the original bytes to dst. Heartbeat and oneway messages are
// forwarded but not paired.
func (p *Proxy) forwardRequests(src io.Reader, dst io.Writer, pending *pendingMap) {
	for {
		msg, err := protocol.Read(src)
		if err != nil {
			if !isCleanClose(err) {
				p.log.Debug().Err(err).Msg("request stream read error")
			}
			return
		}

		if msg.MessageType() == protocol.Request && !msg.IsHeartbeat() && !msg.IsOneway() {
			pending.register(msg.Seq(), msg, time.Now().UnixNano())
		}

		if _, err := dst.Write(msg.Encode()); err != nil {
			p.log.Debug().Err(err).Msg("request stream write error")
			return
		}
	}
}

// forwardResponses reads response frames from src, completes paired entries
// in pending (emitting an Event), and writes the original bytes to dst.
func (p *Proxy) forwardResponses(src io.Reader, dst io.Writer, pending *pendingMap) {
	for {
		msg, err := protocol.Read(src)
		if err != nil {
			if !isCleanClose(err) {
				p.log.Debug().Err(err).Msg("response stream read error")
			}
			return
		}

		if msg.MessageType() == protocol.Response && !msg.IsHeartbeat() {
			if call, ok := pending.complete(msg.Seq()); ok {
				p.sink.Emit(p.buildEventFromPair(call, msg))
			}
		}

		if _, err := dst.Write(msg.Encode()); err != nil {
			p.log.Debug().Err(err).Msg("response stream write error")
			return
		}
	}
}

// buildEventFromPair assembles the Event for a completed request/response pair.
func (p *Proxy) buildEventFromPair(call *pendingCall, resp *protocol.Message) event.Event {
	now := time.Now().UnixNano()

	reqPayload, err := codec.DecodePayload(
		codec.SerializeType(call.req.SerializeType()),
		call.req.Payload,
		p.truncateBytes,
	)
	if err != nil {
		p.log.Debug().Err(err).Msg("decode request payload")
		reqPayload = ""
	}

	respPayload, err := codec.DecodePayload(
		codec.SerializeType(resp.SerializeType()),
		resp.Payload,
		p.truncateBytes,
	)
	if err != nil {
		p.log.Debug().Err(err).Msg("decode response payload")
		respPayload = ""
	}

	status := "ok"
	errMsg := ""
	if resp.MessageStatusType() == protocol.Error {
		status = "error"
		if resp.Metadata != nil {
			errMsg = resp.Metadata[protocol.ServiceError]
		}
	}

	return event.Event{
		Ts:          call.tsNanos,
		Channel:     "rpcx",
		Src:         p.src,
		Dst:         p.dst,
		Method:      formatMethod(call.req.ServicePath, call.req.ServiceMethod),
		Status:      status,
		DurationMs:  (now - call.tsNanos) / int64(time.Millisecond),
		TraceID:     traceIDFromMetadata(call.req.Metadata),
		RequestID:   metaLookup(call.req.Metadata, "x-request-id", "X-Request-Id", "x-request-ID"),
		SagaID:      metaLookup(call.req.Metadata, "saga-id", "x-saga-id"),
		ReqHeaders:  marshalHeaders(call.req.Metadata),
		ReqPayload:  reqPayload,
		RespHeaders: marshalHeaders(resp.Metadata),
		RespPayload: respPayload,
		Error:       errMsg,
	}
}

// buildEventFromPending assembles an Event for an unmatched request — used when
// the connection closes (or proxy shuts down) before a response arrives.
func (p *Proxy) buildEventFromPending(call *pendingCall, reason drainReason) event.Event {
	reqPayload, err := codec.DecodePayload(
		codec.SerializeType(call.req.SerializeType()),
		call.req.Payload,
		p.truncateBytes,
	)
	if err != nil {
		p.log.Debug().Err(err).Msg("decode request payload (drained)")
		reqPayload = ""
	}

	return event.Event{
		Ts:         call.tsNanos,
		Channel:    "rpcx",
		Src:        p.src,
		Dst:        p.dst,
		Method:     formatMethod(call.req.ServicePath, call.req.ServiceMethod),
		Status:     string(reason),
		DurationMs: 0,
		TraceID:    traceIDFromMetadata(call.req.Metadata),
		RequestID:  metaLookup(call.req.Metadata, "x-request-id", "X-Request-Id"),
		SagaID:     metaLookup(call.req.Metadata, "saga-id", "x-saga-id"),
		ReqHeaders: marshalHeaders(call.req.Metadata),
		ReqPayload: reqPayload,
	}
}

func formatMethod(svcPath, svcMethod string) string {
	if svcPath == "" {
		return svcMethod
	}
	return svcPath + ":" + svcMethod
}

// traceIDFromMetadata extracts the W3C trace_id from a `traceparent` header.
// Format: `00-<32-hex-trace-id>-<16-hex-span-id>-<2-hex-flags>`. Returns the
// trace_id segment, or empty if not present/malformed.
func traceIDFromMetadata(meta map[string]string) string {
	tp := metaLookup(meta, "traceparent", "Traceparent")
	if tp == "" {
		return ""
	}
	// crude parse — split on '-', take index 1 if present.
	parts := splitN(tp, '-', 3)
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}

// metaLookup returns the first non-empty match across the given keys.
// rpcx metadata is case-sensitive in the wire format, but propagators emit
// lowercase by convention; we check a few variants for robustness.
func metaLookup(meta map[string]string, keys ...string) string {
	if meta == nil {
		return ""
	}
	for _, k := range keys {
		if v, ok := meta[k]; ok && v != "" {
			return v
		}
	}
	return ""
}

func marshalHeaders(meta map[string]string) string {
	if len(meta) == 0 {
		return ""
	}
	b, err := json.Marshal(meta)
	if err != nil {
		return ""
	}
	return string(b)
}

// splitN splits s on sep up to n parts. Avoids importing strings just for this.
func splitN(s string, sep byte, n int) []string {
	out := make([]string, 0, n)
	start := 0
	for i := 0; i < len(s) && len(out) < n-1; i++ {
		if s[i] == sep {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

// isCleanClose returns true for errors that mean "the other side closed" —
// io.EOF, ErrUnexpectedEOF, and "use of closed network connection" (raised by
// a net.Conn.Close after a Read). These are normal lifecycle events, not bugs.
func isCleanClose(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	return false
}
