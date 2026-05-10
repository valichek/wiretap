package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/smallnest/rpcx/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"wiretap/config"
	"wiretap/event"
)

// captureSink collects emitted Events for assertions.
type captureSink struct {
	mu     sync.Mutex
	events []event.Event
}

func (s *captureSink) Emit(e event.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *captureSink) snapshot() []event.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]event.Event, len(s.events))
	copy(out, s.events)
	return out
}

func newProxyForTest(sink event.Sink) *Proxy {
	return &Proxy{
		listen:        ":0",
		upstream:      "ignored-by-mock-dialer",
		src:           "test-src",
		dst:           "test-dst",
		truncateBytes: 1024,
		sink:          sink,
		log:           zerolog.Nop(),
		dialer:        net.Dial, // overridden in tests
	}
}

func buildRequest(seq uint64, path, method string, payload []byte, meta map[string]string) *protocol.Message {
	m := protocol.NewMessage()
	m.SetMessageType(protocol.Request)
	m.SetSeq(seq)
	m.SetSerializeType(protocol.JSON)
	m.ServicePath = path
	m.ServiceMethod = method
	m.Payload = payload
	m.Metadata = meta
	return m
}

func buildResponse(seq uint64, payload []byte, statusErr bool, errMsg string) *protocol.Message {
	m := protocol.NewMessage()
	m.SetMessageType(protocol.Response)
	m.SetSeq(seq)
	m.SetSerializeType(protocol.JSON)
	m.Payload = payload
	if statusErr {
		m.SetMessageStatusType(protocol.Error)
		m.Metadata = map[string]string{protocol.ServiceError: errMsg}
	}
	return m
}

func TestProxy_dialError_emitsEvent(t *testing.T) {
	sink := &captureSink{}
	p := newProxyForTest(sink)
	p.dialer = func(_, _ string) (net.Conn, error) {
		return nil, errors.New("boom: upstream offline")
	}

	clientA, clientB := net.Pipe()
	defer clientA.Close()
	defer clientB.Close()

	done := make(chan struct{})
	go func() {
		p.handleConn(context.Background(), clientB)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not return after dial failure")
	}

	events := sink.snapshot()
	require.Len(t, events, 1)
	assert.Equal(t, "rpcx", events[0].Channel)
	assert.Equal(t, "test-dst", events[0].Dst)
	assert.Equal(t, "test-src", events[0].Src)
	assert.Equal(t, "dial_error", events[0].Status)
	assert.Contains(t, events[0].Error, "boom")
}

// runPairScenario sets up the full proxy pipe: clientA ↔ proxy ↔ upstreamSide,
// then plays the role of both client (writing request, reading response) and
// upstream (reading request, writing response). Returns captured events after
// the connection closes naturally.
func runPairScenario(t *testing.T, p *Proxy, req, resp *protocol.Message) (gotForwardedReq, gotForwardedResp *protocol.Message) {
	t.Helper()

	clientA, clientB := net.Pipe()
	upstreamA, upstreamB := net.Pipe()

	p.dialer = func(_, _ string) (net.Conn, error) {
		return upstreamA, nil
	}

	connDone := make(chan struct{})
	go func() {
		p.handleConn(context.Background(), clientB)
		close(connDone)
	}()

	// client → proxy: write the request
	writeDone := make(chan struct{})
	go func() {
		_, err := clientA.Write(req.Encode())
		assert.NoError(t, err)
		close(writeDone)
	}()

	// upstream side reads the forwarded request
	gotForwardedReq, err := protocol.Read(upstreamB)
	require.NoError(t, err)
	<-writeDone

	// upstream side writes the response
	respDone := make(chan struct{})
	go func() {
		// echo the request's seq onto the response
		resp.SetSeq(gotForwardedReq.Seq())
		_, err := upstreamB.Write(resp.Encode())
		assert.NoError(t, err)
		close(respDone)
	}()

	// client reads the forwarded response
	gotForwardedResp, err = protocol.Read(clientA)
	require.NoError(t, err)
	<-respDone

	// close the client side to trigger natural shutdown
	require.NoError(t, clientA.Close())
	require.NoError(t, upstreamB.Close())

	select {
	case <-connDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not return")
	}

	return gotForwardedReq, gotForwardedResp
}

func TestProxy_pairCompletion(t *testing.T) {
	sink := &captureSink{}
	p := newProxyForTest(sink)

	req := buildRequest(42, "service.x", "do-thing", []byte(`{"amount":100}`), map[string]string{
		"traceparent":  "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
		"x-request-id": "req-xyz",
	})
	resp := buildResponse(0, []byte(`{"ok":true}`), false, "")

	gotReq, gotResp := runPairScenario(t, p, req, resp)
	assert.Equal(t, uint64(42), gotReq.Seq(), "request seq forwarded")
	assert.Equal(t, uint64(42), gotResp.Seq(), "response seq forwarded")
	assert.Equal(t, []byte(`{"amount":100}`), gotReq.Payload, "request payload forwarded unchanged")
	assert.Equal(t, []byte(`{"ok":true}`), gotResp.Payload, "response payload forwarded unchanged")

	events := sink.snapshot()
	require.Len(t, events, 1, "exactly one paired event")
	ev := events[0]
	assert.Equal(t, "rpcx", ev.Channel)
	assert.Equal(t, "test-src", ev.Src)
	assert.Equal(t, "test-dst", ev.Dst)
	assert.Equal(t, "service.x:do-thing", ev.Method)
	assert.Equal(t, "ok", ev.Status)
	assert.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ev.TraceID)
	assert.Equal(t, "req-xyz", ev.RequestID)
	assert.JSONEq(t, `{"amount":100}`, ev.ReqPayload)
	assert.JSONEq(t, `{"ok":true}`, ev.RespPayload)
	assert.Empty(t, ev.Error)
}

func TestProxy_errorResponse(t *testing.T) {
	sink := &captureSink{}
	p := newProxyForTest(sink)

	req := buildRequest(1, "svc", "method", []byte(`{}`), nil)
	resp := buildResponse(0, []byte(`null`), true, "wallet not found")

	runPairScenario(t, p, req, resp)

	events := sink.snapshot()
	require.Len(t, events, 1)
	assert.Equal(t, "error", events[0].Status)
	assert.Equal(t, "wallet not found", events[0].Error)
}

func TestProxy_payloadTruncation(t *testing.T) {
	sink := &captureSink{}
	p := newProxyForTest(sink)
	p.truncateBytes = 32 // small to force truncation

	bigPayload := make([]byte, 64)
	for i := range bigPayload {
		bigPayload[i] = 'x'
	}

	req := buildRequest(1, "svc", "m", bigPayload, nil)
	resp := buildResponse(0, []byte(`{"ok":true}`), false, "")

	runPairScenario(t, p, req, resp)

	events := sink.snapshot()
	require.Len(t, events, 1)
	assert.Contains(t, events[0].ReqPayload, "__truncated_at_bytes")
	assert.Contains(t, events[0].ReqPayload, `"__original_size":64`)
}

func TestProxy_disconnectDrainsPending(t *testing.T) {
	sink := &captureSink{}
	p := newProxyForTest(sink)

	clientA, clientB := net.Pipe()
	upstreamA, upstreamB := net.Pipe()

	p.dialer = func(_, _ string) (net.Conn, error) {
		return upstreamA, nil
	}

	done := make(chan struct{})
	go func() {
		p.handleConn(context.Background(), clientB)
		close(done)
	}()

	req := buildRequest(7, "svc", "fire-and-disconnect", []byte(`{"x":1}`), nil)

	// write request
	go func() {
		_, _ = clientA.Write(req.Encode())
	}()

	// upstream reads the request but never responds
	gotReq, err := protocol.Read(upstreamB)
	require.NoError(t, err)
	require.Equal(t, uint64(7), gotReq.Seq())

	// client disconnects without waiting for response
	require.NoError(t, clientA.Close())
	require.NoError(t, upstreamB.Close())

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not return after disconnect")
	}

	events := sink.snapshot()
	require.Len(t, events, 1, "drained pending entry should produce one event")
	assert.Equal(t, "upstream_closed", events[0].Status)
	assert.Equal(t, "svc:fire-and-disconnect", events[0].Method)
	assert.JSONEq(t, `{"x":1}`, events[0].ReqPayload)
}

func TestProxy_shutdownDrainsPending(t *testing.T) {
	sink := &captureSink{}
	p := newProxyForTest(sink)

	clientA, clientB := net.Pipe()
	upstreamA, upstreamB := net.Pipe()
	defer clientA.Close()
	defer upstreamB.Close()

	p.dialer = func(_, _ string) (net.Conn, error) {
		return upstreamA, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		p.handleConn(ctx, clientB)
		close(done)
	}()

	req := buildRequest(11, "svc", "method", []byte(`{}`), nil)
	go func() { _, _ = clientA.Write(req.Encode()) }()

	// upstream reads but doesn't respond
	gotReq, err := protocol.Read(upstreamB)
	require.NoError(t, err)
	assert.Equal(t, uint64(11), gotReq.Seq())

	// trigger shutdown via context cancel
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not return after ctx cancel")
	}

	events := sink.snapshot()
	require.Len(t, events, 1)
	assert.Equal(t, "shutdown", events[0].Status, "drained on shutdown should use 'shutdown' reason")
}

func TestProxy_heartbeatNotPaired(t *testing.T) {
	sink := &captureSink{}
	p := newProxyForTest(sink)

	clientA, clientB := net.Pipe()
	upstreamA, upstreamB := net.Pipe()

	p.dialer = func(_, _ string) (net.Conn, error) {
		return upstreamA, nil
	}

	done := make(chan struct{})
	go func() {
		p.handleConn(context.Background(), clientB)
		close(done)
	}()

	hb := protocol.NewMessage()
	hb.SetMessageType(protocol.Request)
	hb.SetHeartbeat(true)
	hb.SetSeq(99)

	go func() { _, _ = clientA.Write(hb.Encode()) }()

	gotReq, err := protocol.Read(upstreamB)
	require.NoError(t, err)
	assert.True(t, gotReq.IsHeartbeat(), "heartbeat is forwarded")

	require.NoError(t, clientA.Close())
	require.NoError(t, upstreamB.Close())

	<-done

	assert.Empty(t, sink.snapshot(), "heartbeat must not produce an event")
}

func TestProxy_runListenerLifecycle(t *testing.T) {
	// integration test: spin up the Run loop on a real localhost port,
	// dial a fake upstream loopback, send one request/response, verify event,
	// then cancel ctx and ensure Run returns cleanly.
	sink := &captureSink{}
	upstreamL, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer upstreamL.Close()

	upstreamReady := make(chan struct{})
	go func() {
		conn, err := upstreamL.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		close(upstreamReady)
		msg, err := protocol.Read(conn)
		if err != nil {
			return
		}
		resp := buildResponse(msg.Seq(), []byte(`{"ok":true}`), false, "")
		_, _ = conn.Write(resp.Encode())
		// keep open until client disconnects
		_, _ = io.Copy(io.Discard, conn)
	}()

	cfg := config.ProxyConfig{
		Kind:     "rpcx",
		Listen:   "127.0.0.1:0",
		Upstream: upstreamL.Addr().String(),
		Src:      "client-x",
		Dst:      "server-y",
	}

	// since we're testing Run, we need to pick a real port. use 0 then re-discover.
	// New uses cfg.Listen verbatim, so we have to use a real port-binding listener.
	// approach: pre-bind, pass the addr to Run via cfg.Listen.
	preBound, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg.Listen = preBound.Addr().String()
	preBound.Close() // free the port; Run will re-bind it

	p := New(cfg, sink, zerolog.Nop(), 1024)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- p.Run(ctx)
	}()

	// give Run a moment to bind
	time.Sleep(50 * time.Millisecond)

	// connect as client
	client, err := net.Dial("tcp", cfg.Listen)
	require.NoError(t, err)
	defer client.Close()

	req := buildRequest(1, "svc", "m", []byte(`{}`), nil)
	_, err = client.Write(req.Encode())
	require.NoError(t, err)

	gotResp, err := protocol.Read(client)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gotResp.Seq())

	// give the proxy a moment to emit the event after writing response
	time.Sleep(50 * time.Millisecond)

	cancel()
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}

	events := sink.snapshot()
	require.NotEmpty(t, events)
	assert.Equal(t, "ok", events[0].Status)
}
