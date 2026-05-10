package proxy

import (
	"sync"

	"github.com/smallnest/rpcx/protocol"
)

// pendingCall holds a captured request awaiting its response. tsNanos is the
// request-arrival timestamp; the response handler computes duration_ms from it.
type pendingCall struct {
	req      *protocol.Message
	tsNanos  int64
}

// pendingMap tracks unmatched requests on a single rpcx connection, keyed by
// Header.Seq(). Two goroutines per connection access this map (request side
// registers, response side completes), so it's mutex-guarded. Contention is
// negligible — only two goroutines, brief critical sections.
type pendingMap struct {
	mu    sync.Mutex
	calls map[uint64]*pendingCall
}

func newPendingMap() *pendingMap {
	return &pendingMap{calls: make(map[uint64]*pendingCall)}
}

// register stores a request under its sequence number. If a request with the
// same seq already exists (rare; would mean client reused a seq before its
// response landed), the old one is replaced — the new request supersedes it
// for pairing purposes.
func (m *pendingMap) register(seq uint64, req *protocol.Message, tsNanos int64) {
	m.mu.Lock()
	m.calls[seq] = &pendingCall{req: req, tsNanos: tsNanos}
	m.mu.Unlock()
}

// complete pops a pending entry by seq. Returns the entry and true if found,
// nil and false if no matching request was registered (response without a
// recorded request — possible during shutdown or if the proxy started mid-flow).
func (m *pendingMap) complete(seq uint64) (*pendingCall, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.calls[seq]
	if !ok {
		return nil, false
	}
	delete(m.calls, seq)
	return c, true
}

// drain returns all remaining pending entries and empties the map. Used at
// connection close to flush requests that never received a response.
func (m *pendingMap) drain() []*pendingCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) == 0 {
		return nil
	}
	out := make([]*pendingCall, 0, len(m.calls))
	for _, c := range m.calls {
		out = append(out, c)
	}
	m.calls = make(map[uint64]*pendingCall)
	return out
}

// size returns the current count of pending entries (for tests/observability).
func (m *pendingMap) size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}
