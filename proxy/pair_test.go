package proxy

import (
	"testing"

	"github.com/smallnest/rpcx/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newReq(seq uint64, path, method string) *protocol.Message {
	m := protocol.NewMessage()
	m.SetMessageType(protocol.Request)
	m.SetSeq(seq)
	m.SetSerializeType(protocol.JSON)
	m.ServicePath = path
	m.ServiceMethod = method
	return m
}

func TestPendingMap_registerAndComplete(t *testing.T) {
	m := newPendingMap()
	req := newReq(1, "svc", "method")

	m.register(1, req, 100)
	assert.Equal(t, 1, m.size())

	got, ok := m.complete(1)
	require.True(t, ok)
	assert.Equal(t, int64(100), got.tsNanos)
	assert.Equal(t, req, got.req)
	assert.Equal(t, 0, m.size(), "completed entry should be removed")
}

func TestPendingMap_completeMissingReturnsFalse(t *testing.T) {
	m := newPendingMap()
	got, ok := m.complete(99)
	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestPendingMap_registerOverwrite(t *testing.T) {
	m := newPendingMap()
	req1 := newReq(1, "svc", "first")
	req2 := newReq(1, "svc", "second")

	m.register(1, req1, 100)
	m.register(1, req2, 200)
	assert.Equal(t, 1, m.size(), "same seq should not produce two entries")

	got, ok := m.complete(1)
	require.True(t, ok)
	assert.Equal(t, "second", got.req.ServiceMethod, "second register wins")
}

func TestPendingMap_drain(t *testing.T) {
	m := newPendingMap()
	m.register(1, newReq(1, "svc", "a"), 100)
	m.register(2, newReq(2, "svc", "b"), 200)
	m.register(3, newReq(3, "svc", "c"), 300)
	require.Equal(t, 3, m.size())

	drained := m.drain()
	assert.Len(t, drained, 3)
	assert.Equal(t, 0, m.size(), "drain empties the map")
}

func TestPendingMap_drainEmpty(t *testing.T) {
	m := newPendingMap()
	assert.Nil(t, m.drain())
}

func TestPendingMap_completeAfterDrainFalse(t *testing.T) {
	m := newPendingMap()
	m.register(1, newReq(1, "svc", "a"), 100)

	_ = m.drain()

	got, ok := m.complete(1)
	assert.False(t, ok)
	assert.Nil(t, got)
}
