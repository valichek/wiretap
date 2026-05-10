package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"wiretap/event"
)

func TestNewRetainer_disabledWhenRetentionZero(t *testing.T) {
	r := NewRetainer(t.TempDir(), nil, 0, zerolog.Nop()) // 0 → disabled
	assert.Nil(t, r, "0 days should yield nil (retention disabled)")

	r = NewRetainer(t.TempDir(), nil, -1*time.Hour, zerolog.Nop())
	assert.Nil(t, r, "negative days should yield nil")
}

func TestRetainer_prunesOldJSONLFilesByMtime(t *testing.T) {
	dir := t.TempDir()

	old1 := filepath.Join(dir, "rpcx-100.jsonl")
	old2 := filepath.Join(dir, "http-200.jsonl")
	recent := filepath.Join(dir, "rpcx-300.jsonl")
	for _, p := range []string{old1, old2, recent} {
		require.NoError(t, os.WriteFile(p, []byte("{}\n"), 0o644))
	}

	old := time.Now().Add(-10 * 24 * time.Hour)
	require.NoError(t, os.Chtimes(old1, old, old))
	require.NoError(t, os.Chtimes(old2, old, old))
	// recent stays at "now"

	r := NewRetainer(dir, nil, 7*24*time.Hour, zerolog.Nop())
	require.NotNil(t, r)
	r.cleanup()

	_, err := os.Stat(old1)
	assert.True(t, os.IsNotExist(err), "old1 should be deleted")
	_, err = os.Stat(old2)
	assert.True(t, os.IsNotExist(err), "old2 should be deleted")
	_, err = os.Stat(recent)
	assert.NoError(t, err, "recent should be kept")
}

func TestRetainer_keepsRecentFiles(t *testing.T) {
	dir := t.TempDir()
	recent := filepath.Join(dir, "rpcx-1.jsonl")
	require.NoError(t, os.WriteFile(recent, []byte("{}\n"), 0o644))

	r := NewRetainer(dir, nil, 24*time.Hour, zerolog.Nop())
	r.cleanup()

	_, err := os.Stat(recent)
	assert.NoError(t, err)
}

func TestRetainer_prunesOldSQLiteRows(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wiretap.db")

	w, err := NewSQLiteWriter(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	now := time.Now()
	old := now.Add(-10 * 24 * time.Hour)

	require.NoError(t, w.Write(event.Event{Ts: old.UnixNano(), Channel: "rpcx", Dst: "x", Method: "old", Status: "ok"}))
	require.NoError(t, w.Write(event.Event{Ts: now.UnixNano(), Channel: "rpcx", Dst: "x", Method: "new", Status: "ok"}))

	r := NewRetainer(dir, w, 7*24*time.Hour, zerolog.Nop())
	r.cleanup()

	rdb, err := sql.Open("sqlite", "file:"+dbPath)
	require.NoError(t, err)
	defer rdb.Close()

	var rows int
	require.NoError(t, rdb.QueryRow(`SELECT count(*) FROM messages`).Scan(&rows))
	assert.Equal(t, 1, rows, "only the recent row remains")

	var method string
	require.NoError(t, rdb.QueryRow(`SELECT method FROM messages`).Scan(&method))
	assert.Equal(t, "new", method)
}

func TestRetainer_pruneSQLiteSkipsWhenNil(t *testing.T) {
	r := NewRetainer(t.TempDir(), nil, 24*time.Hour, zerolog.Nop())
	// no panic
	r.cleanup()
}

func TestRetainer_RunRespectsCtxCancel(t *testing.T) {
	dir := t.TempDir()
	r := NewRetainer(dir, nil, 24*time.Hour, zerolog.Nop())
	r.tick = 50 * time.Millisecond // speed up for tests

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()

	time.Sleep(120 * time.Millisecond) // let it tick at least once
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}
