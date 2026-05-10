package store

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"wiretap/event"
)

func TestPipeline_emitWritesToBothSinks(t *testing.T) {
	dir := t.TempDir()
	jsonl := NewJSONLWriter(dir, 100)
	sqlite, err := NewSQLiteWriter(filepath.Join(dir, "test.db"))
	require.NoError(t, err)

	p := NewPipeline(jsonl, sqlite, 16, nil, zerolog.Nop())

	for i := 0; i < 5; i++ {
		p.Emit(sampleEvent(i))
	}

	require.NoError(t, p.Close(context.Background()))

	lines := readLines(t, filepath.Join(dir, "rpcx-100.jsonl"))
	assert.Len(t, lines, 5)

	rdb, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "test.db"))
	require.NoError(t, err)
	defer rdb.Close()

	var n int
	require.NoError(t, rdb.QueryRow(`SELECT count(*) FROM messages`).Scan(&n))
	assert.Equal(t, 5, n)
}

func TestPipeline_jsonlOnly(t *testing.T) {
	dir := t.TempDir()
	jsonl := NewJSONLWriter(dir, 1)

	p := NewPipeline(jsonl, nil, 16, nil, zerolog.Nop())

	p.Emit(sampleEvent(1))
	p.Emit(sampleEvent(2))

	require.NoError(t, p.Close(context.Background()))

	lines := readLines(t, filepath.Join(dir, "rpcx-1.jsonl"))
	assert.Len(t, lines, 2)
	assert.Equal(t, int64(0), p.SQLiteDrops())
}

func TestPipeline_dropOnFull(t *testing.T) {
	dir := t.TempDir()
	jsonl := NewJSONLWriter(dir, 1)

	// buffer of 1 + tight loop: most emits will see channel full and drop
	p := NewPipeline(jsonl, nil, 1, nil, zerolog.Nop())

	const n = 5000
	for i := 0; i < n; i++ {
		p.Emit(sampleEvent(i))
	}

	drops := p.JSONLDrops()
	require.NoError(t, p.Close(context.Background()))

	lines := readLines(t, filepath.Join(dir, "rpcx-1.jsonl"))
	written := int64(len(lines))

	assert.Greater(t, drops, int64(0), "expected some drops with buffer=1")
	assert.Equal(t, int64(n), drops+written, "drops + written must equal total emitted")
}

func TestPipeline_dropWarnerLogs(t *testing.T) {
	dir := t.TempDir()
	jsonl := NewJSONLWriter(dir, 1)

	var buf bytes.Buffer
	logger := zerolog.New(&buf).Level(zerolog.WarnLevel)

	p := NewPipeline(jsonl, nil, 1, nil, logger)

	// flood to force drops
	for i := 0; i < 10_000; i++ {
		p.Emit(sampleEvent(i))
	}

	// give the 1s ticker a chance to fire
	time.Sleep(1100 * time.Millisecond)

	require.NoError(t, p.Close(context.Background()))

	out := buf.String()
	assert.Contains(t, out, "jsonl drops")
}

func TestPipeline_concurrentEmit(t *testing.T) {
	dir := t.TempDir()
	jsonl := NewJSONLWriter(dir, 1)
	sqlite, err := NewSQLiteWriter(filepath.Join(dir, "test.db"))
	require.NoError(t, err)

	p := NewPipeline(jsonl, sqlite, 64, nil, zerolog.Nop())

	const goroutines = 10
	const perG = 50

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				p.Emit(sampleEvent(g*1000 + i))
			}
		}(g)
	}
	wg.Wait()

	require.NoError(t, p.Close(context.Background()))

	// jsonl drops + written = total emitted (same for sqlite)
	written := int64(len(readLines(t, filepath.Join(dir, "rpcx-1.jsonl"))))
	jsonlDropped := p.JSONLDrops()
	assert.Equal(t, int64(goroutines*perG), written+jsonlDropped)
}

func TestPipeline_dropStatusesFiltersAtWriteTime(t *testing.T) {
	dir := t.TempDir()
	jsonl := NewJSONLWriter(dir, 1)
	sqlite, err := NewSQLiteWriter(filepath.Join(dir, "test.db"))
	require.NoError(t, err)

	// drop dial_error and upstream_closed; "ok" and "error" must pass through
	p := NewPipeline(jsonl, sqlite, 16, []string{"dial_error", "upstream_closed"}, zerolog.Nop())

	mk := func(status string) event.Event {
		return event.Event{Ts: 1, Channel: "rpcx", Dst: "x", Method: "m", Status: status}
	}
	p.Emit(mk("ok"))
	p.Emit(mk("dial_error"))
	p.Emit(mk("error"))
	p.Emit(mk("upstream_closed"))
	p.Emit(mk("upstream_closed"))
	p.Emit(mk("ok"))

	require.NoError(t, p.Close(context.Background()))

	// JSONL should have only the 3 non-dropped events (2 ok + 1 error)
	lines := readLines(t, filepath.Join(dir, "rpcx-1.jsonl"))
	assert.Len(t, lines, 3, "filtered statuses should not be written")

	// SQLite should match
	rdb, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "test.db"))
	require.NoError(t, err)
	defer rdb.Close()
	var n int
	require.NoError(t, rdb.QueryRow(`SELECT count(*) FROM messages`).Scan(&n))
	assert.Equal(t, 3, n)

	// counter — 3 events filtered out (1 dial_error + 2 upstream_closed)
	assert.Equal(t, int64(3), p.FilteredOut())
}

func TestPipeline_emptyDropStatusesPassesThrough(t *testing.T) {
	dir := t.TempDir()
	jsonl := NewJSONLWriter(dir, 1)
	p := NewPipeline(jsonl, nil, 16, nil, zerolog.Nop())

	p.Emit(event.Event{Ts: 1, Channel: "rpcx", Dst: "x", Method: "m", Status: "dial_error"})
	require.NoError(t, p.Close(context.Background()))

	lines := readLines(t, filepath.Join(dir, "rpcx-1.jsonl"))
	assert.Len(t, lines, 1, "with no dropStatuses, every event is written")
	assert.Equal(t, int64(0), p.FilteredOut())
}

func TestPipeline_closeIsBoundedByCtx(t *testing.T) {
	dir := t.TempDir()
	jsonl := NewJSONLWriter(dir, 1)

	p := NewPipeline(jsonl, nil, 1024, nil, zerolog.Nop())

	for i := 0; i < 1000; i++ {
		p.Emit(sampleEvent(i))
	}

	// already-canceled ctx → Close returns quickly even if drain isn't complete
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	require.NoError(t, p.Close(canceled))
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 500*time.Millisecond, "Close with canceled ctx should not wait for drain")
}
