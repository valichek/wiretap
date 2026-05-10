package store

import (
	"database/sql"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"wiretap/event"
)

func openTestDB(t *testing.T) (*SQLiteWriter, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "wiretap.db")

	w, err := NewSQLiteWriter(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	return w, path
}

func TestSQLiteWriter_insertOne(t *testing.T) {
	w, path := openTestDB(t)

	ev := event.Event{
		Ts:          1_700_000_000_000_000_000,
		Channel:     "rpcx",
		Src:         "client-a",
		Dst:         "service-x",
		Method:      "service.x:do-thing",
		Status:      "ok",
		DurationMs:  12,
		TraceID:     "abc",
		RequestID:   "req-1",
		ReqHeaders:  `{"k":"v"}`,
		ReqPayload:  `{"in":1}`,
		RespHeaders: `{"k":"w"}`,
		RespPayload: `{"out":2}`,
	}
	require.NoError(t, w.Write(ev))

	rdb, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	defer rdb.Close()

	var (
		ts                                                                                    int64
		dur                                                                                   int64
		channel, src, dst, method, status, traceID, reqID, reqH, reqP, respH, respP, errField sql.NullString
	)
	row := rdb.QueryRow(`SELECT ts, channel, src, dst, method, status, duration_ms, trace_id, request_id, req_headers, req_payload, resp_headers, resp_payload, error FROM messages`)
	require.NoError(t, row.Scan(&ts, &channel, &src, &dst, &method, &status, &dur, &traceID, &reqID, &reqH, &reqP, &respH, &respP, &errField))

	assert.Equal(t, ev.Ts, ts)
	assert.Equal(t, ev.Channel, channel.String)
	assert.Equal(t, ev.Src, src.String)
	assert.Equal(t, ev.Dst, dst.String)
	assert.Equal(t, ev.Method, method.String)
	assert.Equal(t, ev.Status, status.String)
	assert.Equal(t, ev.DurationMs, dur)
	assert.Equal(t, ev.TraceID, traceID.String)
	assert.Equal(t, ev.RequestID, reqID.String)
	assert.Equal(t, ev.ReqHeaders, reqH.String)
	assert.Equal(t, ev.ReqPayload, reqP.String)
	assert.Equal(t, ev.RespHeaders, respH.String)
	assert.Equal(t, ev.RespPayload, respP.String)
	assert.False(t, errField.Valid, "error column should be NULL")
}

func TestSQLiteWriter_emptyStringsAsNULL(t *testing.T) {
	// nullable columns (src, trace_id, request_id, saga_id, headers, payloads, error)
	// should be NULL when the Event field is empty — required for partial indexes.
	w, path := openTestDB(t)

	require.NoError(t, w.Write(event.Event{
		Ts: 1, Channel: "rpcx", Dst: "x", Method: "m", Status: "ok", DurationMs: 0,
	}))

	rdb, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	defer rdb.Close()

	var src, traceID, reqID, sagaID, reqH, reqP, respH, respP, errField sql.NullString
	row := rdb.QueryRow(`SELECT src, trace_id, request_id, saga_id, req_headers, req_payload, resp_headers, resp_payload, error FROM messages`)
	require.NoError(t, row.Scan(&src, &traceID, &reqID, &sagaID, &reqH, &reqP, &respH, &respP, &errField))

	for name, col := range map[string]sql.NullString{
		"src": src, "trace_id": traceID, "request_id": reqID, "saga_id": sagaID,
		"req_headers": reqH, "req_payload": reqP, "resp_headers": respH, "resp_payload": respP,
		"error": errField,
	} {
		assert.False(t, col.Valid, "%s should be NULL when Event field is empty (got %q)", name, col.String)
	}
}

func TestSQLiteWriter_partialIndexUsesNULL(t *testing.T) {
	// confirms partial index `WHERE trace_id IS NOT NULL` distinguishes rows
	// with trace_id from rows without.
	w, path := openTestDB(t)

	require.NoError(t, w.Write(event.Event{Ts: 1, Channel: "rpcx", Dst: "x", Method: "m", Status: "ok", TraceID: "t1"}))
	require.NoError(t, w.Write(event.Event{Ts: 2, Channel: "rpcx", Dst: "x", Method: "m", Status: "ok"}))
	require.NoError(t, w.Write(event.Event{Ts: 3, Channel: "rpcx", Dst: "x", Method: "m", Status: "ok", TraceID: "t3"}))

	rdb, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	defer rdb.Close()

	var n int
	require.NoError(t, rdb.QueryRow(`SELECT count(*) FROM messages WHERE trace_id IS NOT NULL`).Scan(&n))
	assert.Equal(t, 2, n)
}

func TestSQLiteWriter_schemaIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wiretap.db")

	// open + close + open again — the second open re-runs schemaSQL and must succeed
	w1, err := NewSQLiteWriter(path)
	require.NoError(t, err)
	require.NoError(t, w1.Write(event.Event{Ts: 1, Channel: "rpcx", Dst: "x", Method: "m", Status: "ok"}))
	require.NoError(t, w1.Close())

	w2, err := NewSQLiteWriter(path)
	require.NoError(t, err)
	defer w2.Close()
	require.NoError(t, w2.Write(event.Event{Ts: 2, Channel: "rpcx", Dst: "y", Method: "m", Status: "ok"}))

	rdb, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	defer rdb.Close()

	var n int
	require.NoError(t, rdb.QueryRow(`SELECT count(*) FROM messages`).Scan(&n))
	assert.Equal(t, 2, n, "rows from both opens are present")
}

func TestSQLiteWriter_concurrent(t *testing.T) {
	w, path := openTestDB(t)

	const goroutines = 10
	const perG = 25

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				ev := event.Event{
					Ts:      int64(g)*1000 + int64(i),
					Channel: "rpcx", Dst: "x", Method: "m", Status: "ok",
				}
				require.NoError(t, w.Write(ev))
			}
		}(g)
	}
	wg.Wait()

	rdb, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	defer rdb.Close()

	var n int
	require.NoError(t, rdb.QueryRow(`SELECT count(*) FROM messages`).Scan(&n))
	assert.Equal(t, goroutines*perG, n)
}

func TestSQLiteWriter_simulatesPythonAddonConcurrentWrite(t *testing.T) {
	// the python mitm addon will open the same DB independently; this test
	// simulates that via a second sql.DB handle inserting alongside our writer.
	w, path := openTestDB(t)

	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	external, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	defer external.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			require.NoError(t, w.Write(event.Event{Ts: int64(i), Channel: "rpcx", Dst: "x", Method: "m", Status: "ok"}))
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_, err := external.Exec(insertSQL,
				int64(i)+1000, "http", nil, "upstream", "GET /x", "200", int64(0),
				nil, nil, nil, nil, nil, nil, nil, nil,
			)
			require.NoError(t, err)
		}
	}()

	wg.Wait()

	var n int
	require.NoError(t, external.QueryRow(`SELECT count(*) FROM messages`).Scan(&n))
	assert.Equal(t, 100, n, "WAL mode permits two writers without conflict at this volume")
}

func TestSQLiteWriter_closeIdempotent(t *testing.T) {
	w, _ := openTestDB(t)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close(), "second close should be a no-op")
}

func TestSQLiteWriter_invalidPath(t *testing.T) {
	_, err := NewSQLiteWriter("/nonexistent/path/wiretap.db")
	require.Error(t, err)
}
