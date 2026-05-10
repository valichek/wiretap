package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite" // pure-Go sqlite driver, registers as "sqlite"

	"wiretap/event"
)

// schemaSQL — the messages table + indexes. CREATE … IF NOT EXISTS makes the
// whole script idempotent so we can run it on every open.
const schemaSQL = `
CREATE TABLE IF NOT EXISTS messages (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  ts          INTEGER NOT NULL,
  channel     TEXT    NOT NULL,
  src         TEXT,
  dst         TEXT    NOT NULL,
  method      TEXT    NOT NULL,
  status      TEXT    NOT NULL,
  duration_ms INTEGER NOT NULL,
  trace_id    TEXT,
  request_id  TEXT,
  saga_id     TEXT,
  req_headers  TEXT,
  req_payload  TEXT,
  resp_headers TEXT,
  resp_payload TEXT,
  error       TEXT
);
CREATE INDEX IF NOT EXISTS idx_messages_ts          ON messages(ts);
CREATE INDEX IF NOT EXISTS idx_messages_trace       ON messages(trace_id)   WHERE trace_id  IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_request     ON messages(request_id) WHERE request_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_dst_method  ON messages(dst, method);
`

const insertSQL = `
INSERT INTO messages (
  ts, channel, src, dst, method, status, duration_ms,
  trace_id, request_id, saga_id,
  req_headers, req_payload, resp_headers, resp_payload, error
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`

// SQLiteWriter inserts Events into a SQLite database. WAL mode + busy_timeout
// pragma let the Go binary and the Python mitmproxy addon write concurrently.
//
// No app-level retry-on-busy: busy_timeout=5000 already retries internally;
// per-row commit (no batching) keeps the implementation simple for dev volume.
// See the plan's v2 candidates for batching/retry if profiling shows need.
type SQLiteWriter struct {
	db   *sql.DB
	stmt *sql.Stmt
}

// NewSQLiteWriter opens (or creates) the DB at path, applies pragmas, runs
// CREATE TABLE / CREATE INDEX, and prepares the insert statement.
func NewSQLiteWriter(path string) (*SQLiteWriter, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)", path)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}

	// sql.Open is lazy; ping forces a real connect and errors early on bad path.
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite %s: %w", path, err)
	}

	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ensure schema: %w", err)
	}

	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("prepare insert: %w", err)
	}

	return &SQLiteWriter{db: db, stmt: stmt}, nil
}

// Write inserts one Event. *sql.Stmt is safe for concurrent use, so multiple
// goroutines may call Write simultaneously.
func (w *SQLiteWriter) Write(ev event.Event) error {
	_, err := w.stmt.Exec(
		ev.Ts, ev.Channel, nullable(ev.Src), ev.Dst, ev.Method, ev.Status, ev.DurationMs,
		nullable(ev.TraceID), nullable(ev.RequestID), nullable(ev.SagaID),
		nullable(ev.ReqHeaders), nullable(ev.ReqPayload),
		nullable(ev.RespHeaders), nullable(ev.RespPayload),
		nullable(ev.Error),
	)
	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	return nil
}

// Close releases the prepared statement and DB handle. Calling Close more
// than once is a no-op on the second call.
func (w *SQLiteWriter) Close() error {
	var firstErr error
	if w.stmt != nil {
		if err := w.stmt.Close(); err != nil {
			firstErr = fmt.Errorf("close stmt: %w", err)
		}
		w.stmt = nil
	}
	if w.db != nil {
		if err := w.db.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close db: %w", err)
		}
		w.db = nil
	}
	return firstErr
}

// DeleteOlderThan removes rows with ts < cutoffNanos. Returns row count.
// Used by store.Retainer to enforce retention_days.
func (w *SQLiteWriter) DeleteOlderThan(cutoffNanos int64) (int64, error) {
	if w.db == nil {
		return 0, nil
	}
	res, err := w.db.Exec(`DELETE FROM messages WHERE ts < ?`, cutoffNanos)
	if err != nil {
		return 0, fmt.Errorf("delete older than %d: %w", cutoffNanos, err)
	}
	return res.RowsAffected()
}

// nullable converts an empty string to nil so SQLite stores NULL rather than ''.
// Required so the partial indexes (`WHERE trace_id IS NOT NULL`) work as expected.
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
