"""mitmproxy addon that captures HTTP request/response pairs into wiretap's
JSONL + optional SQLite store. Designed to share the schema and on-disk
layout with the Go-side wiretap-rpcx binary so a single sqlite3 / jq query
spans both channels.

Loaded via:
    mitmweb --mode reverse:<upstream-url> --listen-port <port> \\
            --set src=<client-label> --set dst=<upstream-label> \\
            -s $WIRETAP_DIR/mitm_addon.py

Or via the `bin/wt-mitmweb` helper script, which reads WT_HTTP_SRC /
WT_HTTP_DST / WT_HTTP_UPSTREAM from the environment.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import sqlite3
import time
from typing import Any

from mitmproxy import ctx, http

logger = logging.getLogger(__name__)


SCHEMA_SQL = """
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
"""

INSERT_SQL = """
INSERT INTO messages (
  ts, channel, src, dst, method, status, duration_ms,
  trace_id, request_id, saga_id,
  req_headers, req_payload, resp_headers, resp_payload, error
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


# fields that are always present in JSONL; other fields are omitted when empty
# (matches Go side's `omitempty` semantics).
_ALWAYS_PRESENT = {"ts", "channel", "dst", "method", "status", "duration_ms"}


def _nullable(s: str) -> Any:
    """Empty string → None so SQLite stores NULL; required for partial indexes."""
    return s if s else None


def _extract_trace_id(headers: dict[str, str]) -> str:
    """Parse W3C traceparent: `00-<trace>-<span>-<flags>`. Returns trace segment."""
    tp = _lookup(headers, "traceparent", "Traceparent")
    if not tp:
        return ""
    parts = tp.split("-", 2)
    return parts[1] if len(parts) >= 2 else ""


def _lookup(headers: dict[str, str], *keys: str) -> str:
    """First non-empty match across keys (case-variant fallback)."""
    for k in keys:
        v = headers.get(k)
        if v:
            return v
    return ""


def _encode_body(content_type: str, body: bytes, truncate_bytes: int) -> str:
    """Convert body bytes to a JSON string for storage. Mirrors the Go codec
    package's behavior: parse + re-encode JSON; base64-wrap everything else;
    emit truncation marker for oversized payloads."""
    if not body:
        return ""

    if truncate_bytes > 0 and len(body) > truncate_bytes:
        return json.dumps({
            "__truncated_at_bytes": truncate_bytes,
            "__original_size": len(body),
        })

    if "application/json" in (content_type or "").lower():
        try:
            return json.dumps(json.loads(body))
        except (json.JSONDecodeError, UnicodeDecodeError):
            # fall through: invalid JSON → base64 wrap
            pass

    return json.dumps({
        "_serialize_type": content_type or "unknown",
        "_base64": base64.b64encode(body).decode("ascii"),
    })


def _slim_for_jsonl(ev: dict[str, Any]) -> dict[str, Any]:
    """Drop empty optional fields to mirror Go's `omitempty`."""
    return {k: v for k, v in ev.items() if k in _ALWAYS_PRESENT or v}


def _build_event(flow: http.HTTPFlow, src: str, dst: str, truncate_bytes: int) -> dict[str, Any]:
    req = flow.request
    resp = flow.response
    assert resp is not None  # caller checks

    req_headers = dict(req.headers.items())
    resp_headers = dict(resp.headers.items())

    return {
        "ts": int(req.timestamp_start * 1_000_000_000),
        "channel": "http",
        "src": src,
        "dst": dst,
        "method": f"{req.method} {req.path}",
        "status": str(resp.status_code),
        "duration_ms": int((resp.timestamp_end - req.timestamp_start) * 1000),
        "trace_id": _extract_trace_id(req_headers),
        "request_id": _lookup(req_headers, "x-request-id", "X-Request-Id"),
        "saga_id": _lookup(req_headers, "saga-id", "x-saga-id"),
        "req_headers": json.dumps(req_headers, sort_keys=True) if req_headers else "",
        "req_payload": _encode_body(req.headers.get("content-type", ""), req.raw_content or b"", truncate_bytes),
        "resp_headers": json.dumps(resp_headers, sort_keys=True) if resp_headers else "",
        "resp_payload": _encode_body(resp.headers.get("content-type", ""), resp.raw_content or b"", truncate_bytes),
        "error": str(flow.error) if flow.error else "",
    }


class WiretapAddon:
    """Hooks `response` to capture each HTTP exchange. State (file handle,
    sqlite conn) is opened in `configure` and torn down in `done`."""

    def __init__(self) -> None:
        self.start_unix = int(time.time())
        self.dir: str = ""
        self.src: str = ""
        self.dst: str = ""
        self.truncate_bytes: int = 1_048_576
        self.sqlite_conn: sqlite3.Connection | None = None
        self.jsonl_file = None

    # mitmproxy lifecycle hooks ------------------------------------------------

    def load(self, loader) -> None:
        loader.add_option("src", str, "", "src label stamped on captured events")
        loader.add_option("dst", str, "", "dst label stamped on captured events")
        loader.add_option("tap_sqlite_enabled", bool, False, "force-enable sqlite even if wiretap.db doesn't exist yet")
        loader.add_option("tap_truncate_bytes", int, 1_048_576, "max payload bytes before truncation marker")

    def configure(self, updates) -> None:
        # only re-configure when our options change (mitmproxy calls configure
        # for every option update; we want to re-open only when relevant).
        if not (updates & {"src", "dst", "tap_sqlite_enabled", "tap_truncate_bytes"}) and self.jsonl_file:
            return

        self.dir = os.environ.get("WIRETAP_DIR") or os.path.expanduser("~/.wiretap")
        os.makedirs(self.dir, exist_ok=True)

        self.src = ctx.options.src
        self.dst = ctx.options.dst
        self.truncate_bytes = ctx.options.tap_truncate_bytes

        # JSONL is always-on
        if self.jsonl_file is None:
            path = os.path.join(self.dir, f"http-{self.start_unix}.jsonl")
            self.jsonl_file = open(path, "a", buffering=1)  # line-buffered

        # SQLite — opened if wiretap.db exists or forced via option
        db_path = os.path.join(self.dir, "wiretap.db")
        should_open = os.path.exists(db_path) or ctx.options.tap_sqlite_enabled
        if self.sqlite_conn is None and should_open:
            self.sqlite_conn = sqlite3.connect(
                db_path,
                isolation_level=None,  # autocommit per execute
                check_same_thread=False,
            )
            self.sqlite_conn.execute("PRAGMA journal_mode=WAL")
            self.sqlite_conn.execute("PRAGMA busy_timeout=5000")
            self.sqlite_conn.executescript(SCHEMA_SQL)

    def done(self) -> None:
        if self.jsonl_file:
            self.jsonl_file.close()
            self.jsonl_file = None
        if self.sqlite_conn:
            self.sqlite_conn.close()
            self.sqlite_conn = None

    # capture hook -------------------------------------------------------------

    def response(self, flow: http.HTTPFlow) -> None:
        if flow.response is None:
            return
        ev = _build_event(flow, self.src, self.dst, self.truncate_bytes)
        self._write(ev)

    def _write(self, ev: dict[str, Any]) -> None:
        if self.jsonl_file:
            self.jsonl_file.write(json.dumps(_slim_for_jsonl(ev)) + "\n")

        if self.sqlite_conn:
            try:
                self.sqlite_conn.execute(INSERT_SQL, (
                    ev["ts"], ev["channel"], _nullable(ev["src"]), ev["dst"], ev["method"], ev["status"], ev["duration_ms"],
                    _nullable(ev["trace_id"]), _nullable(ev["request_id"]), _nullable(ev["saga_id"]),
                    _nullable(ev["req_headers"]), _nullable(ev["req_payload"]),
                    _nullable(ev["resp_headers"]), _nullable(ev["resp_payload"]),
                    _nullable(ev["error"]),
                ))
            except sqlite3.Error as e:
                logger.warning("sqlite insert failed: %s", e)


# mitmproxy looks for module-level `addons` to load.
addons = [WiretapAddon()]
