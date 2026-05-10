"""Tests for mitm_addon. Uses mitmproxy.test.tflow to construct fake HTTPFlow
fixtures and mitmproxy.test.taddons to drive lifecycle hooks.

Run from the wiretap repo root:
    cd python && python -m pytest -v
"""

from __future__ import annotations

import base64
import json
import sqlite3

from mitmproxy.test import taddons, tflow

from mitm_addon import (
    WiretapAddon,
    _build_event,
    _encode_body,
    _extract_trace_id,
    _lookup,
    _nullable,
    _slim_for_jsonl,
)


# pure-function unit tests -----------------------------------------------------


def test_extract_trace_id_basic():
    headers = {"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"}
    assert _extract_trace_id(headers) == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


def test_extract_trace_id_missing():
    assert _extract_trace_id({}) == ""


def test_extract_trace_id_malformed():
    assert _extract_trace_id({"traceparent": "garbage"}) == ""


def test_extract_trace_id_case_variant():
    headers = {"Traceparent": "00-trace-span-01"}
    assert _extract_trace_id(headers) == "trace"


def test_lookup_finds_first_nonempty():
    assert _lookup({"a": "1", "b": "2"}, "a", "b") == "1"
    assert _lookup({"a": "", "b": "2"}, "a", "b") == "2"
    assert _lookup({}, "a", "b") == ""


def test_nullable():
    assert _nullable("") is None
    assert _nullable("x") == "x"


def test_encode_body_empty():
    assert _encode_body("application/json", b"", 1024) == ""


def test_encode_body_json_valid():
    out = _encode_body("application/json", b'{"a": 1}', 1024)
    assert json.loads(out) == {"a": 1}


def test_encode_body_json_invalid_falls_back():
    out = _encode_body("application/json", b"not json", 1024)
    decoded = json.loads(out)
    assert decoded["_serialize_type"] == "application/json"
    assert base64.b64decode(decoded["_base64"]) == b"not json"


def test_encode_body_non_json_wraps():
    out = _encode_body("text/plain", b"hello", 1024)
    decoded = json.loads(out)
    assert decoded["_serialize_type"] == "text/plain"
    assert base64.b64decode(decoded["_base64"]) == b"hello"


def test_encode_body_empty_content_type():
    out = _encode_body("", b"\x00\x01\x02", 1024)
    decoded = json.loads(out)
    assert decoded["_serialize_type"] == "unknown"


def test_encode_body_truncated():
    big = b"x" * 2048
    out = _encode_body("application/json", big, 1024)
    decoded = json.loads(out)
    assert decoded["__truncated_at_bytes"] == 1024
    assert decoded["__original_size"] == 2048


def test_encode_body_truncate_zero_disables():
    big = b"x" * 2048
    out = _encode_body("application/json", big, 0)
    # not truncated; not valid JSON either, so wrapped
    decoded = json.loads(out)
    assert "_serialize_type" in decoded


def test_slim_for_jsonl_drops_empty_optional():
    ev = {
        "ts": 1, "channel": "http", "dst": "x", "method": "GET /", "status": "200", "duration_ms": 0,
        "src": "", "trace_id": "", "request_id": "", "saga_id": "",
        "req_headers": "", "req_payload": "", "resp_headers": "", "resp_payload": "", "error": "",
    }
    slim = _slim_for_jsonl(ev)
    assert set(slim.keys()) == {"ts", "channel", "dst", "method", "status", "duration_ms"}


def test_slim_for_jsonl_keeps_populated():
    ev = {
        "ts": 1, "channel": "http", "dst": "x", "method": "GET /", "status": "200", "duration_ms": 0,
        "src": "client", "trace_id": "abc",
        "request_id": "", "saga_id": "",
        "req_headers": "", "req_payload": "", "resp_headers": "", "resp_payload": "", "error": "",
    }
    slim = _slim_for_jsonl(ev)
    assert slim["src"] == "client"
    assert slim["trace_id"] == "abc"
    assert "request_id" not in slim


# event-build tests via tflow --------------------------------------------------


def _make_flow_with(method="POST", path="/v1/wallets", req_body=b'{"name":"alice"}',
                    req_ct="application/json", resp_status=200, resp_body=b'{"id":"w1"}',
                    resp_ct="application/json", req_headers=None):
    # tflow.tflow() is request-only by default; resp=True gives us a default response we mutate
    f = tflow.tflow(resp=True)
    f.request.method = method
    f.request.path = path
    f.request.set_content(req_body)
    if req_ct:
        f.request.headers["content-type"] = req_ct
    if req_headers:
        for k, v in req_headers.items():
            f.request.headers[k] = v

    f.response.status_code = resp_status
    f.response.set_content(resp_body)
    if resp_ct:
        f.response.headers["content-type"] = resp_ct

    return f


def test_build_event_basic():
    f = _make_flow_with(req_headers={
        "traceparent": "00-traceabc-spanxyz-01",
        "x-request-id": "req-7",
    })
    ev = _build_event(f, src="client", dst="server", truncate_bytes=1024)

    assert ev["channel"] == "http"
    assert ev["src"] == "client"
    assert ev["dst"] == "server"
    assert ev["method"] == "POST /v1/wallets"
    assert ev["status"] == "200"
    assert ev["trace_id"] == "traceabc"
    assert ev["request_id"] == "req-7"
    assert json.loads(ev["req_payload"]) == {"name": "alice"}
    assert json.loads(ev["resp_payload"]) == {"id": "w1"}


def test_build_event_no_body():
    f = _make_flow_with(req_body=b"", req_ct="", resp_body=b"", resp_ct="")
    ev = _build_event(f, src="", dst="x", truncate_bytes=1024)

    assert ev["req_payload"] == ""
    assert ev["resp_payload"] == ""


def test_build_event_truncates_oversized_body():
    big = b"x" * 4096
    f = _make_flow_with(req_body=big, req_ct="application/json")
    ev = _build_event(f, src="", dst="x", truncate_bytes=1024)
    decoded = json.loads(ev["req_payload"])
    assert decoded["__truncated_at_bytes"] == 1024
    assert decoded["__original_size"] == 4096


# end-to-end addon tests with taddons ------------------------------------------


def _read_jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def test_addon_writes_jsonl(tmp_path, monkeypatch):
    monkeypatch.setenv("WIRETAP_DIR", str(tmp_path))

    addon = WiretapAddon()
    with taddons.context(addon) as tctx:
        tctx.configure(addon, src="src1", dst="dst1")

        f = _make_flow_with()
        addon.response(f)

        addon.done()

    files = list(tmp_path.glob("http-*.jsonl"))
    assert len(files) == 1
    rows = _read_jsonl(files[0])
    assert len(rows) == 1
    assert rows[0]["channel"] == "http"
    assert rows[0]["src"] == "src1"
    assert rows[0]["dst"] == "dst1"


def test_addon_writes_sqlite_when_enabled(tmp_path, monkeypatch):
    monkeypatch.setenv("WIRETAP_DIR", str(tmp_path))

    addon = WiretapAddon()
    with taddons.context(addon) as tctx:
        tctx.configure(addon, src="s", dst="d", tap_sqlite_enabled=True)

        f = _make_flow_with()
        addon.response(f)

        addon.done()

    db = tmp_path / "wiretap.db"
    assert db.exists(), "sqlite file should be created when tap_sqlite_enabled=True"

    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT count(*) FROM messages").fetchone()
        assert rows[0] == 1

        method, status = conn.execute("SELECT method, status FROM messages").fetchone()
        assert method == "POST /v1/wallets"
        assert status == "200"


def test_addon_skips_sqlite_when_disabled_and_db_absent(tmp_path, monkeypatch):
    monkeypatch.setenv("WIRETAP_DIR", str(tmp_path))

    addon = WiretapAddon()
    with taddons.context(addon) as tctx:
        tctx.configure(addon, src="s", dst="d", tap_sqlite_enabled=False)

        f = _make_flow_with()
        addon.response(f)

        addon.done()

    # sqlite disabled and wiretap.db absent → no DB file created
    assert not (tmp_path / "wiretap.db").exists()
    # but JSONL still wrote
    assert list(tmp_path.glob("http-*.jsonl"))


def test_addon_uses_existing_db(tmp_path, monkeypatch):
    monkeypatch.setenv("WIRETAP_DIR", str(tmp_path))

    db_path = tmp_path / "wiretap.db"
    # create empty db file (e.g. wiretap-rpcx already opened it)
    sqlite3.connect(db_path).close()

    addon = WiretapAddon()
    with taddons.context(addon) as tctx:
        tctx.configure(addon, src="s", dst="d")

        f = _make_flow_with()
        addon.response(f)

        addon.done()

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT count(*) FROM messages").fetchone()
        assert rows[0] == 1
