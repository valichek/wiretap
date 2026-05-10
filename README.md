# wiretap

Passive sidecar that captures every rpcx + HTTP message exchanged between local dev services and writes them to JSONL (always) and SQLite (opt-in) under `$WIRETAP_DIR` (default `~/.wiretap`). Queryable by `trace_id` / `request_id` / method via the `wt-q` and `wt-rpc` shell helpers.

Zero changes to service code — wiring is done by repointing existing env-var overrides at shadow ports in the IDE run configs.

## What it captures

- **rpcx** — any `github.com/smallnest/rpcx` client/server pair where the client's target address can be repointed at a TCP shadow port. JSON, CBOR, and Gob payloads are decoded (CBOR → JSON, Gob → base64 envelope). Paired req/resp with `duration_ms`.
- **HTTP** — any service whose outbound base URL can be repointed at a `mitmweb` reverse proxy. The shipped Python addon writes rows in the same schema as the rpcx side.

Out of scope:
- **Kafka** — wiretap is point-to-point only.
- **Inbound traffic from outside the local stack** (browser, curl, hurl into a service) — wiretap only sees calls that pass through a configured shadow.
- **TLS** — shadows are plain TCP; install mitmproxy's CA cert if you need to tap HTTPS.

## Repo layout

```
wiretap/
├── cmd/wiretap-rpcx/        # main Go binary (rpcx shadow proxies)
├── codec/                   # rpcx payload decode (JSON/CBOR/Gob)
├── config/                  # wiretap.yaml loader
├── event/                   # Event struct + Sink interface
├── proxy/                   # rpcx proxy + per-conn pair tracking
├── store/                   # JSONL + SQLite writers, pipeline, retention
├── python/                  # mitmproxy HTTP addon + tests
├── bin/                     # wt-mitmweb, wt-rpc, wt-http, wt-q helpers
├── .run/                    # IDE run configs
└── wiretap.example.yaml     # default config — 11 rpcx shadows + http reference
```

## Install

### 1. Prerequisites

- Go 1.25+ (`go version`)
- Python 3.11+ (`python3 --version`) — only needed for HTTP capture
- `sqlite3`, `jq` (`brew install sqlite jq` on macOS)
- An IDE that reads `.run/*.run.xml` (e.g. IntelliJ IDEA) — optional but recommended; CLI launch is also supported

### 2. Build the Go binary

From the wiretap repo root:

```bash
go build ./...
go test ./...
```

The first build downloads ~25 MB of deps (rpcx, modernc/sqlite, cbor, zerolog). Tests should be all green.

### 3. Install the mitmproxy addon dependencies

mitmproxy ships its own Python via Homebrew, but that build omits the `sqlite3` stdlib module — which the addon needs. Use a venv instead:

```bash
cd python
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

This installs `mitmproxy>=10` and `pytest>=7` into `python/.venv/`. The `wt-mitmweb` helper auto-detects this venv and prefers it over a system `mitmweb`.

Run the addon test suite to confirm:

```bash
.venv/bin/python -m pytest -v
```

### 4. Make the shell helpers available

Either add `bin/` to your `$PATH`:

```bash
export PATH="$(pwd)/bin:$PATH"   # add to ~/.zshrc with the absolute path you choose
```

Or symlink each helper into a directory already on `$PATH`:

```bash
ln -sf "$(pwd)/bin/"wt-* ~/.local/bin/
```

Verify:

```bash
which wt-rpc wt-q wt-http wt-mitmweb
```

## Configure

### 1. Wiretap config file

The default `wiretap.example.yaml` is checked in and used as-is by the IDE run config. To customize, copy it:

```bash
mkdir -p "${WIRETAP_DIR:-$HOME/.wiretap}"
cp wiretap.example.yaml "${WIRETAP_DIR:-$HOME/.wiretap}/wiretap.yaml"
```

Config resolution order:
- `--config <path>` flag wins
- otherwise `$WIRETAP_DIR/wiretap.yaml` (default `$WIRETAP_DIR=~/.wiretap`)
- otherwise `./wiretap.yaml`

Knobs in `store:`
- `jsonl: true` — always-on by convention; one file per process invocation, named `rpcx-<startUnix>.jsonl` / `http-<startUnix>.jsonl`
- `sqlite: true` — opt-in unified DB at `$WIRETAP_DIR/wiretap.db` (WAL, written by both Go and the Python addon)
- `truncate_payload_bytes: 1048576` — payloads above this are stored as `{"__truncated_at_bytes": N, "__original_size": M}`
- `retention: 2h` — `Nh` / `Nd` / `Nw` (single-unit). Files pruned by mtime, SQLite rows by `ts`. Cleanup runs on startup and hourly. `0` / unset = no pruning
- `drop_statuses: ["dial_error", "upstream_closed"]` — rpcx statuses skipped at write time. Useful to suppress noise when an upstream is intentionally down. Empty list = record everything

### 2. Point services at the shadow ports

Wiretap captures nothing until clients dial the shadows instead of their real upstreams. The mechanism is service-specific — most stacks already have one or more env vars that override the address of a remote service for local-dev use. Common patterns:

- Per-service address override env vars (e.g. `<SVC>_ADDR=localhost:18989`).
- A combined `address mapping` env var that takes `service@host:port` pairs (e.g. `RPC_ADDRESS_OVERRIDE="service-x@127.0.0.1:18987,service-y@127.0.0.1:18989"`).
- A per-method routing env var (e.g. `RPC_CLIENT__METHODS_ADDRESSES="localhost:28989@service-y.checkSignature"`).
- For HTTP: a `<SVC>_BASE_URL` style override repointed at `http://localhost:14344` (the mitmweb listen port).

Set these in your IDE run configs (or shell) so each client dials the matching `listen:` port from `wiretap.yaml`. Restart the service after the change. To disable wiretap, revert the env vars and restart.

The wiretap-rpcx process must be running before you start the clients — otherwise they hit `connect: connection refused`.

## Run

### Option A: IDE run configs (recommended)

Two run configs ship in `.run/`:

- **wiretap-rpcx (local)** — Go run config; launches `cmd/wiretap-rpcx` with `--config wiretap.example.yaml`. One listener per `proxies:` entry.
- **wiretap-mitmweb (local)** — shell run config; launches `bin/wt-mitmweb`, which `exec`s `mitmweb` with the addon attached. Configure src/dst/upstream via `WT_HTTP_SRC` / `WT_HTTP_DST` / `WT_HTTP_UPSTREAM` env vars (see the script). Web UI at `http://localhost:18081` by default.

Click run on both. You should see structured zerolog lines from wiretap-rpcx listing each listener, and a "Web server listening at http://localhost:18081" from mitmweb.

### Option B: CLI

```bash
# rpcx side (from wiretap repo root)
go run ./cmd/wiretap-rpcx --config wiretap.example.yaml

# http side (separate terminal)
./bin/wt-mitmweb
```

`$WIRETAP_DIR` defaults to `~/.wiretap`; override per-shell if needed.

### Verify it's capturing

```bash
# trigger a flow (e.g. via curl/hurl against one of the shadowed clients)
# then:
ls -lt "${WIRETAP_DIR:-$HOME/.wiretap}/"      # rpcx-<unix>.jsonl, http-<unix>.jsonl, wiretap.db
wt-rpc                                        # tail latest rpcx jsonl
wt-q "SELECT count(*) FROM messages"          # should be > 0
wt-http                                       # opens mitmweb UI
```

## Daily debugging session

```bash
# 1. start wiretap-rpcx + wiretap-mitmweb from the IDE
# 2. start the services that participate in your flow
# 3. trigger a flow (curl, hurl, frontend, etc.)
# 4. query

# pivot from a W3C trace_id (e.g. copied from Jaeger or OTEL output)
wt-q -header -column "
  SELECT datetime(ts/1e9,'unixepoch','localtime') AS time,
         channel, src, dst, method, status, duration_ms
    FROM messages
   WHERE trace_id = '<TRACE_ID>'
   ORDER BY ts;
"

# slowest 20 calls in the last 10 min
wt-q -header -column "
  SELECT dst, method, duration_ms
    FROM messages
   WHERE channel='rpcx'
     AND ts > (strftime('%s','now') - 600) * 1e9
   ORDER BY duration_ms DESC LIMIT 20;
"

# live tail with a jq filter
wt-rpc 'select(.dst == "service-x" and .status != "ok")'
```

## Codec notes

- **JSON** payloads stored verbatim (after a parse-then-marshal validation pass).
- **CBOR** decoded via `fxamacker/cbor/v2` with `DefaultMapType=map[string]interface{}` — the resulting Go value is JSON-marshaled before storage.
- **Gob** and **unknown SerializeType** values are wrapped: `{"_serialize_type": "...", "_base64": "..."}`.
- **Truncation** runs before codec dispatch — if the raw frame is over `truncate_payload_bytes`, you get the truncation marker instead of the decoded body.
- **Invalid JSON** (e.g. malformed payload masquerading as `SerializeType=1`) becomes `{"_serialize_type": "json_invalid", "_raw": "..."}`.

## Extending: adding a new shadow

For an additional client → service path:

1. Add a `proxies:` entry to your `wiretap.yaml` — pick a free listen port (e.g. one digit above the upstream port to keep them grepable), set `upstream` to the real service address, label `src` and `dst`.
2. Restart wiretap-rpcx — the new listener comes up alongside the existing ones.
3. Point the client at the new listen port via whatever address-override mechanism it uses.
4. Trigger a call; verify the row materializes via `wt-rpc` or `wt-q`.

For per-method routing (one client wants only specific methods tapped), the simplest path is to add a shadow for the destination service and limit the client's override to those methods.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `wiretap-rpcx` exits with `bind: address already in use` | another wiretap-rpcx is running, or one of the shadow ports collides with a real service |
| Services error with `dial tcp 127.0.0.1:189xx: connect: connection refused` | wiretap-rpcx not running — start it before restarting services pointed at the shadows |
| Rows show up only in JSONL, not in `wiretap.db` | `store.sqlite: false` in `wiretap.yaml` — flip to `true` and restart |
| `wt-mitmweb` fails with `No module named sqlite3` | system mitmweb in use; create the `python/.venv` per Install step 3 |
| HTTP traffic doesn't appear | the client's base-URL env still points at the real upstream — switch it to the mitmweb listen port |
| `WHERE status='dial_error'` returns zero rows | `drop_statuses` filter at write time — check `wiretap.yaml` |
| Old data missing | `retention` pruned it — bump the value or set `0` |
| `wt-q` says "wiretap.db does not exist" | sqlite store disabled; enable it or use `wt-rpc` over the JSONL files |

## Reference

- Schema: see `wiretap.example.yaml` and `store/sqlite.go`
- Query recipes: the `wiretap-query` Claude Code skill (`/wiretap-query` in Claude Code)
