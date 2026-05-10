package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/smallnest/rpcx/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"

	"wiretap/event"
)

// startFakeUpstream listens on 127.0.0.1:0 and accepts connections in a loop,
// echoing one canned Response per accepted conn (Seq mirrored from the request).
// Returns the bound address. The listener is closed via t.Cleanup.
func startFakeUpstream(t *testing.T, respPayload []byte) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = l.Close() })

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return // listener closed
			}
			go func(c net.Conn) {
				defer c.Close()
				req, err := protocol.Read(c)
				if err != nil {
					return
				}
				resp := protocol.NewMessage()
				resp.SetMessageType(protocol.Response)
				resp.SetSeq(req.Seq())
				resp.SetSerializeType(protocol.JSON)
				resp.Payload = respPayload
				_, _ = c.Write(resp.Encode())
				// drain any further bytes until client closes
				_, _ = io.Copy(io.Discard, c)
			}(conn)
		}
	}()

	return l.Addr().String()
}

// pickFreePort binds + closes a TCP port to discover a free one. Race-prone
// in theory, fine in practice for a single integration test.
func pickFreePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func waitForListen(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("listener never came up at %s", addr)
}

func TestRun_endToEnd(t *testing.T) {
	dir := t.TempDir()

	upstreamAddr := startFakeUpstream(t, []byte(`{"ok":true}`))
	shadowAddr := pickFreePort(t)

	// write wiretap.yaml — sqlite enabled to verify both sinks
	yaml := fmt.Sprintf(`
store:
  jsonl: true
  sqlite: true
proxies:
  - {kind: rpcx, listen: "%s", upstream: "%s", src: "test-client", dst: "test-server"}
`, shadowAddr, upstreamAddr)
	cfgPath := filepath.Join(dir, "wiretap.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(yaml), 0o644))

	// run wiretap in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() {
		runDone <- run(ctx, cliFlags{
			dir:        dir,
			configPath: cfgPath,
			logLevel:   "warn",
			prettyLogs: false,
		}, zerolog.Nop())
	}()

	waitForListen(t, shadowAddr, 2*time.Second)

	// dial through the shadow port and exchange one rpcx call
	client, err := net.Dial("tcp", shadowAddr)
	require.NoError(t, err)

	req := protocol.NewMessage()
	req.SetMessageType(protocol.Request)
	req.SetSeq(7)
	req.SetSerializeType(protocol.JSON)
	req.ServicePath = "svc"
	req.ServiceMethod = "ping"
	req.Payload = []byte(`{"in":1}`)

	_, err = client.Write(req.Encode())
	require.NoError(t, err)

	resp, err := protocol.Read(client)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), resp.Seq())
	assert.Equal(t, []byte(`{"ok":true}`), resp.Payload)

	// give pipeline a moment to drain the emitted event
	time.Sleep(150 * time.Millisecond)

	// cancel triggers the full shutdown — proxy closes listener + conns,
	// forward goroutines unblock from their Reads, run() drains pipeline.
	// note: we deliberately do NOT close client first — the proxy's ctx
	// watcher is responsible for tearing down the conn on cancel.
	defer client.Close()
	cancel()

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("run did not return after ctx cancel")
	}

	// verify JSONL — exactly one row, with our method
	jsonlMatches, err := filepath.Glob(filepath.Join(dir, "rpcx-*.jsonl"))
	require.NoError(t, err)
	require.Len(t, jsonlMatches, 1, "exactly one rpcx-*.jsonl should exist")

	f, err := os.Open(jsonlMatches[0])
	require.NoError(t, err)
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	require.NoError(t, sc.Err())
	require.Len(t, lines, 1)

	var ev event.Event
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &ev))
	assert.Equal(t, "rpcx", ev.Channel)
	assert.Equal(t, "test-client", ev.Src)
	assert.Equal(t, "test-server", ev.Dst)
	assert.Equal(t, "svc:ping", ev.Method)
	assert.Equal(t, "ok", ev.Status)
	assert.JSONEq(t, `{"in":1}`, ev.ReqPayload)
	assert.JSONEq(t, `{"ok":true}`, ev.RespPayload)

	// verify SQLite — same row count
	rdb, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "wiretap.db"))
	require.NoError(t, err)
	defer rdb.Close()

	var count int
	require.NoError(t, rdb.QueryRow(`SELECT count(*) FROM messages`).Scan(&count))
	assert.Equal(t, 1, count)

	var method, status string
	require.NoError(t, rdb.QueryRow(`SELECT method, status FROM messages`).Scan(&method, &status))
	assert.Equal(t, "svc:ping", method)
	assert.Equal(t, "ok", status)
}
