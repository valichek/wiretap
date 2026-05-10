package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"wiretap/event"
)

func sampleEvent(seq int) event.Event {
	return event.Event{
		Ts:         int64(seq) * 1_000_000,
		Channel:    "rpcx",
		Src:        "client",
		Dst:        "server",
		Method:     fmt.Sprintf("svc:m%d", seq),
		Status:     "ok",
		DurationMs: int64(seq),
	}
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	require.NoError(t, sc.Err())
	return lines
}

func TestJSONLWriter_writeOneAndAppend(t *testing.T) {
	dir := t.TempDir()
	w := NewJSONLWriter(dir, 1700000000)

	require.NoError(t, w.Write("rpcx", sampleEvent(1)))
	require.NoError(t, w.Write("rpcx", sampleEvent(2)))
	require.NoError(t, w.Write("rpcx", sampleEvent(3)))
	require.NoError(t, w.Close())

	path := filepath.Join(dir, "rpcx-1700000000.jsonl")
	lines := readLines(t, path)
	assert.Len(t, lines, 3)

	var got event.Event
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &got))
	assert.Equal(t, sampleEvent(2), got)
}

func TestJSONLWriter_separateChannels(t *testing.T) {
	dir := t.TempDir()
	w := NewJSONLWriter(dir, 42)

	require.NoError(t, w.Write("rpcx", sampleEvent(1)))
	require.NoError(t, w.Write("http", event.Event{Ts: 1, Channel: "http", Dst: "upstream", Method: "GET /x", Status: "200", DurationMs: 1}))
	require.NoError(t, w.Close())

	rpcxLines := readLines(t, filepath.Join(dir, "rpcx-42.jsonl"))
	httpLines := readLines(t, filepath.Join(dir, "http-42.jsonl"))
	assert.Len(t, rpcxLines, 1)
	assert.Len(t, httpLines, 1)
}

func TestJSONLWriter_filenameUsesStartUnix(t *testing.T) {
	dir := t.TempDir()
	w := NewJSONLWriter(dir, 1234567890)
	require.NoError(t, w.Write("rpcx", sampleEvent(1)))
	require.NoError(t, w.Close())

	expected := filepath.Join(dir, "rpcx-1234567890.jsonl")
	_, err := os.Stat(expected)
	assert.NoError(t, err)
}

func TestJSONLWriter_concurrent(t *testing.T) {
	dir := t.TempDir()
	w := NewJSONLWriter(dir, 1)

	const goroutines = 20
	const perG = 50

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				require.NoError(t, w.Write("rpcx", sampleEvent(g*1000+i)))
			}
		}(g)
	}
	wg.Wait()
	require.NoError(t, w.Close())

	lines := readLines(t, filepath.Join(dir, "rpcx-1.jsonl"))
	assert.Len(t, lines, goroutines*perG, "no lines lost or duplicated under concurrent write")

	// every line is parseable JSON — confirms no interleaved bytes
	for _, line := range lines {
		var got event.Event
		require.NoError(t, json.Unmarshal([]byte(line), &got), "malformed line: %q", line)
	}
}

func TestJSONLWriter_writeAfterClose_reopens(t *testing.T) {
	dir := t.TempDir()
	w := NewJSONLWriter(dir, 1)

	require.NoError(t, w.Write("rpcx", sampleEvent(1)))
	require.NoError(t, w.Close())

	// after Close, a subsequent Write reopens the file in append mode
	require.NoError(t, w.Write("rpcx", sampleEvent(2)))
	require.NoError(t, w.Close())

	lines := readLines(t, filepath.Join(dir, "rpcx-1.jsonl"))
	assert.Len(t, lines, 2)
}

func TestJSONLWriter_openFails(t *testing.T) {
	w := NewJSONLWriter("/nonexistent/path/does/not/exist", 1)
	err := w.Write("rpcx", sampleEvent(1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open")
}
