// Package store provides JSONL and SQLite sinks for captured Events.
package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"wiretap/event"
)

// JSONLWriter appends Events as JSONL lines to per-channel files in dir.
// One file per (channel, startUnix) — that pairs with the wt-up lifecycle:
// each session gets its own files, no rotation logic.
type JSONLWriter struct {
	dir       string
	startUnix int64

	mu      sync.Mutex
	handles map[string]*os.File
}

// NewJSONLWriter constructs a writer rooted at dir. Files are named
// "<channel>-<startUnix>.jsonl". The dir must exist.
func NewJSONLWriter(dir string, startUnix int64) *JSONLWriter {
	return &JSONLWriter{
		dir:       dir,
		startUnix: startUnix,
		handles:   make(map[string]*os.File),
	}
}

// Write appends one event as a JSONL line to the file for its channel.
// Channel-keyed file handles are opened lazily on first Write per channel.
func (w *JSONLWriter) Write(channel string, ev event.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	f, ok := w.handles[channel]
	if !ok {
		path := filepath.Join(w.dir, fmt.Sprintf("%s-%d.jsonl", channel, w.startUnix))

		var err error
		f, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("open %s: %w", path, err)
		}

		w.handles[channel] = f
	}

	line, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	line = append(line, '\n')

	if _, err := f.Write(line); err != nil {
		return fmt.Errorf("write %s: %w", channel, err)
	}
	return nil
}

// Close flushes (via OS) and closes all open file handles. Subsequent Writes
// will reopen them — that's intentional so Close is safe to call mid-shutdown.
func (w *JSONLWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error
	for ch, f := range w.handles {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close %s: %w", ch, err)
		}
	}
	w.handles = make(map[string]*os.File)
	return firstErr
}
