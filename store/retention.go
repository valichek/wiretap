package store

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"
)

// Retainer prunes old captures based on a retention age. It runs an initial
// pass on startup and then ticks once per hour. Returns nil from New when
// retention is disabled (retentionDays <= 0).
type Retainer struct {
	dir     string
	sqlite  *SQLiteWriter // may be nil
	maxAge  time.Duration
	tick    time.Duration
	now     func() time.Time // injectable for tests
	log     zerolog.Logger
}

// NewRetainer constructs a Retainer; returns nil if maxAge <= 0.
func NewRetainer(dir string, sqlite *SQLiteWriter, maxAge time.Duration, log zerolog.Logger) *Retainer {
	if maxAge <= 0 {
		return nil
	}
	return &Retainer{
		dir:    dir,
		sqlite: sqlite,
		maxAge: maxAge,
		tick:   time.Hour,
		now:    time.Now,
		log:    log,
	}
}

// Run runs an initial cleanup, then ticks until ctx is cancelled.
func (r *Retainer) Run(ctx context.Context) {
	r.cleanup()

	t := time.NewTicker(r.tick)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.cleanup()
		}
	}
}

// cleanup deletes JSONL files older than maxAge by mtime, and (if sqlite is
// configured) deletes message rows older than the cutoff. Errors are logged
// but never aborted — best-effort.
func (r *Retainer) cleanup() {
	cutoff := r.now().Add(-r.maxAge)
	r.pruneJSONL(cutoff)
	r.pruneSQLite(cutoff)
}

func (r *Retainer) pruneJSONL(cutoff time.Time) {
	files, err := filepath.Glob(filepath.Join(r.dir, "*.jsonl"))
	if err != nil {
		r.log.Warn().Err(err).Msg("retention: jsonl glob failed")
		return
	}

	var deleted int
	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			r.log.Warn().Err(err).Str("file", f).Msg("retention: stat failed")
			continue
		}

		if info.ModTime().Before(cutoff) {
			if err := os.Remove(f); err != nil {
				r.log.Warn().Err(err).Str("file", f).Msg("retention: remove failed")
				continue
			}
			deleted++
		}
	}

	if deleted > 0 {
		r.log.Info().Int("files", deleted).Dur("max_age", r.maxAge).Msg("retention: pruned jsonl")
	}
}

func (r *Retainer) pruneSQLite(cutoff time.Time) {
	if r.sqlite == nil {
		return
	}

	n, err := r.sqlite.DeleteOlderThan(cutoff.UnixNano())
	if err != nil {
		r.log.Warn().Err(err).Msg("retention: sqlite delete failed")
		return
	}

	if n > 0 {
		r.log.Info().Int64("rows", n).Dur("max_age", r.maxAge).Msg("retention: pruned sqlite")
	}
}
