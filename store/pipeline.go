package store

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"wiretap/event"
)

// Pipeline is a non-blocking sink that fans events out to JSONL and (optionally)
// SQLite consumer goroutines. Emit drops events when a writer's channel is full
// rather than blocking — service traffic must never be slowed by storage I/O.
//
// Pipeline implements event.Sink.
type Pipeline struct {
	jsonl    *JSONLWriter
	sqlite   *SQLiteWriter // nil if disabled
	jsonlCh  chan event.Event
	sqliteCh chan event.Event // nil if no sqlite

	// dropStatuses — events with a status in this set are emitted (Sink.Emit
	// returns normally) but skipped by the writer goroutines, so they never
	// land in JSONL or SQLite. Use to silence dial_error / upstream_closed
	// noise when an upstream is down.
	dropStatuses map[string]struct{}

	log          zerolog.Logger
	jsonlDrops   atomic.Int64
	sqliteDrops  atomic.Int64
	filteredOut  atomic.Int64 // events skipped by dropStatuses (counted once via jsonl path)

	stopWarner chan struct{}
	wg         sync.WaitGroup
}

// NewPipeline starts the consumer goroutines and the drop-rate warner.
// jsonl must not be nil; sqlite may be nil. bufSize is the per-writer channel
// capacity (1024 is the sane default for dev volume). dropStatuses lists
// event statuses (e.g. "dial_error", "upstream_closed") to filter out at
// write time — emit still succeeds, but writers skip them.
func NewPipeline(jsonl *JSONLWriter, sqlite *SQLiteWriter, bufSize int, dropStatuses []string, log zerolog.Logger) *Pipeline {
	dropSet := make(map[string]struct{}, len(dropStatuses))
	for _, s := range dropStatuses {
		dropSet[s] = struct{}{}
	}

	p := &Pipeline{
		jsonl:        jsonl,
		sqlite:       sqlite,
		jsonlCh:      make(chan event.Event, bufSize),
		dropStatuses: dropSet,
		log:          log,
		stopWarner:   make(chan struct{}),
	}

	p.wg.Add(1)
	go p.runJSONL()

	if sqlite != nil {
		p.sqliteCh = make(chan event.Event, bufSize)
		p.wg.Add(1)
		go p.runSQLite()
	}

	p.wg.Add(1)
	go p.runDropWarner()

	return p
}

// Emit pushes ev to each enabled writer's channel without blocking. If a
// writer's channel is full, that writer's drop counter increments and the
// drop warner reports it on its next tick. Emit must NOT be called after Close.
func (p *Pipeline) Emit(ev event.Event) {
	select {
	case p.jsonlCh <- ev:
	default:
		p.jsonlDrops.Add(1)
	}
	if p.sqliteCh != nil {
		select {
		case p.sqliteCh <- ev:
		default:
			p.sqliteDrops.Add(1)
		}
	}
}

func (p *Pipeline) runJSONL() {
	defer p.wg.Done()
	for ev := range p.jsonlCh {
		if p.shouldDrop(ev) {
			// jsonl is the always-on sink — count filtered-out events here, once.
			p.filteredOut.Add(1)
			continue
		}
		if err := p.jsonl.Write(ev.Channel, ev); err != nil {
			p.log.Warn().Err(err).Msg("jsonl write failed")
		}
	}
}

func (p *Pipeline) runSQLite() {
	defer p.wg.Done()
	for ev := range p.sqliteCh {
		if p.shouldDrop(ev) {
			continue // already counted via jsonl path
		}
		if err := p.sqlite.Write(ev); err != nil {
			p.log.Warn().Err(err).Msg("sqlite write failed")
		}
	}
}

func (p *Pipeline) shouldDrop(ev event.Event) bool {
	if len(p.dropStatuses) == 0 {
		return false
	}
	_, ok := p.dropStatuses[ev.Status]
	return ok
}

func (p *Pipeline) runDropWarner() {
	defer p.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if n := p.jsonlDrops.Swap(0); n > 0 {
				p.log.Warn().Int64("dropped", n).Msg("jsonl drops in last second")
			}
			if n := p.sqliteDrops.Swap(0); n > 0 {
				p.log.Warn().Int64("dropped", n).Msg("sqlite drops in last second")
			}
		case <-p.stopWarner:
			return
		}
	}
}

// Close stops the warner, closes the writer channels, waits for in-flight
// events to drain (bounded by ctx), then closes the underlying writers.
// After Close returns, do NOT call Emit.
func (p *Pipeline) Close(ctx context.Context) error {
	close(p.stopWarner)
	close(p.jsonlCh)
	if p.sqliteCh != nil {
		close(p.sqliteCh)
	}

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		// drain timed out; remaining channel entries lost. log final drop counts below.
	}

	// log final cumulative drops (Load, not Swap — keep counters readable for callers
	// who inspect them after Close; runDropWarner is what resets them periodically).
	if n := p.jsonlDrops.Load(); n > 0 {
		p.log.Warn().Int64("dropped_total", n).Msg("jsonl drops at shutdown")
	}
	if n := p.sqliteDrops.Load(); n > 0 {
		p.log.Warn().Int64("dropped_total", n).Msg("sqlite drops at shutdown")
	}

	var firstErr error
	if err := p.jsonl.Close(); err != nil {
		firstErr = err
	}
	if p.sqlite != nil {
		if err := p.sqlite.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// JSONLDrops returns the cumulative count of events dropped on the jsonl path.
// Used by tests; not advertised in the public API.
func (p *Pipeline) JSONLDrops() int64 {
	return p.jsonlDrops.Load()
}

// SQLiteDrops returns the cumulative count of events dropped on the sqlite path.
func (p *Pipeline) SQLiteDrops() int64 {
	return p.sqliteDrops.Load()
}

// FilteredOut returns the cumulative count of events emitted into the
// pipeline but skipped at write time because their status matches dropStatuses.
func (p *Pipeline) FilteredOut() int64 {
	return p.filteredOut.Load()
}

// compile-time sanity that Pipeline satisfies event.Sink
var _ event.Sink = (*Pipeline)(nil)
