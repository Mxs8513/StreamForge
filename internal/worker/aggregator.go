package worker

import (
	"github.com/Mxs8513/StreamForge/internal/event"
)

// WindowStart returns the start (epoch millis) of the tumbling window that ts
// falls into, for a window of windowSize millis. Windows are [start, start+size).
func WindowStart(ts, windowSize int64) int64 {
	return ts - (ts % windowSize)
}

// Aggregator implements keyed, tumbling-window aggregation over keyed state.
//
// Phase 2 uses processing-time windowing: callers pass the processing timestamp
// (ingest time) as ts. The window-assignment timestamp is a parameter rather
// than read from the event so Phase 8 can switch to event-time + watermarks
// without changing this logic, and so tests are deterministic.
type Aggregator struct {
	state        *State
	windowSize   int64 // millis
	maxEventTime int64 // watermark source: highest event_time folded so far
}

// NewAggregator builds an aggregator over the given keyed state.
func NewAggregator(state *State, windowSizeMS int64) *Aggregator {
	return &Aggregator{state: state, windowSize: windowSizeMS}
}

// MaxEventTime is the highest event_time seen — the basis for the watermark that
// decides, deterministically and independently of wall-clock, when a window has
// closed. Replaying the same events reproduces the same watermark progression,
// so the same windows flush with the same contents (the key to exactly-once).
func (a *Aggregator) MaxEventTime() int64 { return a.maxEventTime }

// SnapshotState serializes the keyed state for a checkpoint. Must be called from
// the aggregator goroutine (the sole state owner) so no write races the backup.
func (a *Aggregator) SnapshotState() ([]byte, error) {
	return a.state.Snapshot()
}

// RestoreState loads a checkpoint snapshot into the keyed state. Called before
// the aggregator goroutine starts, so the store is exclusively held.
func (a *Aggregator) RestoreState(data []byte) error {
	return a.state.Restore(data)
}

// Update folds one event into the (window, key) aggregate, where the window is
// derived from ts.
func (a *Aggregator) Update(e *event.Event, ts int64) error {
	if ts > a.maxEventTime {
		a.maxEventTime = ts
	}
	ws := WindowStart(ts, a.windowSize)
	agg, err := a.state.Get(ws, e.Key)
	if err != nil {
		return err
	}
	if agg == nil {
		agg = &PartialAgg{}
	}
	agg.Add(e.Amount, e.EventType)
	return a.state.Put(ws, e.Key, agg)
}

// emit converts a stored window entry into the output record.
func (a *Aggregator) emit(entry WindowEntry) event.OutputRecord {
	return event.OutputRecord{
		Key:           entry.Key,
		WindowStart:   entry.WindowStart,
		WindowEnd:     entry.WindowStart + a.windowSize,
		Count:         entry.Agg.Count,
		SumAmount:     entry.Agg.Sum,
		MinAmount:     entry.Agg.Min,
		MaxAmount:     entry.Agg.Max,
		DistinctTypes: int64(len(entry.Agg.Types)),
	}
}

// FlushClosed emits and removes every window that has closed by time now.
func (a *Aggregator) FlushClosed(now int64) ([]event.OutputRecord, error) {
	entries, err := a.state.ScanClosed(now, a.windowSize)
	if err != nil {
		return nil, err
	}
	return a.toRecords(entries), nil
}

// FlushAll emits and removes every remaining window (end of bounded stream).
func (a *Aggregator) FlushAll() ([]event.OutputRecord, error) {
	entries, err := a.state.ScanAll()
	if err != nil {
		return nil, err
	}
	return a.toRecords(entries), nil
}

func (a *Aggregator) toRecords(entries []WindowEntry) []event.OutputRecord {
	out := make([]event.OutputRecord, 0, len(entries))
	for _, e := range entries {
		out = append(out, a.emit(e))
	}
	return out
}
