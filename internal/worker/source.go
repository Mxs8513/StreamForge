package worker

import (
	"context"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Mxs8513/StreamForge/internal/event"
)

// PartitionReader consumes a single Kafka partition with NO consumer group.
//
// This is deliberate: offsets are owned by StreamForge, not by Kafka's consumer
// group. There is no auto-commit and no CommitMessages call anywhere, so Kafka
// never tracks our progress. The in-process OffsetTracker is the single source
// of truth, which is exactly what lets Phase 4 fold offsets into checkpoints so
// state + offsets advance as one atomic unit (spec §6.2, §7).
type PartitionReader struct {
	reader    *kafka.Reader
	partition int
}

// StartOffset selects where a fresh partition reader begins.
type StartOffset int64

const (
	StartEarliest = StartOffset(kafka.FirstOffset)
	StartLatest   = StartOffset(kafka.LastOffset)
)

// NewPartitionReader opens a groupless reader pinned to one partition. startAt
// is either a relative sentinel (StartEarliest/StartLatest, both negative) or a
// concrete absolute offset (>= 0) restored from a checkpoint.
func NewPartitionReader(brokers []string, topic string, partition int, startAt int64) *PartitionReader {
	cfg := kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: partition,
		MinBytes:  1,
		MaxBytes:  10 << 20,
		MaxWait:   250 * time.Millisecond,
		// GroupID intentionally unset: no consumer group, no offset commits.
	}
	if startAt < 0 {
		cfg.StartOffset = startAt // FirstOffset (-2) or LastOffset (-1)
	}
	r := kafka.NewReader(cfg)
	if startAt >= 0 {
		// Resume from a concrete checkpointed offset.
		_ = r.SetOffset(startAt)
	}
	return &PartitionReader{reader: r, partition: partition}
}

// Read returns the next event and the offset of the message AFTER it (the
// position to resume from). It does not commit anything to Kafka.
func (pr *PartitionReader) Read(ctx context.Context) (*event.Event, int64, error) {
	msg, err := pr.reader.ReadMessage(ctx)
	if err != nil {
		return nil, 0, err
	}
	e, derr := event.Decode(msg.Value)
	if derr != nil {
		return nil, msg.Offset + 1, derr
	}
	e.IngestTime = time.Now().UnixMilli()
	return e, msg.Offset + 1, nil
}

func (pr *PartitionReader) Close() error { return pr.reader.Close() }

// OffsetTracker records the next-to-read offset per partition entirely
// in-process. Phase 4 snapshots this into checkpoints; nothing here is ever
// handed back to Kafka's consumer group.
type OffsetTracker struct {
	mu      sync.Mutex
	offsets map[int]int64
}

// NewOffsetTracker builds an empty tracker.
func NewOffsetTracker() *OffsetTracker {
	return &OffsetTracker{offsets: make(map[int]int64)}
}

// Advance records that partition has been consumed up to nextOffset.
func (t *OffsetTracker) Advance(partition int, nextOffset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.offsets[partition] = nextOffset
}

// Snapshot returns a copy of the current offsets (Phase 4 checkpoint input).
func (t *OffsetTracker) Snapshot() map[int]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[int]int64, len(t.offsets))
	for k, v := range t.offsets {
		out[k] = v
	}
	return out
}
