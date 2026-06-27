package worker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Mxs8513/StreamForge/internal/event"
	"github.com/Mxs8513/StreamForge/internal/storage"
)

// Sink writes emitted output records as Parquet objects to S3/MinIO.
//
// Two paths exist (spec §9): Write publishes directly to the live prefix
// (standalone mode, at-least-once), while WriteStaged writes under
// <prefix>/staging/<checkpoint_id>/ for the exactly-once path — those objects
// become "committed" only when the checkpoint completes (the coordinator records
// them in the checkpoint metadata).
type Sink struct {
	store    *storage.Client
	workerID string
	prefix   string
	seq      atomic.Int64
}

// NewSink builds a sink that writes Parquet under prefix/<workerID>/. The prefix
// (e.g. "output" or "output/run-baseline") namespaces a run so separate runs do
// not mix in object storage.
func NewSink(store *storage.Client, workerID, prefix string) *Sink {
	if prefix == "" {
		prefix = "output"
	}
	return &Sink{store: store, workerID: workerID, prefix: prefix}
}

// Write encodes records to Parquet and uploads one object. No-op on empty input.
func (s *Sink) Write(ctx context.Context, rows []event.OutputRecord) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	data, err := storage.EncodeParquet(rows)
	if err != nil {
		return "", err
	}
	key := fmt.Sprintf("%s/%s/part-%d-%d.parquet",
		s.prefix, s.workerID, time.Now().UnixMilli(), s.seq.Add(1))
	if err := s.store.Put(ctx, key, data); err != nil {
		return "", err
	}
	return key, nil
}

// WriteStaged writes records to the staging area for a checkpoint and returns
// the object key, which the worker reports in its checkpoint ack so the
// coordinator can record it as committed once the checkpoint completes.
func (s *Sink) WriteStaged(ctx context.Context, checkpointID int64, rows []event.OutputRecord) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	data, err := storage.EncodeParquet(rows)
	if err != nil {
		return "", err
	}
	key := fmt.Sprintf("%s/staging/%d/%s-%d.parquet",
		s.prefix, checkpointID, s.workerID, s.seq.Add(1))
	if err := s.store.Put(ctx, key, data); err != nil {
		return "", err
	}
	return key, nil
}
