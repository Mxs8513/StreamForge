// Package checkpoint defines durable checkpoint metadata and its store.
//
// The commit point is a single atomic object PUT of the metadata with status
// COMPLETED (spec §7). Metadata is written exactly once, and only after every
// worker has acked, so partial/aborted checkpoints are never readable as
// COMPLETED: an aborted round simply leaves no metadata object behind, and the
// system keeps running on the previous completed checkpoint.
package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Mxs8513/StreamForge/internal/storage"
)

// Status of a checkpoint. Only COMPLETED is ever persisted.
type Status string

const (
	StatusCompleted Status = "COMPLETED"
)

// Metadata is the durable record describing one completed checkpoint.
type Metadata struct {
	CheckpointID  int64             `json:"checkpoint_id"`
	CreatedAt     int64             `json:"created_at"`
	Status        Status            `json:"status"`
	KafkaOffsets  map[int32]int64   `json:"kafka_offsets"`   // partition -> next offset (global union)
	StateSnapshot map[string]string `json:"state_snapshots"` // worker id -> snapshot object key
	StagedOutputs []string          `json:"staged_outputs"`  // empty until Phase 6
}

const prefix = "checkpoints/"

func metaKey(id int64) string { return fmt.Sprintf("%s%d/metadata.json", prefix, id) }

// SnapshotKey is the object key for a worker's state snapshot in checkpoint id.
func SnapshotKey(id int64, workerID string) string {
	return fmt.Sprintf("%s%d/%s.snap", prefix, id, workerID)
}

// Store reads and writes checkpoint metadata in object storage.
type Store struct {
	s3 *storage.Client
}

// NewStore builds a checkpoint metadata store over object storage.
func NewStore(s3 *storage.Client) *Store { return &Store{s3: s3} }

// Commit writes the COMPLETED metadata as a single atomic PUT. This is the
// commit point: the checkpoint does not exist until this returns successfully.
func (s *Store) Commit(ctx context.Context, m Metadata) error {
	m.Status = StatusCompleted
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return s.s3.Put(ctx, metaKey(m.CheckpointID), data)
}

// LatestCompleted returns the highest-id committed checkpoint, or (nil,false).
func (s *Store) LatestCompleted(ctx context.Context) (*Metadata, bool, error) {
	ids, err := s.completedIDs(ctx)
	if err != nil {
		return nil, false, err
	}
	if len(ids) == 0 {
		return nil, false, nil
	}
	latest := ids[len(ids)-1]
	data, err := s.s3.Get(ctx, metaKey(latest))
	if err != nil {
		return nil, false, err
	}
	var m Metadata
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, false, err
	}
	return &m, true, nil
}

// AllCompleted returns the metadata of every committed checkpoint, ascending by
// id. The union of their StagedOutputs is the committed (exactly-once) output.
func (s *Store) AllCompleted(ctx context.Context) ([]Metadata, error) {
	ids, err := s.completedIDs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Metadata, 0, len(ids))
	for _, id := range ids {
		data, err := s.s3.Get(ctx, metaKey(id))
		if err != nil {
			return nil, err
		}
		var m Metadata
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// completedIDs lists ids that have a metadata.json object, sorted ascending.
func (s *Store) completedIDs(ctx context.Context) ([]int64, error) {
	keys, err := s.s3.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for _, k := range keys {
		if !strings.HasSuffix(k, "/metadata.json") {
			continue
		}
		mid := strings.TrimSuffix(strings.TrimPrefix(k, prefix), "/metadata.json")
		if n, err := strconv.ParseInt(mid, 10, 64); err == nil {
			ids = append(ids, n)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}
