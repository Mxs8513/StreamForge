package coordinator

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Mxs8513/StreamForge/internal/checkpoint"
	"github.com/Mxs8513/StreamForge/internal/metrics"
	pb "github.com/Mxs8513/StreamForge/internal/proto"
)

// Orchestrator drives periodic aligned checkpoints across all workers.
//
// All-or-nothing commit: a checkpoint becomes COMPLETED (a single atomic
// metadata PUT) only after EVERY worker has prepared and acked its commit. If
// any step fails, the round is aborted — prepared workers are resumed and no
// metadata is written, so the system keeps running on the previous checkpoint.
type Orchestrator struct {
	c       *Coordinator
	store   *checkpoint.Store
	timeout time.Duration

	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]pb.WorkerClient
	nextID  int64
}

// NewOrchestrator builds a checkpoint orchestrator.
func NewOrchestrator(c *Coordinator, store *checkpoint.Store, timeout time.Duration) *Orchestrator {
	return &Orchestrator{
		c:       c,
		store:   store,
		timeout: timeout,
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]pb.WorkerClient),
		nextID:  1,
	}
}

// Run triggers a checkpoint every interval until ctx is cancelled.
func (o *Orchestrator) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer o.closeConns()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := o.runOnce(ctx); err != nil {
				log.Printf("coordinator: checkpoint aborted: %v", err)
			}
		}
	}
}

func (o *Orchestrator) runOnce(parent context.Context) error {
	addrs, ready := o.c.ReadyAddrs()
	if !ready {
		return nil // cluster not formed yet
	}
	id := o.nextID
	o.nextID++
	start := time.Now()

	ids := make([]string, 0, len(addrs))
	for wid := range addrs {
		ids = append(ids, wid)
	}

	barrier := &pb.CheckpointBarrier{CheckpointId: id}

	// Phase 1: Prepare every worker (pause sources, drain outbound). Track which
	// workers paused so we can resume them if we have to abort.
	prepared := make([]string, 0, len(ids))
	abort := func(reason string) error {
		// Resume prepared workers by committing them (which resumes), but do not
		// write metadata: an aborted round leaves no COMPLETED record.
		for _, wid := range prepared {
			ctx, cancel := context.WithTimeout(parent, o.timeout)
			cl, err := o.clientFor(addrs[wid])
			if err == nil {
				_, _ = cl.CommitCheckpoint(ctx, barrier)
			}
			cancel()
		}
		return &abortError{reason}
	}

	for _, wid := range ids {
		cl, err := o.clientFor(addrs[wid])
		if err != nil {
			return abort("dial " + wid + ": " + err.Error())
		}
		ctx, cancel := context.WithTimeout(parent, o.timeout)
		_, err = cl.PrepareCheckpoint(ctx, barrier)
		cancel()
		if err != nil {
			return abort("prepare " + wid + ": " + err.Error())
		}
		prepared = append(prepared, wid)
	}

	// Phase 2: Commit every worker, collecting state snapshots, offsets, and the
	// staged output files (which become committed only via the metadata PUT).
	meta := checkpoint.Metadata{
		CheckpointID:  id,
		CreatedAt:     time.Now().UnixMilli(),
		KafkaOffsets:  map[int32]int64{},
		StateSnapshot: map[string]string{},
	}
	for _, wid := range ids {
		cl, _ := o.clientFor(addrs[wid])
		ctx, cancel := context.WithTimeout(parent, o.timeout)
		ack, err := cl.CommitCheckpoint(ctx, barrier)
		cancel()
		if err != nil {
			// Workers committed so far have already resumed; just skip metadata.
			// Their staged files stay uncommitted (no metadata references them)
			// and are regenerated after the membership-change reset.
			return &abortError{"commit " + wid + ": " + err.Error()}
		}
		meta.StateSnapshot[wid] = ack.StateSnapshotUri
		for p, off := range ack.KafkaOffsets {
			meta.KafkaOffsets[p] = off
		}
		meta.StagedOutputs = append(meta.StagedOutputs, ack.StagedOutputs...)
	}

	// Commit point: a single atomic metadata PUT. Until this returns, none of
	// the staged output is committed; after it, all of it is.
	if err := o.store.Commit(parent, meta); err != nil {
		return &abortError{"write metadata: " + err.Error()}
	}
	o.c.MarkCompleted(id)
	metrics.CheckpointDuration.Observe(time.Since(start).Seconds())
	metrics.LastCompletedCheckpoint.Set(float64(id))
	log.Printf("coordinator: checkpoint %d COMPLETED (workers=%d, partitions=%d, staged_files=%d, %dms)",
		id, len(ids), len(meta.KafkaOffsets), len(meta.StagedOutputs), time.Since(start).Milliseconds())
	return nil
}

func (o *Orchestrator) clientFor(addr string) (pb.WorkerClient, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if c, ok := o.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	o.conns[addr] = conn
	o.clients[addr] = pb.NewWorkerClient(conn)
	return o.clients[addr], nil
}

func (o *Orchestrator) closeConns() {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, c := range o.conns {
		_ = c.Close()
	}
}

type abortError struct{ reason string }

func (e *abortError) Error() string { return e.reason }
