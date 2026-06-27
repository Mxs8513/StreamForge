package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/Mxs8513/StreamForge/internal/checkpoint"
	"github.com/Mxs8513/StreamForge/internal/event"
	"github.com/Mxs8513/StreamForge/internal/metrics"
	pb "github.com/Mxs8513/StreamForge/internal/proto"
	"github.com/Mxs8513/StreamForge/internal/storage"
)

// Config configures a worker runtime.
type Config struct {
	WorkerID          string
	Brokers           []string
	Topic             string
	ShuffleAddr       string // host:port this worker's Worker gRPC server listens on
	CoordinatorAddr   string // empty => standalone (own all partitions + buckets)
	KafkaPartitions   int
	NumBuckets        int
	WindowSizeMS      int64
	FlushInterval     time.Duration
	HeartbeatInterval time.Duration
	StartOffset       StartOffset
	Restore           bool // restore from the latest checkpoint on first start
	StateDir          string
	InboxSize         int
}

// Runtime runs a worker across "generations". A generation is one run of the
// dataflow under a fixed assignment. On a membership change (epoch bump from the
// coordinator) the current generation is torn down and a new one is built that
// resets to the last completed checkpoint with the new assignment — the
// Flink-style "restart the job from the last checkpoint" recovery model.
type Runtime struct {
	cfg     Config
	sink    *Sink
	store   *storage.Client
	cpStore *checkpoint.Store

	coordConn *grpc.ClientConn
	coord     pb.CoordinatorClient

	staged  bool // exactly-once staged output (coordinated mode) vs direct (standalone)
	resetCh chan struct{}

	// Active generation primitives, read by the gRPC handlers. Guarded by mu;
	// ready is false during a generation transition so checkpoints/shuffles that
	// arrive mid-transition are rejected rather than racing a half-built state.
	mu           sync.Mutex
	ready        bool
	runningEpoch int64
	latestAsg    resolvedAssignment
	inbox        chan *event.Event
	barrier      *Barrier
	commitCh     chan commitReq
	tracker      *OffsetTracker
}

type commitReq struct {
	checkpointID int64
	resp         chan commitResult
}
type commitResult struct {
	snapshot  []byte
	offsets   map[int]int64
	stagedKey string // staged output object key for this checkpoint ("" if none)
	err       error
}

// NewRuntime builds a runtime over the given sink and object store. The runtime
// opens and owns a fresh keyed-state store per generation.
func NewRuntime(cfg Config, sink *Sink, store *storage.Client) *Runtime {
	if cfg.InboxSize == 0 {
		cfg.InboxSize = 4096
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = time.Second
	}
	return &Runtime{
		cfg:     cfg,
		sink:    sink,
		store:   store,
		cpStore: checkpoint.NewStore(store),
		// Coordinated mode uses exactly-once staged output committed via
		// checkpoints; standalone mode publishes directly (at-least-once).
		staged:  cfg.CoordinatorAddr != "",
		resetCh: make(chan struct{}, 1),
	}
}

func (r *Runtime) active() (chan *event.Event, *Barrier, chan commitReq, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inbox, r.barrier, r.commitCh, r.ready
}

// ---- gRPC Worker service ----

type workerServer struct {
	pb.UnimplementedWorkerServer
	rt *Runtime
}

func (s *workerServer) Shuffle(ctx context.Context, req *pb.ShuffleRequest) (*pb.Empty, error) {
	inbox, _, _, ok := s.rt.active()
	if !ok {
		return nil, fmt.Errorf("worker transitioning")
	}
	select {
	case inbox <- fromPBEvent(req.Event):
		return &pb.Empty{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *workerServer) PrepareCheckpoint(_ context.Context, b *pb.CheckpointBarrier) (*pb.Empty, error) {
	_, barrier, _, ok := s.rt.active()
	if !ok {
		return nil, fmt.Errorf("worker transitioning")
	}
	barrier.Pause()
	log.Printf("worker %s: checkpoint %d prepared (sources paused)", s.rt.cfg.WorkerID, b.CheckpointId)
	return &pb.Empty{}, nil
}

func (s *workerServer) CommitCheckpoint(ctx context.Context, b *pb.CheckpointBarrier) (*pb.CheckpointAck, error) {
	_, barrier, commitCh, ok := s.rt.active()
	if !ok {
		return nil, fmt.Errorf("worker transitioning")
	}
	resp := make(chan commitResult, 1)
	select {
	case commitCh <- commitReq{checkpointID: b.CheckpointId, resp: resp}:
	case <-ctx.Done():
		barrier.Resume()
		return nil, ctx.Err()
	}
	res := <-resp
	barrier.Resume()
	if res.err != nil {
		return nil, res.err
	}
	snapKey := checkpoint.SnapshotKey(b.CheckpointId, s.rt.cfg.WorkerID)
	if err := s.rt.store.Put(ctx, snapKey, res.snapshot); err != nil {
		return nil, fmt.Errorf("upload snapshot: %w", err)
	}
	offsets := make(map[int32]int64, len(res.offsets))
	for p, o := range res.offsets {
		offsets[int32(p)] = o
	}
	var staged []string
	if res.stagedKey != "" {
		staged = []string{res.stagedKey}
	}
	log.Printf("worker %s: checkpoint %d committed (%d bytes, offsets=%v, staged=%q)",
		s.rt.cfg.WorkerID, b.CheckpointId, len(res.snapshot), offsets, res.stagedKey)
	return &pb.CheckpointAck{
		WorkerId:         s.rt.cfg.WorkerID,
		CheckpointId:     b.CheckpointId,
		KafkaOffsets:     offsets,
		StateSnapshotUri: snapKey,
		StagedOutputs:    staged,
	}, nil
}

// ---- lifecycle ----

// Run starts the worker and blocks until ctx is cancelled.
func (r *Runtime) Run(ctx context.Context) error {
	lis, err := net.Listen("tcp", r.cfg.ShuffleAddr)
	if err != nil {
		return err
	}
	gs := grpc.NewServer()
	pb.RegisterWorkerServer(gs, &workerServer{rt: r})
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()
	log.Printf("worker %s: shuffle/checkpoint server on %s", r.cfg.WorkerID, r.cfg.ShuffleAddr)

	if r.cfg.CoordinatorAddr == "" {
		// Standalone: one generation, own everything, no heartbeats.
		asg := r.standaloneAssignment()
		return r.runGeneration(ctx, ctx, asg, r.cfg.Restore)
	}

	conn, err := grpc.NewClient(r.cfg.CoordinatorAddr, grpc.WithTransportCredentials(insecureCreds()))
	if err != nil {
		return fmt.Errorf("dial coordinator: %w", err)
	}
	defer conn.Close()
	r.coordConn = conn
	r.coord = pb.NewCoordinatorClient(conn)

	asg, err := r.registerAndWait(ctx)
	if err != nil {
		return err
	}
	go r.heartbeatLoop(ctx)

	recovering := r.cfg.Restore
	for {
		genCtx, genCancel := context.WithCancel(ctx)
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			select {
			case <-r.resetCh:
				genCancel() // membership changed: end this generation
			case <-genCtx.Done():
			}
		}()

		err := r.runGeneration(ctx, genCtx, asg, recovering)
		genCancel()
		<-watchDone
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil // shutdown
		}
		// Reset to the new assignment, restoring from the last checkpoint.
		r.mu.Lock()
		asg = r.latestAsg
		r.mu.Unlock()
		recovering = true
		log.Printf("worker %s: resetting to epoch %d (partitions=%v, %d buckets)",
			r.cfg.WorkerID, asg.epoch, asg.partitions, len(asg.ownedBuckets))
	}
}

// runGeneration builds and runs the dataflow for one assignment until genCtx is
// cancelled. On shutdown (outerCtx cancelled) it drains to output; on a reset it
// discards in-flight work (the next generation rebuilds from the checkpoint).
func (r *Runtime) runGeneration(outerCtx, genCtx context.Context, asg resolvedAssignment, recovering bool) error {
	stateDir := r.cfg.StateDir
	if stateDir != "" {
		stateDir = fmt.Sprintf("%s/gen-%d", stateDir, asg.epoch)
	}
	state, err := OpenState(stateDir)
	if err != nil {
		return err
	}
	defer state.Close()
	agg := NewAggregator(state, r.cfg.WindowSizeMS)

	startOffsets := map[int]int64{}
	if recovering {
		if so, rerr := r.restoreFromCheckpoint(outerCtx, state, asg); rerr != nil {
			return rerr
		} else {
			startOffsets = so
		}
	}

	inbox := make(chan *event.Event, r.cfg.InboxSize)
	barrier := NewBarrier()
	commitCh := make(chan commitReq)
	tracker := NewOffsetTracker()
	for p, o := range startOffsets {
		tracker.Advance(p, o)
	}
	router := NewRouter(r.cfg.ShuffleAddr, r.cfg.NumBuckets, asg.ownerAddr, inbox)
	defer router.Close()

	r.mu.Lock()
	r.inbox, r.barrier, r.commitCh, r.tracker = inbox, barrier, commitCh, tracker
	r.runningEpoch, r.ready = asg.epoch, true
	r.mu.Unlock()

	stopAgg := make(chan struct{})
	aggDone := make(chan struct{})
	go r.runAggregator(outerCtx, agg, inbox, commitCh, tracker, stopAgg, aggDone)

	var srcWG sync.WaitGroup
	for _, p := range asg.partitions {
		startAt := int64(r.cfg.StartOffset)
		if o, ok := startOffsets[int(p)]; ok {
			startAt = o
		}
		srcWG.Add(1)
		go func(part int, off int64) {
			defer srcWG.Done()
			r.runSource(genCtx, part, off, router, barrier, tracker)
		}(int(p), startAt)
	}

	<-genCtx.Done()
	r.mu.Lock()
	r.ready = false
	r.mu.Unlock()
	barrier.Close()
	srcWG.Wait()   // stop all sources BEFORE the aggregator drains, so no event
	close(stopAgg) // this worker reads can outrace the drain and be lost
	<-aggDone
	return nil
}

// restoreFromCheckpoint rebuilds keyed state for the buckets this worker now
// owns by importing the relevant keys from every worker's snapshot in the latest
// completed checkpoint, and returns the per-partition offsets to resume from.
func (r *Runtime) restoreFromCheckpoint(ctx context.Context, state *State, asg resolvedAssignment) (map[int]int64, error) {
	meta, ok, err := r.cpStore.LatestCompleted(ctx)
	if err != nil {
		return nil, fmt.Errorf("read latest checkpoint: %w", err)
	}
	if !ok {
		log.Printf("worker %s: no completed checkpoint to restore; starting fresh", r.cfg.WorkerID)
		return nil, nil
	}

	owned := make(map[int]bool, len(asg.ownedBuckets))
	for _, b := range asg.ownedBuckets {
		owned[int(b)] = true
	}
	keep := func(sk []byte) bool {
		return owned[event.Bucket(KeyOf(sk), r.cfg.NumBuckets)]
	}

	imported := 0
	for wid, uri := range meta.StateSnapshot {
		if uri == "" {
			continue
		}
		data, gerr := r.store.Get(ctx, uri)
		if gerr != nil {
			return nil, fmt.Errorf("get snapshot %s (%s): %w", uri, wid, gerr)
		}
		if rerr := state.RestoreFiltered(data, keep); rerr != nil {
			return nil, fmt.Errorf("restore from %s: %w", uri, rerr)
		}
		imported++
	}

	starts := make(map[int]int64)
	for _, p := range asg.partitions {
		if o, ok := meta.KafkaOffsets[p]; ok {
			starts[int(p)] = o
		}
	}
	log.Printf("worker %s: restored owned buckets from checkpoint %d (%d snapshots scanned), resume offsets=%v",
		r.cfg.WorkerID, meta.CheckpointID, imported, starts)
	return starts, nil
}

// runAggregator owns all keyed-state access. It runs until stopAgg is closed,
// which the generation does only AFTER every source has stopped, so the drain
// cannot race a source still reading events.
func (r *Runtime) runAggregator(outerCtx context.Context, agg *Aggregator, inbox chan *event.Event, commitCh chan commitReq, tracker *OffsetTracker, stopAgg <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(r.cfg.FlushInterval)
	defer ticker.Stop()
	// pending holds output flushed since the last checkpoint commit. In staged
	// (exactly-once) mode it is written to staging at commit and becomes visible
	// only when that checkpoint completes; in standalone mode flush publishes
	// directly. Owned solely by this goroutine.
	var pending []event.OutputRecord
	var lastWM int64
	idleTicks := 0
	for {
		select {
		case e := <-inbox:
			// Event-time windowing: the window comes from the event's own
			// timestamp, so replay after recovery lands an event in the SAME
			// window — which, with (key,window) commit dedup, gives exactly-once
			// output. (Processing-time would reassign replayed events to later
			// windows, double-counting them.)
			if err := agg.Update(e, e.EventTime); err != nil {
				log.Printf("worker %s: update error: %v", r.cfg.WorkerID, err)
			}
			metrics.EventLatency.Observe(float64(time.Now().UnixMilli()-e.EventTime) / 1000)
		case <-ticker.C:
			// Close windows by event-time watermark (deterministic across replay)
			// rather than wall-clock. allowedLateness holds a window open until we
			// have seen events well past it, so events from a slower partition
			// (common while a survivor replays several partitions at once) still
			// land before their window closes. When the watermark stops advancing
			// for a few ticks the stream is drained/at end: flush the rest.
			wm := agg.MaxEventTime() - allowedLatenessMS
			r.flush(context.Background(), agg, wm, &pending)
			if wm == lastWM {
				if idleTicks++; idleTicks >= 3 {
					r.flushAll(context.Background(), agg, &pending)
					idleTicks = 0
				}
			} else {
				lastWM, idleTicks = wm, 0
			}
		case req := <-commitCh:
			r.handleCommit(agg, tracker, &pending, req)
		case <-stopAgg:
			if outerCtx.Err() != nil {
				r.drain(agg, inbox, &pending) // shutdown
			}
			return
		}
	}
}

// handleCommit drains the inbox into state, snapshots state+offsets, and (in
// staged mode) writes the pending output to staging for this checkpoint. Pending
// is cleared only after the staged write durably succeeds, so a failed stage
// keeps the data for retry/regeneration.
func (r *Runtime) handleCommit(agg *Aggregator, tracker *OffsetTracker, pending *[]event.OutputRecord, req commitReq) {
	for {
		select {
		case e := <-r.activeInbox(): // drain inbox fully into state
			if err := agg.Update(e, e.EventTime); err != nil {
				log.Printf("worker %s: commit update error: %v", r.cfg.WorkerID, err)
			}
		default:
			snap, err := agg.SnapshotState()
			if err != nil {
				req.resp <- commitResult{err: err}
				return
			}
			stagedKey := ""
			if r.staged && len(*pending) > 0 {
				key, werr := r.sink.WriteStaged(context.Background(), req.checkpointID, *pending)
				if werr != nil {
					req.resp <- commitResult{err: werr}
					return
				}
				metrics.RecordsEmitted.Add(float64(len(*pending)))
				stagedKey = key
				*pending = nil // clear only after durable stage
			}
			req.resp <- commitResult{snapshot: snap, offsets: tracker.Snapshot(), stagedKey: stagedKey}
			return
		}
	}
}

// activeInbox returns the current generation's inbox (the one handleCommit runs
// against). Reading it under the lock keeps it consistent with the gRPC handlers.
func (r *Runtime) activeInbox() chan *event.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inbox
}

func (r *Runtime) runSource(ctx context.Context, partition int, startAt int64, router *Router, barrier *Barrier, tracker *OffsetTracker) {
	reader := NewPartitionReader(r.cfg.Brokers, r.cfg.Topic, partition, startAt)
	defer reader.Close()
	for {
		if !barrier.Enter() {
			return
		}
		readCtx, cancel := context.WithTimeout(ctx, readTimeout)
		e, next, err := reader.Read(readCtx)
		cancel()
		if err != nil {
			barrier.Leave()
			if ctx.Err() != nil {
				return
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("worker %s: partition %d read error: %v", r.cfg.WorkerID, partition, err)
			}
			continue
		}
		// Route with retry: a peer may briefly be "transitioning" (rebuilding a
		// generation). The offset is advanced ONLY after a successful route, so a
		// failed/dropped route never loses an event — on a reset it is reprocessed
		// from the checkpoint offset. If the generation is cancelled mid-retry,
		// abandon without advancing (the event will be replayed).
		if r.routeWithRetry(ctx, router, e) {
			tracker.Advance(partition, next)
			metrics.EventsConsumed.Inc()
		}
		barrier.Leave()
	}
}

// routeWithRetry routes e, retrying transient failures until success or ctx
// cancellation. Returns true only if the event was delivered to its owner.
func (r *Runtime) routeWithRetry(ctx context.Context, router *Router, e *event.Event) bool {
	for {
		if err := router.Route(ctx, e); err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// readTimeout bounds how long a source blocks on an empty partition, so a
// checkpoint Prepare (which waits for sources to quiesce) never stalls.
const readTimeout = 250 * time.Millisecond

// allowedLatenessMS holds an event-time window open this long past its end
// before it closes, absorbing cross-partition out-of-order (spec §8). With no
// late events it only delays when a window is committed, not its contents.
const allowedLatenessMS = 1000

func (r *Runtime) drain(agg *Aggregator, inbox chan *event.Event, pending *[]event.OutputRecord) {
	quiet := time.NewTimer(500 * time.Millisecond)
	defer quiet.Stop()
	for {
		select {
		case e := <-inbox:
			if !quiet.Stop() {
				<-quiet.C
			}
			quiet.Reset(500 * time.Millisecond)
			if err := agg.Update(e, e.EventTime); err != nil {
				log.Printf("worker %s: drain update error: %v", r.cfg.WorkerID, err)
			}
		case <-quiet.C:
			rows, err := agg.FlushAll()
			if err != nil {
				log.Printf("worker %s: drain flush error: %v", r.cfg.WorkerID, err)
				return
			}
			r.emit(context.Background(), rows, pending)
			return
		}
	}
}

func (r *Runtime) flush(ctx context.Context, agg *Aggregator, watermark int64, pending *[]event.OutputRecord) {
	rows, err := agg.FlushClosed(watermark)
	if err != nil {
		log.Printf("worker %s: flush error: %v", r.cfg.WorkerID, err)
		return
	}
	r.emit(ctx, rows, pending)
}

// flushAll emits every remaining window (end-of-stream / idle watermark).
func (r *Runtime) flushAll(ctx context.Context, agg *Aggregator, pending *[]event.OutputRecord) {
	rows, err := agg.FlushAll()
	if err != nil {
		log.Printf("worker %s: flushAll error: %v", r.cfg.WorkerID, err)
		return
	}
	r.emit(ctx, rows, pending)
}

// emit routes flushed rows: in staged (exactly-once) mode it buffers them in
// pending until the next checkpoint stages and commits them; in standalone mode
// it publishes directly to the live output prefix (at-least-once).
func (r *Runtime) emit(ctx context.Context, rows []event.OutputRecord, pending *[]event.OutputRecord) {
	if len(rows) == 0 {
		return
	}
	if r.staged {
		*pending = append(*pending, rows...)
		return
	}
	key, err := r.sink.Write(ctx, rows)
	if err != nil {
		log.Printf("worker %s: sink error: %v", r.cfg.WorkerID, err)
		return
	}
	metrics.RecordsEmitted.Add(float64(len(rows)))
	log.Printf("worker %s: flushed %d window rows -> %s", r.cfg.WorkerID, len(rows), key)
}
