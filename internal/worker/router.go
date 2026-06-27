package worker

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/Mxs8513/StreamForge/internal/event"
	"github.com/Mxs8513/StreamForge/internal/metrics"
	pb "github.com/Mxs8513/StreamForge/internal/proto"
)

// Router implements keyBy routing: it hashes an event's key to a bucket and
// either delivers it to the local aggregator (this worker owns the bucket) or
// shuffles it over gRPC to the worker that does. Because every key maps to
// exactly one owning worker, a key's state lives on exactly one worker — which
// is what makes the distributed result identical to the single-worker result.
type Router struct {
	self       string // this worker's shuffle address
	numBuckets int
	ownerAddr  []string            // bucket -> owning worker's shuffle address
	inbox      chan<- *event.Event // local delivery to the aggregator goroutine

	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]pb.WorkerClient
}

// NewRouter builds a router from an assignment's routing table.
func NewRouter(self string, numBuckets int, ownerAddr []string, inbox chan<- *event.Event) *Router {
	return &Router{
		self:       self,
		numBuckets: numBuckets,
		ownerAddr:  ownerAddr,
		inbox:      inbox,
		conns:      make(map[string]*grpc.ClientConn),
		clients:    make(map[string]pb.WorkerClient),
	}
}

// Route sends an event to its owning worker: locally via the inbox, or remotely
// via the Shuffle RPC. The remote call is synchronous, giving natural
// backpressure and ensuring the event is enqueued at the owner before Route
// returns.
func (r *Router) Route(ctx context.Context, e *event.Event) error {
	b := event.Bucket(e.Key, r.numBuckets)
	addr := r.ownerAddr[b]
	if addr == r.self {
		select {
		case r.inbox <- e:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	client, err := r.clientFor(addr)
	if err != nil {
		return err
	}
	pe := toPBEvent(e)
	_, err = client.Shuffle(ctx, &pb.ShuffleRequest{Event: pe})
	if err == nil {
		metrics.ShuffleBytes.Add(float64(proto.Size(pe)))
	}
	return err
}

func (r *Router) clientFor(addr string) (pb.WorkerClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	c := pb.NewWorkerClient(conn)
	r.conns[addr] = conn
	r.clients[addr] = c
	return c, nil
}

// Close tears down all shuffle client connections.
func (r *Router) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.conns {
		_ = c.Close()
	}
}

func toPBEvent(e *event.Event) *pb.Event {
	return &pb.Event{
		EventId:    e.EventID,
		Key:        e.Key,
		EventType:  e.EventType,
		Amount:     e.Amount,
		EventTime:  e.EventTime,
		IngestTime: e.IngestTime,
	}
}

func fromPBEvent(p *pb.Event) *event.Event {
	return &event.Event{
		EventID:    p.EventId,
		Key:        p.Key,
		EventType:  p.EventType,
		Amount:     p.Amount,
		EventTime:  p.EventTime,
		IngestTime: p.IngestTime,
	}
}
