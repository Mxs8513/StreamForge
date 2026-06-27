package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/Mxs8513/StreamForge/internal/proto"
)

func insecureCreds() credentials.TransportCredentials { return insecure.NewCredentials() }

// resolvedAssignment is the worker's slice of work plus the global routing table
// and the epoch it belongs to.
type resolvedAssignment struct {
	epoch        int64
	partitions   []int32
	ownedBuckets []int32
	ownerAddr    []string // bucket -> owning worker shuffle address
}

func fromPBAssignment(a *pb.Assignment) resolvedAssignment {
	return resolvedAssignment{
		epoch:        a.Epoch,
		partitions:   a.KafkaPartitions,
		ownedBuckets: a.KeyBuckets,
		ownerAddr:    a.BucketOwnerAddr,
	}
}

// standaloneAssignment owns every partition and bucket (no coordinator).
func (r *Runtime) standaloneAssignment() resolvedAssignment {
	owner := make([]string, r.cfg.NumBuckets)
	buckets := make([]int32, r.cfg.NumBuckets)
	for b := 0; b < r.cfg.NumBuckets; b++ {
		owner[b] = r.cfg.ShuffleAddr
		buckets[b] = int32(b)
	}
	parts := make([]int32, r.cfg.KafkaPartitions)
	for p := 0; p < r.cfg.KafkaPartitions; p++ {
		parts[p] = int32(p)
	}
	return resolvedAssignment{partitions: parts, ownedBuckets: buckets, ownerAddr: owner}
}

// registerAndWait registers with the coordinator and blocks until it returns a
// ready (fully-formed) assignment.
func (r *Runtime) registerAndWait(ctx context.Context) (resolvedAssignment, error) {
	if _, err := r.coord.Register(ctx, &pb.RegisterRequest{
		WorkerId: r.cfg.WorkerID,
		Address:  r.cfg.ShuffleAddr,
	}); err != nil {
		return resolvedAssignment{}, fmt.Errorf("register: %w", err)
	}
	for {
		a, err := r.coord.GetAssignment(ctx, &pb.GetAssignmentRequest{WorkerId: r.cfg.WorkerID})
		if err != nil {
			return resolvedAssignment{}, fmt.Errorf("get assignment: %w", err)
		}
		if a.Ready {
			asg := fromPBAssignment(a)
			log.Printf("worker %s: joined cluster at epoch %d (partitions=%v, %d buckets)",
				r.cfg.WorkerID, asg.epoch, asg.partitions, len(asg.ownedBuckets))
			return asg, nil
		}
		select {
		case <-ctx.Done():
			return resolvedAssignment{}, ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// heartbeatLoop sends periodic heartbeats. When the coordinator reports a higher
// epoch (a membership change), it stores the new assignment and signals the
// generation loop to reset.
func (r *Runtime) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			resp, err := r.coord.Heartbeat(ctx, &pb.HeartbeatRequest{WorkerId: r.cfg.WorkerID})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("worker %s: heartbeat error: %v", r.cfg.WorkerID, err)
				continue
			}
			a := resp.Assignment
			if a == nil || !a.Ready {
				continue
			}
			r.mu.Lock()
			changed := a.Epoch > r.runningEpoch
			if changed {
				r.latestAsg = fromPBAssignment(a)
			}
			r.mu.Unlock()
			if changed {
				log.Printf("worker %s: epoch advanced to %d -> reset pending", r.cfg.WorkerID, a.Epoch)
				select {
				case r.resetCh <- struct{}{}:
				default:
				}
			}
		}
	}
}
