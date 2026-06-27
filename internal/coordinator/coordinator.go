// Package coordinator implements the single coordinator process (spec §6.1):
// membership, partition + key-bucket assignment, checkpoint orchestration
// (Phase 4), and failure detection + reassignment (Phase 5).
//
// Membership changes are versioned by an epoch. When a worker dies (heartbeat
// timeout) or a new one joins, the epoch bumps and the assignment is recomputed
// over the live members; workers notice the higher epoch and reset to the last
// completed checkpoint with the new assignment.
package coordinator

import (
	"log"
	"sort"
	"sync"
	"time"
)

// member is a registered worker.
type member struct {
	id       string
	address  string // shuffle (Worker gRPC) address
	joinSeq  int    // registration order; gives a stable assignment ordering
	lastSeen time.Time
}

// Coordinator holds cluster membership and computes assignments.
type Coordinator struct {
	mu sync.Mutex

	expectedWorkers int
	kafkaPartitions int
	numBuckets      int
	failureTimeout  time.Duration

	members map[string]*member
	seq     int
	formed  bool  // true once expectedWorkers first registered; stays operational after
	epoch   int64 // bumps on every membership change

	lastCompletedCheckpoint int64
}

// New builds a coordinator. The cluster becomes operational once expectedWorkers
// have registered; after that it keeps running over whatever members are live,
// reassigning when membership changes. A member is declared dead when it has not
// been seen for failureTimeout.
func New(expectedWorkers, kafkaPartitions, numBuckets int, failureTimeout time.Duration) *Coordinator {
	return &Coordinator{
		expectedWorkers: expectedWorkers,
		kafkaPartitions: kafkaPartitions,
		numBuckets:      numBuckets,
		failureTimeout:  failureTimeout,
		members:         make(map[string]*member),
	}
}

// Register records (or refreshes) a worker and returns the current assignment.
// A brand-new member after the cluster has formed bumps the epoch (rebalance).
func (c *Coordinator) Register(id, address string) Assignment {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.members[id]
	if !ok {
		m = &member{id: id, joinSeq: c.seq}
		c.seq++
		c.members[id] = m
		if c.formed {
			c.epoch++
			log.Printf("coordinator: %s joined -> epoch %d (%d live)", id, c.epoch, len(c.members))
		}
	}
	m.address = address
	m.lastSeen = time.Now()
	if !c.formed && len(c.members) >= c.expectedWorkers {
		c.formed = true
		c.epoch++
		log.Printf("coordinator: cluster formed with %d workers -> epoch %d", len(c.members), c.epoch)
	}
	return c.assignmentLocked(id)
}

// Heartbeat refreshes liveness and returns the current assignment.
func (c *Coordinator) Heartbeat(id string) Assignment {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m, ok := c.members[id]; ok {
		m.lastSeen = time.Now()
	}
	return c.assignmentLocked(id)
}

// GetAssignment returns the current assignment for a worker.
func (c *Coordinator) GetAssignment(id string) Assignment {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.assignmentLocked(id)
}

// DetectFailures runs until ctx-style stop: every interval it declares members
// unseen for failureTimeout dead, removes them, and bumps the epoch so survivors
// rebalance. Stop by closing the returned stop channel.
func (c *Coordinator) DetectFailures(interval time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			c.reapDead()
		}
	}
}

func (c *Coordinator) reapDead() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.formed {
		return
	}
	now := time.Now()
	for id, m := range c.members {
		if now.Sub(m.lastSeen) > c.failureTimeout {
			delete(c.members, id)
			c.epoch++
			log.Printf("coordinator: %s declared DEAD (unseen %v) -> epoch %d (%d live)",
				id, now.Sub(m.lastSeen).Truncate(time.Millisecond), c.epoch, len(c.members))
		}
	}
}

// ReadyAddrs returns worker id -> shuffle address once the cluster is formed and
// at least one member is live. The checkpoint orchestrator dials these.
func (c *Coordinator) ReadyAddrs() (map[string]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.formed || len(c.members) == 0 {
		return nil, false
	}
	out := make(map[string]string, len(c.members))
	for id, m := range c.members {
		out[id] = m.address
	}
	return out, true
}

// MarkCompleted records the last successfully committed checkpoint id.
func (c *Coordinator) MarkCompleted(id int64) {
	c.mu.Lock()
	c.lastCompletedCheckpoint = id
	c.mu.Unlock()
}

// Assignment is the plan handed to one worker plus the global routing table.
type Assignment struct {
	Ready                   bool
	KafkaPartitions         []int32
	KeyBuckets              []int32
	NumBuckets              int32
	BucketOwnerAddr         []string // index = bucket id -> owner shuffle address
	LastCompletedCheckpoint int64
	Epoch                   int64
}

// ordered returns members sorted by registration order (stable assignment).
func (c *Coordinator) ordered() []*member {
	ms := make([]*member, 0, len(c.members))
	for _, m := range c.members {
		ms = append(ms, m)
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].joinSeq < ms[j].joinSeq })
	return ms
}

// assignmentLocked computes the assignment for worker id over the live members.
// It is "ready" only once the cluster has formed, so workers block during
// initial ramp-up but keep operating (with fewer workers) after a death.
func (c *Coordinator) assignmentLocked(id string) Assignment {
	if !c.formed {
		return Assignment{Ready: false, NumBuckets: int32(c.numBuckets), Epoch: c.epoch}
	}
	ms := c.ordered()
	n := len(ms)
	if n == 0 {
		return Assignment{Ready: false, NumBuckets: int32(c.numBuckets), Epoch: c.epoch}
	}

	indexOf := make(map[string]int, n)
	for i, m := range ms {
		indexOf[m.id] = i
	}

	bucketOwnerAddr := make([]string, c.numBuckets)
	for b := 0; b < c.numBuckets; b++ {
		bucketOwnerAddr[b] = ms[b%n].address
	}

	self, ok := indexOf[id]
	if !ok {
		// Caller is not a current member (e.g. just declared dead). Hand back a
		// not-ready assignment so it stops processing.
		return Assignment{Ready: false, NumBuckets: int32(c.numBuckets), Epoch: c.epoch}
	}

	var parts []int32
	for p := 0; p < c.kafkaPartitions; p++ {
		if p%n == self {
			parts = append(parts, int32(p))
		}
	}
	var buckets []int32
	for b := 0; b < c.numBuckets; b++ {
		if b%n == self {
			buckets = append(buckets, int32(b))
		}
	}

	return Assignment{
		Ready:                   true,
		KafkaPartitions:         parts,
		KeyBuckets:              buckets,
		NumBuckets:              int32(c.numBuckets),
		BucketOwnerAddr:         bucketOwnerAddr,
		LastCompletedCheckpoint: c.lastCompletedCheckpoint,
		Epoch:                   c.epoch,
	}
}
