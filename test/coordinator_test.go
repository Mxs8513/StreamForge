package test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Mxs8513/StreamForge/internal/coordinator"
	"github.com/Mxs8513/StreamForge/internal/event"
)

// TestAssignmentPartitionsCleanly verifies the coordinator distributes Kafka
// partitions and key-buckets so that every partition and every bucket has
// exactly one owner, and the routing table agrees with per-worker ownership.
// This is the invariant the Phase 3 exit test depends on: each key aggregates
// on exactly one worker.
func TestAssignmentPartitionsCleanly(t *testing.T) {
	const (
		workers    = 3
		partitions = 6
		buckets    = 64
	)
	c := coordinator.New(workers, partitions, buckets, 4*time.Second)

	// Not ready until all workers register.
	a := c.Register("w0", "127.0.0.1:7100")
	require.False(t, a.Ready)
	a = c.Register("w1", "127.0.0.1:7101")
	require.False(t, a.Ready)
	a = c.Register("w2", "127.0.0.1:7102")
	require.True(t, a.Ready)

	addrs := map[string]string{"w0": "127.0.0.1:7100", "w1": "127.0.0.1:7101", "w2": "127.0.0.1:7102"}

	partOwner := map[int32]int{}
	bucketOwnerByWorker := map[int32]string{}
	var routingTable []string

	for i, id := range []string{"w0", "w1", "w2"} {
		asg := c.GetAssignment(id)
		require.True(t, asg.Ready)
		require.Len(t, asg.BucketOwnerAddr, buckets)
		if routingTable == nil {
			routingTable = asg.BucketOwnerAddr
		} else {
			require.Equal(t, routingTable, asg.BucketOwnerAddr, "every worker sees the same routing table")
		}
		for _, p := range asg.KafkaPartitions {
			_, dup := partOwner[p]
			require.Falsef(t, dup, "partition %d assigned twice", p)
			partOwner[p] = i
		}
		for _, b := range asg.KeyBuckets {
			_, dup := bucketOwnerByWorker[b]
			require.Falsef(t, dup, "bucket %d owned twice", b)
			bucketOwnerByWorker[b] = id
			// The routing table must point this bucket at its owner's address.
			require.Equal(t, addrs[id], asg.BucketOwnerAddr[b])
		}
	}

	require.Len(t, partOwner, partitions, "every partition owned exactly once")
	require.Len(t, bucketOwnerByWorker, buckets, "every bucket owned exactly once")

	// Every possible key routes to a bucket that has exactly one owner.
	for k := 0; k < 1000; k++ {
		b := event.Bucket(keyName(k), buckets)
		require.NotEmpty(t, routingTable[b])
	}
}

func keyName(n int) string { return "user_" + itoa(int64(n)) }
