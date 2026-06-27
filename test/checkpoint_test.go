package test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Mxs8513/StreamForge/internal/event"
	"github.com/Mxs8513/StreamForge/internal/worker"
)

// TestStateSnapshotRestore is the core of the Phase 4 exit test ("restarting a
// worker from a checkpoint reproduces its state"): build keyed state, snapshot
// it, restore the bytes into a fresh store, and assert the restored aggregates
// are identical. This is the BadgerDB backup/load round-trip the checkpointer
// relies on.
// TestRestoreFilteredByBucket covers the Phase 5 state-redistribution primitive:
// on recovery a worker imports, from a checkpoint snapshot, only the keys whose
// bucket it now owns. Keys outside the owned bucket set must not be restored.
func TestRestoreFilteredByBucket(t *testing.T) {
	const (
		windowMS   = 10_000
		numBuckets = 4
	)
	src, err := worker.OpenState(t.TempDir())
	require.NoError(t, err)
	agg := worker.NewAggregator(src, windowMS)

	keys := []string{"user_a", "user_b", "user_c", "user_d", "user_e", "user_f"}
	for _, k := range keys {
		e := event.Event{Key: k, EventType: "x", Amount: 1.0}
		require.NoError(t, agg.Update(&e, 1000))
	}
	snap, err := agg.SnapshotState()
	require.NoError(t, err)
	require.NoError(t, src.Close())

	// New owner only owns buckets 0 and 2.
	owned := map[int]bool{0: true, 2: true}
	dst, err := worker.OpenState(t.TempDir())
	require.NoError(t, err)
	defer dst.Close()
	require.NoError(t, dst.RestoreFiltered(snap, func(sk []byte) bool {
		return owned[event.Bucket(worker.KeyOf(sk), numBuckets)]
	}))

	restored := worker.NewAggregator(dst, windowMS)
	rows, err := restored.FlushAll()
	require.NoError(t, err)

	gotKeys := map[string]bool{}
	for _, r := range rows {
		gotKeys[r.Key] = true
	}
	for _, k := range keys {
		want := owned[event.Bucket(k, numBuckets)]
		require.Equalf(t, want, gotKeys[k], "key %s (bucket %d) restore presence", k, event.Bucket(k, numBuckets))
	}
	// At least one key kept and at least one dropped, or the test proves nothing.
	require.NotEmpty(t, gotKeys)
	require.Less(t, len(gotKeys), len(keys))
}

func TestStateSnapshotRestore(t *testing.T) {
	const windowMS = 10_000

	src, err := worker.OpenState(t.TempDir())
	require.NoError(t, err)
	agg := worker.NewAggregator(src, windowMS)

	inputs := []struct {
		e  event.Event
		ts int64
	}{
		{event.Event{Key: "user_a", EventType: "purchase", Amount: 1.0}, 1000},
		{event.Event{Key: "user_a", EventType: "view", Amount: 3.0}, 4000},
		{event.Event{Key: "user_b", EventType: "click", Amount: 5.0}, 2000},
		{event.Event{Key: "user_a", EventType: "purchase", Amount: 10.0}, 12000}, // window 1
	}
	for _, in := range inputs {
		in := in
		require.NoError(t, agg.Update(&in.e, in.ts))
	}

	snap, err := agg.SnapshotState()
	require.NoError(t, err)
	require.NotEmpty(t, snap)
	require.NoError(t, src.Close())

	// Restore into a brand-new, empty store and read it back.
	dst, err := worker.OpenState(t.TempDir())
	require.NoError(t, err)
	defer dst.Close()
	require.NoError(t, dst.Restore(snap))

	restored := worker.NewAggregator(dst, windowMS)
	rows, err := restored.FlushAll()
	require.NoError(t, err)

	got := make(map[string]event.OutputRecord, len(rows))
	for _, r := range rows {
		got[key(r.Key, r.WindowStart)] = r
	}

	expected := []event.OutputRecord{
		{Key: "user_a", WindowStart: 0, WindowEnd: 10000, Count: 2, SumAmount: 4.0, MinAmount: 1.0, MaxAmount: 3.0, DistinctTypes: 2},
		{Key: "user_b", WindowStart: 0, WindowEnd: 10000, Count: 1, SumAmount: 5.0, MinAmount: 5.0, MaxAmount: 5.0, DistinctTypes: 1},
		{Key: "user_a", WindowStart: 10000, WindowEnd: 20000, Count: 1, SumAmount: 10.0, MinAmount: 10.0, MaxAmount: 10.0, DistinctTypes: 1},
	}
	require.Len(t, got, len(expected))
	for _, want := range expected {
		g, ok := got[key(want.Key, want.WindowStart)]
		require.Truef(t, ok, "missing restored window for %s @ %d", want.Key, want.WindowStart)
		require.Equal(t, want.Count, g.Count)
		require.InDelta(t, want.SumAmount, g.SumAmount, 1e-9)
		require.Equal(t, want.MinAmount, g.MinAmount)
		require.Equal(t, want.MaxAmount, g.MaxAmount)
		require.Equal(t, want.DistinctTypes, g.DistinctTypes)
	}
}
