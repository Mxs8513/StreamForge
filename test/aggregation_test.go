package test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Mxs8513/StreamForge/internal/event"
	"github.com/Mxs8513/StreamForge/internal/storage"
	"github.com/Mxs8513/StreamForge/internal/worker"
)

// TestWindowedAggregation is the Phase 2 exit test: for a controlled input the
// emitted aggregates must match a hand-computed expected result. It exercises
// the real keyed-state + windowing path (BadgerDB in-memory) and the Parquet
// round-trip, but feeds explicit timestamps so the result is deterministic
// rather than dependent on wall-clock window assignment.
func TestWindowedAggregation(t *testing.T) {
	state, err := worker.OpenState("") // in-memory BadgerDB
	require.NoError(t, err)
	defer state.Close()

	const windowMS = 10_000
	agg := worker.NewAggregator(state, windowMS)

	// Two windows: [0,10000) and [10000,20000). One key spans both.
	// Window 0, user_a: amounts 1.0, 3.0, 2.0 ; types {purchase, view}
	// Window 0, user_b: amount 5.0           ; types {click}
	// Window 1, user_a: amounts 10.0, 20.0   ; types {purchase}
	inputs := []struct {
		e  event.Event
		ts int64
	}{
		{event.Event{Key: "user_a", EventType: "purchase", Amount: 1.0}, 1000},
		{event.Event{Key: "user_a", EventType: "view", Amount: 3.0}, 4000},
		{event.Event{Key: "user_a", EventType: "purchase", Amount: 2.0}, 9000},
		{event.Event{Key: "user_b", EventType: "click", Amount: 5.0}, 2000},
		{event.Event{Key: "user_a", EventType: "purchase", Amount: 10.0}, 12000},
		{event.Event{Key: "user_a", EventType: "purchase", Amount: 20.0}, 18000},
	}
	for _, in := range inputs {
		in := in
		require.NoError(t, agg.Update(&in.e, in.ts))
	}

	rows, err := agg.FlushAll()
	require.NoError(t, err)

	// Round-trip through Parquet so the test also covers the sink encoding.
	parq, err := storage.EncodeParquet(rows)
	require.NoError(t, err)
	rows, err = storage.DecodeParquet(parq)
	require.NoError(t, err)

	got := make(map[string]event.OutputRecord, len(rows))
	for _, r := range rows {
		got[key(r.Key, r.WindowStart)] = r
	}

	expected := []event.OutputRecord{
		{Key: "user_a", WindowStart: 0, WindowEnd: 10000, Count: 3, SumAmount: 6.0, MinAmount: 1.0, MaxAmount: 3.0, DistinctTypes: 2},
		{Key: "user_b", WindowStart: 0, WindowEnd: 10000, Count: 1, SumAmount: 5.0, MinAmount: 5.0, MaxAmount: 5.0, DistinctTypes: 1},
		{Key: "user_a", WindowStart: 10000, WindowEnd: 20000, Count: 2, SumAmount: 30.0, MinAmount: 10.0, MaxAmount: 20.0, DistinctTypes: 1},
	}

	require.Len(t, got, len(expected))
	for _, want := range expected {
		g, ok := got[key(want.Key, want.WindowStart)]
		require.Truef(t, ok, "missing window row for %s @ %d", want.Key, want.WindowStart)
		require.Equal(t, want.Count, g.Count)
		require.InDelta(t, want.SumAmount, g.SumAmount, 1e-9)
		require.InDelta(t, want.MinAmount, g.MinAmount, 1e-9)
		require.InDelta(t, want.MaxAmount, g.MaxAmount, 1e-9)
		require.Equal(t, want.DistinctTypes, g.DistinctTypes)
		require.Equal(t, want.WindowEnd, g.WindowEnd)
	}
}

func key(k string, ws int64) string {
	return k + "@" + itoa(ws)
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
