package worker

import "sync"

// Barrier pauses the source goroutines so a checkpoint can capture a consistent
// state+offset point. While paused, no source reads a new message, so offsets
// stop advancing and no new shuffles are generated. Pause returns only once
// every in-flight source step has finished (its Route call returned, i.e. the
// event is enqueued at its owner) — that is the worker's quiescent point.
type Barrier struct {
	mu     sync.Mutex
	cond   *sync.Cond
	paused bool
	active int
	closed bool
}

// NewBarrier builds an unpaused barrier.
func NewBarrier() *Barrier {
	b := &Barrier{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// Enter is called by a source before processing one message. It blocks while a
// checkpoint has the sources paused, and returns false once the worker is
// shutting down.
func (b *Barrier) Enter() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.paused && !b.closed {
		b.cond.Wait()
	}
	if b.closed {
		return false
	}
	b.active++
	return true
}

// Leave marks the end of a source step. Always paired with a successful Enter.
func (b *Barrier) Leave() {
	b.mu.Lock()
	b.active--
	b.cond.Broadcast()
	b.mu.Unlock()
}

// Pause stops new source steps and blocks until all in-flight steps drain.
func (b *Barrier) Pause() {
	b.mu.Lock()
	b.paused = true
	for b.active > 0 {
		b.cond.Wait()
	}
	b.mu.Unlock()
}

// Resume lets sources proceed again.
func (b *Barrier) Resume() {
	b.mu.Lock()
	b.paused = false
	b.cond.Broadcast()
	b.mu.Unlock()
}

// Close unblocks any waiting sources for shutdown.
func (b *Barrier) Close() {
	b.mu.Lock()
	b.closed = true
	b.cond.Broadcast()
	b.mu.Unlock()
}
