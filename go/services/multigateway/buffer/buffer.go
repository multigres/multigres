// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package buffer implements failover buffering for multigateway.
//
// During PRIMARY failovers, requests that would otherwise fail with UNAVAILABLE
// are held in a buffer and retried once a new PRIMARY appears in topology.
// This achieves zero application-visible errors during planned failovers.
//
// The design uses a global FIFO queue for eviction (oldest request evicted
// globally, not per-shard) and per-shard watchers so each shard's buffer
// drains independently.
package buffer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/multigres/multigres/go/common/mterrors"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// RetryDoneFunc must be called by the caller after the retry attempt completes.
// This signals to the buffer that the drain slot can be released.
type RetryDoneFunc func()

// entry represents a single buffered request in the global FIFO queue.
type entry struct {
	// done is closed when the failover ends and this entry should retry.
	done chan struct{}
	// deadline is the time after which this entry should be evicted (now + Window).
	deadline time.Time
	// err is set if the entry is evicted before the failover ends.
	err error
	// bufferCtx tracks retry completion. When the caller finishes its retry,
	// it calls bufferCancel, allowing the drain goroutine to release the slot.
	bufferCtx    context.Context
	bufferCancel context.CancelFunc
	// shardKey identifies which shard this entry belongs to.
	shardKey *clustermetadatapb.ShardKey
	// createdAt records when the entry was enqueued for metrics.
	createdAt time.Time
}

// Option configures optional Buffer behavior.
type Option func(*Buffer)

// WithNowFunc overrides the clock used by the buffer. Intended for tests
// that need deterministic time control. Production callers should not set
// this; it defaults to time.Now.
func WithNowFunc(now func() time.Time) Option {
	return func(b *Buffer) { b.now = now }
}

// Buffer is the global coordinator for failover buffering.
// It maintains a global FIFO queue of buffered requests and per-shard
// state machines that track failover state independently.
type Buffer struct {
	config *Config
	logger *slog.Logger
	stats  *stats

	// now returns the current time. Defaults to time.Now; overridden via
	// WithNowFunc in tests for deterministic clock control.
	now func() time.Time

	// ctx is the buffer-scoped context. Canceled on Shutdown() to unblock
	// any drain goroutines waiting on entry.bufferCtx.Done().
	ctx    context.Context
	cancel context.CancelFunc

	// bufferSizeSema limits the total number of buffered requests globally.
	bufferSizeSema *semaphore.Weighted

	mu      sync.Mutex
	buffers map[string]*shardBuffer // keyed by ShardKeyString
	queue   []*entry                // Global FIFO queue (all shards interleaved)
	stopped bool

	timeoutThread *timeoutThread
}

// New creates a new Buffer. config must not be nil.
func New(ctx context.Context, config *Config, logger *slog.Logger, opts ...Option) *Buffer {
	ctx, cancel := context.WithCancel(ctx)
	b := &Buffer{
		config:         config,
		logger:         logger.With("component", "buffer"),
		stats:          newStats(),
		now:            time.Now,
		ctx:            ctx,
		cancel:         cancel,
		bufferSizeSema: semaphore.NewWeighted(int64(config.Size.Get())),
		buffers:        make(map[string]*shardBuffer),
	}
	for _, opt := range opts {
		opt(b)
	}
	b.timeoutThread = newTimeoutThread(b)
	b.timeoutThread.start()
	return b
}

// WaitForFailoverEnd blocks the caller if the shard is currently undergoing
// a failover. When the failover completes, the returned RetryDoneFunc must
// be called after the retry attempt completes. Returns (nil, nil) if
// buffering is not applicable (disabled, wrong target type, timing guard, etc).
func (b *Buffer) WaitForFailoverEnd(ctx context.Context, key *clustermetadatapb.ShardKey) (RetryDoneFunc, error) {
	if !b.config.Enabled.Get() {
		return nil, nil
	}

	sb := b.getOrCreateShardBuffer(key)
	return sb.waitForFailoverEnd(ctx)
}

// WaitIfAlreadyBuffering blocks the caller if the shard is already buffering
// due to a failover detected by a previous request. Unlike WaitForFailoverEnd,
// it does NOT start buffering for idle shards. This is used proactively before
// sending a query to avoid a wasted round-trip to a pooler that is known to be
// failing over.
func (b *Buffer) WaitIfAlreadyBuffering(ctx context.Context, key *clustermetadatapb.ShardKey) (RetryDoneFunc, error) {
	if !b.config.Enabled.Get() {
		return nil, nil
	}

	// Lookup only — don't create a shardBuffer for a shard we've never seen.
	b.mu.Lock()
	sb, ok := b.buffers[commontypes.ShardKeyString(key)]
	b.mu.Unlock()
	if !ok {
		return nil, nil
	}

	return sb.waitIfAlreadyBuffering(ctx)
}

// StopBuffering is called when a new PRIMARY is discovered for the given shard.
// It transitions the shard from BUFFERING to DRAINING.
func (b *Buffer) StopBuffering(key *clustermetadatapb.ShardKey) {
	b.mu.Lock()
	sb, ok := b.buffers[commontypes.ShardKeyString(key)]
	b.mu.Unlock()

	if !ok {
		return
	}
	sb.stopBuffering("new primary", 0)
}

// Shutdown stops all buffering and evicts all pending entries.
// It waits for any in-flight drain goroutines to complete before returning.
func (b *Buffer) Shutdown() {
	b.mu.Lock()
	b.stopped = true
	// Evict all queued entries.
	for _, e := range b.queue {
		e.err = mterrors.MTB03.New()
		close(e.done)
	}
	b.queue = nil

	// Snapshot shard buffers, stop timers, and force BUFFERING shards to
	// IDLE. This prevents a concurrent stopBuffering() from calling
	// drainWg.Go() after we've already called drainWg.Wait(), which would
	// let drain goroutines outlive Shutdown().
	shardBuffers := make([]*shardBuffer, 0, len(b.buffers))
	for _, sb := range b.buffers {
		sb.mu.Lock()
		if sb.maxDurationTimer != nil {
			sb.maxDurationTimer.Stop()
			sb.maxDurationTimer = nil
		}
		if sb.state == stateBuffering {
			sb.state = stateIdle
		}
		sb.mu.Unlock()
		shardBuffers = append(shardBuffers, sb)
	}
	b.mu.Unlock()

	// Cancel the buffer context to unblock any in-flight drain goroutines
	// waiting on entry.bufferCtx.Done().
	b.cancel()

	// Wait for all in-flight drain goroutines to complete.
	for _, sb := range shardBuffers {
		sb.drainWg.Wait()
	}

	b.timeoutThread.stop()
	b.logger.Info("buffer shut down")
}

func (b *Buffer) getOrCreateShardBuffer(key *clustermetadatapb.ShardKey) *shardBuffer {
	b.mu.Lock()
	defer b.mu.Unlock()

	keyStr := commontypes.ShardKeyString(key)
	sb, ok := b.buffers[keyStr]
	if !ok {
		sb = newShardBuffer(b, key)
		b.buffers[keyStr] = sb
	}
	return sb
}

// enqueue adds a new entry to the global FIFO queue. If the buffer is full,
// the oldest entry globally is evicted to make room — even if it belongs to a
// different shard that is closer to recovering. This is intentional: global FIFO
// keeps the implementation simple and avoids per-shard capacity tracking. For the
// common case (single-shard failover) there is no cross-shard interference.
// Must NOT be called with b.mu held.
func (b *Buffer) enqueue(shardKey *clustermetadatapb.ShardKey) (*entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stopped {
		return nil, mterrors.MTB03.New()
	}

	// Try to acquire a slot from the semaphore. The semaphore (not queue
	// length) is the source of truth for capacity because drainEntry()
	// releases slots outside b.mu only after retries complete — entries
	// that have left the queue but are still in-flight during drain must
	// still count against the global limit.
	if !b.bufferSizeSema.TryAcquire(1) {
		// Buffer is full. Evict the oldest entry globally to make room.
		if len(b.queue) == 0 {
			return nil, mterrors.MTB01.New()
		}
		oldest := b.queue[0]
		b.queue = b.queue[1:]
		oldest.err = mterrors.MTB01.New()
		close(oldest.done)
		b.stats.recordEvicted(b.ctx, commontypes.ShardKeyString(oldest.shardKey), "buffer_full")
		// The evicted entry's semaphore slot is conceptually transferred to us,
		// so we don't need to acquire again.
	}

	bufCtx, bufCancel := context.WithCancel(b.ctx)
	now := b.now()
	e := &entry{
		done:         make(chan struct{}),
		deadline:     now.Add(b.config.Window.Get()),
		bufferCtx:    bufCtx,
		bufferCancel: bufCancel,
		shardKey:     shardKey,
		createdAt:    now,
	}
	b.queue = append(b.queue, e)
	b.stats.recordBuffered(b.ctx, commontypes.ShardKeyString(shardKey))

	// Notify timeout thread only when the queue transitions from empty to
	// non-empty. Entries are always appended to the tail, and the timeout
	// thread only watches the head, so subsequent enqueues are irrelevant.
	if len(b.queue) == 1 {
		b.timeoutThread.notify()
	}

	return e, nil
}

// removeEntry removes a specific entry from the global queue and releases
// its semaphore slot. Used when a request's context is canceled.
// Must NOT be called with b.mu held.
func (b *Buffer) removeEntry(e *entry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// A slice scan is faster than a linked list at our max size (1000):
	// sequential pointer comparisons are cache-friendly, while a linked
	// list would add per-element heap allocations and pointer chasing.
	// This path only runs on context cancellation, not the hot path.
	for i, qe := range b.queue {
		if qe == e {
			b.queue = append(b.queue[:i], b.queue[i+1:]...)
			b.bufferSizeSema.Release(1)
			return
		}
	}
}

// drainEntriesForShard removes and returns all entries for the given shard
// from the global queue.
// Must NOT be called with b.mu held.
func (b *Buffer) drainEntriesForShard(shardKey *clustermetadatapb.ShardKey) []*entry {
	b.mu.Lock()
	defer b.mu.Unlock()

	shardKeyStr := commontypes.ShardKeyString(shardKey)
	var drained []*entry
	remaining := b.queue[:0]
	for _, e := range b.queue {
		if commontypes.ShardKeyString(e.shardKey) == shardKeyStr {
			drained = append(drained, e)
		} else {
			remaining = append(remaining, e)
		}
	}
	// Nil out stale pointers in the tail of the backing array so drained
	// entries can be garbage collected before the queue grows back.
	for i := len(remaining); i < len(b.queue); i++ {
		b.queue[i] = nil
	}
	b.queue = remaining
	return drained
}
