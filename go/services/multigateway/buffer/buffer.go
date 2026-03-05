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
	shardKey commontypes.ShardKey
	// createdAt records when the entry was enqueued for metrics.
	createdAt time.Time
}

// Buffer is the global coordinator for failover buffering.
// It maintains a global FIFO queue of buffered requests and per-shard
// state machines that track failover state independently.
type Buffer struct {
	config *Config
	logger *slog.Logger
	stats  *stats

	// ctx is the buffer-scoped context. Canceled on Shutdown() to unblock
	// any drain goroutines waiting on entry.bufferCtx.Done().
	ctx    context.Context
	cancel context.CancelFunc

	// bufferSizeSema limits the total number of buffered requests globally.
	bufferSizeSema *semaphore.Weighted

	mu      sync.Mutex
	buffers map[commontypes.ShardKey]*shardBuffer
	queue   []*entry // Global FIFO queue (all shards interleaved)
	stopped bool

	timeoutThread *timeoutThread
}

// New creates a new Buffer. If config is nil or buffering is disabled,
// the buffer will be a no-op (WaitForFailoverEnd returns nil, nil).
// TODO: if config is nil, this function will panic.
func New(ctx context.Context, config *Config, logger *slog.Logger) *Buffer {
	ctx, cancel := context.WithCancel(ctx)
	b := &Buffer{
		config:         config,
		logger:         logger.With("component", "buffer"),
		stats:          newStats(),
		ctx:            ctx,
		cancel:         cancel,
		bufferSizeSema: semaphore.NewWeighted(int64(config.Size.Get())),
		buffers:        make(map[commontypes.ShardKey]*shardBuffer),
	}
	b.timeoutThread = newTimeoutThread(b)
	b.timeoutThread.start()
	return b
}

// WaitForFailoverEnd blocks the caller if the shard is currently undergoing
// a failover. When the failover completes, the returned RetryDoneFunc must
// be called after the retry attempt completes. Returns (nil, nil) if
// buffering is not applicable (disabled, wrong target type, timing guard, etc).
func (b *Buffer) WaitForFailoverEnd(ctx context.Context, key commontypes.ShardKey) (RetryDoneFunc, error) {
	if !b.config.Enabled.Get() {
		return nil, nil
	}

	sb := b.getOrCreateShardBuffer(key)
	return sb.waitForFailoverEnd(ctx)
}

// StopBuffering is called when a new PRIMARY is discovered for the given shard.
// It transitions the shard from BUFFERING to DRAINING.
func (b *Buffer) StopBuffering(key commontypes.ShardKey) {
	b.mu.Lock()
	sb, ok := b.buffers[key]
	b.mu.Unlock()

	if !ok {
		return
	}
	sb.stopBuffering("new primary", 0)
}

// Shutdown stops all buffering and evicts all pending entries.
func (b *Buffer) Shutdown() {
	b.mu.Lock()
	b.stopped = true
	// Evict all queued entries.
	for _, e := range b.queue {
		e.err = mterrors.MTB03.New()
		close(e.done)
	}
	// TODO: Cleanup up semaphore etc.
	b.queue = nil
	b.mu.Unlock()

	// Cancel the buffer context to unblock any in-flight drain goroutines
	// waiting on entry.bufferCtx.Done().
	b.cancel()

	b.timeoutThread.stop()
	b.logger.Info("buffer shut down")
}

func (b *Buffer) getOrCreateShardBuffer(key commontypes.ShardKey) *shardBuffer {
	b.mu.Lock()
	defer b.mu.Unlock()

	sb, ok := b.buffers[key]
	if !ok {
		sb = newShardBuffer(b, key)
		b.buffers[key] = sb
	}
	return sb
}

// enqueue adds a new entry to the global FIFO queue. If the buffer is full,
// the oldest entry globally is evicted to make room.
// Must NOT be called with b.mu held.
func (b *Buffer) enqueue(shardKey commontypes.ShardKey) (*entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stopped {
		return nil, mterrors.MTB03.New()
	}

	// Try to acquire a slot from the semaphore.
	// TODO: Do we really need the bufferSizeSema, or can we use the
	// size of the queue cause we already have a mutex for protection.
	if !b.bufferSizeSema.TryAcquire(1) {
		// Buffer is full. Evict the oldest entry globally to make room.
		if len(b.queue) == 0 {
			return nil, mterrors.MTB01.New()
		}
		oldest := b.queue[0]
		b.queue = b.queue[1:]
		oldest.err = mterrors.MTB01.New()
		close(oldest.done)
		b.stats.recordEvicted(context.Background(), "buffer_full")
		// The evicted entry's semaphore slot is conceptually transferred to us,
		// so we don't need to acquire again.
	}

	bufCtx, bufCancel := context.WithCancel(b.ctx)
	e := &entry{
		done:         make(chan struct{}),
		deadline:     time.Now().Add(b.config.Window.Get()),
		bufferCtx:    bufCtx,
		bufferCancel: bufCancel,
		shardKey:     shardKey,
		createdAt:    time.Now(),
	}
	b.queue = append(b.queue, e)
	b.stats.recordBuffered(context.Background(), shardKey.String())

	// Notify timeout thread that the queue might have a new head.
	// TODO: This seems wasteful on every enqueue operation.
	b.timeoutThread.notify()

	return e, nil
}

// removeEntry removes a specific entry from the global queue and releases
// its semaphore slot. Used when a request's context is canceled.
// Must NOT be called with b.mu held.
func (b *Buffer) removeEntry(e *entry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// TODO: possibly make queue a double linked list, for O(1) removal.
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
func (b *Buffer) drainEntriesForShard(shardKey commontypes.ShardKey) []*entry {
	b.mu.Lock()
	defer b.mu.Unlock()

	var drained []*entry
	remaining := b.queue[:0]
	for _, e := range b.queue {
		if e.shardKey == shardKey {
			drained = append(drained, e)
		} else {
			remaining = append(remaining, e)
		}
	}
	b.queue = remaining
	return drained
}
