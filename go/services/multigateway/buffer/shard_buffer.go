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

package buffer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	commontypes "github.com/multigres/multigres/go/common/types"
)

// bufferState represents the state of a per-shard buffer.
type bufferState int

const (
	stateIdle      bufferState = iota // Not buffering
	stateBuffering                    // Accepting requests into buffer
	stateDraining                     // Draining buffered requests via retry
)

func (s bufferState) String() string {
	switch s {
	case stateIdle:
		return "IDLE"
	case stateBuffering:
		return "BUFFERING"
	case stateDraining:
		return "DRAINING"
	default:
		return "UNKNOWN"
	}
}

// shardBuffer manages the buffering state machine for a single shard.
// State transitions: IDLE -> BUFFERING -> DRAINING -> IDLE
type shardBuffer struct {
	buf      *Buffer
	shardKey commontypes.ShardKey
	logger   *slog.Logger

	mu               sync.Mutex
	state            bufferState
	lastStart        time.Time   // When buffering last started
	lastEnd          time.Time   // When buffering last ended
	generation       uint64      // Incremented on each IDLE→BUFFERING transition
	maxDurationTimer *time.Timer // Fires when MaxFailoverDuration is exceeded
	drainWg          sync.WaitGroup
}

func newShardBuffer(buf *Buffer, key commontypes.ShardKey) *shardBuffer {
	return &shardBuffer{
		buf:      buf,
		shardKey: key,
		logger:   buf.logger.With("tablegroup", key.TableGroup, "shard", key.Shard),
		state:    stateIdle,
	}
}

// waitForFailoverEnd either starts buffering (IDLE -> BUFFERING) or joins
// an existing buffer (already BUFFERING). Returns (nil, nil) if buffering
// is not applicable for this request.
func (sb *shardBuffer) waitForFailoverEnd(ctx context.Context) (RetryDoneFunc, error) {
	// Fast path: if draining or idle with recent failover, skip.
	sb.mu.Lock()
	switch sb.state {
	case stateDraining:
		// Already draining — the new PRIMARY is available. Don't enqueue
		// into the buffer, but signal the caller to retry immediately.
		sb.mu.Unlock()
		return func() {}, nil
	case stateIdle:
		// Check timing guard: don't start buffering again too soon.
		if !sb.lastEnd.IsZero() {
			minGap := sb.buf.config.MinTimeBetweenFailovers.Get()
			if sb.buf.now().Sub(sb.lastEnd) < minGap {
				sb.mu.Unlock()
				sb.buf.stats.recordSkipped(context.Background(), "min_time_between_failovers")
				sb.logger.Debug("skipping buffering: too soon since last failover",
					"last_end", sb.lastEnd, "min_gap", minGap)
				return nil, nil
			}
		}

		// Transition IDLE -> BUFFERING.
		sb.state = stateBuffering
		sb.generation++
		gen := sb.generation
		sb.lastStart = sb.buf.now()
		sb.logger.Info("failover detected, starting buffering")
		sb.buf.stats.recordFailover(context.Background(), sb.shardKey.String())

		// Start max-duration timer. The generation is captured so that if
		// the timer fires after this failover has already ended and a new
		// one has started, the stale callback is ignored.
		sb.maxDurationTimer = time.AfterFunc(sb.buf.config.MaxFailoverDuration.Get(), func() {
			sb.logger.Warn("max failover duration exceeded, stopping buffering")
			sb.stopBuffering("max duration exceeded", gen)
		})
		sb.mu.Unlock()

	case stateBuffering:
		// Already buffering, just enqueue below.
		sb.mu.Unlock()

	default:
		sb.mu.Unlock()
		return nil, nil
	}

	// Enqueue into the global queue.
	e, err := sb.buf.enqueue(sb.shardKey)
	if err != nil {
		return nil, err
	}

	return sb.waitOnEntry(ctx, e)
}

// waitOnEntry blocks until the entry's done channel is closed or the context is canceled.
func (sb *shardBuffer) waitOnEntry(ctx context.Context, e *entry) (RetryDoneFunc, error) {
	start := sb.buf.now()
	select {
	case <-ctx.Done():
		// Request context canceled (client disconnected, deadline, etc.).
		sb.buf.removeEntry(e)
		// Signal retry completion so that if drainEntry already extracted
		// this entry from the queue, it won't block forever on
		// <-e.bufferCtx.Done(). If the entry was still in the queue,
		// this is harmless (nobody is watching bufferCtx).
		e.bufferCancel()
		sb.buf.stats.recordEvicted(context.Background(), "context_canceled")
		sb.buf.stats.recordWaitDuration(context.Background(), sb.buf.now().Sub(start).Seconds())
		return nil, ctx.Err()
	case <-e.done:
		sb.buf.stats.recordWaitDuration(context.Background(), sb.buf.now().Sub(start).Seconds())
		if e.err != nil {
			// Entry was evicted (buffer full, window timeout, max duration, shutdown).
			return nil, e.err
		}
		// Failover ended successfully — caller should retry.
		return RetryDoneFunc(e.bufferCancel), nil
	}
}

// stopBuffering transitions from BUFFERING to DRAINING and drains all entries.
// If gen is non-zero, the call is only valid for that specific generation
// (used by maxDurationTimer to avoid killing a subsequent failover's buffering).
// Pass gen=0 to stop unconditionally (used by external callers like StopBuffering).
func (sb *shardBuffer) stopBuffering(reason string, gen uint64) {
	sb.mu.Lock()
	if sb.state != stateBuffering {
		sb.mu.Unlock()
		return
	}
	if gen != 0 && sb.generation != gen {
		sb.mu.Unlock()
		sb.logger.Debug("ignoring stale stopBuffering", "reason", reason,
			"timer_gen", gen, "current_gen", sb.generation)
		return
	}

	sb.state = stateDraining
	sb.lastEnd = sb.buf.now()
	if sb.maxDurationTimer != nil {
		sb.maxDurationTimer.Stop()
		sb.maxDurationTimer = nil
	}
	sb.logger.Info("stopping buffering, draining entries", "reason", reason)
	sb.mu.Unlock()

	// Extract all entries for this shard from the global queue.
	entries := sb.buf.drainEntriesForShard(sb.shardKey)
	sb.logger.Info("draining entries", "count", len(entries))

	if len(entries) == 0 {
		sb.mu.Lock()
		sb.state = stateIdle
		sb.mu.Unlock()
		return
	}

	// Drain entries with configured concurrency. Each entry gets its own
	// goroutine; the semaphore limits how many run in parallel.
	concurrency := sb.buf.config.DrainConcurrency.Get()
	sem := make(chan struct{}, concurrency)

	sb.drainWg.Go(func() {
		var wg sync.WaitGroup
		for _, e := range entries {
			sem <- struct{}{} // Acquire drain slot.
			wg.Add(1)
			go func() {
				defer func() {
					<-sem // Release drain slot.
					wg.Done()
				}()
				sb.drainEntry(e)
			}()
		}
		wg.Wait()

		// All entries drained, transition back to IDLE.
		sb.mu.Lock()
		sb.state = stateIdle
		sb.mu.Unlock()
		sb.logger.Info("drain complete, returning to idle")
	})
}

// drainEntry signals a single entry to retry and waits for its completion.
func (sb *shardBuffer) drainEntry(e *entry) {
	// Signal the entry to retry by closing its done channel.
	close(e.done)
	sb.buf.stats.recordDrained(context.Background(), sb.shardKey.String())

	// Wait for the retry to complete (caller invokes RetryDoneFunc which
	// calls bufferCancel).
	<-e.bufferCtx.Done()

	// Release the semaphore slot.
	sb.buf.bufferSizeSema.Release(1)
}
