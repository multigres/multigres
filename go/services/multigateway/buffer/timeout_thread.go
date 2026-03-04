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
	"time"
)

// timeoutThread is a single goroutine that monitors the head of the global
// queue and evicts entries whose window deadline has passed.
type timeoutThread struct {
	buf      *Buffer
	notifyCh chan struct{} // Signaled when queue changes (new enqueue or eviction)
	stopCh   chan struct{} // Closed to stop the goroutine
}

func newTimeoutThread(buf *Buffer) *timeoutThread {
	return &timeoutThread{
		buf:      buf,
		notifyCh: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
	}
}

func (tt *timeoutThread) start() {
	go tt.run()
}

func (tt *timeoutThread) stop() {
	close(tt.stopCh)
}

// notify signals the timeout thread to re-check the queue head.
// Non-blocking: if a notification is already pending, this is a no-op.
func (tt *timeoutThread) notify() {
	select {
	case tt.notifyCh <- struct{}{}:
	default:
	}
}

func (tt *timeoutThread) run() {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		// Check the head of the queue.
		tt.buf.mu.Lock()
		if len(tt.buf.queue) == 0 {
			tt.buf.mu.Unlock()
			// Queue is empty — wait for a notification or stop.
			select {
			case <-tt.notifyCh:
				continue
			case <-tt.stopCh:
				return
			}
		}

		head := tt.buf.queue[0]
		tt.buf.mu.Unlock()

		// Calculate time until the head entry's deadline.
		delay := time.Until(head.deadline)
		if delay <= 0 {
			// Deadline already passed — evict immediately.
			tt.evictHead()
			continue
		}

		// Wait for the deadline, a notification, or stop.
		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			// Reset the timer. We drain it first to avoid races.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(delay)
		}

		select {
		case <-timer.C:
			// Head entry's deadline reached — evict it.
			tt.evictHead()
		case <-tt.notifyCh:
			// Queue changed — re-check head.
			continue
		case <-tt.stopCh:
			return
		}
	}
}

// evictHead removes the first entry from the queue if it's past its deadline.
func (tt *timeoutThread) evictHead() {
	tt.buf.mu.Lock()
	if len(tt.buf.queue) == 0 {
		tt.buf.mu.Unlock()
		return
	}

	head := tt.buf.queue[0]
	if time.Now().Before(head.deadline) {
		// Not yet expired — a newer entry may have been prepended.
		tt.buf.mu.Unlock()
		return
	}

	tt.buf.queue = tt.buf.queue[1:]
	tt.buf.mu.Unlock()

	head.err = errWindowExceeded
	close(head.done)
	tt.buf.bufferSizeSema.Release(1)
	tt.buf.stats.recordEvicted(context.Background(), "window_exceeded")
	tt.buf.logger.Debug("evicted entry: window exceeded", "shard_key", head.shardKey)
}
