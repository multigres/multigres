//  Copyright 2014 Outbrain Inc.

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at

//      http://www.apache.org/licenses/LICENSE-2.0

//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//  Modifications Copyright 2025 Supabase, Inc.

// The following provides a queue for health check requests: an ordered
// queue with no duplicates.
//
// Push() operation never blocks while Consume() blocks on an empty queue.
//

package recovery

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/services/multiorch/config"
)

// queueItem represents an item in the discovery.Queue.
type queueItem struct {
	Key      string
	PushedAt time.Time
}

// Queue is an ordered queue with deduplication.
type Queue struct {
	mu       sync.Mutex
	enqueued map[string]struct{}
	queue    chan queueItem
	logger   *slog.Logger
	config   *config.Config
}

// NewQueue creates a new queue.
func NewQueue(logger *slog.Logger, cfg *config.Config) *Queue {
	return &Queue{
		enqueued: make(map[string]struct{}),
		queue:    make(chan queueItem, config.HealthCheckQueueCapacity),
		logger:   logger,
		config:   cfg,
	}
}

// setKeyCheckEnqueued returns true if a key is already enqueued, if
// not the key will be marked as enqueued and false is returned.
func (q *Queue) setKeyCheckEnqueued(key string) (alreadyEnqueued bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	_, alreadyEnqueued = q.enqueued[key]
	if !alreadyEnqueued {
		q.enqueued[key] = struct{}{}
	}
	return alreadyEnqueued
}

// QueueLen returns the length of the queue.
func (q *Queue) QueueLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.enqueued)
}

// Push enqueues a key if it is not on a queue and is not being
// processed; silently returns otherwise.
func (q *Queue) Push(key string) {
	if q.setKeyCheckEnqueued(key) {
		return
	}
	q.queue <- queueItem{
		Key:      key,
		PushedAt: time.Now(),
	}
}

// Consume fetches a key to process; blocks if queue is empty.
// Returns the key, a release function that must be called when processing is complete,
// and a boolean indicating if a key was successfully consumed (false if context cancelled).
// Example usage:
//
//	poolerID, release, ok := q.Consume(ctx)
//	if !ok {
//	    return // context cancelled
//	}
//	defer release()
//	// process poolerID...
func (q *Queue) Consume(ctx context.Context) (string, func(), bool) {
	select {
	case <-ctx.Done():
		return "", func() {}, false
	case item := <-q.queue:
		pollInterval := q.config.GetPoolerHealthCheckInterval()
		timeOnQueue := time.Since(item.PushedAt)
		if timeOnQueue > pollInterval {
			q.logger.WarnContext(ctx, "pooler spent too long waiting in queue",
				"pooler_id", item.Key,
				"time_on_queue", timeOnQueue,
				"poll_interval", pollInterval,
			)
		}

		release := func() {
			q.mu.Lock()
			defer q.mu.Unlock()
			delete(q.enqueued, item.Key)
		}

		return item.Key, release, true
	}
}
