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

package poolerserver

import "sync"

// RequestsWaiter tracks the number of in-flight requests and provides a channel
// that is closed when the count reaches zero. This is used during graceful drain
// to wait for in-flight requests to complete.
//
// This follows the Vitess pattern from go/vt/vttablet/tabletserver/requests_waiter.go,
// using a channel-based notification (ZeroChan) instead of WaitToBeEmpty so that
// allowOnShutdown=true requests can still flow during the grace period.
type RequestsWaiter struct {
	mu     sync.Mutex
	count  int64
	zeroCh chan struct{}
}

// NewRequestsWaiter creates a new RequestsWaiter.
// The initial count is 0 and ZeroChan returns an already-closed channel.
func NewRequestsWaiter() *RequestsWaiter {
	ch := make(chan struct{})
	close(ch)
	return &RequestsWaiter{
		zeroCh: ch,
	}
}

// Add increments the in-flight request count by n.
// If transitioning from 0 to a positive count, a new zeroCh is created.
func (rw *RequestsWaiter) Add(n int) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if rw.count == 0 && n > 0 {
		// Transitioning from zero: create a fresh (open) channel
		rw.zeroCh = make(chan struct{})
	}
	rw.count += int64(n)
}

// Done decrements the in-flight request count by 1.
// If the count reaches 0, the zeroCh is closed to notify waiters.
func (rw *RequestsWaiter) Done() {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	rw.count--
	if rw.count == 0 {
		close(rw.zeroCh)
	}
}

// GetCount returns the current number of in-flight requests.
func (rw *RequestsWaiter) GetCount() int64 {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.count
}

// ZeroChan returns a channel that is closed when the in-flight count reaches 0.
// If the count is already 0, the returned channel is already closed.
func (rw *RequestsWaiter) ZeroChan() <-chan struct{} {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.zeroCh
}
