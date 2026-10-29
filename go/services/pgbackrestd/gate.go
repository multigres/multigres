// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgbackrestd

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Gate admits requests until it is closed, and lets a closer wait for the
// admitted ones to finish. Checking the count and setting the closed flag
// happen under one lock, so no request can slip in between a closer seeing
// "idle" and the gate closing.
type Gate struct {
	mu       sync.Mutex
	cond     *sync.Cond
	inflight int
	closed   bool
}

// NewGate returns an open gate.
func NewGate() *Gate {
	g := &Gate{}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// TryEnter admits a request unless the gate is closed. The returned release
// must be called exactly once.
func (g *Gate) TryEnter() (release func(), ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, false
	}
	g.inflight++
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			g.inflight--
			g.cond.Broadcast()
			g.mu.Unlock()
		})
	}, true
}

// Close stops admitting requests and waits up to wait (and ctx) for the
// admitted ones to drain. On success the caller owns the closed gate until it
// calls reopen. On timeout the gate is reopened and an error returned, so a
// stuck request cannot leave the gate shut for good.
func (g *Gate) Close(ctx context.Context, wait time.Duration) (reopen func(), err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, errors.New("gate is already closed")
	}
	g.closed = true

	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	// cond.Wait cannot select on a context, so a helper wakes it at expiry.
	go func() {
		<-waitCtx.Done()
		g.mu.Lock()
		g.cond.Broadcast()
		g.mu.Unlock()
	}()
	for g.inflight > 0 {
		if err := waitCtx.Err(); err != nil {
			g.closed = false
			return nil, err
		}
		g.cond.Wait()
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			g.closed = false
			g.mu.Unlock()
		})
	}, nil
}
