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
	stopped  bool
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
	if g.closed || g.stopped {
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if g.stopped {
		return nil, errors.New("gate is stopped")
	}
	if g.closed {
		return nil, errors.New("gate is already closed")
	}
	g.closed = true

	if err := g.waitForDrain(ctx, wait, true); err != nil {
		g.closed = false
		return nil, err
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

// StopAdmission permanently refuses new requests without waiting for handlers.
func (g *Gate) StopAdmission() {
	g.mu.Lock()
	g.stopped = true
	g.cond.Broadcast()
	g.mu.Unlock()
}

// drainStopped waits for shutdown handlers without acquiring a restore gate
// lease or reopening admission. Gate.Close must reject a permanently stopped
// gate, whereas shutdown must still await requests admitted before the stop.
func (g *Gate) drainStopped(ctx context.Context, wait time.Duration) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.stopped {
		return errors.New("gate is not stopped")
	}
	return g.waitForDrain(ctx, wait, false)
}

// waitForDrain is called with mu held. Restore waiters abort on stop; the
// shutdown drainer ignores stop because it cannot reopen admission.
func (g *Gate) waitForDrain(ctx context.Context, wait time.Duration, abortOnStop bool) error {
	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	go func() { <-waitCtx.Done(); g.mu.Lock(); g.cond.Broadcast(); g.mu.Unlock() }()
	for {
		if err := waitCtx.Err(); err != nil {
			return err
		}
		if abortOnStop && g.stopped {
			return errors.New("gate is stopped")
		}
		if g.inflight == 0 {
			return nil
		}
		g.cond.Wait()
	}
}
