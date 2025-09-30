// Copyright 2025 Supabase, Inc.
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

// Package timertools provides various timer tools
package timertools

import (
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// BackoffTicker is similar to time.Ticker but implements exponential backoff.
// It starts with an initial interval and doubles the interval on each tick
// until it reaches the maximum interval. It also adds 10% jitter to prevent
// thundering herd problems.
type BackoffTicker struct {
	C chan time.Time // The channel on which the ticks are delivered.

	mu           sync.Mutex
	timer        *time.Timer
	initialDelay time.Duration
	maxDelay     time.Duration
	currentDelay time.Duration
	stopped      bool
	rand         *rand.Rand
}

// NewBackoffTicker creates a new BackoffTicker with the given initial delay and maximum interval.
// The ticker will start with initialDelay and double on each tick until it reaches maxInterval.
// A 10% jitter is added to each interval to prevent synchronized behavior across multiple tickers.
func NewBackoffTicker(initialDelay, maxInterval time.Duration) *BackoffTicker {
	if initialDelay <= 0 {
		panic("ticker: non-positive interval for NewBackoffTicker")
	}
	if maxInterval < initialDelay {
		panic("ticker: maxInterval must be >= initialInterval")
	}

	bt := &BackoffTicker{
		C:            make(chan time.Time, 1),
		initialDelay: initialDelay,
		maxDelay:     maxInterval,
		currentDelay: initialDelay,
		rand:         rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))),
	}

	// The race detector wants us to lock the mutex before scheduling the timer.
	// This is because time.AfterFunc calls back bt.tick, which updates
	// bt.timer.
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.schedule()
	return bt
}

// Stop turns off a ticker. After Stop, no more ticks will be sent.
// Stop does not close the channel, to prevent a concurrent goroutine
// reading from the channel from seeing an erroneous "tick".
func (bt *BackoffTicker) Stop() {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if bt.stopped {
		return
	}

	bt.stopped = true
	if bt.timer != nil {
		bt.timer.Stop()
		bt.timer = nil
	}
}

// Reset resets the ticker back to its initial interval.
// This is useful when you want to restart the backoff sequence.
func (bt *BackoffTicker) Reset() {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if bt.stopped {
		return
	}

	bt.currentDelay = bt.initialDelay
	if bt.timer != nil {
		bt.timer.Stop()
	}
	bt.schedule()
}

// schedule sets up the next timer with jitter and exponential backoff.
func (bt *BackoffTicker) schedule() {
	// Add 10% jitter: delay = currentDelay * (0.9 + 0.2 * random)
	// This gives us a range of [0.9 * currentDelay, 1.1 * currentDelay]
	jitter := 0.9 + 0.2*bt.rand.Float64()
	delay := time.Duration(float64(bt.currentDelay) * jitter)

	bt.timer = time.AfterFunc(delay, bt.tick)
}

// tick handles the timer firing and schedules the next tick.
func (bt *BackoffTicker) tick() {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if bt.stopped {
		// This code is hard to reach (test) because bt.timer.Stop()
		// will most likely prevent bt.tick from being called again.
		return
	}

	// Send tick (non-blocking)
	select {
	case bt.C <- time.Now():
	default:
	}

	// Double the current delay for exponential backoff, but cap at maxDelay
	bt.currentDelay = time.Duration(math.Min(
		float64(bt.currentDelay*2),
		float64(bt.maxDelay),
	))

	// Schedule the next tick
	bt.schedule()
}

// CurrentInterval returns the current interval that will be used for the next tick.
// This is useful for testing and debugging.
func (bt *BackoffTicker) CurrentInterval() time.Duration {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	return bt.currentDelay
}
