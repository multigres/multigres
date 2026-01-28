// Copyright 2025 Supabase, Inc.
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

package connpoolmanager

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDemandTracker_BasicSampling(t *testing.T) {
	ctx := t.Context()

	var demand atomic.Int64
	demand.Store(5)

	tracker := NewDemandTracker(ctx, &DemandTrackerConfig{
		WindowDuration: 300 * time.Millisecond,
		PollInterval:   100 * time.Millisecond,
		SampleInterval: 10 * time.Millisecond,
		Sampler:        demand.Load,
	})
	defer tracker.Close()

	// Wait for a few samples
	time.Sleep(50 * time.Millisecond)

	// Peak should be at least 5
	assert.GreaterOrEqual(t, tracker.Peak(), int64(5))

	// Increase demand
	demand.Store(10)
	time.Sleep(50 * time.Millisecond)

	// Peak should now be 10
	assert.Equal(t, int64(10), tracker.Peak())
}

func TestDemandTracker_PeakTracking(t *testing.T) {
	ctx := t.Context()

	var demand atomic.Int64

	tracker := NewDemandTracker(ctx, &DemandTrackerConfig{
		WindowDuration: 300 * time.Millisecond,
		PollInterval:   100 * time.Millisecond,
		SampleInterval: 10 * time.Millisecond,
		Sampler:        demand.Load,
	})
	defer tracker.Close()

	// Spike demand to 100
	demand.Store(100)
	time.Sleep(30 * time.Millisecond)

	// Drop demand to 10
	demand.Store(10)
	time.Sleep(30 * time.Millisecond)

	// Peak should still remember 100
	assert.Equal(t, int64(100), tracker.Peak())

	// Current should be 10
	assert.Equal(t, int64(10), tracker.Current())
}

func TestDemandTracker_GetPeakAndRotate(t *testing.T) {
	ctx := t.Context()

	var demand atomic.Int64

	// 3 buckets: 300ms / 100ms = 3
	tracker := NewDemandTracker(ctx, &DemandTrackerConfig{
		WindowDuration: 300 * time.Millisecond,
		PollInterval:   100 * time.Millisecond,
		SampleInterval: 10 * time.Millisecond,
		Sampler:        demand.Load,
	})
	defer tracker.Close()

	// Set initial demand
	demand.Store(50)
	time.Sleep(30 * time.Millisecond)

	// First rotate: should get 50
	peak := tracker.GetPeakAndRotate()
	assert.Equal(t, int64(50), peak)

	// Set higher demand in new bucket
	demand.Store(80)
	time.Sleep(30 * time.Millisecond)

	// Second rotate: should get 80 (max of 50 in old bucket, 80 in current)
	peak = tracker.GetPeakAndRotate()
	assert.Equal(t, int64(80), peak)
}

func TestDemandTracker_SlidingWindowExpiry(t *testing.T) {
	ctx := t.Context()

	var demand atomic.Int64

	// 3 buckets: 150ms / 50ms = 3
	tracker := NewDemandTracker(ctx, &DemandTrackerConfig{
		WindowDuration: 150 * time.Millisecond,
		PollInterval:   50 * time.Millisecond,
		SampleInterval: 5 * time.Millisecond,
		Sampler:        demand.Load,
	})
	defer tracker.Close()

	// Set high demand
	demand.Store(100)
	time.Sleep(20 * time.Millisecond)

	// Verify peak is 100
	assert.Equal(t, int64(100), tracker.Peak())

	// Drop demand
	demand.Store(10)

	// Rotate 3 times to expire all old buckets
	tracker.GetPeakAndRotate()
	time.Sleep(20 * time.Millisecond)
	tracker.GetPeakAndRotate()
	time.Sleep(20 * time.Millisecond)
	tracker.GetPeakAndRotate()
	time.Sleep(20 * time.Millisecond)

	// After 3 rotations, old peak should be gone
	// Peak should now be 10 (current demand)
	assert.Equal(t, int64(10), tracker.Peak())
}

func TestDemandTracker_NumBuckets(t *testing.T) {
	testCases := []struct {
		window      time.Duration
		poll        time.Duration
		wantBuckets int
	}{
		{30 * time.Second, 10 * time.Second, 3},
		{60 * time.Second, 10 * time.Second, 6},
		{10 * time.Second, 10 * time.Second, 1},
		{5 * time.Second, 10 * time.Second, 1}, // Floor to 1
	}

	for _, tc := range testCases {
		ctx, cancel := context.WithCancel(context.Background())

		tracker := NewDemandTracker(ctx, &DemandTrackerConfig{
			WindowDuration: tc.window,
			PollInterval:   tc.poll,
			SampleInterval: 100 * time.Millisecond,
			Sampler:        nil,
		})

		assert.Equal(t, tc.wantBuckets, len(tracker.buckets),
			"window=%v poll=%v", tc.window, tc.poll)

		tracker.Close()
		cancel()
	}
}

func TestDemandTracker_ConcurrentAccess(t *testing.T) {
	ctx := t.Context()

	var demand atomic.Int64
	demand.Store(1)

	tracker := NewDemandTracker(ctx, &DemandTrackerConfig{
		WindowDuration: 100 * time.Millisecond,
		PollInterval:   50 * time.Millisecond,
		SampleInterval: 5 * time.Millisecond,
		Sampler:        demand.Load,
	})
	defer tracker.Close()

	// Concurrent reads and demand changes
	done := make(chan struct{})
	go func() {
		for i := int64(1); i <= 100; i++ {
			demand.Store(i)
			time.Sleep(time.Millisecond)
		}
		close(done)
	}()

	// Concurrent peak reads
	for {
		select {
		case <-done:
			return
		default:
			_ = tracker.Peak()
			_ = tracker.Current()
		}
	}
}

func TestDemandTracker_Close(t *testing.T) {
	ctx := t.Context()

	var demand atomic.Int64
	demand.Store(5)

	tracker := NewDemandTracker(ctx, &DemandTrackerConfig{
		SampleInterval: 10 * time.Millisecond,
		WindowDuration: 100 * time.Millisecond,
		PollInterval:   50 * time.Millisecond,
		Sampler:        demand.Load,
	})

	// Close should not block
	tracker.Close()

	// Methods should still work after close (no panic)
	_ = tracker.Peak()
	_ = tracker.Current()
}

func TestDemandTracker_TwoTrackers(t *testing.T) {
	ctx := t.Context()

	// Simulate regular and reserved pools
	var regularDemand atomic.Int64
	var reservedDemand atomic.Int64

	regularTracker := NewDemandTracker(ctx, &DemandTrackerConfig{
		WindowDuration: 100 * time.Millisecond,
		PollInterval:   50 * time.Millisecond,
		SampleInterval: 5 * time.Millisecond,
		Sampler:        regularDemand.Load,
	})
	defer regularTracker.Close()

	reservedTracker := NewDemandTracker(ctx, &DemandTrackerConfig{
		WindowDuration: 100 * time.Millisecond,
		PollInterval:   50 * time.Millisecond,
		SampleInterval: 5 * time.Millisecond,
		Sampler:        reservedDemand.Load,
	})
	defer reservedTracker.Close()

	// Set different demands
	regularDemand.Store(100)
	reservedDemand.Store(20)
	time.Sleep(30 * time.Millisecond)

	// Verify they're independent
	assert.Equal(t, int64(100), regularTracker.Peak())
	assert.Equal(t, int64(20), reservedTracker.Peak())

	// Rotate regular, verify reserved is unaffected
	regularTracker.GetPeakAndRotate()
	assert.Equal(t, int64(20), reservedTracker.Peak())
}

func TestDemandTracker_AccuratePeakCapture(t *testing.T) {
	// This test verifies that brief spikes in demand are captured
	ctx := t.Context()

	var demand atomic.Int64

	tracker := NewDemandTracker(ctx, &DemandTrackerConfig{
		WindowDuration: 500 * time.Millisecond,
		PollInterval:   100 * time.Millisecond,
		SampleInterval: 5 * time.Millisecond, // Fast sampling
		Sampler:        demand.Load,
	})
	defer tracker.Close()

	// Create a brief spike
	demand.Store(1000)
	time.Sleep(20 * time.Millisecond)
	demand.Store(10)

	// Wait for sampling to capture it
	time.Sleep(30 * time.Millisecond)

	// Peak should have captured the spike
	peak := tracker.Peak()
	require.GreaterOrEqual(t, peak, int64(1000), "Brief spike should be captured")
}
