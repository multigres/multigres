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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDemandTracker_BasicSampling(t *testing.T) {
	var peak atomic.Int64
	peak.Store(5)

	tracker := NewDemandTracker(&DemandTrackerConfig{
		DemandWindow:      30 * time.Second,
		RebalanceInterval: 10 * time.Second,
		Sampler:           peak.Load,
	})
	defer tracker.Close()

	// GetPeakAndRotate calls the sampler
	result := tracker.GetPeakAndRotate()
	assert.Equal(t, int64(5), result)

	// Increase demand
	peak.Store(10)
	result = tracker.GetPeakAndRotate()
	assert.Equal(t, int64(10), result)
}

func TestDemandTracker_PeakTracking(t *testing.T) {
	var peak atomic.Int64

	tracker := NewDemandTracker(&DemandTrackerConfig{
		DemandWindow:      30 * time.Second,
		RebalanceInterval: 10 * time.Second,
		Sampler:           peak.Load,
	})
	defer tracker.Close()

	// Spike demand to 100
	peak.Store(100)
	tracker.GetPeakAndRotate()

	// Drop demand to 10
	peak.Store(10)
	tracker.GetPeakAndRotate()

	// Peak should still remember 100 (it's in the sliding window)
	assert.Equal(t, int64(100), tracker.Peak())
}

func TestDemandTracker_GetPeakAndRotate(t *testing.T) {
	var peak atomic.Int64

	// 3 buckets: 30s / 10s = 3
	tracker := NewDemandTracker(&DemandTrackerConfig{
		DemandWindow:      30 * time.Second,
		RebalanceInterval: 10 * time.Second,
		Sampler:           peak.Load,
	})
	defer tracker.Close()

	// Set initial demand
	peak.Store(50)
	result := tracker.GetPeakAndRotate()
	assert.Equal(t, int64(50), result)

	// Set higher demand in new bucket
	peak.Store(80)
	result = tracker.GetPeakAndRotate()
	// Should get 80 (max of 50 in old bucket, 80 in current)
	assert.Equal(t, int64(80), result)
}

func TestDemandTracker_SlidingWindowExpiry(t *testing.T) {
	var peak atomic.Int64

	// 3 buckets: 30s / 10s = 3
	tracker := NewDemandTracker(&DemandTrackerConfig{
		DemandWindow:      30 * time.Second,
		RebalanceInterval: 10 * time.Second,
		Sampler:           peak.Load,
	})
	defer tracker.Close()

	// Set high demand
	peak.Store(100)
	tracker.GetPeakAndRotate()

	// Verify peak is 100
	assert.Equal(t, int64(100), tracker.Peak())

	// Drop demand
	peak.Store(10)

	// Rotate 3 times to expire all old buckets
	tracker.GetPeakAndRotate()
	tracker.GetPeakAndRotate()
	tracker.GetPeakAndRotate()

	// After 3 rotations, old peak should be gone
	// Peak should now be 10 (current demand)
	assert.Equal(t, int64(10), tracker.Peak())
}

func TestDemandTracker_NumBuckets(t *testing.T) {
	testCases := []struct {
		window      time.Duration
		interval    time.Duration
		wantBuckets int
	}{
		{30 * time.Second, 10 * time.Second, 3},
		{60 * time.Second, 10 * time.Second, 6},
		{10 * time.Second, 10 * time.Second, 1},
		{5 * time.Second, 10 * time.Second, 1}, // Floor to 1
	}

	var demand atomic.Int64
	sampler := demand.Load

	for _, tc := range testCases {
		tracker := NewDemandTracker(&DemandTrackerConfig{
			DemandWindow:      tc.window,
			RebalanceInterval: tc.interval,
			Sampler:           sampler,
		})

		assert.Equal(t, tc.wantBuckets, tracker.NumBuckets(),
			"window=%v interval=%v", tc.window, tc.interval)

		tracker.Close()
	}
}

func TestDemandTracker_ConcurrentAccess(t *testing.T) {
	var peak atomic.Int64
	peak.Store(1)

	tracker := NewDemandTracker(&DemandTrackerConfig{
		DemandWindow:      30 * time.Second,
		RebalanceInterval: 10 * time.Second,
		Sampler:           peak.Load,
	})
	defer tracker.Close()

	// Concurrent reads and demand changes
	done := make(chan struct{})
	go func() {
		for i := int64(1); i <= 100; i++ {
			peak.Store(i)
		}
		close(done)
	}()

	// Concurrent peak reads and rotations
	for {
		select {
		case <-done:
			return
		default:
			_ = tracker.Peak()
			_ = tracker.GetPeakAndRotate()
		}
	}
}

func TestDemandTracker_Close(t *testing.T) {
	var peak atomic.Int64
	peak.Store(5)

	tracker := NewDemandTracker(&DemandTrackerConfig{
		DemandWindow:      30 * time.Second,
		RebalanceInterval: 10 * time.Second,
		Sampler:           peak.Load,
	})

	// Close should not block
	tracker.Close()

	// Methods should still work after close (no panic)
	_ = tracker.Peak()
}

func TestDemandTracker_TwoTrackers(t *testing.T) {
	// Simulate regular and reserved pools
	var regularPeak atomic.Int64
	var reservedPeak atomic.Int64

	regularTracker := NewDemandTracker(&DemandTrackerConfig{
		DemandWindow:      30 * time.Second,
		RebalanceInterval: 10 * time.Second,
		Sampler:           regularPeak.Load,
	})
	defer regularTracker.Close()

	reservedTracker := NewDemandTracker(&DemandTrackerConfig{
		DemandWindow:      30 * time.Second,
		RebalanceInterval: 10 * time.Second,
		Sampler:           reservedPeak.Load,
	})
	defer reservedTracker.Close()

	// Set different demands
	regularPeak.Store(100)
	reservedPeak.Store(20)

	// Sample them
	regularTracker.GetPeakAndRotate()
	reservedTracker.GetPeakAndRotate()

	// Verify they're independent
	assert.Equal(t, int64(100), regularTracker.Peak())
	assert.Equal(t, int64(20), reservedTracker.Peak())

	// Rotate regular, verify reserved is unaffected
	regularTracker.GetPeakAndRotate()
	assert.Equal(t, int64(20), reservedTracker.Peak())
}

func TestDemandTracker_PeakCapturedOnRotate(t *testing.T) {
	// This test verifies that peaks are captured when GetPeakAndRotate is called
	var peak atomic.Int64

	tracker := NewDemandTracker(&DemandTrackerConfig{
		DemandWindow:      30 * time.Second,
		RebalanceInterval: 10 * time.Second,
		Sampler:           peak.Load,
	})
	defer tracker.Close()

	// Set a spike
	peak.Store(1000)

	// GetPeakAndRotate should capture the spike
	result := tracker.GetPeakAndRotate()
	assert.Equal(t, int64(1000), result, "Spike should be captured on rotate")

	// Peak should also show 1000
	assert.Equal(t, int64(1000), tracker.Peak())
}

func TestDemandTracker_InvalidConfigPanics(t *testing.T) {
	var peak atomic.Int64
	sampler := peak.Load

	// DemandWindow <= 0 should panic
	assert.Panics(t, func() {
		NewDemandTracker(&DemandTrackerConfig{
			DemandWindow:      0,
			RebalanceInterval: 10 * time.Second,
			Sampler:           sampler,
		})
	})

	// RebalanceInterval <= 0 should panic
	assert.Panics(t, func() {
		NewDemandTracker(&DemandTrackerConfig{
			DemandWindow:      30 * time.Second,
			RebalanceInterval: 0,
			Sampler:           sampler,
		})
	})

	// Nil sampler should panic
	assert.Panics(t, func() {
		NewDemandTracker(&DemandTrackerConfig{
			DemandWindow:      30 * time.Second,
			RebalanceInterval: 10 * time.Second,
			Sampler:           nil,
		})
	})
}
