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
	"sync"
	"sync/atomic"
	"time"
)

// DemandSampler is a function that returns the current demand (requested count).
// This is typically pool.Requested().
type DemandSampler func() int64

// DemandTrackerConfig holds configuration for creating a DemandTracker.
type DemandTrackerConfig struct {
	// WindowDuration is the total sliding window duration (default: 30s).
	// The tracker keeps demand history for this duration.
	WindowDuration time.Duration

	// PollInterval is how often the rebalancer polls for demand (default: 10s).
	// Number of buckets = WindowDuration / PollInterval.
	PollInterval time.Duration

	// SampleInterval is how often to sample the pool's requested count (default: 100ms).
	SampleInterval time.Duration

	// Sampler is the function that returns current demand (pool.Requested()).
	Sampler DemandSampler
}

// DemandTracker tracks peak demand for a connection pool using a sliding window.
//
// Design:
//   - Keeps N buckets where N = WindowDuration / PollInterval (e.g., 30s/10s = 3 buckets)
//   - Samples the pool's requested count every SampleInterval (~100ms)
//   - Updates the current bucket's max value on each sample
//   - On GetPeakAndRotate(), returns max across all buckets and rotates to next bucket
//
// This design is memory-efficient (fixed number of buckets) while still capturing
// peak demand over the full window duration.
//
// All methods are safe for concurrent use.
type DemandTracker struct {
	buckets []atomic.Int64 // Ring buffer of peak values per poll interval
	current atomic.Int32   // Index of current bucket being updated

	sampler DemandSampler

	// For stopping the sampling goroutine
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewDemandTracker creates a new DemandTracker with sliding window buckets.
// It starts a background goroutine that samples demand at the configured interval.
func NewDemandTracker(ctx context.Context, config *DemandTrackerConfig) *DemandTracker {
	windowDuration := config.WindowDuration
	pollInterval := config.PollInterval
	sampleInterval := config.SampleInterval

	// Calculate number of buckets
	numBuckets := max(int(windowDuration/pollInterval), 1)

	ctx, cancel := context.WithCancel(ctx)
	d := &DemandTracker{
		buckets: make([]atomic.Int64, numBuckets),
		sampler: config.Sampler,
		cancel:  cancel,
	}

	// Start sampling goroutine
	d.wg.Add(1)
	go d.sampleLoop(ctx, sampleInterval)

	return d
}

// sampleLoop periodically samples the pool's requested count and updates the current bucket.
func (d *DemandTracker) sampleLoop(ctx context.Context, interval time.Duration) {
	defer d.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.sample()
		}
	}
}

// sample reads the current demand and updates the current bucket's max.
func (d *DemandTracker) sample() {
	if d.sampler == nil {
		return
	}

	current := d.sampler()
	bucketIdx := d.current.Load()

	// Atomically update max: CAS loop
	for {
		old := d.buckets[bucketIdx].Load()
		if current <= old {
			return
		}
		if d.buckets[bucketIdx].CompareAndSwap(old, current) {
			return
		}
	}
}

// GetPeakAndRotate returns the peak demand across all buckets and rotates to the next bucket.
// The next bucket is reset to 0 so it starts fresh for the new poll interval.
// This is designed to be called by the rebalancer at each poll interval.
func (d *DemandTracker) GetPeakAndRotate() int64 {
	// Find max across all buckets
	var peak int64
	for i := range d.buckets {
		if val := d.buckets[i].Load(); val > peak {
			peak = val
		}
	}

	// Rotate to next bucket
	numBuckets := int32(len(d.buckets))
	nextIdx := (d.current.Load() + 1) % numBuckets
	d.current.Store(nextIdx)

	// Reset the new current bucket to 0
	d.buckets[nextIdx].Store(0)

	return peak
}

// Peak returns the current peak demand across all buckets without rotating.
// This is useful for stats/monitoring.
func (d *DemandTracker) Peak() int64 {
	var peak int64
	for i := range d.buckets {
		if val := d.buckets[i].Load(); val > peak {
			peak = val
		}
	}
	return peak
}

// Current returns the current sampled demand (most recent sample).
// This is useful for stats/monitoring.
func (d *DemandTracker) Current() int64 {
	if d.sampler == nil {
		return 0
	}
	return d.sampler()
}

// Close stops the sampling goroutine.
func (d *DemandTracker) Close() {
	d.cancel()
	d.wg.Wait()
}
