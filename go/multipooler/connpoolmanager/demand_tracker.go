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
	"time"
)

// DemandSampler is a function that returns the peak demand since last call.
// This is typically pool.PeakRequestedAndReset() which returns the peak
// and resets the counter for the next interval.
type DemandSampler func() int64

// DemandTrackerConfig holds configuration for creating a DemandTracker.
type DemandTrackerConfig struct {
	// DemandWindow is how far back in time to consider when calculating peak demand.
	// The rebalancer uses this window to make capacity decisions based on recent usage.
	// Example: 30s means "allocate based on peak demand over the last 30 seconds"
	DemandWindow time.Duration

	// RebalanceInterval is how often the rebalancer runs (and calls GetPeakAndRotate).
	// This determines the granularity of demand tracking.
	// Number of buckets = DemandWindow / RebalanceInterval.
	// Example: 30s window / 10s interval = 3 buckets
	RebalanceInterval time.Duration

	// Sampler is the function that returns peak demand since last call.
	// Typically pool.PeakRequestedAndReset(). The sampler is called once
	// per rebalance interval when GetPeakAndRotate() is invoked.
	Sampler DemandSampler
}

// DemandTracker tracks peak demand for a connection pool using a sliding window.
//
// # Overview
//
// The DemandTracker helps the rebalancer make informed capacity decisions by tracking
// peak connection demand over a configurable time window. This prevents the rebalancer
// from aggressively reducing capacity based on momentary low usage, which could cause
// connection starvation during the next burst.
//
// # How It Works
//
// The tracker uses a ring buffer of "buckets" to implement an efficient sliding window:
//
//	┌─────────────────────────────────────────────────────────────┐
//	│                    Demand Window (30s)                      │
//	├───────────────────┬───────────────────┬─────────────────────┤
//	│    Bucket 0       │    Bucket 1       │    Bucket 2         │
//	│   (10s ago)       │   (20s ago)       │   (current)         │
//	│   peak: 15        │   peak: 8         │   peak: 12          │
//	└───────────────────┴───────────────────┴─────────────────────┘
//	                                              ↑
//	                                         current index
//
//	GetPeakAndRotate() returns: max(15, 8, 12) = 15
//
// Each bucket stores the peak demand observed during one rebalance interval.
// When GetPeakAndRotate() is called (every rebalance interval):
//
//  1. The sampler (pool.PeakRequestedAndReset()) is called to get peak demand
//     since the last call
//  2. This value is stored in the current bucket
//  3. The maximum across ALL buckets is returned (this is the "window peak")
//  4. The tracker rotates to the next bucket and resets it
//
// # Why This Design?
//
// The pool tracks its own peak demand internally via PeakRequestedAndReset().
// This captures brief spikes that would be missed by point-in-time sampling.
// The DemandTracker's job is simply to maintain history over the configured window,
// so the rebalancer can see "what was the peak demand over the last 30 seconds?"
// rather than just "what was the peak demand in the last 10 seconds?".
//
// # Example
//
// With DemandWindow=30s and RebalanceInterval=10s:
//   - 3 buckets are created (30s / 10s = 3)
//   - Every 10s, GetPeakAndRotate() is called by the rebalancer
//   - The returned peak reflects the maximum demand seen in the last 30s
//   - This smooths out temporary dips and prevents premature capacity reduction
//
// All methods are safe for concurrent use.
type DemandTracker struct {
	buckets []atomic.Int64 // Ring buffer of peak values per rebalance interval
	current atomic.Int32   // Index of current bucket being updated

	sampler DemandSampler
}

// NewDemandTracker creates a new DemandTracker with buckets calculated from
// the demand window and rebalance interval.
//
// Number of buckets = DemandWindow / RebalanceInterval (minimum 1).
//
// Panics if:
//   - DemandWindow <= 0
//   - RebalanceInterval <= 0
//   - Sampler is nil
func NewDemandTracker(config *DemandTrackerConfig) *DemandTracker {
	// Validate configuration - panic on invalid config as this is a programming error
	if config.DemandWindow <= 0 {
		panic("DemandTrackerConfig.DemandWindow must be positive")
	}
	if config.RebalanceInterval <= 0 {
		panic("DemandTrackerConfig.RebalanceInterval must be positive")
	}
	if config.Sampler == nil {
		panic("DemandTrackerConfig.Sampler must not be nil")
	}

	// Calculate number of buckets from time durations
	numBuckets := max(int(config.DemandWindow/config.RebalanceInterval), 1)

	return &DemandTracker{
		buckets: make([]atomic.Int64, numBuckets),
		sampler: config.Sampler,
	}
}

// GetPeakAndRotate samples the current demand, returns the peak across the entire
// sliding window, and rotates to the next bucket.
//
// This should be called once per rebalance interval by the rebalancer.
//
// The method:
//  1. Calls the sampler to get the peak demand since last call
//  2. Stores this value in the current bucket
//  3. Returns the maximum value across ALL buckets (the window peak)
//  4. Rotates to the next bucket and resets it for the next interval
//
// Example with 3 buckets [15, 8, 12] where current=2:
//   - Sampler returns 12 (stored in bucket 2)
//   - Returns max(15, 8, 12) = 15
//   - Rotates: current becomes 0, bucket 0 is reset to 0
//   - Next call will overwrite the old "15" with new data
func (d *DemandTracker) GetPeakAndRotate() int64 {
	// Sample the current peak and store in current bucket
	if d.sampler != nil {
		currentIdx := d.current.Load()
		sampled := d.sampler()
		// Update current bucket with sampled value (take max in case of concurrent calls)
		for {
			old := d.buckets[currentIdx].Load()
			if sampled <= old {
				break
			}
			if d.buckets[currentIdx].CompareAndSwap(old, sampled) {
				break
			}
		}
	}

	// Find max across all buckets (the "window peak")
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

	// Reset the new current bucket to 0 (it will accumulate data for the next interval)
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

// NumBuckets returns the number of buckets in the sliding window.
// This is DemandWindow / RebalanceInterval.
func (d *DemandTracker) NumBuckets() int {
	return len(d.buckets)
}

// Close is a no-op kept for API compatibility.
// The DemandTracker does not have any background goroutines.
func (d *DemandTracker) Close() {
	// No-op: no background goroutine to stop
}
