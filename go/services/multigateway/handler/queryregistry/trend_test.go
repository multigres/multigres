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

package queryregistry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFloatRingPushAndSnapshot(t *testing.T) {
	r := newFloatRing(3)
	assert.Empty(t, r.snapshot())

	r.push(1)
	r.push(2)
	assert.Equal(t, []float64{1, 2}, r.snapshot())

	r.push(3)
	assert.Equal(t, []float64{1, 2, 3}, r.snapshot())

	// Wrap: oldest entry (1) is overwritten by 4.
	r.push(4)
	assert.Equal(t, []float64{2, 3, 4}, r.snapshot())

	// Wrap further: 5 evicts 2, 6 evicts 3.
	r.push(5)
	r.push(6)
	assert.Equal(t, []float64{4, 5, 6}, r.snapshot())
}

func TestFloatRingZeroCapacity(t *testing.T) {
	r := newFloatRing(0)
	r.push(1) // no-op, must not panic
	assert.Empty(t, r.snapshot())
}

func TestSampleAllPopulatesTrends(t *testing.T) {
	// Sampler-disabled config so we can drive sampleAll() manually.
	r := NewForTest(Config{
		MaxMemoryBytes: 1 << 20,
		MaxSQLLength:   1024,
	})
	defer r.Close()

	// Manually configure sampler params without starting the goroutine.
	r.sampleInterval = 1 * time.Second
	r.trendCapacity = 4

	fp := "abc"
	sql := "SELECT 1"
	for range 10 {
		r.Record(fp, sql, 2*time.Millisecond, 1, false)
	}
	r.sampleAll() // first sample: deltas reflect everything since registry start

	for range 5 {
		r.Record(fp, sql, 90*time.Millisecond, 0, false)
	}
	r.sampleAll() // second sample: 5 calls in the interval

	snaps := r.Top(0, SortByCalls)
	require.Len(t, snaps, 1)

	// CallRateTrend should hold 2 entries with the second showing ~5/s.
	assert.Len(t, snaps[0].CallRateTrend, 2)
	assert.InDelta(t, 10, snaps[0].CallRateTrend[0], 0.001)
	assert.InDelta(t, 5, snaps[0].CallRateTrend[1], 0.001)

	// p99 trend reflects the 90ms bucket after the slow burst.
	assert.Len(t, snaps[0].P99MsTrend, 2)
	assert.Greater(t, snaps[0].P99MsTrend[1], 50.0)
}

func TestSampleAllRingBufferWraps(t *testing.T) {
	r := NewForTest(Config{
		MaxMemoryBytes: 1 << 20,
		MaxSQLLength:   1024,
	})
	defer r.Close()
	r.sampleInterval = 1 * time.Second
	r.trendCapacity = 3

	r.Record("abc", "SELECT 1", time.Millisecond, 0, false)
	for range 5 {
		r.sampleAll()
	}

	snaps := r.Top(0, SortByCalls)
	require.Len(t, snaps, 1)
	// Capacity = 3, so we keep only the last 3 samples.
	assert.Len(t, snaps[0].CallRateTrend, 3)
}

// TestPercentileTrendIsWindowed guards the rule that p50/p99 trend samples
// reflect the latency distribution of the *last interval*, not the
// cumulative distribution since admission. Pushing cumulative percentiles
// would flatten the sparkline under sustained load and hide recent spikes.
func TestPercentileTrendIsWindowed(t *testing.T) {
	r := NewForTest(Config{
		MaxMemoryBytes:     1 << 20,
		MaxSQLLength:       1024,
		SampleInterval:     1 * time.Second,
		TrendWindowSamples: 4,
	})
	defer r.Close()

	fp := "abc"
	sql := "SELECT 1"

	// Interval 1 — many fast calls. Trend p99 should land in the fast bucket.
	for range 200 {
		r.Record(fp, sql, 800*time.Microsecond, 0, false)
	}
	r.sampleAll()

	// Interval 2 — only slow calls land in this window.
	for range 5 {
		r.Record(fp, sql, 90*time.Millisecond, 0, false)
	}
	r.sampleAll()

	snaps := r.Top(0, SortByCalls)
	require.Len(t, snaps, 1)
	require.Len(t, snaps[0].P99MsTrend, 2)

	// Interval 1 → fast bucket only (≤1ms).
	assert.LessOrEqual(t, snaps[0].P99MsTrend[0], 1.0,
		"p99 trend for first interval should reflect only the fast bucket")
	// Interval 2 → slow bucket only (50–100ms range).
	assert.GreaterOrEqual(t, snaps[0].P99MsTrend[1], 50.0,
		"p99 trend for second interval should reflect the recent slow burst, not be diluted by the cumulative history")
	assert.LessOrEqual(t, snaps[0].P99MsTrend[1], 100.0)
}

func TestDisabledSamplerLeavesTrendsEmpty(t *testing.T) {
	r := NewForTest(Config{MaxMemoryBytes: 1 << 20, MaxSQLLength: 1024})
	defer r.Close()
	// SampleInterval / TrendWindowSamples both zero — sampler disabled.
	require.Zero(t, r.sampleInterval)

	r.Record("abc", "SELECT 1", time.Millisecond, 0, false)
	snaps := r.Top(0, SortByCalls)
	require.Len(t, snaps, 1)
	assert.Empty(t, snaps[0].CallRateTrend)
	assert.Empty(t, snaps[0].P99MsTrend)
	assert.EqualValues(t, 0, snaps[0].SampleIntervalSeconds)
}
