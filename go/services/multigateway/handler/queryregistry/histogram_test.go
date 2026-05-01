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

func TestBucketIndex(t *testing.T) {
	cases := []struct {
		name string
		dur  time.Duration
		want int // expected bucket index
	}{
		{"zero", 0, 0},
		{"below first boundary", 100 * time.Microsecond, 0},
		{"at first boundary", 500 * time.Microsecond, 0},
		{"just above first boundary", 600 * time.Microsecond, 1},
		{"at 1ms boundary", 1 * time.Millisecond, 1},
		{"at 5ms boundary", 5 * time.Millisecond, 2},
		{"at 100ms boundary", 100 * time.Millisecond, 5},
		{"at 1s boundary", 1 * time.Second, 7},
		{"at 10s boundary", 10 * time.Second, 9},
		{"overflow 30s", 30 * time.Second, 10},
		{"overflow 5min", 5 * time.Minute, 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, bucketIndex(tc.dur.Nanoseconds()))
		})
	}
}

func TestPercentileNsEmpty(t *testing.T) {
	var counts [numHistBuckets]uint64
	assert.EqualValues(t, 0, percentileNs(counts, 0.5))
	assert.EqualValues(t, 0, percentileNs(counts, 0.99))
}

func TestPercentileNsAllInOneBucket(t *testing.T) {
	// 100 samples in the [0.5ms, 1ms] bucket (index 1).
	var counts [numHistBuckets]uint64
	counts[1] = 100

	// p50 lies inside that bucket — must fall between 0.5ms and 1ms.
	p50 := percentileNs(counts, 0.50)
	assert.GreaterOrEqual(t, p50, durationBucketsNs[0])
	assert.LessOrEqual(t, p50, durationBucketsNs[1])

	// p99 should also lie inside the same bucket.
	p99 := percentileNs(counts, 0.99)
	assert.GreaterOrEqual(t, p99, durationBucketsNs[0])
	assert.LessOrEqual(t, p99, durationBucketsNs[1])
}

func TestPercentileNsSpread(t *testing.T) {
	// 90 quick (1ms), 9 slow (100ms), 1 very slow (1s).
	var counts [numHistBuckets]uint64
	counts[1] = 90 // [0.5ms, 1ms]
	counts[5] = 9  // [50ms, 100ms]
	counts[7] = 1  // [500ms, 1s]

	p50 := percentileNs(counts, 0.50)
	// 50% of the data lies in the first bucket → p50 inside that range.
	assert.LessOrEqual(t, p50, durationBucketsNs[1])

	p99 := percentileNs(counts, 0.99)
	// p99 falls past the 99th sample → into the slow bucket.
	assert.GreaterOrEqual(t, p99, durationBucketsNs[4])
	assert.LessOrEqual(t, p99, durationBucketsNs[5])
}

func TestRecordPopulatesHistogram(t *testing.T) {
	r := NewForTest(Config{MaxMemoryBytes: 1 << 20, MaxSQLLength: 1024})
	defer r.Close()

	fp := "abc"
	sql := "SELECT 1"
	for range 100 {
		r.Record(fp, sql, 800*time.Microsecond, 0, false) // bucket 1
	}
	for range 5 {
		r.Record(fp, sql, 90*time.Millisecond, 0, false) // bucket 5
	}

	snaps := r.Top(0, SortByCalls)
	require.Len(t, snaps, 1)
	// p50 should fall into the fast bucket (≤1ms).
	assert.LessOrEqual(t, snaps[0].P50Duration, time.Millisecond)
	// p99 should land in the 90ms bucket.
	assert.GreaterOrEqual(t, snaps[0].P99Duration, 50*time.Millisecond)
	assert.LessOrEqual(t, snaps[0].P99Duration, 100*time.Millisecond)
}
