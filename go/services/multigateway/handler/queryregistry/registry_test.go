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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDisabledRegistry(t *testing.T) {
	r := New(Config{MaxMemoryBytes: 0})
	defer r.Close()

	// All calls are no-ops but must not panic.
	r.Record("abc", "SELECT 1", time.Millisecond, 1, false)
	assert.Equal(t, OtherLabel, r.Labelize("abc"))
	assert.Empty(t, r.Top(10, SortByCalls))
	assert.Zero(t, r.Len())
}

func TestNilRegistry(t *testing.T) {
	var r *Registry
	// Nil-safe methods: all no-ops, no panic.
	r.Record("abc", "SELECT 1", time.Millisecond, 1, false)
	assert.Equal(t, OtherLabel, r.Labelize("abc"))
	assert.Empty(t, r.Top(10, SortByCalls))
	assert.Zero(t, r.Len())
	r.Close()
}

func TestRecordAndLabelize(t *testing.T) {
	r := NewForTest(DefaultConfig())
	defer r.Close()

	fp := "abc123"
	sql := "SELECT * FROM users WHERE id = $1"

	r.Record(fp, sql, 2*time.Millisecond, 5, false)
	assert.Equal(t, fp, r.Labelize(fp))

	// Unknown fingerprints bucket into OtherLabel.
	assert.Equal(t, OtherLabel, r.Labelize("never-seen"))
}

func TestRecordStatsAggregation(t *testing.T) {
	r := NewForTest(DefaultConfig())
	defer r.Close()

	fp := "abc123"
	sql := "SELECT 1"

	r.Record(fp, sql, 1*time.Millisecond, 10, false)
	r.Record(fp, sql, 3*time.Millisecond, 20, false)
	r.Record(fp, sql, 2*time.Millisecond, 0, true) // error, no rows

	snapshots := r.Top(10, SortByCalls)
	require.Len(t, snapshots, 1)

	s := snapshots[0]
	assert.Equal(t, fp, s.Fingerprint)
	assert.Equal(t, sql, s.NormalizedSQL)
	assert.EqualValues(t, 3, s.Calls)
	assert.EqualValues(t, 1, s.Errors)
	assert.EqualValues(t, 30, s.TotalRows)
	assert.Equal(t, 6*time.Millisecond, s.TotalDuration)
	assert.Equal(t, 2*time.Millisecond, s.AverageDuration)
	assert.Equal(t, 1*time.Millisecond, s.MinDuration)
	assert.Equal(t, 3*time.Millisecond, s.MaxDuration)
	assert.False(t, s.LastSeen.IsZero())
}

func TestTopSortByCalls(t *testing.T) {
	r := NewForTest(DefaultConfig())
	defer r.Close()

	// fp1: 5 calls; fp2: 10 calls; fp3: 1 call.
	for range 5 {
		r.Record("fp1", "q1", time.Millisecond, 0, false)
	}
	for range 10 {
		r.Record("fp2", "q2", time.Millisecond, 0, false)
	}
	r.Record("fp3", "q3", time.Millisecond, 0, false)

	top := r.Top(3, SortByCalls)
	require.Len(t, top, 3)
	assert.Equal(t, "fp2", top[0].Fingerprint)
	assert.Equal(t, "fp1", top[1].Fingerprint)
	assert.Equal(t, "fp3", top[2].Fingerprint)
}

func TestTopSortByTotalTime(t *testing.T) {
	r := NewForTest(DefaultConfig())
	defer r.Close()

	r.Record("fast", "q1", 1*time.Millisecond, 0, false)
	r.Record("fast", "q1", 1*time.Millisecond, 0, false)
	r.Record("slow", "q2", 100*time.Millisecond, 0, false)

	top := r.Top(2, SortByTotalTime)
	require.Len(t, top, 2)
	assert.Equal(t, "slow", top[0].Fingerprint)
	assert.Equal(t, "fast", top[1].Fingerprint)
}

func TestTopLimit(t *testing.T) {
	r := NewForTest(DefaultConfig())
	defer r.Close()

	for i := range 20 {
		fp := fmt.Sprintf("fp-%02d", i)
		r.Record(fp, "q", time.Millisecond, 0, false)
	}

	assert.Len(t, r.Top(5, SortByCalls), 5)
	assert.Len(t, r.Top(0, SortByCalls), 20) // 0 = unlimited
}

func TestTopEmpty(t *testing.T) {
	r := NewForTest(DefaultConfig())
	defer r.Close()

	assert.Empty(t, r.Top(10, SortByCalls))
}

func TestSQLTruncation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxSQLLength = 10
	r := NewForTest(cfg)
	defer r.Close()

	r.Record("fp", "SELECT * FROM this_is_a_very_long_table_name", time.Millisecond, 0, false)
	top := r.Top(1, SortByCalls)
	require.Len(t, top, 1)
	assert.Len(t, top[0].NormalizedSQL, 10)
	assert.Equal(t, "SELECT * F", top[0].NormalizedSQL)
}

func TestLabelizeEmptyFingerprint(t *testing.T) {
	r := NewForTest(DefaultConfig())
	defer r.Close()

	assert.Equal(t, OtherLabel, r.Labelize(""))
}

func TestRecordEmptyFingerprint(t *testing.T) {
	r := NewForTest(DefaultConfig())
	defer r.Close()

	// Empty fingerprint is ignored (no-op, no panic).
	r.Record("", "SELECT 1", time.Millisecond, 0, false)
	assert.Zero(t, r.Len())
}

func TestConcurrentRecord(t *testing.T) {
	r := NewForTest(DefaultConfig())
	defer r.Close()

	const goroutines = 20
	const perGoroutine = 500

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range perGoroutine {
				r.Record("shared", "q", time.Microsecond, 1, false)
			}
		})
	}
	wg.Wait()

	top := r.Top(1, SortByCalls)
	require.Len(t, top, 1)
	assert.EqualValues(t, goroutines*perGoroutine, top[0].Calls)
	assert.EqualValues(t, goroutines*perGoroutine, top[0].TotalRows)
}

func TestCardinalityBounded(t *testing.T) {
	// Tiny memory budget so the registry can hold only a few entries.
	// We drive far more distinct fingerprints through it and assert the
	// tracked set stays small — the whole point of TinyLFU admission.
	r := New(Config{MaxMemoryBytes: 4096, MaxSQLLength: 64})
	defer r.Close()

	const churn = 5000
	for i := range churn {
		fp := fmt.Sprintf("fp-%05d", i)
		// Two hits each so the doorkeeper lets them through admission; the
		// policy still evicts cold entries once the cache fills.
		r.Record(fp, fmt.Sprintf("SELECT * FROM t%d", i), time.Microsecond, 0, false)
		r.Record(fp, fmt.Sprintf("SELECT * FROM t%d", i), time.Microsecond, 0, false)
	}

	// Tracked fingerprints must be far less than the churn count — this is
	// what keeps Prometheus label cardinality bounded.
	assert.Less(t, r.Len(), 500, "registry should be bounded well below churn count")
}

func TestDoorkeeperRejectsOneOff(t *testing.T) {
	// This test uses the production New (with doorkeeper enabled) to verify
	// that one-off queries don't enter the registry on first hit. A second
	// hit is required to pass the bloom-filter admission.
	r := New(DefaultConfig())
	defer r.Close()

	r.Record("one-off", "SELECT 1", time.Millisecond, 0, false)
	// Doorkeeper rejects on first admission: nothing tracked yet.
	assert.Equal(t, OtherLabel, r.Labelize("one-off"))

	// Second call: doorkeeper sees the bit already set and admits.
	r.Record("one-off", "SELECT 1", time.Millisecond, 0, false)
	assert.Equal(t, "one-off", r.Labelize("one-off"))
}
