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

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/testtiming"
)

func TestAggregate(t *testing.T) {
	measurements := []testtiming.Measurement{
		{Op: "manager ready: pooler-0", Elapsed: 3 * time.Second, Limit: 30 * time.Second},
		{Op: "manager ready: pooler-1", Elapsed: 5 * time.Second, Limit: 30 * time.Second},
		{Op: "shard bootstrap", Elapsed: 45 * time.Second, Limit: time.Minute},
	}
	data, limits := aggregate(measurements)

	// pooler suffixes are stripped so the two manager-ready samples aggregate.
	require.Equal(t, []float64{3, 5}, data["manager ready"])
	require.Equal(t, []float64{45}, data["shard bootstrap"])
	require.Len(t, data, 2)

	assert.Equal(t, 30.0, limits["manager ready"])
	assert.Equal(t, 60.0, limits["shard bootstrap"])
}

func TestCleanLabel(t *testing.T) {
	cases := map[string]string{
		"manager ready: pooler-0":  "manager ready",
		"manager ready: pooler-12": "manager ready",
		"shard bootstrap":          "shard bootstrap",
		"op: pooler-x":             "op: pooler-x", // non-numeric suffix is left alone
	}
	for in, want := range cases {
		assert.Equal(t, want, cleanLabel(in), "cleanLabel(%q)", in)
	}
}

func TestMeanCI99(t *testing.T) {
	// n=1: mean is the sample, half-width is 0 (no CI from one point).
	mean, margin := meanCI99([]float64{4})
	assert.Equal(t, 4.0, mean)
	assert.Equal(t, 0.0, margin)

	// n=5 with a known spread: mean=3, sample stddev=sqrt(2.5)≈1.5811.
	// margin = t(0.995, df=4)≈4.6041 * sqrt(2.5)/sqrt(5) ≈ 3.256.
	mean, margin = meanCI99([]float64{1, 2, 3, 4, 5})
	assert.InDelta(t, 3.0, mean, 1e-9)
	assert.InDelta(t, 3.256, margin, 1e-3)
}

func TestStatusCircle(t *testing.T) {
	assert.Equal(t, "🟢", statusCircle(50, 80)) // both below thresholds
	assert.Equal(t, "🟡", statusCircle(70, 80)) // p95 hits warn
	assert.Equal(t, "🔴", statusCircle(95, 90)) // p99 hits crit (takes priority)
	assert.Equal(t, "🔴", statusCircle(50, 95)) // p99 alone trips crit
}

func TestFormatHelpers(t *testing.T) {
	assert.Equal(t, "500ms", formatElapsed(0.5))
	assert.Equal(t, "3.0s", formatElapsed(3))
	assert.Equal(t, "3.0s (10%)", formatCell(3, 30))
	assert.Equal(t, "3.0s ±1.0s (10%, n=4)", formatCICell(3, 1, 30, 4))
	assert.Equal(t, "30s", formatLimit(30))
	assert.Equal(t, "1m0s", formatLimit(60))
}

func TestWriteTable(t *testing.T) {
	data := map[string][]float64{
		"manager ready":   {3, 5},
		"shard bootstrap": {45},
	}
	limits := map[string]float64{
		"manager ready":   30,
		"shard bootstrap": 60,
	}

	var sb strings.Builder
	require.NoError(t, writeTable(&sb, data, limits))
	got := sb.String()

	assert.Contains(t, got, "## Test Timing")
	assert.Contains(t, got, "| Operation | Timeout | mean ±99%CI | min | p50 | p95 | p99 | max |")
	// manager ready: mean of {3,5}=4s over a 30s limit → green, n=2.
	assert.Contains(t, got, "🟢 manager ready | 30s |")
	assert.Contains(t, got, "(13%, n=2)")
	// shard bootstrap: 45s over 60s = 75% → p95/p99 hit warn (🟡).
	assert.Contains(t, got, "🟡 shard bootstrap | 1m0s |")
	// rows are sorted alphabetically: manager ready before shard bootstrap.
	assert.Less(t, strings.Index(got, "manager ready"), strings.Index(got, "shard bootstrap"))
}
