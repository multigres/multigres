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

package metricsgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSeriesFor(t *testing.T) {
	assert.Equal(t,
		[]string{"foo_bucket", "foo_count", "foo_sum"},
		seriesFor("foo", true),
	)
	assert.Equal(t, []string{"foo_total"}, seriesFor("foo_total", false))
}

func TestKeepListRegex(t *testing.T) {
	metrics := []Metric{
		{PrometheusName: "b_total"},
		{PrometheusName: "a_seconds", histogram: true},
		{PrometheusName: "b_total"}, // duplicate fragment collapses
	}
	// Fragments are sorted and de-duplicated; histograms collapse to a group.
	assert.Equal(t, "a_seconds_(bucket|count|sum)|b_total", KeepListRegex(metrics))
}

func TestKeepListByBinary(t *testing.T) {
	metrics := []Metric{
		{PrometheusName: "a_total", Binaries: []string{"x"}},
		{PrometheusName: "b_seconds", histogram: true, Binaries: []string{"x", "y"}},
		{PrometheusName: "c_total", Binaries: []string{"y"}},
	}
	got := KeepListByBinary(metrics)
	assert.Len(t, got, 2)
	// Shared metric b appears under both binaries; histograms collapse to a group.
	assert.Equal(t, "a_total|b_seconds_(bucket|count|sum)", got["x"])
	assert.Equal(t, "b_seconds_(bucket|count|sum)|c_total", got["y"])
}

func TestAssignBinaries(t *testing.T) {
	metrics := []Metric{
		{Package: "shared"},
		{Package: "gw-only"},
		{Package: "orphan"},
	}
	binSets := map[string]map[string]bool{
		"multigateway": {"shared": true, "gw-only": true},
		"multipooler":  {"shared": true},
	}
	assignBinaries(metrics, binSets)
	assert.Equal(t, []string{"multigateway", "multipooler"}, metrics[0].Binaries) // sorted
	assert.Equal(t, []string{"multigateway"}, metrics[1].Binaries)
	assert.Empty(t, metrics[2].Binaries) // reached by no binary
}

func TestRelPos(t *testing.T) {
	assert.Equal(t,
		"go/services/multigateway/metrics.go:90",
		relPos("/abs/path/repo/go/services/multigateway/metrics.go:90"),
	)
	// Already relative / no /go/ marker is returned unchanged.
	assert.Equal(t, "metrics.go:1", relPos("metrics.go:1"))
}

// TestConstructorMapping documents that the constructor set covers all four
// otlptranslator naming buckets, so a new instrument kind added upstream
// surfaces here rather than silently producing wrong names.
func TestConstructorMapping(t *testing.T) {
	assert.Len(t, constructors, 14, "every metric.Meter instrument constructor must be mapped")
}
