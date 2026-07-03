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

package metriccatalog

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCatalogNotEmpty guards against the generator silently producing an empty
// catalog (e.g. a detection regression).
func TestCatalogNotEmpty(t *testing.T) {
	assert.NotEmpty(t, Metrics, "catalog is empty; run 'make metrics'")
	assert.NotEmpty(t, PrometheusKeepListRegex)
}

// TestMetricsInvariants checks every catalog entry is internally consistent.
func TestMetricsInvariants(t *testing.T) {
	seen := make(map[string]bool)
	for _, m := range Metrics {
		t.Run(m.OTelName+"@"+m.Package, func(t *testing.T) {
			assert.NotEmpty(t, m.OTelName, "OTelName must be set")
			assert.NotEmpty(t, m.Constructor, "Constructor must be set")
			assert.NotEmpty(t, m.PrometheusName, "PrometheusName must be set")
			assert.NotEmpty(t, m.Series, "Series must not be empty")

			key := m.OTelName + "\x00" + m.Package
			assert.False(t, seen[key], "duplicate catalog entry for %s in %s", m.OTelName, m.Package)
			seen[key] = true
		})
	}
}

// TestKeepListMatchesEverySeries ensures the keep-list regex, used verbatim in
// Prometheus relabel configs, matches every series in the catalog (anchored,
// the way Prometheus applies relabel regexes) and nothing spurious.
func TestKeepListMatchesEverySeries(t *testing.T) {
	re, err := regexp.Compile("^(?:" + PrometheusKeepListRegex + ")$")
	require.NoError(t, err, "PrometheusKeepListRegex must be a valid regex")

	for _, series := range AllPrometheusSeries() {
		assert.True(t, re.MatchString(series), "keep list should match exported series %q", series)
	}

	for _, name := range []string{
		"node_cpu_seconds_total",    // unrelated infra metric
		"mg_gateway_query_duration", // histogram base without unit/suffix
		"mg_gateway_query",          // prefix only
	} {
		assert.False(t, re.MatchString(name), "keep list should not match %q", name)
	}
}

// TestEveryMetricHasABinary ensures attribution ran: a metric exposed by no
// binary is almost always a dead definition or a generator regression.
func TestEveryMetricHasABinary(t *testing.T) {
	for _, m := range Metrics {
		assert.NotEmpty(t, m.Binaries, "%s is exposed by no binary", m.OTelName)
	}
}

// TestKeepListByBinaryConsistent checks the per-binary keep lists are complete,
// compile, match their binary's series, and union back up to the global list.
func TestKeepListByBinaryConsistent(t *testing.T) {
	seriesByBinary := make(map[string][]string)
	for _, m := range Metrics {
		for _, b := range m.Binaries {
			require.Contains(t, PrometheusKeepListByBinary, b,
				"binary %q (from %s) is missing a keep list", b, m.OTelName)
			seriesByBinary[b] = append(seriesByBinary[b], m.Series...)
		}
	}

	globalRe := regexp.MustCompile("^(?:" + PrometheusKeepListRegex + ")$")
	for bin, regex := range PrometheusKeepListByBinary {
		re, err := regexp.Compile("^(?:" + regex + ")$")
		require.NoError(t, err, "keep list for %s must be a valid regex", bin)
		for _, s := range seriesByBinary[bin] {
			assert.True(t, re.MatchString(s), "%s keep list should match its series %q", bin, s)
			// A per-binary series must also be in the global union.
			assert.True(t, globalRe.MatchString(s), "global keep list should match %q", s)
		}
	}
}

// TestResourceMetricsPresentForEveryBinary verifies the process- and Go-runtime
// resource metrics are cataloged and, because they are wired centrally in
// telemetry, exposed by every binary. This guards against the runtime entries
// being dropped from the generator (all other invariants still hold with fewer
// metrics, so only an explicit presence check catches a regression here).
func TestResourceMetricsPresentForEveryBinary(t *testing.T) {
	resourceSeries := []string{
		"process_cpu_time_seconds_total",
		"process_memory_usage_bytes",
		"process_memory_virtual_bytes",
		"go_config_gogc_percent",
		"go_goroutine_count",
		"go_memory_allocated_bytes_total",
		"go_memory_allocations_total",
		"go_memory_gc_goal_bytes",
		"go_memory_used_bytes",
		"go_processor_limit",
	}

	all := make(map[string]bool)
	for _, s := range AllPrometheusSeries() {
		all[s] = true
	}

	require.NotEmpty(t, PrometheusKeepListByBinary)
	for _, series := range resourceSeries {
		assert.Truef(t, all[series], "resource metric %q missing from catalog; run 'make metrics'", series)

		// Every binary bootstraps telemetry, so each per-binary keep list must
		// admit these series.
		for bin, regex := range PrometheusKeepListByBinary {
			re := regexp.MustCompile("^(?:" + regex + ")$")
			assert.Truef(t, re.MatchString(series), "%s keep list should include %q", bin, series)
		}
	}
}

// TestAllPrometheusSeries confirms the flattened helper returns one entry per
// series across all metrics.
func TestAllPrometheusSeries(t *testing.T) {
	want := 0
	for _, m := range Metrics {
		want += len(m.Series)
	}
	assert.Len(t, AllPrometheusSeries(), want)
}
