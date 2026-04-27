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

package metrics

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

func TestPoolerMetricsExposed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Execute a query through multigateway to generate metrics.
	conn, err := client.Connect(ctx, ctx, &client.Config{
		Host:        "localhost",
		Port:        setup.MultigatewayPgPort,
		User:        shardsetup.DefaultTestUser,
		Password:    shardsetup.TestPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Query(ctx, "SELECT 1")
	require.NoError(t, err)

	// Scrape multipooler Prometheus metrics from the primary.
	primary := setup.GetPrimary(t)
	poolerPort, ok := setup.MetricsPorts[primary.Name]
	require.True(t, ok, "no metrics port for primary %s", primary.Name)

	poolerMetrics := scrapeMetrics(t, poolerPort)

	// Verify multipooler metrics are present.
	// OTel converts dots to underscores in Prometheus export.
	poolerExpected := []string{
		"mg_pooler_up",
		"mg_pooler_pools",
		"mg_pooler_users",
		"mg_pooler_databases",
		"mg_pooler_server_connections",
		"mg_pooler_client_waiting_connections",
		"mg_pooler_config_max_server_connections",
		// OTel Prometheus exporter inserts unit suffix before _total for counters with unit "s".
		"mg_pooler_client_wait_time_seconds_total",
		"mg_pooler_queries_pooled_total",
		"mg_pooler_pool_capacity",
		"mg_pooler_pool_current_connections",
		"mg_pooler_reserved_active_connections",
	}
	for _, name := range poolerExpected {
		assert.Contains(t, poolerMetrics, name, "multipooler should expose %s", name)
	}

	// Scrape multigateway Prometheus metrics.
	gatewayPort, ok := setup.MetricsPorts["multigateway"]
	require.True(t, ok, "no metrics port for multigateway")

	gatewayMetrics := scrapeMetrics(t, gatewayPort)

	// Verify multigateway metrics are present.
	assert.Contains(t, gatewayMetrics, "mg_gateway_client_connections",
		"multigateway should expose mg_gateway_client_connections")
}

func TestPoolerMetricValues(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open a connection through multigateway and run queries.
	conn, err := client.Connect(ctx, ctx, &client.Config{
		Host:        "localhost",
		Port:        setup.MultigatewayPgPort,
		User:        shardsetup.DefaultTestUser,
		Password:    shardsetup.TestPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer conn.Close()

	// Create a table, insert rows, and query them to generate pool activity.
	_, err = conn.Query(ctx, "CREATE TABLE IF NOT EXISTS metrics_test (id int, val text)")
	require.NoError(t, err)
	_, err = conn.Query(ctx, "INSERT INTO metrics_test VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	require.NoError(t, err)
	_, err = conn.Query(ctx, "SELECT * FROM metrics_test")
	require.NoError(t, err)

	// Scrape multipooler metrics from the primary.
	primary := setup.GetPrimary(t)
	poolerPort := setup.MetricsPorts[primary.Name]
	poolerText := scrapeMetrics(t, poolerPort)
	poolerVals := parseMetrics(poolerText)

	// Pooler health: should be up.
	assertMetricValue(t, poolerVals, "mg_pooler_up", nil, 1)

	// Database count: always 1 per multipooler instance.
	assertMetricValue(t, poolerVals, "mg_pooler_databases", nil, 1)

	// Global capacity: default is 100.
	assertMetricValue(t, poolerVals, "mg_pooler_config_max_server_connections", nil, 100)

	// Pool count: at least 1 user pool (the test user).
	assertMetricGE(t, poolerVals, "mg_pooler_pools", nil, 1)

	// User count: matches pool count.
	assertMetricGE(t, poolerVals, "mg_pooler_users", nil, 1)

	// Queries pooled: we executed 3 queries, so total borrows should be >= 3.
	// Other setup queries (CREATE TABLE from multigateway readiness check) may also contribute.
	assertMetricGE(t, poolerVals, "mg_pooler_queries_pooled_total", nil, 3)

	// Server connections: should have some idle connections after queries complete.
	assertMetricGE(t, poolerVals, "mg_pooler_server_connections", map[string]string{"state": "idle"}, 1)

	// No clients should be waiting (no contention in this test).
	assertMetricValue(t, poolerVals, "mg_pooler_client_waiting_connections", nil, 0)

	// No active reserved connections (we're not in a transaction).
	assertMetricValue(t, poolerVals, "mg_pooler_reserved_active_connections", nil, 0)

	// Per-user capacity: should be > 0 for the test user.
	assertMetricGE(t, poolerVals, "mg_pooler_pool_capacity", map[string]string{"user": shardsetup.DefaultTestUser}, 1)

	// Per-user current connections: should be > 0 (pool has open connections).
	assertMetricGE(t, poolerVals, "mg_pooler_pool_current_connections", map[string]string{"user": shardsetup.DefaultTestUser}, 1)

	// Scrape multigateway metrics.
	gatewayPort := setup.MetricsPorts["multigateway"]
	gatewayText := scrapeMetrics(t, gatewayPort)
	gatewayVals := parseMetrics(gatewayText)

	// Gateway client connections: our connection should be counted.
	assertMetricGE(t, gatewayVals, "mg_gateway_client_connections", nil, 1)

	// Clean up test table.
	_, _ = conn.Query(ctx, "DROP TABLE IF EXISTS metrics_test")
}

// scrapeMetrics fetches the Prometheus text format from the given port and returns it as a string.
func scrapeMetrics(t *testing.T, port int) string {
	t.Helper()

	url := fmt.Sprintf("http://localhost:%d/metrics", port)
	resp, err := http.Get(url) //nolint:gosec // test-only code with localhost URL
	require.NoError(t, err, "failed to scrape metrics from %s", url)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode, "metrics endpoint returned non-200")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "failed to read metrics response body")

	return string(body)
}

// metricSample represents a single Prometheus metric sample.
type metricSample struct {
	name   string
	labels map[string]string
	value  float64
}

// parseMetrics parses Prometheus text format into a slice of metricSamples.
func parseMetrics(text string) []metricSample {
	var samples []metricSample
	scanner := bufio.NewScanner(strings.NewReader(text))
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		s := metricSample{labels: make(map[string]string)}

		// Split "metric_name{labels} value" or "metric_name value"
		nameEnd := strings.IndexAny(line, "{ ")
		if nameEnd == -1 {
			continue
		}
		s.name = line[:nameEnd]

		rest := line[nameEnd:]
		if strings.HasPrefix(rest, "{") {
			labelEnd := strings.Index(rest, "}")
			if labelEnd == -1 {
				continue
			}
			labelStr := rest[1:labelEnd]
			for pair := range strings.SplitSeq(labelStr, ",") {
				kv := strings.SplitN(pair, "=", 2)
				if len(kv) == 2 {
					s.labels[kv[0]] = strings.Trim(kv[1], "\"")
				}
			}
			rest = rest[labelEnd+1:]
		}

		valStr := strings.TrimSpace(rest)
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}
		s.value = val

		samples = append(samples, s)
	}
	return samples
}

// findMetric returns the value of a metric matching the name and optional label filters.
// Returns (value, true) if found, (0, false) if not.
func findMetric(samples []metricSample, name string, labels map[string]string) (float64, bool) {
	for _, s := range samples {
		if s.name != name {
			continue
		}
		match := true
		for k, v := range labels {
			if s.labels[k] != v {
				match = false
				break
			}
		}
		if match {
			return s.value, true
		}
	}
	return 0, false
}

// assertMetricValue asserts that a metric has exactly the expected value.
func assertMetricValue(t *testing.T, samples []metricSample, name string, labels map[string]string, expected float64) {
	t.Helper()
	val, ok := findMetric(samples, name, labels)
	if !ok {
		t.Errorf("metric %s%v not found", name, labels)
		return
	}
	assert.Equal(t, expected, val, "metric %s%v", name, labels)
}

// assertMetricGE asserts that a metric value is >= the expected minimum.
func assertMetricGE(t *testing.T, samples []metricSample, name string, labels map[string]string, minExpected float64) {
	t.Helper()
	val, ok := findMetric(samples, name, labels)
	if !ok {
		t.Errorf("metric %s%v not found", name, labels)
		return
	}
	assert.GreaterOrEqual(t, val, minExpected, "metric %s%v", name, labels)
}
