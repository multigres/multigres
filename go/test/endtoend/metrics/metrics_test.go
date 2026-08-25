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
	"context"
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

	poolerMetrics := utils.ScrapeMetrics(t, poolerPort)

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

	gatewayMetrics := utils.ScrapeMetrics(t, gatewayPort)

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
	poolerText := utils.ScrapeMetrics(t, poolerPort)
	poolerVals := utils.ParseMetrics(poolerText)

	// Pooler health: should be up.
	utils.AssertMetricValue(t, poolerVals, "mg_pooler_up", nil, 1)

	// Database count: always 1 per multipooler instance.
	utils.AssertMetricValue(t, poolerVals, "mg_pooler_databases", nil, 1)

	// Global capacity: default is 100.
	utils.AssertMetricValue(t, poolerVals, "mg_pooler_config_max_server_connections", nil, 100)

	// Pool count: at least 1 user pool (the test user).
	utils.AssertMetricGE(t, poolerVals, "mg_pooler_pools", nil, 1)

	// User count: matches pool count.
	utils.AssertMetricGE(t, poolerVals, "mg_pooler_users", nil, 1)

	// Queries pooled: we executed 3 queries, so total borrows should be >= 3.
	// Other setup queries (CREATE TABLE from multigateway readiness check) may also contribute.
	utils.AssertMetricGE(t, poolerVals, "mg_pooler_queries_pooled_total", nil, 3)

	// Server connections: should have some idle connections after queries complete.
	utils.AssertMetricGE(t, poolerVals, "mg_pooler_server_connections", map[string]string{"state": "idle"}, 1)

	// No clients should be waiting (no contention in this test).
	utils.AssertMetricValue(t, poolerVals, "mg_pooler_client_waiting_connections", nil, 0)

	// No active reserved connections (we're not in a transaction).
	utils.AssertMetricValue(t, poolerVals, "mg_pooler_reserved_active_connections", nil, 0)

	// Per-user capacity: should be > 0 for the test user.
	utils.AssertMetricGE(t, poolerVals, "mg_pooler_pool_capacity", map[string]string{"user": shardsetup.DefaultTestUser}, 1)

	// Per-user current connections: should be > 0 (pool has open connections).
	utils.AssertMetricGE(t, poolerVals, "mg_pooler_pool_current_connections", map[string]string{"user": shardsetup.DefaultTestUser}, 1)

	// Scrape multigateway metrics.
	gatewayPort := setup.MetricsPorts["multigateway"]
	gatewayText := utils.ScrapeMetrics(t, gatewayPort)
	gatewayVals := utils.ParseMetrics(gatewayText)

	// Gateway client connections: our connection should be counted.
	utils.AssertMetricGE(t, gatewayVals, "mg_gateway_client_connections", nil, 1)

	// Clean up test table.
	_, _ = conn.Query(ctx, "DROP TABLE IF EXISTS metrics_test")
}

// TestQueryPathMetricsExposed drives a query, an explicit transaction, and a
// query that fails at execution, then verifies the query-path observability
// metrics (executor per-query, connection lifecycle, transactions, replication
// / serving health, and gateway phase latency) are exported.
func TestQueryPathMetricsExposed(t *testing.T) {
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

	// Successful autocommit query: exercises query.duration/rows/pool_acquire on
	// the regular pool and the gateway parse/plan/exec phase histograms.
	_, err = conn.Query(ctx, "SELECT 1")
	require.NoError(t, err)

	// Explicit transaction: reserves a connection and concludes with COMMIT,
	// exercising txn.outcomes{outcome=commit} and txn.duration.
	_, err = conn.Query(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = conn.Query(ctx, "SELECT 1")
	require.NoError(t, err)
	_, err = conn.Query(ctx, "COMMIT")
	require.NoError(t, err)

	// Query that parses/plans cleanly but fails at execution (division by zero,
	// SQLSTATE 22012) so the error surfaces from PostgreSQL through the pooler,
	// exercising query.errors with a backend error_source.
	_, err = conn.Query(ctx, "SELECT 1/0")
	require.Error(t, err, "SELECT 1/0 should fail at execution")

	// Scrape multipooler metrics from the primary (writes/transactions route there).
	primary := setup.GetPrimary(t)
	poolerPort, ok := setup.MetricsPorts[primary.Name]
	require.True(t, ok, "no metrics port for primary %s", primary.Name)
	poolerMetrics := utils.ScrapeMetrics(t, poolerPort)

	// Histograms export as <name>_bucket/_sum/_count; counters as <name>_total.
	// OTel maps dots to underscores and appends _seconds for the "s" unit.
	poolerExpected := []string{
		"mg_pooler_query_duration_seconds",
		"mg_pooler_query_rows",
		"mg_pooler_query_pool_acquire_duration_seconds",
		"mg_pooler_query_errors_total",
		"mg_pooler_server_conn_opened_total",
		"mg_pooler_server_conn_setup_duration_seconds",
		"mg_pooler_txn_outcomes_total",
		"mg_pooler_txn_duration_seconds",
		"mg_pooler_replication_lag_seconds",
		"mg_pooler_serving_transitions_total",
	}
	for _, name := range poolerExpected {
		assert.Contains(t, poolerMetrics, name, "multipooler should expose %s", name)
	}

	// Value checks: the COMMIT we issued is counted, and the failing query is
	// counted as a pooler-side error.
	poolerVals := utils.ParseMetrics(poolerMetrics)
	utils.AssertMetricGE(t, poolerVals, "mg_pooler_txn_outcomes_total", map[string]string{"outcome": "commit"}, 1)
	utils.AssertMetricGE(t, poolerVals, "mg_pooler_query_errors_total", map[string]string{"error_source": "backend"}, 1)

	// Scrape multigateway metrics and verify the phase-latency histograms.
	gatewayPort, ok := setup.MetricsPorts["multigateway"]
	require.True(t, ok, "no metrics port for multigateway")
	gatewayMetrics := utils.ScrapeMetrics(t, gatewayPort)
	gatewayExpected := []string{
		"mg_gateway_query_parse_duration_seconds",
		"mg_gateway_query_plan_duration_seconds",
		"mg_gateway_query_exec_duration_seconds",
	}
	for _, name := range gatewayExpected {
		assert.Contains(t, gatewayMetrics, name, "multigateway should expose %s", name)
	}
}

// resourceMetricSeries are the process- and Go-runtime-level series that every
// Multigres component exports by virtue of going through telemetry.InitTelemetry
// (see go/tools/telemetry/processmetrics.go). The process.* gauges are the
// kubectl-top replacement; the go.* series come from the OpenTelemetry runtime
// instrumentation. These names must match the generated keep-list in
// go/observability/metriccatalog — this test is the end-to-end check that the
// hard-coded runtime entries in metricsgen still match what is scraped.
var resourceMetricSeries = []string{
	// Process CPU/memory, read via gopsutil.
	"process_cpu_time_seconds_total",
	"process_memory_usage_bytes",
	"process_memory_virtual_bytes",
	// Go runtime instrumentation.
	"go_config_gogc_percent",
	"go_goroutine_count",
	"go_memory_allocated_bytes_total",
	"go_memory_allocations_total",
	"go_memory_gc_goal_bytes",
	"go_memory_used_bytes",
	"go_processor_limit",
}

// TestResourceMetricsExposed verifies that the process CPU/memory and Go runtime
// metrics are exported on the live Prometheus endpoints of the multipooler and
// multigateway, with a plausible resident-memory value. Because these are wired
// centrally in telemetry, exporting them here demonstrates every component does.
func TestResourceMetricsExposed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	// Check both a multipooler (primary) and the multigateway, so we cover
	// distinct binaries that share the telemetry bootstrap.
	primary := setup.GetPrimary(t)
	poolerPort, ok := setup.MetricsPorts[primary.Name]
	require.True(t, ok, "no metrics port for primary %s", primary.Name)
	gatewayPort, ok := setup.MetricsPorts["multigateway"]
	require.True(t, ok, "no metrics port for multigateway")

	for name, port := range map[string]int{primary.Name: poolerPort, "multigateway": gatewayPort} {
		text := utils.ScrapeMetrics(t, port)
		for _, series := range resourceMetricSeries {
			assert.Containsf(t, text, series, "%s should expose %s", name, series)
		}

		// Resident set size must be positive for a live process.
		vals := utils.ParseMetrics(text)
		utils.AssertMetricGE(t, vals, "process_memory_usage_bytes", nil, 1)
	}
}
