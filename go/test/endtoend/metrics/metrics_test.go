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
	"fmt"
	"io"
	"net/http"
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
	gatewayExpected := []string{
		"mg_gateway_client_connections",
	}
	for _, name := range gatewayExpected {
		assert.Contains(t, gatewayMetrics, name, "multigateway should expose %s", name)
	}
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
