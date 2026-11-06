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

package queryserving

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

const gatewayStreamsMetric = "mg_gateway_notification_streams"

// TestListenNotifyFailover verifies an idle LISTEN session survives a planned
// leader change without any client action: the gateway reopens its
// notification stream against the new leader and notifications sent
// afterwards are delivered.
func TestListenNotifyFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end tests in short mode")
	}
	clustersetup.RequireMultigresTopology(t, "LISTEN/NOTIFY across a planned failover")

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithMultigateway(),
		shardsetup.WithMultiadmin(),
		shardsetup.WithMetricsExport(),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()
	setup.StartMultiorchs(t.Context(), t)
	setup.WaitForQueryServing(t)
	require.NotNil(t, setup.GetPrimary(t), "primary should exist after bootstrap")

	ctx := utils.WithTimeout(t, 3*time.Minute)
	connStr := fmt.Sprintf(
		"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		setup.ClientPort(), shardsetup.TestPostgresPassword)
	gatewayMetricsPort := setup.MetricsPorts["multigateway"]

	// Client A listens and then idles for the rest of the test.
	listener, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer listener.Close(ctx)
	_, err = listener.Exec(ctx, "LISTEN failover_ch")
	require.NoError(t, err)

	// Client B notifies before the failover.
	notifier, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	_, err = notifier.Exec(ctx, "NOTIFY failover_ch, 'before'")
	require.NoError(t, err)
	notifier.Close(ctx)
	requireNotification(t, ctx, listener, "before")
	requireStreamCount(t, gatewayMetricsPort, 1)

	triggerFailover(t, setup)

	// The gateway reopens the stream once it observes the new leader serving.
	// Wait for that so the assertion is about delivery, not timing.
	reconnectStart := time.Now()
	requireStreamCount(t, gatewayMetricsPort, 1)
	t.Logf("notification stream re-established %s after failover completed", time.Since(reconnectStart))

	notifier, err = pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer notifier.Close(ctx)
	_, err = notifier.Exec(ctx, "NOTIFY failover_ch, 'after'")
	require.NoError(t, err)
	requireNotification(t, ctx, listener, "after")

	// Exactly one stream for the session: not stale, not duplicated.
	utils.AssertMetricValue(t, utils.ParseMetrics(utils.ScrapeMetrics(t, gatewayMetricsPort)),
		gatewayStreamsMetric, nil, 1)
}

func requireNotification(t *testing.T, ctx context.Context, conn *pgx.Conn, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	n, err := conn.WaitForNotification(ctx)
	require.NoError(t, err, "waiting for notification %q", payload)
	require.Equal(t, "failover_ch", n.Channel)
	require.Equal(t, payload, n.Payload)
}

func requireStreamCount(t *testing.T, port int, want float64) {
	t.Helper()
	require.Eventually(t, func() bool {
		v, ok := utils.FindMetric(utils.ParseMetrics(utils.ScrapeMetrics(t, port)), gatewayStreamsMetric, nil)
		return ok && v == want
	}, 60*time.Second, 500*time.Millisecond, "gateway should report %v active notification stream(s)", want)
}
