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

package multipooler

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// replStatsSetupManager runs a dedicated cluster for replicationstats metrics
// validation, separate from filesystemSetupManager/s3SetupManager: it needs
// WithMetricsExport() (which also starts multigateway) and a short poll
// interval, neither of which the backup-focused shared clusters carry —
// adding them there would slow down and change behavior for every backup
// test in this package.
var replStatsSetupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	return shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMetricsExport(),
		shardsetup.WithMultipoolerExtraArgs("--replication-stats-poll-interval-milliseconds=250"),
	)
})

// getReplStatsSetup returns the shared cluster for replicationstats metrics tests.
func getReplStatsSetup(t *testing.T) *MultipoolerTestSetup {
	t.Helper()
	return newMultipoolerTestSetup(replStatsSetupManager.Get(t))
}

// scrapeAndFind scrapes the pooler's Prometheus endpoint fresh and returns
// the value of the first sample matching name/labels. See utils.FindMetric
// for match semantics (labels is a subset match).
func scrapeAndFind(t *testing.T, port int, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	return utils.FindMetric(utils.ParseMetrics(utils.ScrapeMetrics(t, port)), name, labels)
}

// TestReplicationStatsMetricsExported drives a real logical-replication
// session through the multipooler StreamReplication RPC (the same tagged
// reserved.Pool.NewLogicalReplicationConn path replicationstats.Poller
// scrapes pg_stat_replication/pg_replication_slots for) and verifies the
// poller's OTel gauges report sane values on the primary's Prometheus
// endpoint: near-zero replication lag and a non-negative retained-WAL count
// for the slot we created.
func TestReplicationStatsMetricsExported(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getReplStatsSetup(t)
	table, pub := setupReplicationFixture(t, setup)
	slot := fmt.Sprintf("replstats_slot_%d", fixtureCounter.Add(1))
	t.Cleanup(func() { dropReplicationSlot(t, setup, slot) })

	poolerPort, ok := setup.MetricsPorts[setup.PrimaryMultipooler.Name]
	require.True(t, ok, "no metrics port for primary %s", setup.PrimaryMultipooler.Name)

	ctx := utils.WithTimeout(t, 60*time.Second)
	rt := openReplTunnel(t, dialPoolerClient(t, setup), ctx)

	rt.sendQuery(fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	rt.drainUntilReadyForQuery()
	rt.sendQuery(fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s', binary 'true')",
		slot, pub))
	rt.expectCopyBothResponse()

	insertRow(t, setup, table, 1, "replstats")
	ackLSN := rt.streamOneFrame(20 * time.Second)

	// Ack so Postgres records reply_time/replay_lag for this walsender — both
	// stay NULL (and mg.pooler.replication.replay_lag/last_ack_age go unset)
	// until at least one Standby Status Update arrives.
	rt.sendCopyData(buildStandbyStatus(ackLSN))

	require.Eventually(t, func() bool {
		lag, ok := scrapeAndFind(t, poolerPort, "mg_pooler_replication_replay_lag_seconds", nil)
		return ok && lag >= 0
	}, 15*time.Second, 300*time.Millisecond,
		"mg_pooler_replication_replay_lag_seconds must be exported once the poller observes our tagged connection")

	lag, ok := scrapeAndFind(t, poolerPort, "mg_pooler_replication_replay_lag_seconds", nil)
	require.True(t, ok)
	assert.GreaterOrEqual(t, lag, 0.0)
	assert.Less(t, lag, 30.0, "replay_lag should be near-zero for a caught-up local test connection")

	// OTel's Prometheus exporter appends a unit suffix derived from the
	// instrument's WithUnit — "_bytes" here, mirroring "_seconds" above for
	// the "s"-unit gauges (see metrics.go's meter.Int64Gauge WithUnit("By")).
	require.Eventually(t, func() bool {
		_, ok := scrapeAndFind(t, poolerPort, "mg_pooler_replication_slot_retained_wal_bytes", map[string]string{"slot_name": slot})
		return ok
	}, 15*time.Second, 300*time.Millisecond,
		"mg_pooler_replication_slot_retained_wal_bytes must be exported for our active slot")

	retained, ok := scrapeAndFind(t, poolerPort, "mg_pooler_replication_slot_retained_wal_bytes", map[string]string{"slot_name": slot})
	require.True(t, ok)
	assert.GreaterOrEqual(t, retained, 0.0)
}

// TestReplicationStatsPollerStopsOnStandbyRole verifies the poller only runs
// on the writable leader: the standby pooler in this cluster must never
// export replicationstats gauges (its pg_stat_replication is always empty —
// only a primary has walsenders — but this also pins the leader-only gating
// itself, unit-tested in replicationstats.TestTracker_* and
// switcher.TestRoleSwitcher_*, against a real running process).
func TestReplicationStatsPollerStopsOnStandbyRole(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getReplStatsSetup(t)

	standbyPort, ok := setup.MetricsPorts[setup.StandbyMultipooler.Name]
	require.True(t, ok, "no metrics port for standby %s", setup.StandbyMultipooler.Name)

	_, ok = scrapeAndFind(t, standbyPort, "mg_pooler_replication_replay_lag_seconds", nil)
	assert.False(t, ok, "standby must never export replicationstats gauges — it is never the writable leader")
}
