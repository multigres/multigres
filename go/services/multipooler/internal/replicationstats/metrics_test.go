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

package replicationstats

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/tools/telemetry"
)

func setupTestMetrics(t *testing.T) (*Metrics, *sdkmetric.ManualReader) {
	t.Helper()
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))
	m, err := NewMetrics()
	require.NoError(t, err)
	return m, setup.MetricReader
}

func collectAggregation(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.Aggregation {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == name {
				return m.Data
			}
		}
	}
	return nil
}

func TestMetrics_NilReceiverIsNoop(t *testing.T) {
	var m *Metrics
	require.NotPanics(t, func() {
		m.setSnapshot([]ConnStats{{ConnID: "1", User: "u"}})
		_ = m.currentSnapshot()
		require.NoError(t, m.Close())
	})
}

func TestMetrics_ObserveEmitsAllFields(t *testing.T) {
	m, reader := setupTestMetrics(t)

	m.setSnapshot([]ConnStats{{
		User: "replicator", ConnID: "42",
		ReplayLag: 1.5, HaveReplayLag: true,
		LastAckAge: 2.5, HaveAck: true,
		LastMsgAge: 3.5, HaveMsgAge: true,
		SlotName: "sub1", HaveSlot: true, RetainedWAL: 4096,
	}})

	lagAgg := collectAggregation(t, reader, "mg.pooler.replication.replay_lag")
	require.NotNil(t, lagAgg, "replay_lag gauge not emitted")
	lagGauge := lagAgg.(metricdata.Gauge[float64])
	require.Len(t, lagGauge.DataPoints, 1)
	require.Equal(t, 1.5, lagGauge.DataPoints[0].Value)
	connID, present := lagGauge.DataPoints[0].Attributes.Value(attribute.Key(attrConnID))
	require.True(t, present)
	require.Equal(t, "42", connID.AsString())

	ackAgg := collectAggregation(t, reader, "mg.pooler.replication.last_ack_age")
	require.NotNil(t, ackAgg)
	require.Equal(t, 2.5, ackAgg.(metricdata.Gauge[float64]).DataPoints[0].Value)

	msgAgg := collectAggregation(t, reader, "mg.pooler.replication.last_message_age")
	require.NotNil(t, msgAgg)
	require.Equal(t, 3.5, msgAgg.(metricdata.Gauge[float64]).DataPoints[0].Value)

	slotAgg := collectAggregation(t, reader, "mg.pooler.replication.slot_retained_wal")
	require.NotNil(t, slotAgg)
	slotGauge := slotAgg.(metricdata.Gauge[int64])
	require.Equal(t, int64(4096), slotGauge.DataPoints[0].Value)
	slotName, present := slotGauge.DataPoints[0].Attributes.Value(attribute.Key(attrSlotName))
	require.True(t, present)
	require.Equal(t, "sub1", slotName.AsString())

	activeAgg := collectAggregation(t, reader, "mg.pooler.replication.active_connections")
	require.NotNil(t, activeAgg, "active_connections gauge not emitted")
	activeGauge := activeAgg.(metricdata.Gauge[int64])
	require.Len(t, activeGauge.DataPoints, 1)
	require.Equal(t, int64(1), activeGauge.DataPoints[0].Value)
	activeUser, present := activeGauge.DataPoints[0].Attributes.Value(attribute.Key(attrUser))
	require.True(t, present)
	require.Equal(t, "replicator", activeUser.AsString())
}

// TestMetrics_ObserveSkipsUnsetFields verifies a connection missing a value
// (e.g. no ack yet, no matching slot) doesn't emit a spurious zero for that
// gauge.
func TestMetrics_ObserveSkipsUnsetFields(t *testing.T) {
	m, reader := setupTestMetrics(t)

	m.setSnapshot([]ConnStats{{User: "replicator", ConnID: "1"}})

	require.Nil(t, collectAggregation(t, reader, "mg.pooler.replication.replay_lag"))
	require.Nil(t, collectAggregation(t, reader, "mg.pooler.replication.last_ack_age"))
	require.Nil(t, collectAggregation(t, reader, "mg.pooler.replication.last_message_age"))
	require.Nil(t, collectAggregation(t, reader, "mg.pooler.replication.slot_retained_wal"))

	// The connection itself is still active, so it's still counted.
	activeAgg := collectAggregation(t, reader, "mg.pooler.replication.active_connections")
	require.NotNil(t, activeAgg)
	require.Equal(t, int64(1), activeAgg.(metricdata.Gauge[int64]).DataPoints[0].Value)
}

// TestMetrics_RetiredConnectionStopsReporting is the regression test for the
// whole point of using observable (not synchronous) gauges here: conn_id is
// never reused (reserved.Pool seeds it from a boot-time nanosecond counter),
// so a synchronous gauge's cumulative aggregation would retain connection
// 1's data point forever once recorded. An observable gauge's callback only
// reports what's actively in the current snapshot, so a retired connection
// must disappear from the very next collection rather than lingering as a
// stale "zombie" series.
func TestMetrics_RetiredConnectionStopsReporting(t *testing.T) {
	m, reader := setupTestMetrics(t)

	m.setSnapshot([]ConnStats{{User: "replicator", ConnID: "1", ReplayLag: 0.5, HaveReplayLag: true}})
	lagAgg := collectAggregation(t, reader, "mg.pooler.replication.replay_lag")
	require.NotNil(t, lagAgg, "connection 1 should be reported while active")
	require.Len(t, lagAgg.(metricdata.Gauge[float64]).DataPoints, 1)

	// Connection 1 disconnects; connection 2 (a fresh, never-reused ID) takes
	// its place.
	m.setSnapshot([]ConnStats{{User: "replicator", ConnID: "2", ReplayLag: 0.1, HaveReplayLag: true}})
	lagAgg = collectAggregation(t, reader, "mg.pooler.replication.replay_lag")
	require.NotNil(t, lagAgg)
	dataPoints := lagAgg.(metricdata.Gauge[float64]).DataPoints
	require.Len(t, dataPoints, 1, "connection 1 must not linger as a stale series once retired")
	connID, present := dataPoints[0].Attributes.Value(attribute.Key(attrConnID))
	require.True(t, present)
	require.Equal(t, "2", connID.AsString())

	// All connections disconnect.
	m.setSnapshot(nil)
	require.Nil(t, collectAggregation(t, reader, "mg.pooler.replication.replay_lag"),
		"no connections active means no data points at all, not a stale one")
	require.Nil(t, collectAggregation(t, reader, "mg.pooler.replication.active_connections"))
}

func TestMetrics_ActiveConnectionsCountedPerUser(t *testing.T) {
	m, reader := setupTestMetrics(t)

	m.setSnapshot([]ConnStats{
		{User: "replicator", ConnID: "1"},
		{User: "replicator", ConnID: "2"},
		{User: "other", ConnID: "3"},
	})

	activeAgg := collectAggregation(t, reader, "mg.pooler.replication.active_connections")
	require.NotNil(t, activeAgg)
	dataPoints := activeAgg.(metricdata.Gauge[int64]).DataPoints
	require.Len(t, dataPoints, 2, "one data point per distinct user")

	byUser := make(map[string]int64, len(dataPoints))
	for _, dp := range dataPoints {
		user, present := dp.Attributes.Value(attribute.Key(attrUser))
		require.True(t, present)
		byUser[user.AsString()] = dp.Value
	}
	require.Equal(t, int64(2), byUser["replicator"])
	require.Equal(t, int64(1), byUser["other"])
}

func TestMetrics_CloseStopsReporting(t *testing.T) {
	m, reader := setupTestMetrics(t)

	m.setSnapshot([]ConnStats{{User: "replicator", ConnID: "1", ReplayLag: 0.5, HaveReplayLag: true}})
	require.NotNil(t, collectAggregation(t, reader, "mg.pooler.replication.replay_lag"))

	require.NoError(t, m.Close())
	require.NoError(t, m.Close(), "Close must be idempotent")

	require.Nil(t, collectAggregation(t, reader, "mg.pooler.replication.replay_lag"),
		"the callback must not run after Close, even though the snapshot was never cleared")
}
