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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

func writableState() servingstate.State {
	return servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRolePrimary}}
}

func notWritableState() servingstate.State {
	return servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}}
}

func TestTracker_OnStateChangeStartsPollerWhenWritable(t *testing.T) {
	qs := mock.NewQueryService()
	qs.AddQueryPattern("FROM pg_stat_replication", mock.MakeQueryResult(columns(), [][]any{}))
	m, err := NewMetrics()
	require.NoError(t, err)

	tr := NewTracker(qs, m, slog.Default(), 250)
	defer tr.Close()

	assert.False(t, tr.Poller().IsOpen())

	require.NoError(t, tr.OnStateChange(t.Context(), writableState()))
	assert.True(t, tr.Poller().IsOpen())
}

func TestTracker_OnStateChangeStopsPollerWhenNotWritable(t *testing.T) {
	qs := mock.NewQueryService()
	qs.AddQueryPattern("FROM pg_stat_replication", mock.MakeQueryResult(columns(), [][]any{}))
	m, err := NewMetrics()
	require.NoError(t, err)

	tr := NewTracker(qs, m, slog.Default(), 250)
	defer tr.Close()

	require.NoError(t, tr.OnStateChange(t.Context(), writableState()))
	assert.True(t, tr.Poller().IsOpen())

	require.NoError(t, tr.OnStateChange(t.Context(), notWritableState()))
	assert.False(t, tr.Poller().IsOpen())
}

func TestTracker_CloseStopsPollerRegardlessOfState(t *testing.T) {
	qs := mock.NewQueryService()
	qs.AddQueryPattern("FROM pg_stat_replication", mock.MakeQueryResult(columns(), [][]any{}))
	m, err := NewMetrics()
	require.NoError(t, err)

	tr := NewTracker(qs, m, slog.Default(), 250)

	require.NoError(t, tr.OnStateChange(t.Context(), writableState()))
	assert.True(t, tr.Poller().IsOpen())

	tr.Close()
	assert.False(t, tr.Poller().IsOpen())
}

// TestTracker_CloseUnregistersMetricsCallback verifies Close retires the
// OTel callback for good, not just the poller's ticker. This matters
// because manager.startReplicationStats creates a brand-new Tracker (and
// Metrics) on every connection reopen cycle — without unregistering the old
// one, each cycle would leak one more permanently-registered callback.
func TestTracker_CloseUnregistersMetricsCallback(t *testing.T) {
	qs := mock.NewQueryService()
	qs.AddQueryPattern("FROM pg_stat_replication", mock.MakeQueryResult(columns(), [][]any{}))
	m, reader := setupTestMetrics(t)

	tr := NewTracker(qs, m, slog.Default(), 250)
	require.NoError(t, tr.OnStateChange(t.Context(), writableState()))

	m.setSnapshot([]ConnStats{{User: "replicator", ConnID: "1", ReplayLag: 0.5, HaveReplayLag: true}})
	require.NotNil(t, collectAggregation(t, reader, "mg.pooler.replication.replay_lag"))

	tr.Close()

	assert.Nil(t, collectAggregation(t, reader, "mg.pooler.replication.replay_lag"),
		"Tracker.Close must unregister the metrics callback for good, not just stop the poller")
}
