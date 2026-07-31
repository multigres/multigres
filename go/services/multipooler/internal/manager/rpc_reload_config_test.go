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

package manager

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
)

// Config load times are read as Unix epoch seconds (see readConfLoadTime). The
// baseline the RPC captures is time.Now(), so the year-2100 epoch always counts
// as advanced and the year-2000 epoch always counts as stale, independent of
// when the test runs.
const (
	futureConfLoadTime = "4102444800" // 2100-01-01 00:00:00 UTC
	pastConfLoadTime   = "946684800"  // 2000-01-01 00:00:00 UTC
)

// newReloadConfigTestManager builds a MultipoolerManager wired with a mock
// query service and a mock pgctld client, ready for ReloadConfig tests.
func newReloadConfigTestManager(t *testing.T, pgctld *mockPgctldClient) (*MultipoolerManager, *mock.QueryService) {
	t.Helper()
	mockQueryService := mock.NewQueryService()
	multipooler := &clustermetadatapb.Multipooler{
		ShardKey: &clustermetadatapb.ShardKey{TableGroup: "default", Shard: "0-inf"},
	}
	pm := &MultipoolerManager{
		logger:       slog.New(slog.DiscardHandler),
		qsc:          &mockPoolerController{queryService: mockQueryService},
		config:       &Config{},
		record:       newRecordFromProto(multipooler),
		serviceID:    &clustermetadatapb.ID{Cell: "test-cell", Name: "test-pooler"},
		state:        ManagerStateReady,
		actionLock:   actionlock.NewActionLock(),
		pgctldClient: pgctld,
	}
	return pm, mockQueryService
}

// TestReloadConfig_Success verifies that a reload triggers pgctld and returns
// the advanced pg_conf_load_time() once observed.
func TestReloadConfig_Success(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	qs.AddQueryPattern("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{futureConfLoadTime}}))

	resp, err := pm.ReloadConfig(context.Background())
	require.NoError(t, err)

	assert.True(t, pgctld.reloadConfigCalled, "pgctld ReloadConfig should be triggered")
	require.NotNil(t, resp.ConfigLoadTime, "config_load_time should be set on success")
	assert.Equal(t, 2100, resp.GetConfigLoadTime().AsTime().Year())
}

// TestReloadConfig_WaitsForAdvance verifies that the poll loop keeps reading
// pg_conf_load_time() until it advances past the trigger baseline, ignoring a
// stale pre-reload value.
func TestReloadConfig_WaitsForAdvance(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	// First poll observes the stale (pre-reload) value; second observes the
	// advanced one. The loop must skip the first and return the second.
	qs.AddQueryPatternOnce("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{pastConfLoadTime}}))
	qs.AddQueryPatternOnce("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{futureConfLoadTime}}))

	resp, err := pm.ReloadConfig(context.Background())
	require.NoError(t, err)

	require.NotNil(t, resp.ConfigLoadTime)
	assert.Equal(t, 2100, resp.GetConfigLoadTime().AsTime().Year(), "should return the advanced value, not the stale one")
	require.NoError(t, qs.ExpectationsWereMet(), "both polls should be consumed")
}

// TestReloadConfig_PostgresNotRunning verifies that when pgctld's reload fails
// (e.g. PostgreSQL not running), ReloadConfig returns an empty response (nil
// config_load_time) without a hard error and does not read pg_conf_load_time().
func TestReloadConfig_PostgresNotRunning(t *testing.T) {
	pgctld := &mockPgctldClient{
		reloadConfigError: errors.New("PostgreSQL is not running"),
	}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	resp, err := pm.ReloadConfig(context.Background())
	require.NoError(t, err, "not-running should be surfaced as an empty response, not an error")

	assert.True(t, pgctld.reloadConfigCalled)
	assert.Nil(t, resp.ConfigLoadTime, "no timestamp when the reload was not delivered")
	// No pg_conf_load_time query should have been registered/consumed.
	require.NoError(t, qs.ExpectationsWereMet())
}

// TestReloadConfig_ConfLoadTimeQueryFails verifies that when the reload signal
// succeeds but reading pg_conf_load_time() fails, the error is surfaced.
func TestReloadConfig_ConfLoadTimeQueryFails(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	qs.AddQueryPatternWithError("pg_conf_load_time", errors.New("connection refused"))

	_, err := pm.ReloadConfig(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "confirm configuration reload")
}
