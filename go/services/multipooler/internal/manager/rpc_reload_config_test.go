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
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
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

	resp, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{})
	require.NoError(t, err)

	assert.True(t, pgctld.reloadConfigCalled, "pgctld ReloadConfig should be triggered")
	require.NotNil(t, resp.ConfigLoadTime, "config_load_time should be set on success")
	assert.Equal(t, 2100, resp.GetConfigLoadTime().AsTime().Year())
	assert.Empty(t, resp.GetMismatches())
	assert.False(t, resp.GetNeedsRestart())
}

// fileSettingsColumns are the columns checkExpectedSettings selects from the
// pg_file_settings / pg_settings join, in order.
var fileSettingsColumns = []string{"name", "setting", "applied", "error", "context"}

// addFileSettings registers the pre-reload pg_file_settings check query on the
// mock with the given rows. Each row is {name, setting, applied, error, context};
// use nil for a NULL error or context.
func addFileSettings(qs *mock.QueryService, rows [][]any) {
	qs.AddQueryPattern("pg_file_settings", mock.MakeQueryResult(fileSettingsColumns, rows))
}

// TestReloadConfig_ExpectedSettings_ReloadsWhenFileMatches verifies that when the
// file already carries every expected setting with the desired value and each is
// reload-applicable, ReloadConfig performs the reload and returns a set
// config_load_time (the success signal) with no mismatches.
func TestReloadConfig_ExpectedSettings_ReloadsWhenFileMatches(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	addFileSettings(qs, [][]any{
		{"work_mem", "32MB", true, nil, "user"},
		{"max_connections", "100", true, nil, "postmaster"},
	})
	qs.AddQueryPattern("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{futureConfLoadTime}}))

	resp, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{
		ExpectedSettings: map[string]string{"work_mem": "32MB", "max_connections": "100"},
	})
	require.NoError(t, err)

	assert.True(t, pgctld.reloadConfigCalled, "a matching file should trigger the reload")
	require.NotNil(t, resp.GetConfigLoadTime(), "config_load_time is the success signal")
	assert.Empty(t, resp.GetMismatches())
	assert.False(t, resp.GetNeedsRestart())
}

// TestReloadConfig_ExpectedSettings_StaleFile verifies that when the file still
// carries the old value (the write has not synced yet), ReloadConfig does NOT
// reload and reports the mismatch with the actual file value.
func TestReloadConfig_ExpectedSettings_StaleFile(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	// No pg_conf_load_time pattern is registered: the reload must not be reached.
	addFileSettings(qs, [][]any{
		{"work_mem", "16MB", true, nil, "user"},
	})

	resp, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{
		ExpectedSettings: map[string]string{"work_mem": "32MB"},
	})
	require.NoError(t, err)

	assert.False(t, pgctld.reloadConfigCalled, "a stale file must not trigger the reload")
	assert.Nil(t, resp.GetConfigLoadTime(), "no reload means no config_load_time")
	assert.False(t, resp.GetNeedsRestart())
	require.Len(t, resp.GetMismatches(), 1)
	m := resp.GetMismatches()[0]
	assert.Equal(t, "work_mem", m.GetName())
	assert.False(t, m.GetRequiresRestart())
	assert.Empty(t, m.GetError(), "a stale value is valid, just not the desired one")
}

// TestReloadConfig_ExpectedSettings_Missing verifies that an expected setting
// absent from the file entirely blocks the reload and is reported by name.
func TestReloadConfig_ExpectedSettings_Missing(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	// The file mentions some other setting but not the expected one.
	addFileSettings(qs, [][]any{
		{"work_mem", "32MB", true, nil, "user"},
	})

	resp, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{
		ExpectedSettings: map[string]string{"statement_timeout": "5s"},
	})
	require.NoError(t, err)

	assert.False(t, pgctld.reloadConfigCalled, "an absent setting must not trigger the reload")
	assert.Nil(t, resp.GetConfigLoadTime())
	require.Len(t, resp.GetMismatches(), 1)
	m := resp.GetMismatches()[0]
	assert.Equal(t, "statement_timeout", m.GetName())
	assert.False(t, m.GetRequiresRestart())
	assert.Empty(t, m.GetError())
}

// TestReloadConfig_ExpectedSettings_NeedsRestart verifies that when the file
// carries the desired value for a postmaster-context GUC that a reload cannot
// apply (applied=false), ReloadConfig skips the reload and reports needs_restart.
func TestReloadConfig_ExpectedSettings_NeedsRestart(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	addFileSettings(qs, [][]any{
		{"shared_buffers", "256MB", false, "setting could not be applied", "postmaster"},
	})

	resp, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{
		ExpectedSettings: map[string]string{"shared_buffers": "256MB"},
	})
	require.NoError(t, err)

	assert.False(t, pgctld.reloadConfigCalled, "a restart-only change must not trigger the reload")
	assert.Nil(t, resp.GetConfigLoadTime())
	assert.True(t, resp.GetNeedsRestart())
	require.Len(t, resp.GetMismatches(), 1)
	m := resp.GetMismatches()[0]
	assert.Equal(t, "shared_buffers", m.GetName())
	assert.True(t, m.GetRequiresRestart())
	assert.Equal(t, "setting could not be applied", m.GetError())
}

// TestReloadConfig_ExpectedSettings_EffectiveOccurrence verifies that when a
// setting appears more than once in the file, the applied occurrence is the one
// compared against the expected value regardless of its position.
func TestReloadConfig_ExpectedSettings_EffectiveOccurrence(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	// Two occurrences ordered by seqno: the earlier one is applied, the later one
	// is shadowed (applied=false). The applied occurrence carries the value that
	// would take effect.
	addFileSettings(qs, [][]any{
		{"work_mem", "32MB", true, nil, "user"},
		{"work_mem", "64MB", false, nil, "user"},
	})
	qs.AddQueryPattern("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{futureConfLoadTime}}))

	resp, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{
		ExpectedSettings: map[string]string{"work_mem": "32MB"},
	})
	require.NoError(t, err)

	assert.True(t, pgctld.reloadConfigCalled)
	require.NotNil(t, resp.GetConfigLoadTime(), "the applied occurrence matches expected, so the reload runs")
	assert.Empty(t, resp.GetMismatches())
}

// TestReloadConfig_ExpectedSettings_QueryFails verifies that a failure to read
// pg_file_settings during the pre-reload check is surfaced as an error and the
// reload is not attempted.
func TestReloadConfig_ExpectedSettings_QueryFails(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	qs.AddQueryPatternWithError("pg_file_settings", errors.New("permission denied"))

	_, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{
		ExpectedSettings: map[string]string{"work_mem": "32MB"},
	})
	require.Error(t, err)
	assert.False(t, pgctld.reloadConfigCalled, "a failed check must not trigger the reload")
	assert.Contains(t, err.Error(), "check expected settings before reload")
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

	resp, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{})
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

	resp, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{})
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

	_, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "confirm configuration reload")
}
