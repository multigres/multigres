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
	assert.True(t, resp.GetAllApplied(), "an empty expectation is vacuously all-applied")
	assert.Empty(t, resp.GetMismatches())
	assert.False(t, resp.GetNeedsRestart())
}

// fileSettingsColumns are the columns verifyExpectedSettings selects from the
// pg_file_settings / pg_settings join, in order.
var fileSettingsColumns = []string{"name", "setting", "applied", "error", "pending_restart"}

// addFileSettings registers the pg_file_settings verification query on the mock
// with the given rows. Each row is {name, setting, applied, error, pending_restart};
// use nil for a NULL error or pending_restart.
func addFileSettings(qs *mock.QueryService, rows [][]any) {
	qs.AddQueryPattern("pg_file_settings", mock.MakeQueryResult(fileSettingsColumns, rows))
}

// TestReloadConfig_ExpectedSettings_AllApplied verifies that when every expected
// setting is present, matches, and is applied, the response reports all_applied
// with no mismatches.
func TestReloadConfig_ExpectedSettings_AllApplied(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	qs.AddQueryPattern("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{futureConfLoadTime}}))
	addFileSettings(qs, [][]any{
		{"work_mem", "32MB", true, nil, nil},
		{"max_connections", "100", true, nil, false},
	})

	resp, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{
		ExpectedSettings: map[string]string{"work_mem": "32MB", "max_connections": "100"},
	})
	require.NoError(t, err)

	assert.True(t, resp.GetAllApplied())
	assert.Empty(t, resp.GetMismatches())
	assert.False(t, resp.GetNeedsRestart())
}

// TestReloadConfig_ExpectedSettings_StaleFile verifies that when the file
// PostgreSQL re-read still carries the old value (kubelet has not synced the
// write yet), the mismatch is reported with the actual file value and no restart
// is signalled.
func TestReloadConfig_ExpectedSettings_StaleFile(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	qs.AddQueryPattern("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{futureConfLoadTime}}))
	addFileSettings(qs, [][]any{
		{"work_mem", "16MB", true, nil, false},
	})

	resp, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{
		ExpectedSettings: map[string]string{"work_mem": "32MB"},
	})
	require.NoError(t, err)

	assert.False(t, resp.GetAllApplied())
	assert.False(t, resp.GetNeedsRestart())
	require.Len(t, resp.GetMismatches(), 1)
	m := resp.GetMismatches()[0]
	assert.Equal(t, "work_mem", m.GetName())
	assert.Equal(t, "32MB", m.GetExpected())
	assert.Equal(t, "16MB", m.GetActual())
	assert.True(t, m.GetPresent())
	assert.True(t, m.GetApplied())
	assert.False(t, m.GetPendingRestart())
}

// TestReloadConfig_ExpectedSettings_Missing verifies that an expected setting
// absent from the file entirely is reported with present=false and an empty
// actual value.
func TestReloadConfig_ExpectedSettings_Missing(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	qs.AddQueryPattern("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{futureConfLoadTime}}))
	// The file mentions some other setting but not the expected one.
	addFileSettings(qs, [][]any{
		{"work_mem", "32MB", true, nil, false},
	})

	resp, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{
		ExpectedSettings: map[string]string{"statement_timeout": "5s"},
	})
	require.NoError(t, err)

	assert.False(t, resp.GetAllApplied())
	require.Len(t, resp.GetMismatches(), 1)
	m := resp.GetMismatches()[0]
	assert.Equal(t, "statement_timeout", m.GetName())
	assert.Equal(t, "5s", m.GetExpected())
	assert.Empty(t, m.GetActual())
	assert.False(t, m.GetPresent())
	assert.False(t, m.GetApplied())
}

// TestReloadConfig_ExpectedSettings_NeedsRestart verifies that a setting written
// correctly in the file but not applied because it requires a restart
// (pg_settings.pending_restart) sets needs_restart. PostgreSQL surfaces the
// generic "setting could not be applied" error for this case, so the restart
// signal comes from pending_restart rather than the error text.
func TestReloadConfig_ExpectedSettings_NeedsRestart(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	qs.AddQueryPattern("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{futureConfLoadTime}}))
	addFileSettings(qs, [][]any{
		{"shared_buffers", "256MB", false, "setting could not be applied", true},
	})

	resp, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{
		ExpectedSettings: map[string]string{"shared_buffers": "256MB"},
	})
	require.NoError(t, err)

	assert.False(t, resp.GetAllApplied())
	assert.True(t, resp.GetNeedsRestart())
	require.Len(t, resp.GetMismatches(), 1)
	m := resp.GetMismatches()[0]
	assert.Equal(t, "shared_buffers", m.GetName())
	assert.Equal(t, "256MB", m.GetActual())
	assert.True(t, m.GetPresent())
	assert.False(t, m.GetApplied())
	assert.True(t, m.GetPendingRestart())
	assert.Equal(t, "setting could not be applied", m.GetError())
}

// TestReloadConfig_ExpectedSettings_EffectiveOccurrence verifies that when a
// setting appears more than once in the file, the applied occurrence is the one
// compared against the expected value regardless of its position.
func TestReloadConfig_ExpectedSettings_EffectiveOccurrence(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	qs.AddQueryPattern("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{futureConfLoadTime}}))
	// Two occurrences ordered by seqno: the earlier one is applied, the later one
	// is shadowed (applied=false). The applied occurrence carries the value that
	// took effect.
	addFileSettings(qs, [][]any{
		{"work_mem", "32MB", true, nil, false},
		{"work_mem", "64MB", false, nil, false},
	})

	resp, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{
		ExpectedSettings: map[string]string{"work_mem": "32MB"},
	})
	require.NoError(t, err)

	assert.True(t, resp.GetAllApplied(), "applied occurrence matches expected")
	assert.Empty(t, resp.GetMismatches())
}

// TestReloadConfig_ExpectedSettings_QueryFails verifies that a failure to read
// pg_file_settings after a successful reload is surfaced as an error.
func TestReloadConfig_ExpectedSettings_QueryFails(t *testing.T) {
	pgctld := &mockPgctldClient{}
	pm, qs := newReloadConfigTestManager(t, pgctld)

	qs.AddQueryPattern("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{futureConfLoadTime}}))
	qs.AddQueryPatternWithError("pg_file_settings", errors.New("permission denied"))

	_, err := pm.ReloadConfig(context.Background(), &multipoolermanagerdatapb.ReloadConfigRequest{
		ExpectedSettings: map[string]string{"work_mem": "32MB"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "verify reloaded settings")
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
