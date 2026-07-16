// Copyright 2025 Supabase, Inc.
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

package manager

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// expectReloadConfig sets up the mock query expectations for one successful call
// to MultipoolerManager.reloadPostgresConfig: read pg_conf_load_time (pre), run
// pg_reload_conf, then read pg_conf_load_time (post) returning a different value
// so the wait loop exits on the first poll.
func expectReloadConfig(m *mock.QueryService) {
	m.AddQueryPatternOnce("SELECT pg_conf_load_time",
		mock.MakeQueryResult([]string{"pg_conf_load_time"}, [][]any{{"2026-01-01 00:00:00+00"}}))
	m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce("SELECT pg_conf_load_time",
		mock.MakeQueryResult([]string{"pg_conf_load_time"}, [][]any{{"2026-01-01 00:00:01+00"}}))
}

// expectReloadConfigFailure sets up the mock query expectations for a call to
// MultipoolerManager.reloadPostgresConfig where pg_reload_conf itself fails.
// The wait loop is never entered.
func expectReloadConfigFailure(m *mock.QueryService, reloadErr error) {
	m.AddQueryPatternOnce("SELECT pg_conf_load_time",
		mock.MakeQueryResult([]string{"pg_conf_load_time"}, [][]any{{"2026-01-01 00:00:00+00"}}))
	m.AddQueryPatternOnceWithError("SELECT pg_reload_conf", reloadErr)
}

// mustPoolerIDFromAppName constructs a consensus.ReplicaID from a "cell_name" application name string.
// Panics if parsing or construction fails; for use in tests only.
func mustPoolerIDFromAppName(appName string) consensus.ReplicaID {
	id, err := consensus.ParseApplicationName(appName)
	if err != nil {
		panic(err)
	}
	pid, err := consensus.NewReplicaID(id)
	if err != nil {
		panic(err)
	}
	return pid
}

func TestNewPoolerID(t *testing.T) {
	tests := []struct {
		name            string
		id              *clustermetadatapb.ID
		expectedAppName string // always asserted, including error cases
		expectError     bool
	}{
		{
			name:            "standard ID",
			id:              &clustermetadatapb.ID{Cell: "us-west", Name: "replica-1"},
			expectedAppName: "us-west_replica-1",
		},
		{
			name:            "single character values",
			id:              &clustermetadatapb.ID{Cell: "a", Name: "b"},
			expectedAppName: "a_b",
		},
		{
			name:            "hyphenated names",
			id:              &clustermetadatapb.ID{Cell: "us-east-1a", Name: "primary-db-001"},
			expectedAppName: "us-east-1a_primary-db-001",
		},
		{
			name:            "numeric values",
			id:              &clustermetadatapb.ID{Cell: "zone1", Name: "pooler-001"},
			expectedAppName: "zone1_pooler-001",
		},
		{
			name:            "exactly 63 characters",
			id:              &clustermetadatapb.ID{Cell: "us-east-1a", Name: strings.Repeat("x", 52)}, // 11+52=63
			expectedAppName: "us-east-1a_" + strings.Repeat("x", 52),
		},
		// Error cases: approximate appName returned alongside the error.
		{
			name:            "nil ID",
			id:              nil,
			expectError:     true,
			expectedAppName: "<nil>",
		},
		{
			name:            "empty cell",
			id:              &clustermetadatapb.ID{Name: "replica-1"},
			expectError:     true,
			expectedAppName: "<unknown>_replica-1",
		},
		{
			name:            "empty name",
			id:              &clustermetadatapb.ID{Cell: "zone1"},
			expectError:     true,
			expectedAppName: "zone1_<unknown>",
		},
		{
			name:            "cell contains underscore",
			id:              &clustermetadatapb.ID{Cell: "us_west", Name: "replica-1"},
			expectError:     true,
			expectedAppName: "us_west_replica-1",
		},
		{
			name:            "name contains underscore",
			id:              &clustermetadatapb.ID{Cell: "zone1", Name: "replica_1"},
			expectError:     true,
			expectedAppName: "zone1_replica_1",
		},
		{
			name:            "exceeds 63 characters",
			id:              &clustermetadatapb.ID{Cell: "us-east-1a", Name: strings.Repeat("x", 53)}, // 11+53=64
			expectError:     true,
			expectedAppName: "us-east-1a_" + strings.Repeat("x", 53), // full string returned, not truncated
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := consensus.NewReplicaID(tt.id)
			if tt.expectError {
				require.Error(t, err)
				assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.id, result.ID())
			}
			assert.Equal(t, tt.expectedAppName, result.AppName())
		})
	}
}

func TestPoolerIDFromAppName(t *testing.T) {
	tests := []struct {
		name         string
		appName      string
		expectedCell string
		expectedName string
		expectError  bool
	}{
		{
			name:         "standard app name",
			appName:      "us-west_replica-1",
			expectedCell: "us-west",
			expectedName: "replica-1",
		},
		{
			name:         "zone with hyphens and numbers",
			appName:      "us-east-1a_primary-db-001",
			expectedCell: "us-east-1a",
			expectedName: "primary-db-001",
		},
		{
			name:         "simple zone and name",
			appName:      "zone1_pooler-001",
			expectedCell: "zone1",
			expectedName: "pooler-001",
		},
		{
			name:         "no underscore - best-effort ID uses appName as Name",
			appName:      "nounderscore",
			expectedName: "nounderscore", // best-effort: raw appName preserved in Name
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := consensus.NewReplicaIDFromAppName(tt.appName)
			if tt.expectError {
				require.Error(t, err)
				// Best-effort ID is always returned: caller can include it rather than drop the member
				require.NotNil(t, result.ID())
				assert.Equal(t, clustermetadatapb.ID_MULTIPOOLER, result.ID().Component)
				assert.Equal(t, tt.expectedName, result.ID().Name, "Name should preserve raw appName")
			} else {
				require.NoError(t, err)
				assert.Equal(t, clustermetadatapb.ID_MULTIPOOLER, result.ID().Component)
				assert.Equal(t, tt.expectedCell, result.ID().Cell)
				assert.Equal(t, tt.expectedName, result.ID().Name)
				assert.Equal(t, tt.appName, result.AppName())
			}
		})
	}
}

func TestToPoolerIDs(t *testing.T) {
	t.Run("all valid", func(t *testing.T) {
		ids := []*clustermetadatapb.ID{
			{Cell: "zone1", Name: "replica-1"},
			{Cell: "zone2", Name: "replica-2"},
		}
		result, err := consensus.ToReplicaIDs(ids)
		require.NoError(t, err)
		require.Len(t, result, 2)
		assert.Equal(t, "zone1_replica-1", result[0].AppName())
		assert.Equal(t, "zone2_replica-2", result[1].AppName())
	})

	t.Run("nil slice", func(t *testing.T) {
		result, err := consensus.ToReplicaIDs(nil)
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("invalid entry returns approximate value and error", func(t *testing.T) {
		ids := []*clustermetadatapb.ID{
			{Cell: "zone1", Name: "replica-1"},
			nil,
		}
		result, err := consensus.ToReplicaIDs(ids)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ids[1]")
		// Full slice returned despite the error.
		require.Len(t, result, 2)
		assert.Equal(t, "zone1_replica-1", result[0].AppName())
		assert.Equal(t, "<nil>", result[1].AppName())
	})

	t.Run("first error is reported when multiple entries are invalid", func(t *testing.T) {
		ids := []*clustermetadatapb.ID{
			nil,
			{Cell: "zone1", Name: "replica-1"},
			nil,
		}
		result, err := consensus.ToReplicaIDs(ids)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ids[0]") // first error, not ids[2]
		require.Len(t, result, 3)
		assert.Equal(t, "<nil>", result[0].AppName())
		assert.Equal(t, "zone1_replica-1", result[1].AppName())
		assert.Equal(t, "<nil>", result[2].AppName())
	})
}

func TestFormatStandbyList(t *testing.T) {
	tests := []struct {
		name     string
		names    []consensus.ReplicaID
		expected string
	}{
		{
			name:     "empty list",
			names:    []consensus.ReplicaID{},
			expected: "",
		},
		{
			name:     "single standby",
			names:    []consensus.ReplicaID{mustPoolerIDFromAppName("zone1_replica-1")},
			expected: `"zone1_replica-1"`,
		},
		{
			name:     "multiple standbys",
			names:    []consensus.ReplicaID{mustPoolerIDFromAppName("zone1_replica-1"), mustPoolerIDFromAppName("zone2_replica-2"), mustPoolerIDFromAppName("zone3_replica-3")},
			expected: `"zone1_replica-1", "zone2_replica-2", "zone3_replica-3"`,
		},
		{
			name:     "two standbys",
			names:    []consensus.ReplicaID{mustPoolerIDFromAppName("east_standby-a"), mustPoolerIDFromAppName("west_standby-b")},
			expected: `"east_standby-a", "west_standby-b"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, consensus.FormatStandbyList(tt.names))
		})
	}
}

func TestBuildSynchronousStandbyNamesValue(t *testing.T) {
	tests := []struct {
		name        string
		method      multipoolermanagerdatapb.SynchronousMethod
		numSync     int32
		names       []consensus.ReplicaID
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "empty standby list returns empty string",
			method:      multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			numSync:     1,
			names:       []consensus.ReplicaID{},
			expected:    "",
			expectError: false,
		},
		{
			name:        "FIRST method with single standby",
			method:      multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			numSync:     1,
			names:       []consensus.ReplicaID{mustPoolerIDFromAppName("zone1_replica-1")},
			expected:    `FIRST 1 ("zone1_replica-1")`,
			expectError: false,
		},
		{
			name:        "FIRST method with multiple standbys",
			method:      multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			numSync:     2,
			names:       []consensus.ReplicaID{mustPoolerIDFromAppName("zone1_replica-1"), mustPoolerIDFromAppName("zone2_replica-2"), mustPoolerIDFromAppName("zone3_replica-3")},
			expected:    `FIRST 2 ("zone1_replica-1", "zone2_replica-2", "zone3_replica-3")`,
			expectError: false,
		},
		{
			name:        "ANY method with multiple standbys",
			method:      multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			numSync:     1,
			names:       []consensus.ReplicaID{mustPoolerIDFromAppName("zone1_replica-1"), mustPoolerIDFromAppName("zone2_replica-2")},
			expected:    `ANY 1 ("zone1_replica-1", "zone2_replica-2")`,
			expectError: false,
		},
		{
			name:        "ANY method with three standbys and numSync=2",
			method:      multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			numSync:     2,
			names:       []consensus.ReplicaID{mustPoolerIDFromAppName("a_1"), mustPoolerIDFromAppName("b_2"), mustPoolerIDFromAppName("c_3")},
			expected:    `ANY 2 ("a_1", "b_2", "c_3")`,
			expectError: false,
		},
		{
			name:        "invalid method returns error",
			method:      multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_UNSPECIFIED,
			numSync:     1,
			names:       []consensus.ReplicaID{mustPoolerIDFromAppName("zone1_replica-1")},
			expected:    "",
			expectError: true,
			errorMsg:    "invalid synchronous method",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := consensus.BuildSynchronousStandbyNamesValue(tt.method, tt.numSync, tt.names)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestValidateStandbyIDs(t *testing.T) {
	tests := []struct {
		name        string
		standbyIDs  []*clustermetadatapb.ID
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid single standby",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "zone1", Name: "replica-1"},
			},
			expectError: false,
		},
		{
			name: "valid multiple standbys",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "zone1", Name: "replica-1"},
				{Cell: "zone2", Name: "replica-2"},
			},
			expectError: false,
		},
		{
			name: "valid with hyphens",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "us-west-1", Name: "replica-db-001"},
			},
			expectError: false,
		},
		{
			name:        "empty list returns error",
			standbyIDs:  []*clustermetadatapb.ID{},
			expectError: true,
			errorMsg:    "standby_ids cannot be empty",
		},
		{
			name: "nil ID returns error",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "zone1", Name: "replica-1"},
				nil,
			},
			expectError: true,
			errorMsg:    "ids[1]: nil ID",
		},
		{
			name: "empty cell returns error",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "", Name: "replica-1"},
			},
			expectError: true,
			errorMsg:    "ids[0]: empty cell",
		},
		{
			name: "empty name returns error",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "zone1", Name: ""},
			},
			expectError: true,
			errorMsg:    "ids[0]: empty name",
		},
		{
			name: "underscore in cell returns error",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "us_west", Name: "replica-1"},
			},
			expectError: true,
			errorMsg:    "cell contains underscore",
		},
		{
			name: "underscore in name returns error",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "zone1", Name: "replica_1"},
			},
			expectError: true,
			errorMsg:    "name contains underscore",
		},
		{
			name: "multiple underscores in cell",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "us_west_1a", Name: "replica"},
			},
			expectError: true,
			errorMsg:    "cell contains underscore",
		},
		{
			name: "multiple underscores in name",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "zone1", Name: "replica_test_1"},
			},
			expectError: true,
			errorMsg:    "name contains underscore",
		},
		{
			name: "underscore in second standby",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "zone1", Name: "replica-1"},
				{Cell: "zone2", Name: "replica_2"},
			},
			expectError: true,
			errorMsg:    "ids[1]: name contains underscore",
		},
		{
			name: "exceeds 63 character limit",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "us-east-1a", Name: strings.Repeat("x", 53)}, // "us-east-1a_" (11) + 53 = 64
			},
			expectError: true,
			errorMsg:    "ids[0]: application name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			names, err := consensus.ValidateStandbyIDs(tt.standbyIDs)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
				assert.Len(t, names, len(tt.standbyIDs))
			}
		})
	}
}

func TestApplyAddOperation(t *testing.T) {
	s1 := mustPoolerIDFromAppName("zone1_replica-1")
	s2 := mustPoolerIDFromAppName("zone2_replica-2")
	s3 := mustPoolerIDFromAppName("zone3_replica-3")

	tests := []struct {
		name     string
		current  []consensus.ReplicaID
		incoming []consensus.ReplicaID
		expected []consensus.ReplicaID
	}{
		{
			name:     "add to empty list",
			current:  []consensus.ReplicaID{},
			incoming: []consensus.ReplicaID{s1},
			expected: []consensus.ReplicaID{s1},
		},
		{
			name:     "add new standby to existing list",
			current:  []consensus.ReplicaID{s1},
			incoming: []consensus.ReplicaID{s2},
			expected: []consensus.ReplicaID{s1, s2},
		},
		{
			name:     "add multiple new standbys",
			current:  []consensus.ReplicaID{s1},
			incoming: []consensus.ReplicaID{s2, s3},
			expected: []consensus.ReplicaID{s1, s2, s3},
		},
		{
			name:     "idempotent - add existing standby",
			current:  []consensus.ReplicaID{s1, s2},
			incoming: []consensus.ReplicaID{s1},
			expected: []consensus.ReplicaID{s1, s2},
		},
		{
			name:     "idempotent - add mix of existing and new",
			current:  []consensus.ReplicaID{s1, s2},
			incoming: []consensus.ReplicaID{s2, s3},
			expected: []consensus.ReplicaID{s1, s2, s3},
		},
		{
			name:     "add empty list does nothing",
			current:  []consensus.ReplicaID{s1},
			incoming: []consensus.ReplicaID{},
			expected: []consensus.ReplicaID{s1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := consensus.ApplyAddOperation(tt.current, tt.incoming)
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}

func TestApplyRemoveOperation(t *testing.T) {
	s1 := mustPoolerIDFromAppName("zone1_replica-1")
	s2 := mustPoolerIDFromAppName("zone2_replica-2")
	s3 := mustPoolerIDFromAppName("zone3_replica-3")

	tests := []struct {
		name     string
		current  []consensus.ReplicaID
		remove   []consensus.ReplicaID
		expected []consensus.ReplicaID
	}{
		{
			name:     "remove from single item list",
			current:  []consensus.ReplicaID{s1},
			remove:   []consensus.ReplicaID{s1},
			expected: []consensus.ReplicaID{},
		},
		{
			name:     "remove one from multiple",
			current:  []consensus.ReplicaID{s1, s2, s3},
			remove:   []consensus.ReplicaID{s2},
			expected: []consensus.ReplicaID{s1, s3},
		},
		{
			name:     "remove multiple standbys",
			current:  []consensus.ReplicaID{s1, s2, s3},
			remove:   []consensus.ReplicaID{s1, s3},
			expected: []consensus.ReplicaID{s2},
		},
		{
			name:     "idempotent - remove non-existent standby",
			current:  []consensus.ReplicaID{s1, s2},
			remove:   []consensus.ReplicaID{s3},
			expected: []consensus.ReplicaID{s1, s2},
		},
		{
			name:     "idempotent - remove mix of existing and non-existent",
			current:  []consensus.ReplicaID{s1, s2},
			remove:   []consensus.ReplicaID{s2, s3},
			expected: []consensus.ReplicaID{s1},
		},
		{
			name:     "remove empty list does nothing",
			current:  []consensus.ReplicaID{s1, s2},
			remove:   []consensus.ReplicaID{},
			expected: []consensus.ReplicaID{s1, s2},
		},
		{
			name:     "remove from empty list",
			current:  []consensus.ReplicaID{},
			remove:   []consensus.ReplicaID{s1},
			expected: []consensus.ReplicaID{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := consensus.ApplyRemoveOperation(tt.current, tt.remove)
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}

func TestPostgresMode(t *testing.T) {
	tests := []struct {
		name        string
		setupMock   func(*mock.QueryService)
		expectError bool
		expectMode  pgmode.Mode
	}{
		{
			name: "primary server - not in recovery",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
			},
			expectError: false,
			expectMode:  pgmode.Primary,
		},
		{
			name: "standby server - in recovery",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
			},
			expectError: false,
			expectMode:  pgmode.InRecovery,
		},
		{
			name: "query error",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("SELECT pg_is_in_recovery", errors.New("connection done"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(t, "default", "0-inf")

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			mode, err := pm.postgresMode(ctx)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectMode, mode)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestGetPrimaryLSN(t *testing.T) {
	tests := []struct {
		name        string
		setupMock   func(*mock.QueryService)
		expectError bool
		expectedLSN string
	}{
		{
			name: "successful query",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT pg_current_wal_lsn", mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/3000000"}}))
			},
			expectError: false,
			expectedLSN: "0/3000000",
		},
		{
			name: "different LSN format",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT pg_current_wal_lsn", mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"1/ABCD1234"}}))
			},
			expectError: false,
			expectedLSN: "1/ABCD1234",
		},
		{
			name: "query error",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("SELECT pg_current_wal_lsn", errors.New("connection done"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(t, "default", "0-inf")

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			result, err := pm.getPrimaryLSN(ctx)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedLSN, result)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestGetStandbyReplayLSN(t *testing.T) {
	tests := []struct {
		name        string
		setupMock   func(*mock.QueryService)
		expectError bool
		expectedLSN string
	}{
		{
			name: "successful query",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn"}, [][]any{{"0/2000000"}}))
			},
			expectError: false,
			expectedLSN: "0/2000000",
		},
		{
			name: "different LSN format",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn"}, [][]any{{"5/FFFF0000"}}))
			},
			expectError: false,
			expectedLSN: "5/FFFF0000",
		},
		{
			name: "query error",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("SELECT pg_last_wal_replay_lsn", errors.New("connection done"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(t, "default", "0-inf")

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			result, err := pm.getStandbyReplayLSN(ctx)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedLSN, result)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestParseSyncReplicationConfig(t *testing.T) {
	tests := []struct {
		name           string
		standbyNames   string
		syncCommit     string
		expectError    bool
		validateResult func(t *testing.T, config *multipoolermanagerdatapb.SynchronousReplicationConfiguration)
	}{
		{
			name:         "FIRST method with multiple standbys",
			standbyNames: `FIRST 2 ("zone1_replica-1", "zone2_replica-2", "zone3_replica-3")`,
			syncCommit:   "on",
			validateResult: func(t *testing.T, config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) {
				assert.Equal(t, multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST, config.SynchronousMethod)
				assert.Equal(t, int32(2), config.NumSync)
				assert.Equal(t, 3, len(config.StandbyIds))
				assert.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON, config.SynchronousCommit)
			},
		},
		{
			name:         "ANY method with single standby",
			standbyNames: `ANY 1 ("zone1_replica-1")`,
			syncCommit:   "remote_apply",
			validateResult: func(t *testing.T, config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) {
				assert.Equal(t, multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY, config.SynchronousMethod)
				assert.Equal(t, int32(1), config.NumSync)
				assert.Equal(t, 1, len(config.StandbyIds))
				assert.Equal(t, "zone1", config.StandbyIds[0].Cell)
				assert.Equal(t, "replica-1", config.StandbyIds[0].Name)
				assert.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY, config.SynchronousCommit)
			},
		},
		{
			name:         "empty synchronous_standby_names",
			standbyNames: "",
			syncCommit:   "local",
			validateResult: func(t *testing.T, config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) {
				assert.Equal(t, 0, len(config.StandbyIds))
				assert.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL, config.SynchronousCommit)
			},
		},
		{
			name:         "synchronous_commit off",
			standbyNames: "",
			syncCommit:   "off",
			validateResult: func(t *testing.T, config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) {
				assert.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_OFF, config.SynchronousCommit)
			},
		},
		{
			name:         "synchronous_commit remote_write",
			standbyNames: "",
			syncCommit:   "remote_write",
			validateResult: func(t *testing.T, config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) {
				assert.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE, config.SynchronousCommit)
			},
		},
		{
			name:         "unknown synchronous_commit value",
			standbyNames: "",
			syncCommit:   "bogus",
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseSyncReplicationConfig(tt.standbyNames, tt.syncCommit)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				if tt.validateResult != nil {
					tt.validateResult(t, result)
				}
			}
		})
	}
}

func TestSetSynchronousStandbyNames(t *testing.T) {
	tests := []struct {
		name              string
		synchronousMethod multipoolermanagerdatapb.SynchronousMethod
		numSync           int32
		names             []consensus.ReplicaID
		setupMock         func(*mock.QueryService)
		expectError       bool
	}{
		{
			name:              "FIRST method with multiple standbys",
			synchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			numSync:           1,
			names:             []consensus.ReplicaID{mustPoolerIDFromAppName("cell1_pooler1"), mustPoolerIDFromAppName("cell1_pooler2")},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM SET synchronous_standby_names", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:              "ANY method with multiple standbys",
			synchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			numSync:           2,
			names:             []consensus.ReplicaID{mustPoolerIDFromAppName("cell1_pooler1"), mustPoolerIDFromAppName("cell2_pooler2"), mustPoolerIDFromAppName("cell2_pooler3")},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM SET synchronous_standby_names", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:              "db exec error",
			synchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			numSync:           1,
			names:             []consensus.ReplicaID{mustPoolerIDFromAppName("cell1_pooler1")},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("ALTER SYSTEM SET synchronous_standby_names", errors.New("exec error"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(t, "default", "0-inf")

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			err := pm.setSynchronousStandbyNames(ctx, tt.synchronousMethod, tt.numSync, tt.names)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestValidateExpectedLSN(t *testing.T) {
	tests := []struct {
		name          string
		expectedLSN   string
		setupMock     func(*mock.QueryService)
		expectError   bool
		errorContains string
	}{
		{
			name:        "empty expectedLSN - no validation",
			expectedLSN: "",
			setupMock:   func(m *mock.QueryService) {},
			expectError: false,
		},
		{
			name:        "LSN match with paused replay",
			expectedLSN: "0/3000000",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"},
					[][]any{{"0/3000000", "t"}}))
			},
			expectError: false,
		},
		{
			name:        "LSN match with running replay (warning only)",
			expectedLSN: "0/3000000",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"},
					[][]any{{"0/3000000", "f"}}))
			},
			expectError: false,
		},
		{
			name:        "LSN mismatch",
			expectedLSN: "0/3000000",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"},
					[][]any{{"0/2000000", "t"}}))
			},
			expectError:   true,
			errorContains: "LSN mismatch",
		},
		{
			name:        "query error",
			expectedLSN: "0/3000000",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("SELECT pg_last_wal_replay_lsn", errors.New("database error"))
			},
			expectError:   true,
			errorContains: "failed to get current replay LSN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(t, "default", "0-inf")

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			err := pm.validateExpectedLSN(ctx, tt.expectedLSN)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestValidateSyncReplicationParams(t *testing.T) {
	tests := []struct {
		name        string
		numSync     int32
		standbyIDs  []*clustermetadatapb.ID
		expectError bool
		errorMsg    string
	}{
		{
			name:    "Valid single standby",
			numSync: 1,
			standbyIDs: []*clustermetadatapb.ID{
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "standby1",
				},
			},
			expectError: false,
		},
		{
			name:    "Valid multiple standbys",
			numSync: 2,
			standbyIDs: []*clustermetadatapb.ID{
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "standby1",
				},
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "standby2",
				},
			},
			expectError: false,
		},
		{
			name:        "Valid empty standbys",
			numSync:     0,
			standbyIDs:  []*clustermetadatapb.ID{},
			expectError: false,
		},
		{
			name:        "Valid numSync zero with nil standbys",
			numSync:     0,
			standbyIDs:  nil,
			expectError: false,
		},
		{
			name:        "Invalid negative numSync",
			numSync:     -1,
			standbyIDs:  []*clustermetadatapb.ID{},
			expectError: true,
			errorMsg:    "num_sync must be non-negative, got: -1",
		},
		{
			name:    "Invalid numSync exceeds standby count",
			numSync: 3,
			standbyIDs: []*clustermetadatapb.ID{
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "standby1",
				},
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "standby2",
				},
			},
			expectError: true,
			errorMsg:    "num_sync (3) cannot exceed number of standby_ids (2)",
		},
		{
			name:    "Invalid nil standby ID",
			numSync: 1,
			standbyIDs: []*clustermetadatapb.ID{
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "standby1",
				},
				nil,
			},
			expectError: true,
			errorMsg:    "ids[1]: nil ID",
		},
		{
			name:    "Invalid empty cell",
			numSync: 1,
			standbyIDs: []*clustermetadatapb.ID{
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "",
					Name:      "standby1",
				},
			},
			expectError: true,
			errorMsg:    "ids[0]: empty cell",
		},
		{
			name:    "Invalid empty name",
			numSync: 1,
			standbyIDs: []*clustermetadatapb.ID{
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "",
				},
			},
			expectError: true,
			errorMsg:    "ids[0]: empty name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := consensus.ValidateSyncReplicationParams(tt.numSync, tt.standbyIDs)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				code := mterrors.Code(err)
				assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, code)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPauseReplication(t *testing.T) {
	tests := []struct {
		name           string
		mode           multipoolermanagerdatapb.ReplicationPauseMode
		wait           bool
		setupMock      func(*mock.QueryService)
		expectError    bool
		errorContains  string
		expectStatus   bool // true if we expect a non-nil status to be returned
		validateResult func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus)
	}{
		{
			name: "PauseReplayOnly with wait=true",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
			wait: true,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT pg_wal_replay_pause", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/3000000", "0/3000100", "t", "paused", "2025-01-15 10:00:00+00", "host=primary port=5432", "streaming", nil, nil, nil}}))
			},
			expectError:  false,
			expectStatus: true,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Equal(t, "0/3000000", status.LastReplayLsn)
				assert.True(t, status.IsWalReplayPaused)
			},
		},
		{
			name: "PauseReplayOnly with wait=false",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT pg_wal_replay_pause", mock.MakeQueryResult(nil, nil))
			},
			expectError:  false,
			expectStatus: false,
		},
		{
			name: "PauseReplayOnly fails on pause command",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
			wait: true,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("SELECT pg_wal_replay_pause", errors.New("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to pause WAL replay",
		},
		{
			name: "PauseReceiverOnly with wait=true",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			wait: true,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
				m.AddQueryPatternOnce("SELECT COUNT", mock.MakeQueryResult([]string{"count", "status", "primary_conninfo"}, [][]any{{int64(0), "", ""}}))
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/4000000", "", "f", "not paused", "2025-01-15 11:00:00+00", "", "", nil, nil, nil}}))
			},
			expectError:  false,
			expectStatus: true,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Equal(t, "0/4000000", status.LastReplayLsn)
				assert.False(t, status.IsWalReplayPaused)
				assert.Empty(t, status.LastReceiveLsn)
			},
		},
		{
			name: "PauseReceiverOnly with wait=false",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
			},
			expectError:  false,
			expectStatus: false,
		},
		{
			name: "PauseReceiverOnly fails on ALTER SYSTEM",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("ALTER SYSTEM RESET primary_conninfo", errors.New("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to clear primary_conninfo",
		},
		{
			name: "PauseReceiverOnly fails on reload",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfigFailure(m, errors.New("reload failed"))
			},
			expectError:   true,
			errorContains: "failed to reload PostgreSQL configuration",
		},
		{
			name: "PauseReplayAndReceiver with wait=true",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER,
			wait: true,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
				m.AddQueryPatternOnce("SELECT COUNT", mock.MakeQueryResult([]string{"count", "status", "primary_conninfo"}, [][]any{{int64(0), "", ""}}))
				// First query for waitForReceiverDisconnect - consumed after first match
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/5000000", "", "f", "not paused", "2025-01-15 12:00:00+00", "", "", nil, nil, nil}}))
				m.AddQueryPatternOnce("SELECT pg_wal_replay_pause", mock.MakeQueryResult(nil, nil))
				// Second query for waitForReplicationPause
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/5000000", "", "t", "paused", "2025-01-15 12:00:00+00", "", "", nil, nil, nil}}))
			},
			expectError:  false,
			expectStatus: true,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Equal(t, "0/5000000", status.LastReplayLsn)
				assert.True(t, status.IsWalReplayPaused)
			},
		},
		{
			name: "PauseReplayAndReceiver with wait=false",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
				m.AddQueryPatternOnce("SELECT COUNT", mock.MakeQueryResult([]string{"count", "status", "primary_conninfo"}, [][]any{{int64(0), "", ""}}))
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/5000000", "", "f", "not paused", "2025-01-15 12:00:00+00", "", "", nil, nil, nil}}))
				m.AddQueryPatternOnce("SELECT pg_wal_replay_pause", mock.MakeQueryResult(nil, nil))
			},
			expectError:  false,
			expectStatus: false,
		},
		{
			name: "PauseReplayAndReceiver fails on clearing conninfo",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("ALTER SYSTEM RESET primary_conninfo", errors.New("reset failed"))
			},
			expectError:   true,
			errorContains: "failed to clear primary_conninfo",
		},
		{
			name: "PauseReplayAndReceiver fails on receiver disconnect wait",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
				m.AddQueryPatternOnceWithError("SELECT COUNT", errors.New("query failed"))
			},
			expectError:   true,
			errorContains: "failed to query pg_stat_wal_receiver",
		},
		{
			// count > 0 but status=waiting with empty primary_conninfo should be
			// treated as effectively disconnected: the walreceiver can't transition
			// out of WAITING without a non-empty primary_conninfo, and we hold the
			// action lock so nothing else can repopulate it.
			name: "PauseReceiverOnly accepts WALRCV_WAITING+empty primary_conninfo as disconnected",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			wait: true,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
				m.AddQueryPatternOnce("SELECT COUNT", mock.MakeQueryResult(
					[]string{"count", "status", "primary_conninfo"},
					[][]any{{int64(1), "waiting", ""}}))
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/6000000", "", "f", "not paused", "2025-01-15 13:00:00+00", "", "waiting", nil, nil, nil}}))
			},
			expectError:  false,
			expectStatus: true,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Equal(t, "0/6000000", status.LastReplayLsn)
			},
		},
		{
			name: "PauseReplayAndReceiver fails on pause",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
				m.AddQueryPatternOnce("SELECT COUNT", mock.MakeQueryResult([]string{"count", "status", "primary_conninfo"}, [][]any{{int64(0), "", ""}}))
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/5000000", "", "f", "not paused", "2025-01-15 12:00:00+00", "", "", nil, nil, nil}}))
				m.AddQueryPatternOnceWithError("SELECT pg_wal_replay_pause", errors.New("pause failed"))
			},
			expectError:   true,
			errorContains: "failed to pause WAL replay",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(t, "default", "0-inf")

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			status, err := pm.pauseReplication(ctx, tt.mode, tt.wait)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, status)
			} else {
				require.NoError(t, err)
				if tt.expectStatus {
					require.NotNil(t, status, "Expected non-nil status when wait=true")
					if tt.validateResult != nil {
						tt.validateResult(t, status)
					}
				} else {
					assert.Nil(t, status, "Expected nil status when wait=false")
				}
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestWaitForReplayStabilizeRetriesPostgresRestart(t *testing.T) {
	pm, m := newTestManagerWithMock(t, "default", "0-inf")
	m.AddQueryPatternOnceWithError("^SELECT pg_last_wal_replay_lsn", os.ErrNotExist)
	for range 3 {
		m.AddQueryPatternOnce("^SELECT pg_last_wal_replay_lsn", mock.MakeQueryResult(
			[]string{"replay_lsn", "is_paused"}, [][]any{{"0/5000000", false}}))
	}
	m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
		[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "last_xact_replay_ts", "primary_conninfo", "status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
		[][]any{{"0/5000000", "0/5000000", false, "not paused", nil, "", nil, nil, nil, nil}}))

	status, err := pm.waitForReplayStabilize(t.Context())
	require.NoError(t, err)
	require.Equal(t, "0/5000000", status.GetLastReplayLsn())
	require.NoError(t, m.ExpectationsWereMet())
}

// TestWaitForReceiverDisconnect_Timeout covers the diagnostic timeout branch
// added with the better-diagnostics fix: when the wait budget expires, the
// function should return the canonical DEADLINE_EXCEEDED error rather than
// leaking a pool-context-expired wrapper. The 10s budget is hardcoded, but
// shrinking the parent context shortens the effective deadline via the
// min-deadline semantics of context.WithTimeout.
func TestWaitForReceiverDisconnect_Timeout(t *testing.T) {
	t.Run("deadline already expired surfaces timeout before polling", func(t *testing.T) {
		pm, _ := newTestManagerWithMock(t, "default", "0-inf")
		// Deadline already in the past: the first retry attempt observes the
		// expired context and returns the timeout without ever polling, so no
		// query mock is needed.
		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()

		status, err := pm.waitForReceiverDisconnect(ctx)
		require.Error(t, err)
		assert.Nil(t, status)
		assert.Contains(t, err.Error(), "timeout waiting for WAL receiver to disconnect")
		assert.Equal(t, mtrpcpb.Code_DEADLINE_EXCEEDED, mterrors.Code(err))
	})

	t.Run("deadline expires while polls keep returning still-streaming", func(t *testing.T) {
		pm, mockQueryService := newTestManagerWithMock(t, "default", "0-inf")
		// Persistent (non-consumeOnce) pattern: the function polls repeatedly
		// (immediately, then with exponential backoff) until the deadline trips.
		mockQueryService.AddQueryPattern("SELECT COUNT", mock.MakeQueryResult(
			[]string{"count", "status", "primary_conninfo"},
			[][]any{{int64(1), "streaming", "host=primary port=5432"}}))

		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()

		status, err := pm.waitForReceiverDisconnect(ctx)
		require.Error(t, err)
		assert.Nil(t, status)
		assert.Contains(t, err.Error(), "timeout waiting for WAL receiver to disconnect")
		assert.Equal(t, mtrpcpb.Code_DEADLINE_EXCEEDED, mterrors.Code(err))
	})

	t.Run("parent context cancelled surfaces cancellation, not timeout", func(t *testing.T) {
		pm, _ := newTestManagerWithMock(t, "default", "0-inf")
		ctx, cancel := context.WithCancel(t.Context())
		cancel() // cancel immediately

		status, err := pm.waitForReceiverDisconnect(ctx)
		require.Error(t, err)
		assert.Nil(t, status)
		assert.Contains(t, err.Error(), "context cancelled while waiting for WAL receiver to disconnect")
		assert.NotEqual(t, mtrpcpb.Code_DEADLINE_EXCEEDED, mterrors.Code(err))
	})
}

func TestResetPrimaryConnInfo(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*mock.QueryService)
		expectError   bool
		errorContains string
	}{
		{
			name: "successful reset",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
			},
			expectError: false,
		},
		{
			name: "ALTER SYSTEM fails",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("ALTER SYSTEM RESET primary_conninfo", errors.New("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to clear primary_conninfo",
		},
		{
			name: "pg_reload_conf fails",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfigFailure(m, errors.New("reload failed"))
			},
			expectError:   true,
			errorContains: "failed to reload PostgreSQL configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(t, "default", "0-inf")

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			err := pm.resetPrimaryConnInfo(ctx)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestClearSyncReplicationForDemotion(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*mock.QueryService)
		expectError   bool
		errorContains string
	}{
		{
			name: "successful clear synchronous replication",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET synchronous_standby_names", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
			},
			expectError: false,
		},
		{
			name: "ALTER SYSTEM fails",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("ALTER SYSTEM RESET synchronous_standby_names", errors.New("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to clear synchronous_standby_names for demotion",
		},
		{
			name: "pg_reload_conf fails",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET synchronous_standby_names", mock.MakeQueryResult(nil, nil))
				expectReloadConfigFailure(m, errors.New("reload failed"))
			},
			expectError:   true,
			errorContains: "failed to reload configuration for demotion",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(t, "default", "0-inf")

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			err := pm.clearSyncReplicationForDemotion(ctx)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestQueryReplicationStatus(t *testing.T) {
	tests := []struct {
		name           string
		setupMock      func(*mock.QueryService)
		expectError    bool
		validateResult func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus)
	}{
		{
			name: "All fields with valid values",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/3000000", "0/3000100", "f", "not paused", "2025-01-15 10:00:00+00", "host=primary port=5432", "streaming", "2025-01-15 10:00:05+00", "10s", "60s"}}))
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Equal(t, "0/3000000", status.LastReplayLsn)
				assert.Equal(t, "0/3000100", status.LastReceiveLsn)
				assert.False(t, status.IsWalReplayPaused)
				assert.Equal(t, "not paused", status.WalReplayPauseState)
				assert.Equal(t, "2025-01-15 10:00:00+00", status.LastXactReplayTimestamp)
				assert.NotNil(t, status.PrimaryConnInfo)
				assert.Equal(t, "primary", status.PrimaryConnInfo.Host)
				assert.Equal(t, "streaming", status.WalReceiverStatus)
				assert.NotNil(t, status.LastMsgReceiveTime)
				assert.NotNil(t, status.WalReceiverStatusInterval)
				assert.Equal(t, 10*time.Second, status.WalReceiverStatusInterval.AsDuration())
				assert.NotNil(t, status.WalReceiverTimeout)
				assert.Equal(t, 60*time.Second, status.WalReceiverTimeout.AsDuration())
			},
		},
		{
			name: "NULL LSN values (primary server case)",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"", "", "f", "not paused", "", "", "", nil, nil, nil}}))
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Empty(t, status.LastReplayLsn, "LastReplayLsn should be empty when NULL")
				assert.Empty(t, status.LastReceiveLsn, "LastReceiveLsn should be empty when NULL")
				assert.False(t, status.IsWalReplayPaused)
				assert.Equal(t, "not paused", status.WalReplayPauseState)
				assert.Empty(t, status.LastXactReplayTimestamp, "LastXactReplayTimestamp should be empty when NULL")
				assert.Empty(t, status.WalReceiverStatus, "WalReceiverStatus should be empty on primary")
				assert.Nil(t, status.LastMsgReceiveTime)
				assert.Nil(t, status.WalReceiverStatusInterval)
				assert.Nil(t, status.WalReceiverTimeout)
			},
		},
		{
			name: "Paused replication with valid LSNs",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/4000000", "0/4000200", "t", "paused", "2025-01-15 11:00:00+00", "host=primary port=5432 user=replicator application_name=standby1", "streaming", "2025-01-15 11:00:05+00", "10s", "60s"}}))
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Equal(t, "0/4000000", status.LastReplayLsn)
				assert.Equal(t, "0/4000200", status.LastReceiveLsn)
				assert.True(t, status.IsWalReplayPaused)
				assert.Equal(t, "paused", status.WalReplayPauseState)
				assert.Equal(t, "2025-01-15 11:00:00+00", status.LastXactReplayTimestamp)
				assert.NotNil(t, status.PrimaryConnInfo)
				assert.Equal(t, "primary", status.PrimaryConnInfo.Host)
				assert.Equal(t, int32(5432), status.PrimaryConnInfo.Port)
				assert.Equal(t, "replicator", status.PrimaryConnInfo.User)
				assert.Equal(t, "standby1", status.PrimaryConnInfo.ApplicationName)
				assert.Equal(t, "streaming", status.WalReceiverStatus)
				assert.NotNil(t, status.WalReceiverStatusInterval)
				assert.Equal(t, 10*time.Second, status.WalReceiverStatusInterval.AsDuration())
				assert.NotNil(t, status.WalReceiverTimeout)
				assert.Equal(t, 60*time.Second, status.WalReceiverTimeout.AsDuration())
			},
		},
		{
			name: "Mixed NULL and valid values",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/5000000", "", "f", "not paused", "", "host=primary port=5432", "", nil, nil, nil}}))
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Equal(t, "0/5000000", status.LastReplayLsn, "LastReplayLsn should be populated")
				assert.Empty(t, status.LastReceiveLsn, "LastReceiveLsn should be empty when NULL")
				assert.False(t, status.IsWalReplayPaused)
				assert.Empty(t, status.LastXactReplayTimestamp, "LastXactReplayTimestamp should be empty when NULL")
				assert.Empty(t, status.WalReceiverStatus)
				assert.Nil(t, status.LastMsgReceiveTime)
				assert.Nil(t, status.WalReceiverStatusInterval)
				assert.Nil(t, status.WalReceiverTimeout)
			},
		},
		{
			name: "Query error",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("pg_last_wal_replay_lsn", errors.New("connection done"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(t, "default", "0-inf")

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			status, err := pm.queryReplicationStatus(ctx)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, status)
			} else {
				require.NoError(t, err)
				require.NotNil(t, status)
				if tt.validateResult != nil {
					tt.validateResult(t, status)
				}
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}
