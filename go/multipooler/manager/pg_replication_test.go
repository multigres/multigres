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
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/mterrors"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestGenerateApplicationName(t *testing.T) {
	tests := []struct {
		name     string
		id       *clustermetadatapb.ID
		expected string
	}{
		{
			name: "standard ID",
			id: &clustermetadatapb.ID{
				Cell: "us-west",
				Name: "replica-1",
			},
			expected: "us-west_replica-1",
		},
		{
			name: "single character values",
			id: &clustermetadatapb.ID{
				Cell: "a",
				Name: "b",
			},
			expected: "a_b",
		},
		{
			name: "hyphenated names",
			id: &clustermetadatapb.ID{
				Cell: "us-east-1a",
				Name: "primary-db-001",
			},
			expected: "us-east-1a_primary-db-001",
		},
		{
			name: "numeric values",
			id: &clustermetadatapb.ID{
				Cell: "zone1",
				Name: "pooler-001",
			},
			expected: "zone1_pooler-001",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateApplicationName(tt.id)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatStandbyList(t *testing.T) {
	tests := []struct {
		name       string
		standbyIDs []*clustermetadatapb.ID
		expected   string
	}{
		{
			name:       "empty list",
			standbyIDs: []*clustermetadatapb.ID{},
			expected:   "",
		},
		{
			name: "single standby",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "zone1", Name: "replica-1"},
			},
			expected: `"zone1_replica-1"`,
		},
		{
			name: "multiple standbys",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "zone1", Name: "replica-1"},
				{Cell: "zone2", Name: "replica-2"},
				{Cell: "zone3", Name: "replica-3"},
			},
			expected: `"zone1_replica-1", "zone2_replica-2", "zone3_replica-3"`,
		},
		{
			name: "two standbys",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "east", Name: "standby-a"},
				{Cell: "west", Name: "standby-b"},
			},
			expected: `"east_standby-a", "west_standby-b"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatStandbyList(tt.standbyIDs)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildSynchronousStandbyNamesValue(t *testing.T) {
	tests := []struct {
		name        string
		method      multipoolermanagerdatapb.SynchronousMethod
		numSync     int32
		standbyIDs  []*clustermetadatapb.ID
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "empty standby list returns empty string",
			method:      multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			numSync:     1,
			standbyIDs:  []*clustermetadatapb.ID{},
			expected:    "",
			expectError: false,
		},
		{
			name:    "FIRST method with single standby",
			method:  multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			numSync: 1,
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "zone1", Name: "replica-1"},
			},
			expected:    `FIRST 1 ("zone1_replica-1")`,
			expectError: false,
		},
		{
			name:    "FIRST method with multiple standbys",
			method:  multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			numSync: 2,
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "zone1", Name: "replica-1"},
				{Cell: "zone2", Name: "replica-2"},
				{Cell: "zone3", Name: "replica-3"},
			},
			expected:    `FIRST 2 ("zone1_replica-1", "zone2_replica-2", "zone3_replica-3")`,
			expectError: false,
		},
		{
			name:    "ANY method with multiple standbys",
			method:  multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			numSync: 1,
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "zone1", Name: "replica-1"},
				{Cell: "zone2", Name: "replica-2"},
			},
			expected:    `ANY 1 ("zone1_replica-1", "zone2_replica-2")`,
			expectError: false,
		},
		{
			name:    "ANY method with three standbys and numSync=2",
			method:  multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			numSync: 2,
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "a", Name: "1"},
				{Cell: "b", Name: "2"},
				{Cell: "c", Name: "3"},
			},
			expected:    `ANY 2 ("a_1", "b_2", "c_3")`,
			expectError: false,
		},
		{
			name:    "invalid method returns error",
			method:  multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_UNSPECIFIED,
			numSync: 1,
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "zone1", Name: "replica-1"},
			},
			expected:    "",
			expectError: true,
			errorMsg:    "invalid synchronous method",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := buildSynchronousStandbyNamesValue(tt.method, tt.numSync, tt.standbyIDs)

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
			errorMsg:    "standby_ids[1] is nil",
		},
		{
			name: "empty cell returns error",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "", Name: "replica-1"},
			},
			expectError: true,
			errorMsg:    "standby_ids[0] has empty cell",
		},
		{
			name: "empty name returns error",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "zone1", Name: ""},
			},
			expectError: true,
			errorMsg:    "standby_ids[0] has empty name",
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
			errorMsg:    "standby_ids[1] name contains underscore",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateStandbyIDs(tt.standbyIDs)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSyncReplicationConfigMatches(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	pm := &MultiPoolerManager{
		logger: logger,
	}

	standby1 := &clustermetadatapb.ID{Cell: "zone1", Name: "replica-1"}
	standby2 := &clustermetadatapb.ID{Cell: "zone2", Name: "replica-2"}
	standby3 := &clustermetadatapb.ID{Cell: "zone3", Name: "replica-3"}

	tests := []struct {
		name      string
		current   *multipoolermanagerdatapb.SynchronousReplicationConfiguration
		requested *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest
		expected  bool
	}{
		{
			name: "perfect match",
			current: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           2,
				StandbyIds:        []*clustermetadatapb.ID{standby1, standby2, standby3},
			},
			requested: &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           2,
				StandbyIds:        []*clustermetadatapb.ID{standby1, standby2, standby3},
			},
			expected: true,
		},
		{
			name: "different synchronous commit level",
			current: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           1,
				StandbyIds:        []*clustermetadatapb.ID{standby1},
			},
			requested: &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           1,
				StandbyIds:        []*clustermetadatapb.ID{standby1},
			},
			expected: false,
		},
		{
			name: "different synchronous method",
			current: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           1,
				StandbyIds:        []*clustermetadatapb.ID{standby1, standby2},
			},
			requested: &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				NumSync:           1,
				StandbyIds:        []*clustermetadatapb.ID{standby1, standby2},
			},
			expected: false,
		},
		{
			name: "different num_sync",
			current: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           1,
				StandbyIds:        []*clustermetadatapb.ID{standby1, standby2},
			},
			requested: &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           2,
				StandbyIds:        []*clustermetadatapb.ID{standby1, standby2},
			},
			expected: false,
		},
		{
			name: "different standby count",
			current: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           1,
				StandbyIds:        []*clustermetadatapb.ID{standby1, standby2},
			},
			requested: &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           1,
				StandbyIds:        []*clustermetadatapb.ID{standby1, standby2, standby3},
			},
			expected: false,
		},
		{
			name: "different standbys same count",
			current: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           1,
				StandbyIds:        []*clustermetadatapb.ID{standby1, standby2},
			},
			requested: &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           1,
				StandbyIds:        []*clustermetadatapb.ID{standby1, standby3},
			},
			expected: false,
		},
		{
			name: "same standbys different order - should match",
			current: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           2,
				StandbyIds:        []*clustermetadatapb.ID{standby1, standby2, standby3},
			},
			requested: &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           2,
				StandbyIds:        []*clustermetadatapb.ID{standby3, standby1, standby2},
			},
			expected: true,
		},
		{
			name: "empty standbys in both",
			current: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           0,
				StandbyIds:        []*clustermetadatapb.ID{},
			},
			requested: &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           0,
				StandbyIds:        []*clustermetadatapb.ID{},
			},
			expected: true,
		},
		{
			name: "empty vs non-empty standbys",
			current: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           0,
				StandbyIds:        []*clustermetadatapb.ID{},
			},
			requested: &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
				SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
				SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync:           0,
				StandbyIds:        []*clustermetadatapb.ID{standby1},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pm.syncReplicationConfigMatches(tt.current, tt.requested)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestApplyAddOperation(t *testing.T) {
	standby1 := &clustermetadatapb.ID{Cell: "zone1", Name: "replica-1"}
	standby2 := &clustermetadatapb.ID{Cell: "zone2", Name: "replica-2"}
	standby3 := &clustermetadatapb.ID{Cell: "zone3", Name: "replica-3"}

	tests := []struct {
		name            string
		currentStandbys []*clustermetadatapb.ID
		newStandbys     []*clustermetadatapb.ID
		expected        []*clustermetadatapb.ID
	}{
		{
			name:            "add to empty list",
			currentStandbys: []*clustermetadatapb.ID{},
			newStandbys:     []*clustermetadatapb.ID{standby1},
			expected:        []*clustermetadatapb.ID{standby1},
		},
		{
			name:            "add new standby to existing list",
			currentStandbys: []*clustermetadatapb.ID{standby1},
			newStandbys:     []*clustermetadatapb.ID{standby2},
			expected:        []*clustermetadatapb.ID{standby1, standby2},
		},
		{
			name:            "add multiple new standbys",
			currentStandbys: []*clustermetadatapb.ID{standby1},
			newStandbys:     []*clustermetadatapb.ID{standby2, standby3},
			expected:        []*clustermetadatapb.ID{standby1, standby2, standby3},
		},
		{
			name:            "idempotent - add existing standby",
			currentStandbys: []*clustermetadatapb.ID{standby1, standby2},
			newStandbys:     []*clustermetadatapb.ID{standby1},
			expected:        []*clustermetadatapb.ID{standby1, standby2},
		},
		{
			name:            "idempotent - add mix of existing and new",
			currentStandbys: []*clustermetadatapb.ID{standby1, standby2},
			newStandbys:     []*clustermetadatapb.ID{standby2, standby3},
			expected:        []*clustermetadatapb.ID{standby1, standby2, standby3},
		},
		{
			name:            "add empty list does nothing",
			currentStandbys: []*clustermetadatapb.ID{standby1},
			newStandbys:     []*clustermetadatapb.ID{},
			expected:        []*clustermetadatapb.ID{standby1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyAddOperation(tt.currentStandbys, tt.newStandbys)
			assert.Equal(t, len(tt.expected), len(result), "length should match")

			// Convert to maps for order-independent comparison
			expectedMap := make(map[string]bool)
			for _, id := range tt.expected {
				expectedMap[generateApplicationName(id)] = true
			}
			resultMap := make(map[string]bool)
			for _, id := range result {
				resultMap[generateApplicationName(id)] = true
			}
			assert.Equal(t, expectedMap, resultMap)
		})
	}
}

func TestApplyRemoveOperation(t *testing.T) {
	standby1 := &clustermetadatapb.ID{Cell: "zone1", Name: "replica-1"}
	standby2 := &clustermetadatapb.ID{Cell: "zone2", Name: "replica-2"}
	standby3 := &clustermetadatapb.ID{Cell: "zone3", Name: "replica-3"}

	tests := []struct {
		name             string
		currentStandbys  []*clustermetadatapb.ID
		standbysToRemove []*clustermetadatapb.ID
		expected         []*clustermetadatapb.ID
	}{
		{
			name:             "remove from single item list",
			currentStandbys:  []*clustermetadatapb.ID{standby1},
			standbysToRemove: []*clustermetadatapb.ID{standby1},
			expected:         []*clustermetadatapb.ID{},
		},
		{
			name:             "remove one from multiple",
			currentStandbys:  []*clustermetadatapb.ID{standby1, standby2, standby3},
			standbysToRemove: []*clustermetadatapb.ID{standby2},
			expected:         []*clustermetadatapb.ID{standby1, standby3},
		},
		{
			name:             "remove multiple standbys",
			currentStandbys:  []*clustermetadatapb.ID{standby1, standby2, standby3},
			standbysToRemove: []*clustermetadatapb.ID{standby1, standby3},
			expected:         []*clustermetadatapb.ID{standby2},
		},
		{
			name:             "idempotent - remove non-existent standby",
			currentStandbys:  []*clustermetadatapb.ID{standby1, standby2},
			standbysToRemove: []*clustermetadatapb.ID{standby3},
			expected:         []*clustermetadatapb.ID{standby1, standby2},
		},
		{
			name:             "idempotent - remove mix of existing and non-existent",
			currentStandbys:  []*clustermetadatapb.ID{standby1, standby2},
			standbysToRemove: []*clustermetadatapb.ID{standby2, standby3},
			expected:         []*clustermetadatapb.ID{standby1},
		},
		{
			name:             "remove empty list does nothing",
			currentStandbys:  []*clustermetadatapb.ID{standby1, standby2},
			standbysToRemove: []*clustermetadatapb.ID{},
			expected:         []*clustermetadatapb.ID{standby1, standby2},
		},
		{
			name:             "remove from empty list",
			currentStandbys:  []*clustermetadatapb.ID{},
			standbysToRemove: []*clustermetadatapb.ID{standby1},
			expected:         []*clustermetadatapb.ID{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyRemoveOperation(tt.currentStandbys, tt.standbysToRemove)
			assert.Equal(t, len(tt.expected), len(result), "length should match")

			// Convert to maps for order-independent comparison
			expectedMap := make(map[string]bool)
			for _, id := range tt.expected {
				expectedMap[generateApplicationName(id)] = true
			}
			resultMap := make(map[string]bool)
			for _, id := range result {
				resultMap[generateApplicationName(id)] = true
			}
			assert.Equal(t, expectedMap, resultMap)
		})
	}
}

func TestApplyReplaceOperation(t *testing.T) {
	standby1 := &clustermetadatapb.ID{Cell: "zone1", Name: "replica-1"}
	standby2 := &clustermetadatapb.ID{Cell: "zone2", Name: "replica-2"}
	standby3 := &clustermetadatapb.ID{Cell: "zone3", Name: "replica-3"}

	tests := []struct {
		name        string
		newStandbys []*clustermetadatapb.ID
		expected    []*clustermetadatapb.ID
	}{
		{
			name:        "replace with single standby",
			newStandbys: []*clustermetadatapb.ID{standby1},
			expected:    []*clustermetadatapb.ID{standby1},
		},
		{
			name:        "replace with multiple standbys",
			newStandbys: []*clustermetadatapb.ID{standby1, standby2, standby3},
			expected:    []*clustermetadatapb.ID{standby1, standby2, standby3},
		},
		{
			name:        "replace with empty list",
			newStandbys: []*clustermetadatapb.ID{},
			expected:    []*clustermetadatapb.ID{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyReplaceOperation(tt.newStandbys)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsPrimary(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tests := []struct {
		name         string
		setupMock    func(sqlmock.Sqlmock)
		expectError  bool
		expectResult bool
	}{
		{
			name: "primary server - not in recovery",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false)
				mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").WillReturnRows(rows)
			},
			expectError:  false,
			expectResult: true,
		},
		{
			name: "standby server - in recovery",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true)
				mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").WillReturnRows(rows)
			},
			expectError:  false,
			expectResult: false,
		},
		{
			name: "query error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").WillReturnError(sql.ErrConnDone)
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer mockDB.Close()

			tt.setupMock(mock)

			pm := &MultiPoolerManager{
				logger: logger,
				db:     mockDB,
			}

			ctx := context.Background()
			result, err := pm.isPrimary(ctx)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectResult, result)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestGetPrimaryLSN(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tests := []struct {
		name        string
		setupMock   func(sqlmock.Sqlmock)
		expectError bool
		expectedLSN string
	}{
		{
			name: "successful query",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("0/3000000")
				mock.ExpectQuery("SELECT pg_current_wal_lsn\\(\\)::text").WillReturnRows(rows)
			},
			expectError: false,
			expectedLSN: "0/3000000",
		},
		{
			name: "different LSN format",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"pg_current_wal_lsn"}).AddRow("1/ABCD1234")
				mock.ExpectQuery("SELECT pg_current_wal_lsn\\(\\)::text").WillReturnRows(rows)
			},
			expectError: false,
			expectedLSN: "1/ABCD1234",
		},
		{
			name: "query error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT pg_current_wal_lsn\\(\\)::text").WillReturnError(sql.ErrConnDone)
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer mockDB.Close()

			tt.setupMock(mock)

			pm := &MultiPoolerManager{
				logger: logger,
				db:     mockDB,
			}

			ctx := context.Background()
			result, err := pm.getPrimaryLSN(ctx)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedLSN, result)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestGetStandbyReplayLSN(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tests := []struct {
		name        string
		setupMock   func(sqlmock.Sqlmock)
		expectError bool
		expectedLSN string
	}{
		{
			name: "successful query",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"pg_last_wal_replay_lsn"}).AddRow("0/2000000")
				mock.ExpectQuery("SELECT pg_last_wal_replay_lsn\\(\\)::text").WillReturnRows(rows)
			},
			expectError: false,
			expectedLSN: "0/2000000",
		},
		{
			name: "different LSN format",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"pg_last_wal_replay_lsn"}).AddRow("5/FFFF0000")
				mock.ExpectQuery("SELECT pg_last_wal_replay_lsn\\(\\)::text").WillReturnRows(rows)
			},
			expectError: false,
			expectedLSN: "5/FFFF0000",
		},
		{
			name: "query error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT pg_last_wal_replay_lsn\\(\\)::text").WillReturnError(sql.ErrConnDone)
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer mockDB.Close()

			tt.setupMock(mock)

			pm := &MultiPoolerManager{
				logger: logger,
				db:     mockDB,
			}

			ctx := context.Background()
			result, err := pm.getStandbyReplayLSN(ctx)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedLSN, result)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestGetSynchronousReplicationConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tests := []struct {
		name           string
		setupMock      func(sqlmock.Sqlmock)
		expectError    bool
		validateResult func(t *testing.T, config *multipoolermanagerdatapb.SynchronousReplicationConfiguration)
	}{
		{
			name: "FIRST method with multiple standbys",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows1 := sqlmock.NewRows([]string{"synchronous_standby_names"}).
					AddRow(`FIRST 2 ("zone1_replica-1", "zone2_replica-2", "zone3_replica-3")`)
				mock.ExpectQuery("SHOW synchronous_standby_names").WillReturnRows(rows1)

				rows2 := sqlmock.NewRows([]string{"synchronous_commit"}).AddRow("on")
				mock.ExpectQuery("SHOW synchronous_commit").WillReturnRows(rows2)
			},
			expectError: false,
			validateResult: func(t *testing.T, config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) {
				assert.Equal(t, multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST, config.SynchronousMethod)
				assert.Equal(t, int32(2), config.NumSync)
				assert.Equal(t, 3, len(config.StandbyIds))
				assert.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON, config.SynchronousCommit)
			},
		},
		{
			name: "ANY method with single standby",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows1 := sqlmock.NewRows([]string{"synchronous_standby_names"}).
					AddRow(`ANY 1 ("zone1_replica-1")`)
				mock.ExpectQuery("SHOW synchronous_standby_names").WillReturnRows(rows1)

				rows2 := sqlmock.NewRows([]string{"synchronous_commit"}).AddRow("remote_apply")
				mock.ExpectQuery("SHOW synchronous_commit").WillReturnRows(rows2)
			},
			expectError: false,
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
			name: "empty synchronous_standby_names",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows1 := sqlmock.NewRows([]string{"synchronous_standby_names"}).AddRow("")
				mock.ExpectQuery("SHOW synchronous_standby_names").WillReturnRows(rows1)

				rows2 := sqlmock.NewRows([]string{"synchronous_commit"}).AddRow("local")
				mock.ExpectQuery("SHOW synchronous_commit").WillReturnRows(rows2)
			},
			expectError: false,
			validateResult: func(t *testing.T, config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) {
				assert.Equal(t, 0, len(config.StandbyIds))
				assert.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL, config.SynchronousCommit)
			},
		},
		{
			name: "synchronous_commit off",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows1 := sqlmock.NewRows([]string{"synchronous_standby_names"}).AddRow("")
				mock.ExpectQuery("SHOW synchronous_standby_names").WillReturnRows(rows1)

				rows2 := sqlmock.NewRows([]string{"synchronous_commit"}).AddRow("off")
				mock.ExpectQuery("SHOW synchronous_commit").WillReturnRows(rows2)
			},
			expectError: false,
			validateResult: func(t *testing.T, config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) {
				assert.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_OFF, config.SynchronousCommit)
			},
		},
		{
			name: "synchronous_commit remote_write",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows1 := sqlmock.NewRows([]string{"synchronous_standby_names"}).AddRow("")
				mock.ExpectQuery("SHOW synchronous_standby_names").WillReturnRows(rows1)

				rows2 := sqlmock.NewRows([]string{"synchronous_commit"}).AddRow("remote_write")
				mock.ExpectQuery("SHOW synchronous_commit").WillReturnRows(rows2)
			},
			expectError: false,
			validateResult: func(t *testing.T, config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) {
				assert.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE, config.SynchronousCommit)
			},
		},
		{
			name: "query error on synchronous_standby_names",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW synchronous_standby_names").WillReturnError(sql.ErrConnDone)
			},
			expectError: true,
		},
		{
			name: "query error on synchronous_commit",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows1 := sqlmock.NewRows([]string{"synchronous_standby_names"}).AddRow("")
				mock.ExpectQuery("SHOW synchronous_standby_names").WillReturnRows(rows1)

				mock.ExpectQuery("SHOW synchronous_commit").WillReturnError(sql.ErrConnDone)
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer mockDB.Close()

			tt.setupMock(mock)

			pm := &MultiPoolerManager{
				logger: logger,
				db:     mockDB,
			}

			ctx := context.Background()
			result, err := pm.getSynchronousReplicationConfig(ctx)

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

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSetSynchronousStandbyNames(t *testing.T) {
	tests := []struct {
		name              string
		synchronousMethod multipoolermanagerdatapb.SynchronousMethod
		numSync           int32
		standbyIDs        []*clustermetadatapb.ID
		expectError       bool
		expectedValue     string
	}{
		{
			name:              "FIRST method with multiple standbys",
			synchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			numSync:           1,
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "cell1", Name: "pooler1"},
				{Cell: "cell1", Name: "pooler2"},
			},
			expectError:   false,
			expectedValue: `FIRST 1 ("cell1_pooler1", "cell1_pooler2")`,
		},
		{
			name:              "ANY method with multiple standbys",
			synchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			numSync:           2,
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "cell1", Name: "pooler1"},
				{Cell: "cell2", Name: "pooler2"},
				{Cell: "cell2", Name: "pooler3"},
			},
			expectError:   false,
			expectedValue: `ANY 2 ("cell1_pooler1", "cell2_pooler2", "cell2_pooler3")`,
		},
		{
			name:              "db exec error",
			synchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			numSync:           1,
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "cell1", Name: "pooler1"},
			},
			expectError:   true,
			expectedValue: `FIRST 1 ("cell1_pooler1")`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer mockDB.Close()

			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			pm := &MultiPoolerManager{
				logger: logger,
				db:     mockDB,
			}

			expectedSQL := fmt.Sprintf("ALTER SYSTEM SET synchronous_standby_names = '%s'", tt.expectedValue)
			if tt.expectError {
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WillReturnError(fmt.Errorf("exec error"))
			} else {
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WillReturnResult(sqlmock.NewResult(0, 0))
			}

			ctx := context.Background()
			err = pm.setSynchronousStandbyNames(ctx, tt.synchronousMethod, tt.numSync, tt.standbyIDs)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestValidateExpectedLSN(t *testing.T) {
	tests := []struct {
		name          string
		expectedLSN   string
		currentLSN    string
		isPaused      bool
		queryError    error
		expectError   bool
		errorContains string
	}{
		{
			name:        "empty expectedLSN - no validation",
			expectedLSN: "",
			expectError: false,
		},
		{
			name:        "LSN match with paused replay",
			expectedLSN: "0/3000000",
			currentLSN:  "0/3000000",
			isPaused:    true,
			expectError: false,
		},
		{
			name:        "LSN match with running replay (warning only)",
			expectedLSN: "0/3000000",
			currentLSN:  "0/3000000",
			isPaused:    false,
			expectError: false,
		},
		{
			name:          "LSN mismatch",
			expectedLSN:   "0/3000000",
			currentLSN:    "0/2000000",
			isPaused:      true,
			expectError:   true,
			errorContains: "LSN mismatch",
		},
		{
			name:          "query error",
			expectedLSN:   "0/3000000",
			queryError:    fmt.Errorf("database error"),
			expectError:   true,
			errorContains: "failed to get current replay LSN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Skip mock setup if expectedLSN is empty (function returns early)
			if tt.expectedLSN == "" {
				mockDB, _, err := sqlmock.New()
				require.NoError(t, err)
				defer mockDB.Close()

				logger := slog.New(slog.NewTextHandler(io.Discard, nil))
				pm := &MultiPoolerManager{
					logger: logger,
					db:     mockDB,
				}

				ctx := context.Background()
				err = pm.validateExpectedLSN(ctx, tt.expectedLSN)
				assert.NoError(t, err)
				return
			}

			mockDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer mockDB.Close()

			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			pm := &MultiPoolerManager{
				logger: logger,
				db:     mockDB,
			}

			query := "SELECT pg_last_wal_replay_lsn()::text, pg_is_wal_replay_paused()"
			if tt.queryError != nil {
				mock.ExpectQuery(regexp.QuoteMeta(query)).
					WillReturnError(tt.queryError)
			} else {
				rows := sqlmock.NewRows([]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"}).
					AddRow(tt.currentLSN, tt.isPaused)
				mock.ExpectQuery(regexp.QuoteMeta(query)).
					WillReturnRows(rows)
			}

			ctx := context.Background()
			err = pm.validateExpectedLSN(ctx, tt.expectedLSN)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
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
			errorMsg:    "standby_ids[1] is nil",
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
			errorMsg:    "standby_ids[0] has empty cell",
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
			errorMsg:    "standby_ids[0] has empty name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSyncReplicationParams(tt.numSync, tt.standbyIDs)

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

func TestQueryReplicationStatus(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		name           string
		setupMock      func(mock sqlmock.Sqlmock)
		expectError    bool
		validateResult func(t *testing.T, status *multipoolermanagerdatapb.ReplicationStatus)
	}{
		{
			name: "All fields with valid values",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{
					"pg_last_wal_replay_lsn",
					"pg_last_wal_receive_lsn",
					"pg_is_wal_replay_paused",
					"pg_get_wal_replay_pause_state",
					"pg_last_xact_replay_timestamp",
					"current_setting",
				}).AddRow(
					"0/3000000",              // replay_lsn
					"0/3000100",              // receive_lsn
					false,                    // is_paused
					"not paused",             // pause_state
					"2025-01-15 10:00:00+00", // last_xact_time
					"host=primary port=5432", // primary_conninfo
				)
				mock.ExpectQuery("SELECT").WillReturnRows(rows)
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.ReplicationStatus) {
				assert.Equal(t, "0/3000000", status.LastReplayLsn)
				assert.Equal(t, "0/3000100", status.LastReceiveLsn)
				assert.False(t, status.IsWalReplayPaused)
				assert.Equal(t, "not paused", status.WalReplayPauseState)
				assert.Equal(t, "2025-01-15 10:00:00+00", status.LastXactReplayTimestamp)
				assert.NotNil(t, status.PrimaryConnInfo)
				assert.Equal(t, "primary", status.PrimaryConnInfo.Host)
			},
		},
		{
			name: "NULL LSN values (primary server case)",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{
					"pg_last_wal_replay_lsn",
					"pg_last_wal_receive_lsn",
					"pg_is_wal_replay_paused",
					"pg_get_wal_replay_pause_state",
					"pg_last_xact_replay_timestamp",
					"current_setting",
				}).AddRow(
					nil,          // replay_lsn is NULL on primary
					nil,          // receive_lsn is NULL on primary
					false,        // is_paused
					"not paused", // pause_state
					nil,          // last_xact_time is NULL on primary
					"",           // empty primary_conninfo on primary
				)
				mock.ExpectQuery("SELECT").WillReturnRows(rows)
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.ReplicationStatus) {
				assert.Empty(t, status.LastReplayLsn, "LastReplayLsn should be empty when NULL")
				assert.Empty(t, status.LastReceiveLsn, "LastReceiveLsn should be empty when NULL")
				assert.False(t, status.IsWalReplayPaused)
				assert.Equal(t, "not paused", status.WalReplayPauseState)
				assert.Empty(t, status.LastXactReplayTimestamp, "LastXactReplayTimestamp should be empty when NULL")
			},
		},
		{
			name: "Paused replication with valid LSNs",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{
					"pg_last_wal_replay_lsn",
					"pg_last_wal_receive_lsn",
					"pg_is_wal_replay_paused",
					"pg_get_wal_replay_pause_state",
					"pg_last_xact_replay_timestamp",
					"current_setting",
				}).AddRow(
					"0/4000000",              // replay_lsn
					"0/4000200",              // receive_lsn
					true,                     // is_paused
					"paused",                 // pause_state
					"2025-01-15 11:00:00+00", // last_xact_time
					"host=primary port=5432 user=replicator application_name=standby1",
				)
				mock.ExpectQuery("SELECT").WillReturnRows(rows)
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.ReplicationStatus) {
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
			},
		},
		{
			name: "Mixed NULL and valid values",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{
					"pg_last_wal_replay_lsn",
					"pg_last_wal_receive_lsn",
					"pg_is_wal_replay_paused",
					"pg_get_wal_replay_pause_state",
					"pg_last_xact_replay_timestamp",
					"current_setting",
				}).AddRow(
					"0/5000000",              // replay_lsn is valid
					nil,                      // receive_lsn is NULL (e.g., no active connection)
					false,                    // is_paused
					"not paused",             // pause_state
					nil,                      // last_xact_time is NULL
					"host=primary port=5432", // primary_conninfo
				)
				mock.ExpectQuery("SELECT").WillReturnRows(rows)
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.ReplicationStatus) {
				assert.Equal(t, "0/5000000", status.LastReplayLsn, "LastReplayLsn should be populated")
				assert.Empty(t, status.LastReceiveLsn, "LastReceiveLsn should be empty when NULL")
				assert.False(t, status.IsWalReplayPaused)
				assert.Empty(t, status.LastXactReplayTimestamp, "LastXactReplayTimestamp should be empty when NULL")
			},
		},
		{
			name: "Query error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT").WillReturnError(sql.ErrConnDone)
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock database
			mockDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer mockDB.Close()

			// Setup mock expectations
			tt.setupMock(mock)

			// Create minimal manager with mock DB
			pm := &MultiPoolerManager{
				logger: logger,
				db:     mockDB,
			}

			// Call the method
			ctx := context.Background()
			status, err := pm.queryReplicationStatus(ctx)

			// Validate results
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

			// Ensure all expectations were met
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
