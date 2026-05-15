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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multipooler/executor/mock"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// newPrimaryPooler builds a minimal MultiPooler with the postgres port set,
// suitable for use as the primary in an InformRequest.
func newPrimaryPooler(name, host string, port int32) *clustermetadatapb.MultiPooler {
	return &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      name,
		},
		Hostname: host,
		PortMap:  map[string]int32{"postgres": port},
	}
}

func TestInform_ValidationErrors(t *testing.T) {
	tests := []struct {
		name           string
		req            *consensusdatapb.InformRequest
		expectErrMatch string
	}{
		{
			name:           "NilPrimary",
			req:            &consensusdatapb.InformRequest{Position: makeRulePosition(5)},
			expectErrMatch: "primary is required",
		},
		{
			name:           "NilPosition",
			req:            &consensusdatapb.InformRequest{Primary: newPrimaryPooler("p1", "host", 5432)},
			expectErrMatch: "position is required",
		},
		{
			name: "MissingHostname",
			req: &consensusdatapb.InformRequest{
				Primary:  &clustermetadatapb.MultiPooler{Id: &clustermetadatapb.ID{Name: "p1"}, PortMap: map[string]int32{"postgres": 5432}},
				Position: makeRulePosition(5),
			},
			expectErrMatch: "primary hostname is required",
		},
		{
			name: "MissingPostgresPort",
			req: &consensusdatapb.InformRequest{
				Primary:  &clustermetadatapb.MultiPooler{Id: &clustermetadatapb.ID{Name: "p1"}, Hostname: "host"},
				Position: makeRulePosition(5),
			},
			expectErrMatch: "has no postgres port configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQueryService := mock.NewQueryService()
			pm, _ := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{pos: makeRulePosition(5)})

			resp, err := pm.Inform(t.Context(), tt.req)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectErrMatch)
			assert.Nil(t, resp)
		})
	}
}

// TestInform_NoOpWhenPositionNotHigher verifies that Inform returns success
// without touching primary_conninfo when the supplied position is equal to or
// lower than this pooler's own observed position. This is the staleness gate
// that keeps out-of-order recovery rounds from clobbering fresh state.
func TestInform_NoOpWhenPositionNotHigher(t *testing.T) {
	tests := []struct {
		name        string
		selfTerm    int64
		incomingPos *clustermetadatapb.PoolerPosition
	}{
		{
			name:        "EqualPosition",
			selfTerm:    7,
			incomingPos: makeRulePosition(7),
		},
		{
			name:        "LowerPosition",
			selfTerm:    7,
			incomingPos: makeRulePosition(3),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQueryService := mock.NewQueryService()
			// The only postgres call Inform must make in the no-op path is
			// observePosition, which is handled by the fakeRuleStore — no SQL
			// at all should hit the mock query service.
			pm, _ := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{pos: makeRulePosition(tt.selfTerm)})

			req := &consensusdatapb.InformRequest{
				Primary:  newPrimaryPooler("new-primary", "primary-host", 5432),
				Position: tt.incomingPos,
			}
			resp, err := pm.Inform(t.Context(), req)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.ConsensusStatus)

			// Verify primary tracking was NOT updated — Inform short-circuited
			// before reaching the apply branch.
			pm.mu.Lock()
			storedID := pm.primaryPoolerID
			pm.mu.Unlock()
			assert.Nil(t, storedID, "primary should not be recorded on no-op Inform")

			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

// TestInform_StandbyAppliesNewPrimary verifies the standby branch: when the
// receiver is in recovery and the incoming position is higher, Inform issues
// the same setPrimaryConnInfo work (stop replication -> ALTER SYSTEM SET
// primary_conninfo -> reload -> start replication).
func TestInform_StandbyAppliesNewPrimary(t *testing.T) {
	mockQueryService := mock.NewQueryService()

	// Inform's isPrimary check: returns true for "in recovery" -> standby.
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))

	// setPrimaryConnInfoLocked's own isPrimary guardrail.
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))

	// pauseReplication(REPLAY_ONLY): pg_wal_replay_pause + verify paused.
	mockQueryService.AddQueryPatternOnce("SELECT pg_wal_replay_pause",
		mock.MakeQueryResult(nil, nil))
	replayStateCols := []string{"replay_lsn", "is_paused"}
	mockQueryService.AddQueryPattern("^SELECT pg_last_wal_replay_lsn",
		mock.MakeQueryResult(replayStateCols, [][]any{{"0/100", true}}))

	// Capture the rendered primary_conninfo so we can assert host=primary-host.
	var capturedConnInfoSQL string
	mockQueryService.AddQueryPatternWithCallback(
		"ALTER SYSTEM SET primary_conninfo",
		mock.MakeQueryResult(nil, nil),
		func(sql string) { capturedConnInfoSQL = sql },
	)
	expectReloadConfig(mockQueryService)

	// startReplicationAfter=true: waitForDatabaseConnection + pg_wal_replay_resume.
	mockQueryService.AddQueryPattern("^SELECT 1$", mock.MakeQueryResult(nil, nil))
	mockQueryService.AddQueryPatternOnce("SELECT pg_wal_replay_resume",
		mock.MakeQueryResult(nil, nil))

	pm, _ := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{pos: makeRulePosition(3)})

	req := &consensusdatapb.InformRequest{
		Primary:  newPrimaryPooler("new-primary", "primary-host", 5432),
		Position: makeRulePosition(10),
	}
	resp, err := pm.Inform(t.Context(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ConsensusStatus)

	assert.Contains(t, capturedConnInfoSQL, "host=primary-host",
		"rendered primary_conninfo should reference the new primary host")

	pm.mu.Lock()
	storedID := pm.primaryPoolerID
	storedHost := pm.primaryHost
	pm.mu.Unlock()
	require.NotNil(t, storedID, "primary should be recorded after standby update")
	assert.Equal(t, "new-primary", storedID.Name)
	assert.Equal(t, "primary-host", storedHost)
}

// The "stale primary" branch — when isPrimary returns true and the position is
// higher — delegates to demoteStalePrimaryLocked, which is exhaustively
// exercised by TestDemoteStalePrimary_UpdatesConsensusTerm. Asserting only the
// routing decision in a unit test here would require duplicating that test's
// full pgctld + query mock setup; the routing itself is a one-line if/else
// and easy to read.
