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

package manager

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
)

// staticLeaderStatus, CachedConsensusStatus, StartsAsPrimary,
// LeaderInRecoveryAction and ResignGuard are unit-tested directly against
// consensus.ConsensusManager in the consensus package. The tests below only
// cover the MultipoolerManager-level integration: that startPostgres,
// determineRemedialAction and ResignLeadership actually consult it.

func TestStartPostgres_AsPrimaryOnlyForStaticLeader(t *testing.T) {
	tests := []struct {
		name          string
		staticLeader  bool
		wantAsPrimary bool
	}{
		{name: "consensus leader", staticLeader: false, wantAsPrimary: false},
		{name: "static leader", staticLeader: true, wantAsPrimary: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockPgctld := &mockPgctldClient{}
			var opts []testManagerOption
			if tt.staticLeader {
				opts = append(opts, withStaticLeader())
			}
			pm := newTestManager(t, opts...)
			pm.pgctldClient = mockPgctld

			require.NoError(t, pm.startPostgres(t.Context()))

			require.NotNil(t, mockPgctld.startRequest)
			assert.Equal(t, tt.wantAsPrimary, mockPgctld.startRequest.GetAsPrimary())
		})
	}
}

func TestDetermineRemedialAction_StaticLeader(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "self"}
	running := func(mode pgmode.Mode) postgresState {
		return postgresState{pgctldAvailable: true, postgresRunning: true, pgMode: mode}
	}

	tests := []struct {
		name         string
		staticLeader bool
		state        postgresState
		wantAction   remedialAction
	}{
		{
			// The bootstrap restore leaves postgres a standby; nothing else will promote it.
			name:         "static leader with standby promotes",
			staticLeader: true,
			state:        running(pgmode.InRecovery),
			wantAction:   remedialActionPromoteStaticLeader,
		},
		{
			// The mode could not be read this tick: do not act on a guess.
			name:         "static leader with unknown mode waits",
			staticLeader: true,
			state:        running(pgmode.Unknown),
			wantAction:   remedialActionNone,
		},
		{
			// Without a static leader and with no rule yet, the monitor waits for a
			// coordinator, as in Multigres.
			name:         "consensus pooler with standby and no rule waits",
			staticLeader: false,
			state:        running(pgmode.InRecovery),
			wantAction:   remedialActionNone,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []testManagerOption
			opts = append(opts, withServiceID(selfID))
			if tt.staticLeader {
				opts = append(opts, withStaticLeader())
			}
			pm := newTestManager(t, opts...)

			assert.Equal(t, tt.wantAction, pm.determineRemedialAction(t.Context(), tt.state))
		})
	}
}

func TestDetermineRemedialAction_StaticLeaderPrimaryDoesNotPromote(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "self"}
	pm := newTestManager(t, withServiceID(selfID), withStaticLeader())

	got := pm.determineRemedialAction(t.Context(), postgresState{pgctldAvailable: true, postgresRunning: true, pgMode: pgmode.Primary})

	assert.NotEqual(t, remedialActionPromoteStaticLeader, got)
	assert.NotEqual(t, remedialActionDemoteStalePrimary, got)
	assert.NotEqual(t, remedialActionResignLeadership, got)
}

func TestResignLeadership_RejectedForStaticLeader(t *testing.T) {
	mockPgctld := &mockPgctldClient{}
	pm := newTestManager(t, withStaticLeader())
	pm.pgctldClient = mockPgctld

	_, err := pm.ResignLeadership(t.Context(), &multipoolermanagerdatapb.ResignLeadershipRequest{})

	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	assert.Contains(t, err.Error(), "static leader")
	// Rejected before touching postgres.
	assert.False(t, mockPgctld.restartCalled)
}
