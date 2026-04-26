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

package consensus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// coordA and coordB are distinct coordinator IDs used across revocation tests.
var (
	coordA = &clustermetadatapb.ID{Name: "coord-a"}
	coordB = &clustermetadatapb.ID{Name: "coord-b"}

	ts1 = timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	ts2 = timestamppb.New(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
)

func TestValidateRevocation(t *testing.T) {
	revocationAt5 := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       5,
		AcceptedCoordinatorId:  coordA,
		CoordinatorInitiatedAt: ts1,
	}

	tests := []struct {
		name       string
		status     *clustermetadatapb.ConsensusStatus
		revocation *clustermetadatapb.TermRevocation
		wantErr    string
	}{
		{
			name:       "NilRevocation_Refused",
			status:     nil,
			revocation: nil,
			wantErr:    "revocation is nil",
		},
		{
			name:       "NilStatus_Accepted",
			status:     nil,
			revocation: revocationAt5,
		},
		{
			name:       "NilPosition_Accepted",
			status:     &clustermetadatapb.ConsensusStatus{},
			revocation: revocationAt5,
		},
		{
			name: "WALSafety_RuleTermBelowRevocation_Accepted",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionAtCoordTerm(4),
			},
			revocation: revocationAt5,
		},
		{
			name: "WALSafety_RuleTermEqualsRevocation_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionAtCoordTerm(5),
			},
			revocation: revocationAt5,
			wantErr:    "coordinator term 5 >= revoked_below_term 5",
		},
		{
			name: "WALSafety_RuleTermAboveRevocation_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionAtCoordTerm(7),
			},
			revocation: revocationAt5,
			wantErr:    "coordinator term 7 >= revoked_below_term 5",
		},
		{
			name: "StoredTerm_HigherThanRequested_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:      10,
					AcceptedCoordinatorId: coordA,
				},
			},
			revocation: revocationAt5,
			wantErr:    "already accepted term 10 > requested 5",
		},
		{
			name: "StoredTerm_LowerThanRequested_Accepted",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:      3,
					AcceptedCoordinatorId: coordA,
				},
			},
			revocation: revocationAt5,
		},
		{
			name: "SameTerm_SameCoordinator_SameTimestamp_Idempotent",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: revocationAt5,
			},
			revocation: revocationAt5,
		},
		{
			name: "SameTerm_DifferentCoordinator_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       5,
					AcceptedCoordinatorId:  coordA,
					CoordinatorInitiatedAt: ts1,
				},
			},
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       5,
				AcceptedCoordinatorId:  coordB,
				CoordinatorInitiatedAt: ts1,
			},
			wantErr: "already accepted term 5 from coordinator",
		},
		{
			name: "SameTerm_SameCoordinator_DifferentTimestamp_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       5,
					AcceptedCoordinatorId:  coordA,
					CoordinatorInitiatedAt: ts1,
				},
			},
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       5,
				AcceptedCoordinatorId:  coordA,
				CoordinatorInitiatedAt: ts2,
			},
			wantErr: "different coordinator_initiated_at",
		},
		{
			name: "SameTerm_BothNilTimestamp_Idempotent",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:      5,
					AcceptedCoordinatorId: coordA,
				},
			},
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:      5,
				AcceptedCoordinatorId: coordA,
			},
		},
		{
			name: "WALAndStoredTerm_BothChecked_WALFails",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionAtCoordTerm(6),
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:      3,
					AcceptedCoordinatorId: coordA,
				},
			},
			revocation: revocationAt5,
			wantErr:    "coordinator term 6 >= revoked_below_term 5",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateRevocation(tc.status, tc.revocation)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

// positionAtCoordTerm builds a PoolerPosition whose committed rule is at the
// given coordinator term.
func positionAtCoordTerm(coordTerm int64) *clustermetadatapb.PoolerPosition {
	return &clustermetadatapb.PoolerPosition{
		Rule: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{
				CoordinatorTerm: coordTerm,
			},
		},
	}
}
