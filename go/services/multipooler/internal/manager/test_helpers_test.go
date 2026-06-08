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

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// newRecordFromProto returns a poolerRecord backed by a no-op topo store,
// suitable for tests that need a manager constructed via struct literal.
func newRecordFromProto(mp *clustermetadatapb.MultiPooler) *poolerRecord {
	return newPoolerRecord(newTestLogger(), &fakeTopoStore{}, mp)
}

// setPoolerTypeForTest mutates the pooler type on the manager's record while
// holding the manager's action lock. Used by tests that need to put the
// manager into a specific topology state before exercising an RPC.
//
// Keeps the Type ↔ CurrentLeadership invariant satisfied by also writing a
// LeaderObservation that names this pooler when transitioning to PRIMARY,
// and clearing it otherwise.
func setPoolerTypeForTest(t *testing.T, pm *MultiPoolerManager, poolerType clustermetadatapb.PoolerType) {
	t.Helper()
	ctx, err := pm.actionLock.Acquire(t.Context(), "test-set-type")
	require.NoError(t, err)
	defer pm.actionLock.Release(ctx)
	require.NoError(t, pm.record.Mutate(ctx, func(s *MutablePoolerRecordState) {
		s.Type = poolerType
		if poolerType == clustermetadatapb.PoolerType_PRIMARY {
			s.CurrentLeadership = &clustermetadatapb.LeaderObservation{
				LeaderId:         pm.record.Id(),
				LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			}
		} else {
			s.CurrentLeadership = nil
		}
	}))
}
