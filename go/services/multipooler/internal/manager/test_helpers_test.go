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
// suitable for tests that need a manager constructed via struct literal. It
// panics if the seed violates the record invariant — test protos are
// programmer-supplied, so a violation is a test bug, not a runtime condition.
func newRecordFromProto(mp *clustermetadatapb.Multipooler) *poolerRecord {
	r, err := newPoolerRecord(newTestLogger(), &fakeTopoStore{}, mp)
	if err != nil {
		panic(err)
	}
	return r
}

// mustNewPoolerRecord builds a poolerRecord and fails the test if the seed
// violates the record invariant. Use from tests that supply their own topo
// store and need to inspect it.
func mustNewPoolerRecord(t *testing.T, ts poolerTopoStore, mp *clustermetadatapb.Multipooler) *poolerRecord {
	t.Helper()
	r, err := newPoolerRecord(newTestLogger(), ts, mp)
	require.NoError(t, err)
	return r
}

// setPoolerTypeForTest mutates the pooler type on the manager's record while
// holding the manager's action lock. Used by tests that need to put the
// manager into a specific topology state before exercising an RPC.
func setPoolerTypeForTest(t *testing.T, pm *MultipoolerManager, poolerType clustermetadatapb.PoolerType) {
	t.Helper()
	ctx, err := pm.actionLock.Acquire(t.Context(), "test-set-type")
	require.NoError(t, err)
	defer pm.actionLock.Release(ctx)
	require.NoError(t, pm.record.Mutate(ctx, func(s *MutablePoolerRecordState) {
		// Type is derived from routing_state: a PRIMARY carries a PRIMARY
		// routing_state, any other type carries none.
		if poolerType == clustermetadatapb.PoolerType_PRIMARY {
			s.RoutingState = &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY}
		} else {
			s.RoutingState = nil
		}
	}))
}
