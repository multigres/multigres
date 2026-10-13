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
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// readinessNode builds a ConsensusStatus and its matching PoolerHealthState
// carrying the given Status.PostgresReady value. A nil health snapshot (pass
// hasHealth=false) exercises the "no observation yet" default.
func readinessNode(name string, ready, hasHealth bool) (*clustermetadatapb.ConsensusStatus, *multiorchdatapb.PoolerHealthState) {
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name}
	cs := &clustermetadatapb.ConsensusStatus{Id: id}
	if !hasHealth {
		return cs, nil
	}
	h := &multiorchdatapb.PoolerHealthState{
		Status: &multipoolermanagerdatapb.Status{PostgresReady: ready},
	}
	return cs, h
}

func TestPostgresReadyLess(t *testing.T) {
	lessFor := func(aCS, bCS *clustermetadatapb.ConsensusStatus, aH, bH *multiorchdatapb.PoolerHealthState) func(a, b *clustermetadatapb.ConsensusStatus) bool {
		return postgresReadyLess(map[string]*multiorchdatapb.PoolerHealthState{
			topoclient.ClusterIDString(aCS.GetId()): aH,
			topoclient.ClusterIDString(bCS.GetId()): bH,
		})
	}

	t.Run("ready sorts before not-ready", func(t *testing.T) {
		a, aH := readinessNode("ready", true, true)
		b, bH := readinessNode("not_ready", false, true)
		less := lessFor(a, b, aH, bH)
		assert.True(t, less(a, b))
		assert.False(t, less(b, a))
	})

	t.Run("both ready is not less in either direction", func(t *testing.T) {
		a, aH := readinessNode("r1", true, true)
		b, bH := readinessNode("r2", true, true)
		less := lessFor(a, b, aH, bH)
		assert.False(t, less(a, b))
		assert.False(t, less(b, a))
	})

	t.Run("both not-ready is not less in either direction", func(t *testing.T) {
		a, aH := readinessNode("nr1", false, true)
		b, bH := readinessNode("nr2", false, true)
		less := lessFor(a, b, aH, bH)
		assert.False(t, less(a, b))
		assert.False(t, less(b, a))
	})

	t.Run("missing health snapshot defaults to not-ready", func(t *testing.T) {
		// No observation yet must not be assumed equivalent to a ready node.
		a, aH := readinessNode("ready", true, true)
		b, bH := readinessNode("unobserved", false, false)
		less := lessFor(a, b, aH, bH)
		assert.True(t, less(a, b), "observed-ready must sort before an unobserved node")
		assert.False(t, less(b, a))
	})
}

func TestSelectFittestLeader(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))

	t.Run("empty candidates returns nil", func(t *testing.T) {
		assert.Nil(t, selectFittestLeader(nil, nil, rng))
	})

	t.Run("single candidate wins regardless of fitness", func(t *testing.T) {
		a, aH := readinessNode("only", false, true)
		healthByID := map[string]*multiorchdatapb.PoolerHealthState{
			topoclient.ClusterIDString(a.GetId()): aH,
		}
		got := selectFittestLeader([]*clustermetadatapb.ConsensusStatus{a}, healthByID, rng)
		assert.Same(t, a, got)
	})

	t.Run("readiness dominates the signal/slot tiebreak", func(t *testing.T) {
		// b is actively signaling (higher-priority than a's resign intent) and
		// slot-ready, but not postgres-ready -- readiness must still win.
		resign := clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION
		active := clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_ACTIVE
		aID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "ready_resigning"}
		bID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "notready_active"}
		a := &clustermetadatapb.ConsensusStatus{Id: aID}
		b := &clustermetadatapb.ConsensusStatus{Id: bID}
		healthByID := map[string]*multiorchdatapb.PoolerHealthState{
			topoclient.ClusterIDString(aID): {
				Status:             &multipoolermanagerdatapb.Status{PostgresReady: true, FailoverSlotsReady: 0},
				AvailabilityStatus: &clustermetadatapb.AvailabilityStatus{LeadershipStatus: &clustermetadatapb.LeadershipStatus{Signal: resign}},
			},
			topoclient.ClusterIDString(bID): {
				Status:             &multipoolermanagerdatapb.Status{PostgresReady: false, FailoverSlotsReady: 5},
				AvailabilityStatus: &clustermetadatapb.AvailabilityStatus{LeadershipStatus: &clustermetadatapb.LeadershipStatus{Signal: active}},
			},
		}
		got := selectFittestLeader([]*clustermetadatapb.ConsensusStatus{a, b}, healthByID, rng)
		assert.Same(t, a, got, "postgres-ready must win even against a not-ready node that otherwise scores better")
	})

	t.Run("tied on readiness falls through to the signal/slot tiebreak", func(t *testing.T) {
		active := clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_ACTIVE
		resign := clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION
		aID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "ready_active"}
		bID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "ready_resigning"}
		a := &clustermetadatapb.ConsensusStatus{Id: aID}
		b := &clustermetadatapb.ConsensusStatus{Id: bID}
		healthByID := map[string]*multiorchdatapb.PoolerHealthState{
			topoclient.ClusterIDString(aID): {
				Status:             &multipoolermanagerdatapb.Status{PostgresReady: true},
				AvailabilityStatus: &clustermetadatapb.AvailabilityStatus{LeadershipStatus: &clustermetadatapb.LeadershipStatus{Signal: active}},
			},
			topoclient.ClusterIDString(bID): {
				Status:             &multipoolermanagerdatapb.Status{PostgresReady: true},
				AvailabilityStatus: &clustermetadatapb.AvailabilityStatus{LeadershipStatus: &clustermetadatapb.LeadershipStatus{Signal: resign}},
			},
		}
		got := selectFittestLeader([]*clustermetadatapb.ConsensusStatus{b, a}, healthByID, rng)
		assert.Same(t, a, got, "among equally-ready nodes, the non-resigning one must win")
	})

	t.Run("fully tied candidates: same seed reproduces the same winner", func(t *testing.T) {
		a, aH := readinessNode("first", true, true)
		b, bH := readinessNode("second", true, true)
		healthByID := map[string]*multiorchdatapb.PoolerHealthState{
			topoclient.ClusterIDString(a.GetId()): aH,
			topoclient.ClusterIDString(b.GetId()): bH,
		}
		candidates := []*clustermetadatapb.ConsensusStatus{a, b}

		got1 := selectFittestLeader(candidates, healthByID, rand.New(rand.NewPCG(1, 2)))
		got2 := selectFittestLeader(candidates, healthByID, rand.New(rand.NewPCG(1, 2)))
		require.NotNil(t, got1)
		assert.Same(t, got1, got2, "the same seed must reproduce the same winner")
	})

	t.Run("fully tied candidates: both can win across different seeds", func(t *testing.T) {
		a, aH := readinessNode("first", true, true)
		b, bH := readinessNode("second", true, true)
		healthByID := map[string]*multiorchdatapb.PoolerHealthState{
			topoclient.ClusterIDString(a.GetId()): aH,
			topoclient.ClusterIDString(b.GetId()): bH,
		}
		candidates := []*clustermetadatapb.ConsensusStatus{a, b}

		var sawA, sawB bool
		for seed := range 20 {
			got := selectFittestLeader(candidates, healthByID, rand.New(rand.NewPCG(uint64(seed), uint64(seed))))
			require.True(t, got == a || got == b)
			if got == a {
				sawA = true
			} else {
				sawB = true
			}
		}
		assert.True(t, sawA && sawB, "both tied candidates must be reachable, not just one")
	})
}
