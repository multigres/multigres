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

package metadataclient

import (
	"testing"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func pooler(name, tableGroup string, mode clustermetadatapb.PoolerManagementMode, role clustermetadatapb.RoutingRole, term int64) *clustermetadatapb.Multipooler {
	return &clustermetadatapb.Multipooler{
		Id:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name},
		ShardRef:       &clustermetadatapb.ShardRef{Database: "db", TableGroup: tableGroup, Shard: "0-inf"},
		ManagementMode: mode,
		RoutingState:   &clustermetadatapb.RoutingState{Role: role, Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: term}},
	}
}

const (
	managed   = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED
	unmanaged = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	primary   = clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY
	replica   = clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA
)

func TestDefaultPrimaryIgnoresOtherCohortsAndUnmanaged(t *testing.T) {
	want := pooler("default-1", "default", managed, primary, 2)
	list := []*clustermetadatapb.Multipooler{
		pooler("default-2", "default", managed, replica, 2),
		// A second managed cohort's primary and an external source are both
		// "primary" but never hold cluster metadata.
		pooler("dest-1", "destTG", managed, primary, 99),
		pooler("external-1", "default", unmanaged, primary, 99),
		want,
	}
	got, err := DefaultPrimary(list)
	require.NoError(t, err)
	require.Same(t, want, got)
}

func TestDefaultPrimaryTreatsUnspecifiedModeAsManaged(t *testing.T) {
	legacy := pooler("legacy", "default", clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNSPECIFIED, primary, 1)
	got, err := DefaultPrimary([]*clustermetadatapb.Multipooler{legacy})
	require.NoError(t, err)
	require.Same(t, legacy, got)
}

func TestDefaultPrimaryHigherRuleWinsAndEqualRulesFailClosed(t *testing.T) {
	older := pooler("old", "default", managed, primary, 2)
	newer := pooler("new", "default", managed, primary, 3)
	got, err := DefaultPrimary([]*clustermetadatapb.Multipooler{older, newer})
	require.NoError(t, err)
	require.Same(t, newer, got)
	got, err = DefaultPrimary([]*clustermetadatapb.Multipooler{newer, older})
	require.NoError(t, err)
	require.Same(t, newer, got)

	twin := pooler("twin", "default", managed, primary, 3)
	_, err = DefaultPrimary([]*clustermetadatapb.Multipooler{newer, twin})
	require.ErrorContains(t, err, "ambiguous")

	// A stale overlap below a unique higher rule is not ambiguity.
	staleTwin := pooler("stale-twin", "default", managed, primary, 2)
	for _, list := range [][]*clustermetadatapb.Multipooler{{older, staleTwin, newer}, {newer, older, staleTwin}} {
		got, err = DefaultPrimary(list)
		require.NoError(t, err)
		require.Same(t, newer, got)
	}
}

func TestDefaultPrimaryUnavailable(t *testing.T) {
	_, err := DefaultPrimary(nil)
	require.ErrorContains(t, err, "unavailable")
	_, err = DefaultPrimary([]*clustermetadatapb.Multipooler{pooler("replica", "default", managed, replica, 1)})
	require.ErrorContains(t, err, "unavailable")
}
