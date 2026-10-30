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

package poolergateway

import (
	"sort"
	"strings"
	"time"
)

// MultipoolerRows returns one row per pooler this gateway tracks, in the
// column order of the multigres.multipoolers view (engine.ClusterViews):
// name, cell, database, table_group, shard, routing_role, leadership,
// serving_status, lifecycle, lifecycle_reason, lifecycle_updated,
// replication_lag, connected, health_last_response, discovery_state,
// last_change. Role, serving status and lag come from the pooler's health
// stream when the gateway has one, else from the topology record; leadership
// is the gateway's own routing view (leader, stale-leader, follower).
func (pg *PoolerGateway) MultipoolerRows() [][]any {
	entries := pg.cache.All()
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i].Pooler.GetId(), entries[j].Pooler.GetId()
		if a.GetCell() != b.GetCell() {
			return a.GetCell() < b.GetCell()
		}
		return a.GetName() < b.GetName()
	})
	rows := make([][]any, 0, len(entries))
	for _, e := range entries {
		p := e.Pooler
		var (
			role          = p.GetRoutingState().GetRole()
			serving       = p.GetServingStatus()
			lag           any
			connected     bool
			lastResponse  any
			leadership    = leadershipFollower
			conn          = e.Rider
			lifecycle     = p.GetLifecycleStatus()
			lifecycleTime any
		)
		if conn != nil {
			leadership = pg.loadBalancer.leadershipFor(conn)
			if h := conn.Health(); h != nil {
				role = h.RoutingState.GetRole()
				serving = h.ServingStatus
				connected = !conn.healthTimedOut.Load()
				if h.ReplicationLagNs > 0 {
					lag = time.Duration(h.ReplicationLagNs)
				}
				if !h.LastResponse.IsZero() {
					lastResponse = h.LastResponse
				}
			}
		}
		if lifecycle.GetUpdated() != nil {
			lifecycleTime = lifecycle.GetUpdated().AsTime()
		}
		rows = append(rows, []any{
			p.GetId().GetName(),
			p.GetId().GetCell(),
			p.GetShardRef().GetDatabase(),
			p.GetShardRef().GetTableGroup(),
			p.GetShardRef().GetShard(),
			enumLabel(role.String(), "ROUTING_ROLE_"),
			leadership,
			enumLabel(serving.String(), ""),
			enumLabel(lifecycle.GetStatus().String(), "LIFECYCLE_"),
			lifecycle.GetReason(),
			lifecycleTime,
			lag,
			connected,
			lastResponse,
			e.State.String(),
			e.LastChange,
		})
	}
	return rows
}

// enumLabel lowercases a proto enum name with its prefix removed:
// ROUTING_ROLE_PRIMARY → primary, LIFECYCLE_ACTIVE → active.
func enumLabel(name, prefix string) string {
	return strings.ToLower(strings.TrimPrefix(name, prefix))
}
