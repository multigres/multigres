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

package multigateway

import (
	"encoding/json"
	"net/http"
	"slices"

	"github.com/multigres/multigres/go/services/multigateway/shardingcatalogcache"
)

// shardingCatalogStatus is the /debug/sharding-catalog response: the snapshot
// the gateway holds for each database it has seen. It shows names, ids, states,
// versions and rule counts only.
type shardingCatalogStatus struct {
	Databases []shardingCatalogDatabase `json:"databases"`
}

type shardingCatalogDatabase struct {
	Database string `json:"database"`
	// Installed is false until a snapshot has been fetched.
	Installed      bool                        `json:"installed"`
	RoutingVersion int64                       `json:"routing_version"`
	PlanEpoch      int64                       `json:"plan_epoch"`
	TableGroups    []shardingCatalogTableGroup `json:"tablegroups"`
}

type shardingCatalogTableGroup struct {
	ID     int64                  `json:"id"`
	Name   string                 `json:"name"`
	State  string                 `json:"state"`
	Shards []shardingCatalogShard `json:"shards"`
	// ShardRules is the number of shard rules routing the tablegroup.
	ShardRules int `json:"shard_rules"`
}

type shardingCatalogShard struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	State string `json:"state"`
}

// shardingCatalogStatusOf renders the cache's current snapshots.
func shardingCatalogStatusOf(c *shardingcatalogcache.Cache) shardingCatalogStatus {
	out := shardingCatalogStatus{Databases: []shardingCatalogDatabase{}}
	names := c.Databases()
	slices.Sort(names)
	for _, name := range names {
		db := shardingCatalogDatabase{Database: name, TableGroups: []shardingCatalogTableGroup{}}
		if snap := c.Snapshot(name); snap != nil {
			db.Installed = true
			db.RoutingVersion = snap.RoutingVersion()
			db.PlanEpoch = snap.PlanEpoch()
			for _, g := range snap.TableGroups() {
				tg := shardingCatalogTableGroup{
					ID: g.ID(), Name: g.Name(), State: g.State().String(), Shards: []shardingCatalogShard{}, ShardRules: len(g.ShardRules()),
				}
				for _, sh := range g.Shards() {
					tg.Shards = append(tg.Shards, shardingCatalogShard{ID: sh.ID(), Name: sh.Name(), State: sh.State().String()})
				}
				db.TableGroups = append(db.TableGroups, tg)
			}
		}
		out.Databases = append(out.Databases, db)
	}
	return out
}

// handleShardingCatalog serves /debug/sharding-catalog as JSON.
func (mg *Multigateway) handleShardingCatalog(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(shardingCatalogStatusOf(mg.shardingCatalog)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
