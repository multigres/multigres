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
	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
)

// AppRouting is where the gateway sends the application's traffic: all application SQL of one
// database goes to one tablegroup, which may be the default tablegroup itself. It is set at
// startup from the gateway's configuration and never changes, so there is no unknown or
// changing state to handle. A gateway without application routing (a nil *AppRouting)
// sends application SQL to the default tablegroup, as it always did.
type AppRouting struct {
	database   string
	tableGroup string
}

// NewAppRouting returns the routing that sends the application SQL of database to tableGroup.
func NewAppRouting(database, tableGroup string) *AppRouting {
	return &AppRouting{database: database, tableGroup: tableGroup}
}

// Rewrite points an application target at the application tablegroup. A target of another
// database is left alone. The target's key is replaced rather than edited, since a buffered
// request may hold the old one.
func (r *AppRouting) Rewrite(target *query.Target) {
	if r == nil || target == nil || target.GetShardRef() == nil {
		return
	}
	sk := target.GetShardRef()
	if sk.GetDatabase() != r.database {
		return
	}
	target.ShardRef = &clustermetadatapb.ShardRef{Database: sk.GetDatabase(), TableGroup: r.tableGroup, Shard: sk.GetShard()}
}

// AuthTableGroup is the tablegroup whose poolers answer credential lookups for database: the
// application tablegroup, whose roles and password verifiers are the ones the application
// database uses. Without application routing, or for a database it does not follow, it is the
// default one.
func (r *AppRouting) AuthTableGroup(database string) string {
	if r == nil || database != r.database {
		return constants.DefaultTableGroup
	}
	return r.tableGroup
}

// SetAppRouting wires application routing into the gateway. It must be called before the
// gateway serves: the field is not synchronized with the requests that read it.
func (pg *PoolerGateway) SetAppRouting(r *AppRouting) {
	pg.appRouting = r
}
