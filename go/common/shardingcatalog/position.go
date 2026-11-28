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

package shardingcatalog

import (
	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// Position orders the snapshots a catalog shard's primaries serve: the
// consensus rule under which the serving primary held the shard, then the
// routing_version. A primary deposed by a failover serves under a lower rule
// than its successor, so its snapshot is ordered before the successor's, even
// when it is at a higher routing_version.
type Position struct {
	Rule           *clustermetadatapb.RuleNumber
	RoutingVersion int64
}

// Compare returns -1, 0 or 1 as p is before, equal to or after other.
func (p Position) Compare(other Position) int {
	if c := commonconsensus.CompareRuleNumbers(p.Rule, other.Rule); c != 0 {
		return c
	}
	switch {
	case p.RoutingVersion < other.RoutingVersion:
		return -1
	case p.RoutingVersion > other.RoutingVersion:
		return 1
	}
	return 0
}
