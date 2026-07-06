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

// Package servingstate defines the effective serving state that the multipooler
// manager's StateManager delivers to components via OnStateChange.
//
// It lives in its own leaf package because the components that react to state
// changes (heartbeat, query server) satisfy the manager's interface structurally
// and cannot import the manager package back.
package servingstate

import clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

// State is the effective serving state the StateManager delivers to components
// via OnStateChange. It carries the full routing state (role + qualifying rule)
// plus the serving intent. Consensus leadership is deliberately absent: it is a
// consensus-layer fact, derived from the ConsensusStatus by whoever needs it, not
// something the serving layer traffics in. Both leader-bound query modes
// (WRITABLE and CONSISTENT) gate on the routing role.
type State struct {
	// Routing is the full routing/HA state — the writability role plus the rule
	// that qualifies it. It is the single fact the query gates, heartbeat writer,
	// and LISTEN/NOTIFY react to, and the health streamer + topology record project
	// it onto the wire clustermetadata.RoutingState.
	Routing RoutingState

	// ServingStatus is the serving intent (SERVING / DISABLED / DRAINING).
	ServingStatus clustermetadatapb.PoolerServingStatus
}

// Writable reports whether this pooler may accept user transactions (writes) —
// true iff the routing role is PRIMARY. See RoutingRole.Writable.
func (s State) Writable() bool {
	return s.Routing.Writable()
}

// RoutingState is the full routing/HA state: the writability role plus the rule
// that qualifies it. Pass this around rather than a bare RoutingRole — reduce to
// the role alone only for logging, metric tags, or backup annotations, where the
// rule cannot be used. It is the internal carrier; ToProto migrates it to the
// clustermetadata.RoutingState proto at the wire boundary (health stream +
// topology record).
type RoutingState struct {
	// Role is the writability routing role.
	Role RoutingRole

	// Rule qualifies the role: the committed, non-revoked rule naming this pooler
	// when PRIMARY (write authority). Nil when not PRIMARY.
	Rule *clustermetadatapb.RuleNumber
}

// Writable reports whether the role is PRIMARY (writable). See RoutingRole.Writable.
func (rs RoutingState) Writable() bool {
	return rs.Role.Writable()
}

// ToProto converts the routing state to its wire form.
func (rs RoutingState) ToProto() *clustermetadatapb.RoutingState {
	return &clustermetadatapb.RoutingState{
		Role: rs.Role.ToProto(),
		Rule: rs.Rule,
	}
}

// RoutingRole is a pooler's role for query ROUTING and HA purposes. It is about
// WRITABILITY, not consensus leadership: PRIMARY means this pooler is the
// writable leader (postgres out of recovery AND it is the highest non-revoked
// committed leader), REPLICA means it is not. It is deliberately distinct from
// consensus role (leader/follower/observer, from ConsensusStatus) and from
// postgres recovery mode (primary/standby).
//
// It is an enum rather than a bool so the write-safety meaning travels with the
// value as it flows through OnStateChange rather than eroding into an anonymous
// boolean. This Go type is the internal carrier; when writability is published
// to the gateway (health stream + topology record) it migrates to the
// clustermetadata.RoutingRole proto enum, whose values line up 1:1.
type RoutingRole int

const (
	// RoutingRoleUnknown is the zero value: the routing role has not been
	// established yet (cold start). Treated as not-writable — never routed writes.
	RoutingRoleUnknown RoutingRole = iota
	// RoutingRolePrimary means this pooler is the writable leader: it may admit
	// user transactions (see Writable for the exact conjunction). Writes route here.
	RoutingRolePrimary
	// RoutingRoleReplica means this pooler is not the writable leader.
	RoutingRoleReplica
)

// String returns a human-readable name for the routing role.
func (r RoutingRole) String() string {
	switch r {
	case RoutingRolePrimary:
		return "primary"
	case RoutingRoleReplica:
		return "replica"
	case RoutingRoleUnknown:
		return "unknown"
	default:
		return "invalid"
	}
}

// Writable reports whether this pooler may accept user transactions (writes) —
// true iff the routing role is PRIMARY. "Writable" is specifically about user
// write traffic, and PRIMARY means, in practice:
//
//   - postgres is not in recovery mode, AND
//   - the pooler is the active consensus leader (commonconsensus.IsActiveLeader):
//     its current-term rule is committed in its own WAL, is not revoked, and is
//     not superseded by a higher rule it knows of.
//
// We key on the *committed* rule: a just-pg_promote()'d pooler is not marked
// PRIMARY until its new rule commits — we do not mark it primary early — and a
// deposed leader whose rule was revoked or superseded is not PRIMARY. The
// property this buys is that an admitted write can never land on an older
// consensus timeline.
//
// (Even the edge case where an uncommitted rule's writes become visible — e.g.
// crash recovery surfacing them as phantom transactions — is not a correctness
// bug: such writes hang on the unmet durability quorum rather than corrupting.
// That is not the expected path, just reassurance that the boundary is safe.)
func (r RoutingRole) Writable() bool {
	return r == RoutingRolePrimary
}

// ToProto converts the internal routing role to the clustermetadata.RoutingRole
// proto enum, whose values line up 1:1. Unknown maps to ROUTING_ROLE_UNKNOWN.
func (r RoutingRole) ToProto() clustermetadatapb.RoutingRole {
	switch r {
	case RoutingRolePrimary:
		return clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY
	case RoutingRoleReplica:
		return clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA
	default:
		return clustermetadatapb.RoutingRole_ROUTING_ROLE_UNKNOWN
	}
}
