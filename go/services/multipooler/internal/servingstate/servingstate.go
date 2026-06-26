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
// changes (heartbeat, pubsub, query server) satisfy the manager's StateAware
// interface structurally and cannot import the manager package back.
package servingstate

import clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

// State is the effective serving state components react to. It carries two
// distinct leadership notions deliberately — see the field docs — so a consumer
// is forced to pick the one its decision actually needs rather than conflating
// routing with write-safety.
type State struct {
	// IsHighestKnownLeader reports highest-known leadership: the highest rule we
	// know of (including ReplicationPrimary observations recorded via
	// SetPrimary/Promote, which can be ahead of what we've committed) names us.
	// It drives routing — the PoolerType label and the gateway-staleness check —
	// and must NOT be used to gate writes.
	//
	// It deliberately ignores revocation: a deposed-but-still-running leader
	// (committed at a now-revoked term) is not Writable, but it remains the
	// highest-known leader for routing so read-consistent queries can still be
	// served from it. Only Writable is revocation-aware.
	IsHighestKnownLeader bool

	// Writable reports write-safety: postgres is out of recovery AND our highest
	// *committed* rule names us leader. This is the authority for accepting
	// writes — heartbeats, LISTEN/NOTIFY, and the gateway's write traffic. Unlike
	// IsHighestKnownLeader it is never true on the strength of a not-yet-committed
	// rule, so it stays false during the window between pg_promote() and the rule
	// commit.
	Writable bool

	// ServingStatus is the serving intent (SERVING / DISABLED / DRAINING).
	ServingStatus clustermetadatapb.PoolerServingStatus
}
