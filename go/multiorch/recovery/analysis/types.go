// Copyright 2025 Supabase, Inc.
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

package analysis

// CheckName uniquely identifies a health check.
type CheckName string

// ProblemCode identifies the category of problem.
// Multiple checks can return the same ProblemCode.
// This enables many-to-one mapping: many checks → one recovery action.
type ProblemCode string

const (
	// Shard bootstrap problems (highest priority - shard cannot function at all).
	ProblemShardNeedsBootstrap ProblemCode = "ShardNeedsBootstrap"
	ProblemShardHasNoPrimary   ProblemCode = "ShardHasNoPrimary"

	// Primary problems (catastrophic - block everything else).
	ProblemPrimaryIsDead      ProblemCode = "PrimaryIsDead"
	ProblemPrimaryDiskStalled ProblemCode = "PrimaryDiskStalled"

	// Primary configuration problems (can fix while primary alive).
	ProblemPrimaryNotAcceptingWrites ProblemCode = "PrimaryNotAcceptingWrites"
	ProblemPrimaryMisconfigured      ProblemCode = "PrimaryMisconfigured"
	ProblemPrimaryIsReadOnly         ProblemCode = "PrimaryIsReadOnly"

	// Replica problems (require healthy primary).
	ProblemReplicaNotReplicating ProblemCode = "ReplicaNotReplicating"
	ProblemReplicaWrongPrimary   ProblemCode = "ReplicaWrongPrimary"
	ProblemReplicaLagging        ProblemCode = "ReplicaLagging"
	ProblemReplicaMisconfigured  ProblemCode = "ReplicaMisconfigured"
	ProblemReplicaIsWritable     ProblemCode = "ReplicaIsWritable"

	// Non-actionable: if all hosts are down, there is no way we can failover.
	ProblemPrimaryAndReplicasDead ProblemCode = "PrimaryAndReplicasDead"
)

// Category groups checks by what they monitor.
type Category string

const (
	CategoryPrimary       Category = "Primary"
	CategoryReplica       Category = "Replica"
	CategoryConfiguration Category = "Configuration"
)

// Priority defines the priority of recovery actions.
// Higher values = higher priority (will be attempted first).
type Priority int

const (
	// PriorityShardBootstrap is for shard-wide bootstrap issues like no primary exists.
	// This is the highest priority - the shard cannot function at all.
	// Arbitrarily high priority, to leave room for other priorities.
	PriorityShardBootstrap Priority = 10000

	// PriorityEmergency is for catastrophic issues like dead primary.
	// These must be fixed before anything else can proceed.
	PriorityEmergency Priority = 1000

	// PriorityHigh is for serious issues that don't block everything.
	// Examples: replica not replicating, replica pointing to wrong primary.
	PriorityHigh Priority = 500

	// PriorityNormal is for configuration drift and minor issues.
	// Examples: primary not accepting writes, replica is writable.
	PriorityNormal Priority = 100
)

// ProblemScope indicates whether a problem affects the whole shard or just a single pooler.
type ProblemScope string

const (
	// ScopeShard indicates the problem affects the entire shard (e.g., primary dead).
	// Recovery requires refreshing all poolers in the shard and may involve shard-wide operations like failover.
	ScopeShard ProblemScope = "Shard"

	// ScopePooler indicates the problem affects only a specific pooler.
	// Recovery only requires refreshing the affected pooler and potentially the primary.
	ScopePooler ProblemScope = "Pooler"
)
