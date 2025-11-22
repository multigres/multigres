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
	// Primary problems (catastrophic - block everything else).
	ProblemPrimaryDead         ProblemCode = "PrimaryDead"
	ProblemPrimaryDiskStalled  ProblemCode = "PrimaryDiskStalled"
	ProblemClusterHasNoPrimary ProblemCode = "ClusterHasNoPrimary"

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

	// Non-actionable (detect but don't auto-recover).
	ProblemPrimaryAndReplicasDead    ProblemCode = "PrimaryAndReplicasDead"
	ProblemAllReplicasNotReplicating ProblemCode = "AllReplicasNotReplicating"
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
	// PriorityClusterBootstrap is for cluster-wide bootstrap issues like no primary exists.
	// This is the highest priority - the cluster cannot function at all.
	PriorityClusterBootstrap Priority = 2000

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

// ProblemScope indicates whether a problem affects the whole cluster or just a single pooler.
type ProblemScope string

const (
	// ScopeClusterWide indicates the problem affects the entire shard (e.g., primary dead).
	// Recovery requires refreshing all poolers in the shard and may involve cluster-wide operations like failover.
	ScopeClusterWide ProblemScope = "ClusterWide"

	// ScopeSinglePooler indicates the problem affects only a specific pooler.
	// Recovery only requires refreshing the affected pooler and potentially the primary.
	ScopeSinglePooler ProblemScope = "SinglePooler"
)
