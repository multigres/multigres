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

package store

import (
	"time"

	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// PrimaryInfo represents information about a primary pooler in the shard.
// Used for stale primary detection to identify which primary is most advanced.
type PrimaryInfo struct {
	ID            *clustermetadatapb.ID // Pooler ID
	ConsensusTerm int64                 // Current consensus term (for logging/context)
	PrimaryTerm   int64                 // Term when this node was promoted to primary
}

// ReplicationAnalysis represents the analyzed state of a single pooler
// and its replication topology. This is the in-memory equivalent of
// VTOrc's replication_analysis table.
type ReplicationAnalysis struct {
	// Identity
	PoolerID *clustermetadatapb.ID
	ShardKey commontypes.ShardKey

	// Pooler properties
	PoolerType           clustermetadatapb.PoolerType
	CurrentServingStatus clustermetadatapb.PoolerServingStatus
	IsPrimary            bool
	// Represents if the poolerID is reachable and it's returing a
	// valid status response
	LastCheckValid   bool
	IsStale          bool
	IsInitialized    bool // Whether this pooler has been initialized
	HasDataDirectory bool // Whether this pooler has a PostgreSQL data directory (PG_VERSION exists)
	AnalyzedAt       time.Time

	// Primary-specific fields
	PrimaryLSN               string
	ReadOnly                 bool
	CountReplicas            uint
	CountReachableReplicas   uint
	CountReplicatingReplicas uint
	CountLaggingReplicas     uint

	// Replica-specific fields
	ReplicationStopped     bool
	ReplicaLagMillis       int64
	IsLagging              bool
	ReplicaReplayLSN       string
	ReplicaReceiveLSN      string
	IsWalReplayPaused      bool
	WalReplayPauseState    string
	PrimaryConnInfoHost    string
	PrimaryConnInfoPort    int32
	PrimaryID              *clustermetadatapb.ID
	PrimaryPoolerID        *clustermetadatapb.ID
	PrimaryReachable       bool
	PrimaryTimestamp       time.Time
	PrimaryLSNStr          string
	ReplicationLagBytes    int64
	IsInPrimaryStandbyList bool          // Whether this replica is in the primary's synchronous_standby_names
	WalReceiverStatus      string        // WAL receiver status: "streaming", "stopping", "starting", "waiting", or empty
	HeartbeatLag           time.Duration // Time since primary last wrote a heartbeat, as observed via replicated row. -1 if unavailable.

	// Stale primary detection: populated for PRIMARY nodes only
	PrimaryTerm           int64          // This pooler's primary term (term when promoted)
	OtherPrimariesInShard []*PrimaryInfo // All other primaries detected in the shard
	HighestTermPrimary    *PrimaryInfo   // Primary with highest PrimaryTerm (rewind source)
	ConsensusTerm         int64          // This node's consensus term (from health check)

	// Primary health details (for distinguishing pooler-down vs postgres-down)
	PrimaryPoolerReachable bool // True if primary pooler health check succeeded (IsLastCheckValid)
	PrimaryPostgresRunning bool // True if primary Postgres is running (IsPostgresRunning from health check)

	// Total replica poolers in the shard (regardless of reachability).
	CountReplicaPoolersInShard uint
	// Replica poolers reachable by multiorch via gRPC (IsLastCheckValid == true).
	CountReachableReplicaPoolersInShard uint
	// Replicas confirming the primary is alive: reachable, streaming WAL from
	// the correct primary, and with healthy heartbeat.
	CountReplicasConfirmingPrimaryAliveInShard uint

	// AllReplicasConfirmPrimaryAlive is true only if ALL replicas in the shard
	// confirm the primary is alive (streaming WAL, correct primary, healthy heartbeat).
	// If even one replica fails the check, this is false.
	AllReplicasConfirmPrimaryAlive bool
}
