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
	LastCheckValid       bool
	IsStale              bool
	IsUnreachable        bool
	IsInitialized        bool // Whether this pooler has been initialized
	AnalyzedAt           time.Time

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
	IsInPrimaryStandbyList bool // Whether this replica is in the primary's synchronous_standby_names
	TimelineDiverged       bool // Whether this replica has a diverged timeline that needs pg_rewind

	// Stale primary detection: populated for PRIMARY nodes only
	OtherPrimaryInShard *clustermetadatapb.ID // ID of another PRIMARY in same shard (if detected)
	OtherPrimaryTerm    int64                 // Consensus term of the other primary
	ConsensusTerm       int64                 // This node's consensus term (from health check)

	// Primary health details (for distinguishing pooler-down vs postgres-down)
	PrimaryPoolerReachable bool // True if primary pooler health check succeeded (IsLastCheckValid)
	PrimaryPostgresRunning bool // True if primary Postgres is running (IsPostgresRunning from health check)

	// ReplicasConnectedToPrimary is true only if ALL replicas in the shard are still
	// connected to the primary Postgres (have primary_conninfo configured and are receiving WAL).
	// Used to avoid failover when only the primary pooler is down but Postgres is still running.
	// If even one replica has lost connection, this is false.
	ReplicasConnectedToPrimary bool
}
