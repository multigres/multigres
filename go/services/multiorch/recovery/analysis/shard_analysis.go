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

package analysis

import (
	"time"

	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// ShardAnalysis groups all per-pooler analyses for a single shard.
// It is the input type for the Analyzer interface.
type ShardAnalysis struct {
	ShardKey commontypes.ShardKey
	Analyses []*PoolerAnalysis
}

// PrimaryInfo represents information about a primary pooler in the shard.
// Used for stale primary detection to identify which primary is most advanced.
type PrimaryInfo struct {
	ID            *clustermetadatapb.ID // Pooler ID
	ConsensusTerm int64                 // Current consensus term (for logging/context)
	PrimaryTerm   int64                 // Term when this node was promoted to primary
}

// PoolerAnalysis represents the analyzed state of a single pooler
// and its replication topology. This is the in-memory equivalent of
// VTOrc's replication_analysis table.
type PoolerAnalysis struct {
	// Identity
	PoolerID *clustermetadatapb.ID
	ShardKey commontypes.ShardKey

	// Pooler properties
	PoolerType clustermetadatapb.PoolerType
	IsPrimary  bool
	// Represents if the poolerID is reachable and it's returning a
	// valid status response
	LastCheckValid   bool
	IsStale          bool
	IsInitialized    bool // Whether this pooler is fully initialized and ready to join the cohort
	HasDataDirectory bool // Whether this pooler has a PostgreSQL data directory (PG_VERSION exists)
	AnalyzedAt       time.Time

	// Replica-specific fields
	ReplicationStopped     bool
	PrimaryConnInfoHost    string
	PrimaryPoolerID        *clustermetadatapb.ID
	PrimaryReachable       bool
	IsInPrimaryStandbyList bool // Whether this replica is in the primary's synchronous_standby_names

	// Stale primary detection: populated for PRIMARY nodes only
	PrimaryTerm           int64          // This pooler's primary term (term when promoted)
	OtherPrimariesInShard []*PrimaryInfo // All other primaries detected in the shard
	HighestTermPrimary    *PrimaryInfo   // Primary with highest PrimaryTerm (rewind source)
	ConsensusTerm         int64          // This node's consensus term (from health check)

	// Primary health details (for distinguishing pooler-down vs postgres-down)
	PrimaryPoolerReachable bool // True if primary pooler health check succeeded (IsLastCheckValid)
	PrimaryPostgresRunning bool // True if primary Postgres is running (IsPostgresRunning from health check)

	// ReplicasConnectedToPrimary is true only if ALL replicas in the shard are still
	// connected to the primary Postgres (have primary_conninfo configured and are receiving WAL).
	// Used to avoid failover when only the primary pooler is down but Postgres is still running.
	// If even one replica has lost connection, this is false.
	ReplicasConnectedToPrimary bool
}

// analyzeAllPoolers runs fn against each pooler analysis in sa, collecting all problems.
// Errors are accumulated — the first error encountered is returned alongside any problems collected.
func analyzeAllPoolers(sa *ShardAnalysis, fn func(*PoolerAnalysis) (*types.Problem, error)) ([]types.Problem, error) {
	var problems []types.Problem
	var firstErr error
	for _, poolerAnalysis := range sa.Analyses {
		p, err := fn(poolerAnalysis)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if p != nil {
			problems = append(problems, *p)
		}
	}
	return problems, firstErr
}
