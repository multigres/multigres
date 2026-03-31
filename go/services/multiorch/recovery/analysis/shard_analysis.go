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
	"cmp"
	"time"

	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/tools/pgutil"
)

// ShardAnalysis groups all per-pooler analyses for a single shard.
// It is the input type for the Analyzer interface.
type ShardAnalysis struct {
	ShardKey commontypes.ShardKey
	Analyses []*PoolerAnalysis

	// Shard-level aggregates computed once by the generator.

	// Primaries is the list of all reachable poolers in the shard that are reporting
	// as PRIMARY. More than one entry indicates a split-brain / stale-primary scenario.
	Primaries []*PoolerAnalysis

	// HighestTermPrimary is the primary with the highest PrimaryTerm among all
	// primaries in Primaries. Nil when Primaries is empty or there is a tie.
	HighestTermPrimary *PoolerAnalysis

	// ReplicasConnectedToPrimary is true only if ALL replicas in the shard are still
	// connected to the primary Postgres. Used to avoid failover when only the primary
	// pooler process is down but Postgres is still running.
	ReplicasConnectedToPrimary bool
}

// Replicas returns the PoolerAnalysis entries for all replica poolers.
func (sa *ShardAnalysis) Replicas() []*PoolerAnalysis {
	var replicas []*PoolerAnalysis
	for _, pa := range sa.Analyses {
		if !pa.IsPrimary {
			replicas = append(replicas, pa)
		}
	}
	return replicas
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

	// Primary term and WAL position
	PrimaryTerm   int64 // This pooler's primary term (term when promoted)
	ConsensusTerm int64 // This node's consensus term (from health check)
	// LSN is the most relevant WAL position for this node.
	// For primaries this should probably be the last committed LSN (pg_last_committed_xact()).
	// For replicas this should probably be the last applied/replayed LSN (pg_last_wal_replay_lsn()).
	// TODO: consider also tracking flush LSN (pg_current_wal_flush_lsn()) for primaries
	// to distinguish committed vs written-but-not-committed data.
	LSN pgutil.LSN

	// Primary health details (for distinguishing pooler-down vs postgres-down)
	PrimaryPoolerReachable bool // True if primary pooler health check succeeded (IsLastCheckValid)
	PrimaryPostgresRunning bool // True if primary Postgres is running (IsPostgresRunning from health check)
}

// comparePrimaryTimeline compares two primary PoolerAnalysis entries by PrimaryTerm only.
// Returns negative if a is less advanced than b, 0 if equal, positive if a is more advanced.
// LSN is intentionally excluded: for primaries, PrimaryTerm must be unique per promotion, so
// equal PrimaryTerms indicate a consensus bug rather than a resolvable tie.
func comparePrimaryTimeline(a, b *PoolerAnalysis) int {
	return cmp.Compare(a.PrimaryTerm, b.PrimaryTerm)
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
