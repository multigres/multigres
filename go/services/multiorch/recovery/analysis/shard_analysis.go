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

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// ShardAnalysis groups all per-pooler analyses for a single shard.
// It is the input type for the Analyzer interface.
type ShardAnalysis struct {
	ShardKey *clustermetadatapb.ShardKey
	Analyses []*PoolerAnalysis

	// NumInitialized is the count of reachable, initialized poolers in this shard.
	// Pre-computed by the generator for use in analyzers.
	NumInitialized int

	// BootstrapDurabilityPolicy is the durability policy configured for this shard's database.
	// May be nil if not yet configured or not available.
	BootstrapDurabilityPolicy *clustermetadatapb.DurabilityPolicy

	// Shard-level aggregates computed once by the generator.

	// Primaries is the list of all reachable poolers in the shard that are reporting
	// as PRIMARY. More than one entry indicates a split-brain / stale-primary scenario.
	Primaries []*PoolerAnalysis

	// HighestTermReachablePrimary is the primary with the highest PrimaryTerm among all
	// primaries in Primaries. Nil when Primaries is empty or there is a tie.
	HighestTermReachablePrimary *PoolerAnalysis

	// HighestTermDiscoveredPrimaryID is the pooler ID of the highest-term primary known to exist
	// in this shard's topology, regardless of whether it is currently reachable.
	// Nil if no primary has been recorded in topology yet.
	HighestTermDiscoveredPrimaryID *clustermetadatapb.ID

	// PrimaryReachable is true if the topology primary's pooler is reachable AND
	// its Postgres is running. False when TopologyPrimaryID is nil.
	PrimaryReachable bool

	// PrimaryPoolerReachable is true if the topology primary's pooler health check
	// succeeded, independently of whether Postgres is running.
	// False when TopologyPrimaryID is nil.
	PrimaryPoolerReachable bool

	// PrimaryStandbyIDs is the synchronous_standby_names list from the topology primary.
	// Nil when TopologyPrimaryID is nil or the primary has no sync replication config.
	// Use IsInStandbyList to check membership.
	PrimaryStandbyIDs []*clustermetadatapb.ID

	// HasInitializedReplica is true if at least one non-primary, reachable,
	// initialized pooler exists in the shard. Used by PrimaryIsDeadAnalyzer to
	// avoid false positives when the shard has no replica that can observe the primary.
	HasInitializedReplica bool

	// ReplicasConnectedToPrimary is true only if ALL replicas in the shard are still
	// connected to the primary Postgres. Used to avoid failover when only the primary
	// pooler process is down but Postgres is still running.
	ReplicasConnectedToPrimary bool

	// PrimaryPostgresReady is true if the topology primary's Postgres is accepting connections
	// (pg_isready succeeds). Distinct from PrimaryReachable: the pooler may be reachable
	// but Postgres may not yet be ready (e.g. still starting up).
	PrimaryPostgresReady bool

	// PrimaryPostgresRunning is true if the topology primary's Postgres process exists,
	// even if it is not accepting connections. False when the process is dead (SIGKILL).
	PrimaryPostgresRunning bool

	// PrimaryLastPostgresReadyTime is the last time the topology primary's Postgres
	// responded healthy (IsPostgresReady was true). Zero if never seen ready.
	// Used to time-bound failover suppression when replicas are still connected.
	PrimaryLastPostgresReadyTime time.Time

	// PrimaryHasResigned is true when the topology primary has voluntarily requested
	// replacement via the REQUESTING_DEMOTION signal (set during EmergencyDemote).
	// When true, the PrimaryIsDead failover suppression logic (which normally waits
	// for replicas to disconnect before declaring the primary dead) is bypassed
	// because the resignation is an explicit and intentional signal, not an ambiguous
	// network/process failure.
	PrimaryHasResigned bool

	// PromotingPrimaryID is the ID of the topology primary that is currently running
	// pg_promote() but has not yet transitioned to accepting connections. Nil when no
	// promotion is in progress.
	// Used by PrimaryIsDeadAnalyzer to suppress spurious failover detection during the
	// brief window (~5–10s) when the newly promoted node's postgres is not yet ready.
	PromotingPrimaryID *clustermetadatapb.ID
}

// IsInStandbyList reports whether the given pooler ID appears in the primary's
// synchronous standby list. Returns false when no standby list is available.
func (sa *ShardAnalysis) IsInStandbyList(id *clustermetadatapb.ID) bool {
	for _, standbyID := range sa.PrimaryStandbyIDs {
		if standbyID.Cell == id.Cell && standbyID.Name == id.Name {
			return true
		}
	}
	return false
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
	ShardKey *clustermetadatapb.ShardKey

	// Pooler properties
	PoolerType clustermetadatapb.PoolerType
	IsPrimary  bool
	// Represents if the poolerID is reachable and it's returning a
	// valid status response
	LastCheckValid   bool
	IsStale          bool
	IsInitialized    bool // Whether this pooler is fully initialized and ready to join the cohort
	HasDataDirectory bool // Whether this pooler has a PostgreSQL data directory (PG_VERSION exists)
	// CohortMembers are the strongly-typed IDs from the most recent
	// multigres.leadership_history record. Nil or empty both indicate no cohort
	// has been established. When IsInitialized=true, an empty list means the
	// 0-member bootstrap record is present — Phase 2 is needed.
	CohortMembers []*clustermetadatapb.ID
	AnalyzedAt    time.Time

	// Replica-specific fields
	ReplicationStopped  bool
	PrimaryConnInfoHost string

	// This is no longer needed and can be derived from ConsensusStatus, but is
	// left here for now.
	ConsensusTerm int64 // This node's consensus term (from health check)

	// ConsensusStatus from the pooler's most recent StatusResponse snapshot.
	// Used to derive the primary term via commonconsensus.PrimaryTerm(ConsensusStatus).
	ConsensusStatus *clustermetadatapb.ConsensusStatus
}

// comparePrimaryTimeline compares two primary PoolerAnalysis entries by primary term only.
// Returns negative if a is less advanced than b, 0 if equal, positive if a is more advanced.
// LSN is intentionally excluded: primary terms must be unique per promotion, so equal terms
// indicate a consensus bug rather than a resolvable tie.
func comparePrimaryTimeline(a, b *PoolerAnalysis) int {
	return cmp.Compare(
		commonconsensus.PrimaryTerm(a.ConsensusStatus),
		commonconsensus.PrimaryTerm(b.ConsensusStatus),
	)
}

// analyzeAllPoolers runs fn against each pooler analysis in sa, collecting all problems.
// Both the shard analysis and the per-pooler analysis are passed so callbacks can
// access shard-level fields (e.g. PrimaryReachable) alongside pooler-specific state.
// Errors are accumulated — the first error encountered is returned alongside any problems collected.
func analyzeAllPoolers(sa *ShardAnalysis, fn func(*ShardAnalysis, *PoolerAnalysis) (*types.Problem, error)) ([]types.Problem, error) {
	var problems []types.Problem
	var firstErr error
	for _, poolerAnalysis := range sa.Analyses {
		p, err := fn(sa, poolerAnalysis)
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
