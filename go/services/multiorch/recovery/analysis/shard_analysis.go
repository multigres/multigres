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
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// ShardAnalysis groups all per-pooler analyses for a single shard.
// It is the input type for the Analyzer interface.
type ShardAnalysis struct {
	ShardKey commontypes.ShardKey
	Analyses []*PoolerAnalysis

	// NumInitialized is the count of reachable, initialized poolers in this shard.
	// Pre-computed by the generator for use in analyzers.
	NumInitialized int

	// BootstrapDurabilityPolicy is the durability policy configured for this shard's database.
	// May be nil if not yet configured or not available.
	BootstrapDurabilityPolicy *clustermetadatapb.DurabilityPolicy

	// Shard-level aggregates computed once by the generator.

	// Leaders is the list of all reachable poolers in the shard that are reporting
	// as the consensus leader. More than one entry usually indicates a stale-leader but
	// could be a sign of split-brain if there's a consensus bug or unsafe manual override.
	Leaders []*PoolerAnalysis

	// HighestTermReachableLeader is the leader with the highest LeaderTerm among all
	// leaders in Leaders. Nil when Leaders is empty or there is a tie.
	HighestTermReachableLeader *PoolerAnalysis

	// HighestTermDiscoveredLeaderID is the pooler ID of the highest-term leader known to exist
	// in this shard's topology, regardless of whether it is currently reachable.
	// Nil if no leader has been recorded in topology yet.
	HighestTermDiscoveredLeaderID *clustermetadatapb.ID

	// LeaderReachable is true if the topology leader's pooler is reachable AND
	// its Postgres is running. False when TopologyLeaderID is nil.
	LeaderReachable bool

	// LeaderPoolerReachable is true if the topology leader's pooler health check
	// succeeded, independently of whether Postgres is running.
	// False when TopologyLeaderID is nil.
	LeaderPoolerReachable bool

	// LeaderStandbyIDs is the synchronous_standby_names list from the topology leader.
	// Nil when TopologyLeaderID is nil or the leader has no sync replication config.
	// Use IsInStandbyList to check membership.
	LeaderStandbyIDs []*clustermetadatapb.ID

	// HasInitializedReplica is true if at least one non-leader, reachable, initialized pooler exists
	// in the shard. This is a postgres-layer check (is there a standby that has joined the cluster?),
	// not a consensus-layer check — it does not require the pooler to be a cohort member. Used by
	// LeaderIsDeadAnalyzer to avoid false positives when no postgres standby can observe the leader.
	HasInitializedReplica bool

	// ReplicasConnectedToLeader is true only if ALL postgres standbys in the shard are still
	// connected to the leader's Postgres via WAL streaming (pg_stat_wal_receiver). Used to avoid
	// failover when only the leader pooler process is down but Postgres is still running.
	ReplicasConnectedToLeader bool

	// LeaderPostgresReady is true if the topology leader's Postgres is accepting connections
	// (pg_isready succeeds). Distinct from LeaderReachable: the pooler may be reachable
	// but Postgres may not yet be ready (e.g. still starting up).
	LeaderPostgresReady bool

	// LeaderPostgresRunning is true if the topology leader's Postgres process exists,
	// even if it is not accepting connections. False when the process is dead (SIGKILL).
	LeaderPostgresRunning bool

	// LeaderLastPostgresReadyTime is the last time the topology leader's Postgres
	// responded healthy (IsPostgresReady was true). Zero if never seen ready.
	// Used to time-bound failover suppression when followers are still connected.
	LeaderLastPostgresReadyTime time.Time

	// LeaderHasResigned is true when the topology leader has voluntarily requested
	// replacement via the REQUESTING_DEMOTION signal (set during EmergencyDemote).
	// When true, the LeaderIsDead failover suppression logic (which normally waits
	// for followers to disconnect before declaring the leader dead) is bypassed
	// because the resignation is an explicit and intentional signal, not an ambiguous
	// network/process failure.
	LeaderHasResigned bool

	// PromotingPrimaryID is the ID of the topology primary that is currently running
	// pg_promote() but has not yet transitioned to accepting connections. Nil when no
	// promotion is in progress.
	// Used by LeaderIsDeadAnalyzer to suppress spurious failover detection during the
	// brief window (~5–10s) when the newly promoted node's postgres is not yet ready.
	PromotingPrimaryID *clustermetadatapb.ID
}

// IsInStandbyList reports whether the given pooler ID appears in the leader's
// synchronous standby list. Returns false when no standby list is available.
func (sa *ShardAnalysis) IsInStandbyList(id *clustermetadatapb.ID) bool {
	for _, standbyID := range sa.LeaderStandbyIDs {
		if standbyID.Cell == id.Cell && standbyID.Name == id.Name {
			return true
		}
	}
	return false
}

// Replicas returns the PoolerAnalysis entries for all follower poolers.
func (sa *ShardAnalysis) Replicas() []*PoolerAnalysis {
	var replicas []*PoolerAnalysis
	for _, pa := range sa.Analyses {
		if !pa.IsLeader {
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
	IsLeader   bool
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

// compareLeaderTimeline compares two leader PoolerAnalysis entries by the
// coordinator term of each pooler's current rule (via commonconsensus.LeaderTerm).
// Returns negative if a is less advanced than b, 0 if equal, positive if a is
// more advanced. LSN is intentionally excluded: for leaders, the coordinator
// term must be unique per promotion, so equal terms indicate a consensus bug
// rather than a resolvable tie.
func compareLeaderTimeline(a, b *PoolerAnalysis) int {
	return cmp.Compare(
		commonconsensus.LeaderTerm(a.ConsensusStatus),
		commonconsensus.LeaderTerm(b.ConsensusStatus),
	)
}

// analyzeAllPoolers runs fn against each pooler analysis in sa, collecting all problems.
// Both the shard analysis and the per-pooler analysis are passed so callbacks can
// access shard-level fields (e.g. LeaderReachable) alongside pooler-specific state.
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
