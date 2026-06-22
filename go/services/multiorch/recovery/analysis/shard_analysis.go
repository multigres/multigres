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
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// ShardAnalysis groups all per-pooler analyses for a single shard.
// It is the input type for the Analyzer interface.
type ShardAnalysis struct {
	ShardKey *clustermetadatapb.ShardKey
	Analyses []*PoolerAnalysis

	// TombstoneIDs is the set of pooler IDs the cache has marked as SHUTDOWN
	// tombstones cluster-wide. Analyzers consult it to detect cohort members
	// that have explicitly drained (and therefore had their riders evicted
	// from the live cache, so they don't appear in Analyses) versus poolers
	// that are merely missing from the cache for transient reasons. Cohort
	// scope is enforced naturally: cohort membership is per-shard, so a
	// missing cohort member found here is necessarily a shutdown of THIS
	// shard's pooler.
	TombstoneIDs map[topoclient.ComponentID]struct{}

	// HighestShardRule is the highest known consensus rule across all poolers in
	// the shard (commonconsensus.HighestKnownRule), or nil if no leader is known.
	// It is the single source of leader identity: GetLeaderId() names the shard
	// leader and GetCohortMembers() is its recorded synchronous cohort. Reachability
	// of that leader is captured separately by the LeaderReachable/LeaderPooler*
	// fields below.
	HighestShardRule *clustermetadatapb.ShardRule

	// Leader is the health of the pooler that HighestShardRule names as leader, or
	// nil if we have no health for it. The rule can name a leader we have never
	// observed; in that case we don't know where to point replicas, so consumers
	// that need the leader's host/port (e.g. ReplicaNotReplicating) gate on Leader
	// being non-nil rather than on reachability — an unreachable-but-known leader
	// is still the official term leader.
	Leader *store.Pooler

	// NumInitialized is the count of reachable, initialized poolers in this shard.
	// Pre-computed by the generator for use in analyzers.
	NumInitialized int

	// BootstrapDurabilityPolicy is the durability policy configured for this shard's database.
	// May be nil if not yet configured or not available.
	BootstrapDurabilityPolicy *clustermetadatapb.DurabilityPolicy

	// Shard-level aggregates computed once by the generator.

	// LeaderReachable is true if the topology leader's pooler is reachable AND
	// its Postgres is running. False when TopologyLeaderID is nil.
	LeaderReachable bool

	// LeaderPoolerReachable is true if the topology leader's pooler health check
	// succeeded, independently of whether Postgres is running.
	// False when TopologyLeaderID is nil.
	LeaderPoolerReachable bool

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
	// replacement via the REQUESTING_DEMOTION signal (set during Recruit's
	// primary-demotion path or graceful shutdown of a leader). LeaderResignedAnalyzer
	// keys off this to trigger immediate failover, separately from the LeaderIsDead
	// reachability-based path.
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
	for _, standbyID := range sa.HighestShardRule.GetCohortMembers() {
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
		if !pa.NamesSelfAsLeader {
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
	NamesSelfAsLeader bool
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

	// Replica-specific fields. WalReplayNotPaused is true when the standby's WAL
	// replay is not paused. The zero value (false) means "not running", so an
	// unpopulated analysis errs toward repair rather than assuming health.
	WalReplayNotPaused  bool
	PrimaryConnInfoHost string

	// This is no longer needed and can be derived from ConsensusStatus, but is
	// left here for now.
	ConsensusTerm int64 // This node's consensus term (from health check)

	// ConsensusStatus from the pooler's most recent StatusResponse snapshot.
	// Used to derive the primary term via commonconsensus.PrimaryTerm(ConsensusStatus).
	ConsensusStatus *clustermetadatapb.ConsensusStatus

	// AvailabilityStatus carries the pooler's self-reported willingness signals
	// (cohort eligibility, leader-resignation request). May be nil for older
	// poolers that don't publish it.
	AvailabilityStatus *clustermetadatapb.AvailabilityStatus
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
