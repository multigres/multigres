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

import (
	"fmt"
	"slices"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// DefaultReplicaLagThreshold is the threshold above which a replica is considered lagging.
const DefaultReplicaLagThreshold = 10 * time.Second

// replicationHeartbeatStalenessMultiplier is applied to wal_receiver_status_interval
// to compute the heartbeat staleness threshold. The replica sends a status message
// to the primary every wal_receiver_status_interval; the primary echoes a keepalive
// reply. Three missed intervals means the primary has gone silent well before the
// wal_receiver_timeout (60s) would disconnect the WAL receiver.
const replicationHeartbeatStalenessMultiplier = 3

// defaultReplicationHeartbeatStalenessThreshold is the fallback threshold used
// when wal_receiver_status_interval is not available in the replica's health
// state. Equals replicationHeartbeatStalenessMultiplier × the default
// wal_receiver_status_interval (10s).
const defaultReplicationHeartbeatStalenessThreshold = 30 * time.Second

// PoolersByShard is a structured map for efficient lookups.
// Structure: [database][tablegroup][shard][pooler_id] -> PoolerHealthState
type PoolersByShard map[string]map[string]map[string]map[string]*multiorchdatapb.PoolerHealthState

// AnalysisGenerator creates ReplicationAnalysis from the pooler store.
type AnalysisGenerator struct {
	poolerStore    *store.PoolerStore
	poolersByShard PoolersByShard
	// policyLookup returns the bootstrap durability policy for a database name.
	// May be nil; when nil, ShardAnalysis.BootstrapDurabilityPolicy is left nil.
	policyLookup func(database string) *clustermetadatapb.DurabilityPolicy
	now          func() time.Time
}

// NewAnalysisGenerator creates a new analysis generator.
// It eagerly builds the poolersByShard map from the current store state.
// policyLookup is optional; pass nil if the bootstrap policy is unavailable.
func NewAnalysisGenerator(poolerStore *store.PoolerStore, policyLookup func(database string) *clustermetadatapb.DurabilityPolicy) *AnalysisGenerator {
	g := &AnalysisGenerator{
		poolerStore:  poolerStore,
		policyLookup: policyLookup,
		now:          time.Now,
	}
	g.poolersByShard = g.buildPoolersByShard()
	return g
}

// GenerateShardAnalyses groups per-pooler analyses into one ShardAnalysis per shard.
func (g *AnalysisGenerator) GenerateShardAnalyses() []*ShardAnalysis {
	type shardEntry struct {
		key     commontypes.ShardKey
		poolers map[string]*multiorchdatapb.PoolerHealthState
	}
	byKey := make(map[commontypes.ShardKey]*shardEntry)

	for database, tableGroups := range g.poolersByShard {
		for tableGroup, shards := range tableGroups {
			for shard, poolers := range shards {
				key := commontypes.ShardKey{Database: database, TableGroup: tableGroup, Shard: shard}
				byKey[key] = &shardEntry{key: key, poolers: poolers}
			}
		}
	}

	result := make([]*ShardAnalysis, 0, len(byKey))
	for _, entry := range byKey {
		result = append(result, g.buildShardAnalysis(entry.key, entry.poolers))
	}
	return result
}

// GenerateShardAnalysis returns a ShardAnalysis for a specific shard.
// Returns an error if no poolers for that shard are found in the store.
func (g *AnalysisGenerator) GenerateShardAnalysis(shardKey commontypes.ShardKey) (*ShardAnalysis, error) {
	poolers, ok := g.poolersByShard[shardKey.Database][shardKey.TableGroup][shardKey.Shard]
	if !ok || len(poolers) == 0 {
		return nil, fmt.Errorf("shard not found: %s", shardKey)
	}
	return g.buildShardAnalysis(shardKey, poolers), nil
}

// buildShardAnalysis constructs a ShardAnalysis for a shard, including shard-level aggregates.
func (g *AnalysisGenerator) buildShardAnalysis(shardKey commontypes.ShardKey, poolers map[string]*multiorchdatapb.PoolerHealthState) *ShardAnalysis {
	sa := &ShardAnalysis{ShardKey: shardKey}
	for _, pooler := range poolers {
		sa.Analyses = append(sa.Analyses, g.generateAnalysisForPooler(pooler, shardKey))
	}
	g.computeShardLevelFields(sa, poolers)
	return sa
}

// buildPoolersByShard creates a structured map by iterating the store once.
// Since ProtoStore.Range() returns clones, we don't need explicit DeepCopy.
func (g *AnalysisGenerator) buildPoolersByShard() PoolersByShard {
	poolersByShard := make(PoolersByShard)

	g.poolerStore.Range(func(poolerID string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // skip nil entries
		}

		database := pooler.MultiPooler.Database
		tableGroup := pooler.MultiPooler.TableGroup
		shard := pooler.MultiPooler.Shard

		// Initialize nested maps if needed
		if poolersByShard[database] == nil {
			poolersByShard[database] = make(map[string]map[string]map[string]*multiorchdatapb.PoolerHealthState)
		}
		if poolersByShard[database][tableGroup] == nil {
			poolersByShard[database][tableGroup] = make(map[string]map[string]*multiorchdatapb.PoolerHealthState)
		}
		if poolersByShard[database][tableGroup][shard] == nil {
			poolersByShard[database][tableGroup][shard] = make(map[string]*multiorchdatapb.PoolerHealthState)
		}

		// Store the pooler (already a clone from Range)
		poolersByShard[database][tableGroup][shard][poolerID] = pooler
		return true // continue
	})

	return poolersByShard
}

// GetPoolersInShard returns all pooler IDs in the same shard as the given pooler.
// Uses the cached poolersByShard for efficient lookup.
func (g *AnalysisGenerator) GetPoolersInShard(poolerIDStr string) ([]string, error) {
	// Get pooler from store to determine its shard
	pooler, ok := g.poolerStore.Get(poolerIDStr)
	if !ok {
		return nil, fmt.Errorf("pooler not found in store: %s", poolerIDStr)
	}

	if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
		return nil, fmt.Errorf("pooler or ID is nil: %s", poolerIDStr)
	}

	database := pooler.MultiPooler.Database
	tableGroup := pooler.MultiPooler.TableGroup
	shard := pooler.MultiPooler.Shard

	// Use cached poolersByShard for efficient lookup
	poolers, ok := g.poolersByShard[database][tableGroup][shard]
	if !ok {
		return []string{}, nil
	}

	poolerIDs := make([]string, 0, len(poolers))
	for id := range poolers {
		poolerIDs = append(poolerIDs, id)
	}

	return poolerIDs, nil
}

// GenerateAnalysisForPooler generates and returns the ShardAnalysis for the shard containing
// the given pooler ID. Used primarily in tests to inspect shard-level fields like
// ReplicasConnectedToPrimary without running the full analysis loop.
func (g *AnalysisGenerator) GenerateAnalysisForPooler(poolerIDStr string) (*ShardAnalysis, error) {
	pooler, ok := g.poolerStore.Get(poolerIDStr)
	if !ok {
		return nil, fmt.Errorf("pooler not found in store: %s", poolerIDStr)
	}
	if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
		return nil, fmt.Errorf("pooler or ID is nil: %s", poolerIDStr)
	}

	database := pooler.MultiPooler.Database
	tableGroup := pooler.MultiPooler.TableGroup
	shard := pooler.MultiPooler.Shard

	poolers, ok := g.poolersByShard[database][tableGroup][shard]
	if !ok || len(poolers) == 0 {
		return nil, fmt.Errorf("shard not found for pooler: %s", poolerIDStr)
	}

	shardKey := commontypes.ShardKey{Database: database, TableGroup: tableGroup, Shard: shard}
	return g.buildShardAnalysis(shardKey, poolers), nil
}

// generateAnalysisForPooler creates a ReplicationAnalysis for a single pooler.
func (g *AnalysisGenerator) generateAnalysisForPooler(
	pooler *multiorchdatapb.PoolerHealthState,
	shardKey commontypes.ShardKey,
) *PoolerAnalysis {
	// Determine pooler type from health check (PoolerType).
	// Nodes are never created with topology type PRIMARY, so health check is authoritative.
	// Fall back to topology type only if health check type is UNKNOWN.
	poolerType := pooler.GetStatus().GetPoolerType()
	if poolerType == clustermetadatapb.PoolerType_UNKNOWN {
		poolerType = pooler.MultiPooler.Type
	}

	analysis := &PoolerAnalysis{
		PoolerID:         pooler.MultiPooler.Id,
		ShardKey:         shardKey,
		PoolerType:       poolerType,
		IsLeader:         commonconsensus.IsLeader(pooler.GetConsensusStatus().GetConsensusStatus()),
		LastCheckValid:   pooler.IsLastCheckValid,
		IsInitialized:    store.IsInitialized(pooler),
		HasDataDirectory: pooler.GetStatus().GetHasDataDirectory(),
		CohortMembers:    pooler.GetStatus().GetCohortMembers(),
		AnalyzedAt:       time.Now(),
	}

	// Compute staleness
	analysis.IsStale = !pooler.IsUpToDate

	// Store consensus status.
	if pooler.ConsensusStatus != nil {
		analysis.ConsensusTerm = pooler.ConsensusStatus.GetConsensusStatus().GetTermRevocation().GetRevokedBelowTerm()
		analysis.ConsensusStatus = pooler.ConsensusStatus.ConsensusStatus
	}

	// If this is a REPLICA, populate replica-specific fields
	if !analysis.IsLeader {
		if rs := pooler.GetStatus().GetReplicationStatus(); rs != nil {
			analysis.ReplicationStopped = rs.IsWalReplayPaused

			// Extract primary connection info
			if rs.PrimaryConnInfo != nil {
				analysis.PrimaryConnInfoHost = rs.PrimaryConnInfo.Host
			}
		}
	}

	return analysis
}

// findHighestTermRawLeader returns the raw PoolerHealthState with the highest LeaderTerm
// among all PRIMARY-typed poolers, regardless of reachability. Returns nil if none found.
func findHighestTermRawLeader(poolers map[string]*multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState {
	// TODO: If multiple poolers claim to be leader at the same term, we should surface an error that
	// manual intervention is needed.

	var best *multiorchdatapb.PoolerHealthState
	var bestTerm int64
	for _, pooler := range poolers {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			continue
		}
		// Note: if a node is the consensus leader but reports PoolerType_REPLICA (e.g.
		// its postgres crashed and restarted in recovery mode without going through the
		// normal resign flow), it is skipped here and won't appear in
		// HighestTermDiscoveredLeaderID, so LeaderIsDead detection would miss it.
		// The normal resign path avoids this because PoolerType stays PRIMARY until
		// DemoteStalePrimary completes.
		if pooler.GetStatus().GetPoolerType() != clustermetadatapb.PoolerType_PRIMARY {
			continue
		}
		term := commonconsensus.LeaderTerm(pooler.GetConsensusStatus().GetConsensusStatus())
		if best == nil || term > bestTerm {
			best = pooler
			bestTerm = term
		}
	}
	return best
}

// allFollowersConnectedToLeader checks if ALL replicas in the shard are connected to the leader's postgres.
// A replica is considered connected if:
// 1. Its health check is valid (IsLastCheckValid)
// 2. It has PrimaryConnInfo configured pointing to the leader's postgres
// 3. It has received WAL (LastReceiveLsn is not empty)
//
// Returns true only if all replicas meet these criteria.
// Returns false if there are no replicas or any replica is disconnected.
func (g *AnalysisGenerator) allFollowersConnectedToLeader(
	primary *multiorchdatapb.PoolerHealthState,
	poolers map[string]*multiorchdatapb.PoolerHealthState,
) bool {
	primaryIDStr := topoclient.MultiPoolerIDString(primary.MultiPooler.Id)
	primaryHost := primary.MultiPooler.Hostname
	primaryPort := primary.MultiPooler.PortMap["postgres"]

	replicaCount := 0
	connectedCount := 0

	for poolerID, pooler := range poolers {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			continue
		}

		// Skip the leader itself
		if poolerID == primaryIDStr {
			continue
		}

		// Skip non-replicas. Note: a node whose postgres crashed into recovery mode
		// without going through the normal resign flow could report PoolerType_REPLICA
		// while still being the consensus leader. Such a node would be incorrectly
		// counted as a follower here, overstating connected-follower count.
		replicaType := pooler.GetStatus().GetPoolerType()
		if replicaType == clustermetadatapb.PoolerType_UNKNOWN {
			replicaType = pooler.MultiPooler.Type
		}
		if replicaType != clustermetadatapb.PoolerType_REPLICA {
			continue
		}

		replicaCount++

		// Check if replica is connected to the leader's postgres
		if !g.isFollowerConnectedToLeader(pooler, primaryHost, primaryPort) {
			continue
		}

		connectedCount++
	}

	// All replicas must be connected (and there must be at least one replica)
	return replicaCount > 0 && connectedCount == replicaCount
}

// isFollowerConnectedToLeader checks if a single replica is actively connected to the leader's postgres.
// It verifies both that the connection is configured correctly and that the WAL receiver is
// actively exchanging keepalives with the leader's postgres via pg_stat_wal_receiver.
func (g *AnalysisGenerator) isFollowerConnectedToLeader(
	replica *multiorchdatapb.PoolerHealthState,
	primaryHost string,
	primaryPort int32,
) bool {
	// Replica must be reachable
	if !replica.IsLastCheckValid {
		return false
	}

	// Replica must have replication status
	rs := replica.GetStatus().GetReplicationStatus()
	if rs == nil {
		return false
	}

	// Replica must have PrimaryConnInfo pointing to the primary
	connInfo := rs.PrimaryConnInfo
	if connInfo == nil || connInfo.Host == "" {
		return false
	}

	// Verify the replica is pointing to the correct primary. Note: if this is
	// not the case, there is a more fundamental problem (e.g., misconfiguration
	// or split-brain). This is not correctly indicated by a simple "false"
	// return value, but we still want to return false here to avoid falsely
	// triggering failover analyzers that rely on this method.
	if connInfo.Host != primaryHost || connInfo.Port != primaryPort {
		return false
	}

	// Replica must have received WAL (indicates connection was established)
	if rs.LastReceiveLsn == "" {
		return false
	}

	// WAL receiver must be in streaming state
	if rs.WalReceiverStatus != "streaming" {
		return false
	}

	// If last_msg_receive_time is available, verify the leader's postgres is still
	// sending keepalives. The threshold is
	// replicationHeartbeatStalenessMultiplier × wal_receiver_status_interval,
	// falling back to defaultReplicationHeartbeatStalenessThreshold when the
	// interval is unknown.
	//
	// If the last heartbeat is older than WAL receiver timeout, the connection
	// is effectively dead even if the replica hasn't noticed yet, so we check
	// that as well.
	if ts := rs.LastMsgReceiveTime; ts != nil {
		threshold := defaultReplicationHeartbeatStalenessThreshold
		delay := g.now().Sub(ts.AsTime())
		if d := rs.WalReceiverTimeout; d != nil && delay > d.AsDuration() {
			return false
		}
		if d := rs.WalReceiverStatusInterval; d != nil && d.AsDuration() > 0 {
			threshold = replicationHeartbeatStalenessMultiplier * d.AsDuration()
		}
		if delay > threshold {
			return false
		}
	}

	return true
}

// computeShardLevelFields populates shard-level aggregates on sa after all per-pooler
// analyses have been built. These fields describe the shard as a whole rather than
// any individual pooler, so they are computed once here rather than per-pooler.
func (g *AnalysisGenerator) computeShardLevelFields(sa *ShardAnalysis, poolers map[string]*multiorchdatapb.PoolerHealthState) {
	// Bootstrap durability policy lookup.
	if g.policyLookup != nil {
		sa.BootstrapDurabilityPolicy = g.policyLookup(sa.ShardKey.Database)
	}

	// Count reachable, initialized poolers for bootstrap analysis.
	for _, pa := range sa.Analyses {
		if pa.LastCheckValid && pa.IsInitialized {
			sa.NumInitialized++
		}
	}

	// Collect all reachable primaries in the shard.
	for _, pa := range sa.Analyses {
		if pa.IsLeader && pa.LastCheckValid {
			sa.Leaders = append(sa.Leaders, pa)
		}
	}

	// Determine the highest-term reachable leader (used for stale-leader detection).
	sa.HighestTermReachableLeader = findHighestTermLeader(sa.Leaders)

	// Compute the highest-term leader in the shard regardless of reachability.
	// This may differ from HighestTermReachableLeader when the leader pooler is down.
	topologyPrimary := findHighestTermRawLeader(poolers)
	if topologyPrimary != nil {
		sa.HighestTermDiscoveredLeaderID = topologyPrimary.MultiPooler.Id
		sa.LeaderPoolerReachable = topologyPrimary.IsLastCheckValid
		sa.LeaderPostgresReady = topologyPrimary.GetStatus().GetPostgresReady()
		sa.LeaderPostgresRunning = topologyPrimary.GetStatus().GetPostgresRunning()
		sa.LeaderHasResigned = types.LeaderNeedsReplacement(topologyPrimary)
		sa.LeaderReachable = topologyPrimary.IsLastCheckValid && topologyPrimary.GetStatus().GetPostgresReady() && !sa.LeaderHasResigned
		if topologyPrimary.LastPostgresReadyTime != nil {
			sa.LeaderLastPostgresReadyTime = topologyPrimary.LastPostgresReadyTime.AsTime()
		}

		// Populate the standby list from the topology primary (used by IsInStandbyList).
		if ps := topologyPrimary.GetStatus().GetPrimaryStatus(); ps != nil && ps.SyncReplicationConfig != nil {
			sa.LeaderStandbyIDs = ps.SyncReplicationConfig.StandbyIds
		}
	}

	// HasInitializedReplica: any non-primary, reachable, initialized pooler.
	for _, pa := range sa.Analyses {
		if !pa.IsLeader && pa.LastCheckValid && pa.IsInitialized {
			sa.HasInitializedReplica = true
			break
		}
	}

	// Determine if all followers are still connected to the leader's postgres.
	// Use the highest-term discovered leader (which may be unreachable) so we can detect the
	// "pooler down but postgres still running" scenario that FollowersConnectedToLeader
	// is designed to catch.
	if topologyPrimary != nil {
		sa.FollowersConnectedToLeader = g.allFollowersConnectedToLeader(topologyPrimary, poolers)
	}
}

// findHighestTermLeader returns the leader PoolerAnalysis with the highest LeaderTerm.
// Returns nil if leaders is empty, all have LeaderTerm=0, or there is a tie.
//
// Invariant: In a properly initialized shard, LeaderTerm is always >0 for PRIMARY poolers.
// LeaderTerm is set during promotion and only cleared during demotion. This function
// is defensive and returns nil if all leaders have LeaderTerm=0, but this should
// never happen in a properly initialized shard.
func findHighestTermLeader(primaries []*PoolerAnalysis) *PoolerAnalysis {
	if len(primaries) == 0 {
		return nil
	}

	mostAdvanced := slices.MaxFunc(primaries, compareLeaderTimeline)

	// Defensive: should not happen in initialized shards, but guard against invalid state
	if commonconsensus.LeaderTerm(mostAdvanced.ConsensusStatus) == 0 {
		return nil
	}

	// Tie detection: multiple leaders with the same LeaderTerm indicates a consensus bug.
	// LeaderTerm should be unique per leader and monotonically increasing. If two leaders
	// claim the same LeaderTerm, something went wrong in the consensus protocol (bug in
	// promotion logic, data corruption, or split-brain).
	//
	// TODO: Rather than requiring manual intervention, multiorch could automatically resolve
	// this by starting a new term and reappointing one of the leaders, which would update
	// its leader_term and make the others stale. For now, we skip automatic demotion to
	// avoid making the situation worse without understanding the root cause.
	for _, p := range primaries {
		if p != mostAdvanced && compareLeaderTimeline(p, mostAdvanced) == 0 {
			return nil
		}
	}

	return mostAdvanced
}
