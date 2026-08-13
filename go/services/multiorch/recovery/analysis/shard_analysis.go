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
	// Analyses holds the cache rider for every pooler in the shard. Analyzers
	// read raw health via Rider.Health() and derive judgments through the
	// package helpers (walReplayNotPaused, primaryConnInfoHost, …) and the
	// consensus SelfConsensusRole API rather than
	// reading pre-baked digest fields.
	Analyses []*store.Pooler

	// Now is the evaluation timestamp (orchestrator clock) captured when this
	// analysis was generated. Analyzers use it — together with Policy — to judge
	// observation freshness explicitly, rather than reading pre-baked liveness
	// verdicts from the generator.
	Now time.Time

	// Policy carries the availability thresholds in effect for this evaluation.
	Policy AvailabilityPolicy

	// TombstoneIDs is the set of pooler IDs the cache has marked as SHUTDOWN
	// tombstones cluster-wide. Analyzers consult it to detect cohort members
	// that have explicitly drained (and therefore had their riders evicted
	// from the live cache, so they don't appear in Analyses) versus poolers
	// that are merely missing from the cache for transient reasons. Cohort
	// scope is enforced naturally: cohort membership is per-shard, so a
	// missing cohort member found here is necessarily a shutdown of THIS
	// shard's pooler.
	TombstoneIDs map[topoclient.ComponentID]struct{}

	// HighestPosition is the highest known consensus position across all poolers
	// in the shard (commonconsensus.HighestKnownRule), or nil if no leader is
	// known. It is the single source of leader identity: HighestRule() names
	// the shard leader and its GetCohortMembers() is the recorded synchronous
	// cohort. Whether that leader is currently serving is judged by
	// leaderServing() from the rider, not stored here.
	HighestPosition *clustermetadatapb.RulePosition

	// Leader is the health of the pooler that HighestPosition's rule names as
	// leader, or nil if we have no health for it. The rule can name a leader we
	// have never observed; in that case we don't know where to point replicas,
	// so consumers that need the leader's host/port (e.g. ReplicaNotReplicating)
	// gate on Leader being non-nil rather than on reachability — an
	// unreachable-but-known leader is still the official term leader.
	Leader *store.Pooler

	// BootstrapDurabilityPolicy is the durability policy configured for this shard's database.
	// May be nil if not yet configured or not available.
	BootstrapDurabilityPolicy *clustermetadatapb.DurabilityPolicy
}

// Replicas returns the riders for all follower poolers.
func (sa *ShardAnalysis) Replicas() []*store.Pooler {
	var replicas []*store.Pooler
	for _, p := range sa.Analyses {
		if commonconsensus.SelfConsensusRole(p.Health().GetConsensusStatus()) != commonconsensus.ConsensusRoleLeader {
			replicas = append(replicas, p)
		}
	}
	return replicas
}

// The helpers below derive analyzer-relevant judgments from a pooler's raw
// health (Health()). They replaced the digested PoolerAnalysis fields so the
// rider stays the single source of truth and there is no parallel cached copy.

// poolerID returns the pooler's ID from its health record.
func poolerID(p *store.Pooler) *clustermetadatapb.ID {
	return p.Health().GetMultipooler().GetId()
}

// freshAndInitialized reports whether p's health snapshot is within freshness
// and initialized — the bar for treating p's report as usable evidence at all
// (its consensus state, replication status, identity, etc.), independent of
// whether p could actually be recruited into a new term. Use this for "do I
// have any trustworthy signal to reason from" judgments; use recruitable for
// "could this member actually win a Recruit round" judgments — a pooler's
// report can be fresh and initialized while it is not recruitable (e.g.
// draining), and the two questions must not be conflated. Named for exactly
// what it checks, not "reachable": this is purely about data trustworthiness,
// a third axis distinct from both network connectivity (StreamConnected) and
// recruitment eligibility (recruitable).
func freshAndInitialized(p *store.Pooler, now time.Time, freshness time.Duration) bool {
	hs, ok := p.HealthWithin(now, freshness)
	return ok && hs.GetStatus().GetIsInitialized()
}

// recruitable reports whether p is, as of now, usable as a target for
// recruitment or cohort admission: fresh and initialized (see above), plus it
// has not self-declared cohort-ineligible (draining — e.g. graceful shutdown
// or an admin-stopped WAL receiver), and it has no outstanding
// RecruitBlockedUntil (hasn't caught back up from a pg_rewind yet). Both
// ineligibility and a recruit-position floor are enforced synchronously
// server-side (Recruit rejects with FAILED_PRECONDITION), so this check does
// not itself guard correctness — it exists so callers' feasibility judgments
// (is a recruitment quorum reachable, is this pooler admissible) agree with
// what an actual Recruit attempt would do, instead of counting a member that
// would just be refused.
func recruitable(p *store.Pooler, now time.Time, freshness time.Duration) bool {
	if !freshAndInitialized(p, now, freshness) {
		return false
	}
	hs, _ := p.HealthWithin(now, freshness)
	if types.PoolerIsCohortIneligible(hs.GetAvailabilityStatus()) {
		return false
	}
	return hs.GetConsensusStatus().GetRecruitBlockedUntil() == nil
}

// walReplayNotPaused reports whether the standby's WAL replay is active. A
// pooler with no replication status returns false; callers that distinguish an
// unavailable observation from a negative one must check for nil first.
func walReplayNotPaused(p *store.Pooler) bool {
	rs := p.Health().GetStatus().GetReplicationStatus()
	if rs == nil {
		return false
	}
	return !rs.GetIsWalReplayPaused()
}

// primaryConnInfoHost returns the standby's configured primary host, or "" if
// replication is not configured.
func primaryConnInfoHost(p *store.Pooler) string {
	return p.Health().GetStatus().GetReplicationStatus().GetPrimaryConnInfo().GetHost()
}

// walReceiverActive reports whether the standby's WAL receiver is actively
// streaming or waiting. "waiting" is healthy: the receiver is connected but
// the primary has no new WAL to send. Any other status (including "") means
// the receiver is not running — this covers timeline divergence where the
// receiver connects, gets FATAL, and exits, leaving primary_conninfo on disk.
func walReceiverActive(p *store.Pooler) bool {
	rs := p.Health().GetStatus().GetReplicationStatus()
	if rs == nil {
		return false
	}
	return rs.GetWalReceiverStatus() == "streaming" || rs.GetWalReceiverStatus() == "waiting"
}

// walReceiverStreaming is a stricter form of walReceiverActive: it additionally
// rejects the brief window after a receiver reconnect where postgres reports
// "streaming" before any WAL has actually arrived (LastReceiveLsn still empty) —
// the same FATAL-retry flicker FixReplication guards against. It is the signal
// that a standby has bridged any initial archive catch-up and is genuinely
// pulling WAL from the leader, which is what cohort admission requires (a cohort
// member must advance only by streaming, never the archive). "waiting" stays
// healthy: the receiver is connected and current, the primary just has nothing
// new to send.
func walReceiverStreaming(p *store.Pooler) bool {
	rs := p.Health().GetStatus().GetReplicationStatus()
	if rs == nil {
		return false
	}
	switch rs.GetWalReceiverStatus() {
	case "waiting":
		return true
	case "streaming":
		return rs.GetLastReceiveLsn() != ""
	default:
		return false
	}
}

// compareLeaderTimeline compares two leader riders by rule position. LSN is
// intentionally excluded from the comparison (CompareRulePosition already
// stops at decision-then-proposal): for leaders, the coordinator term must be
// unique per promotion, so equal terms indicate a consensus bug rather than a
// resolvable tie.
func compareLeaderTimeline(a, b *store.Pooler) int {
	return commonconsensus.CompareRulePosition(
		a.Health().GetConsensusStatus().GetCurrentPosition().GetPosition(),
		b.Health().GetConsensusStatus().GetCurrentPosition().GetPosition(),
	)
}

// analyzeAllPoolers runs fn against each pooler analysis in sa, collecting all problems.
// Both the shard analysis and the per-pooler analysis are passed so callbacks can
// access shard-level context alongside pooler-specific state.
// Errors are accumulated — the first error encountered is returned alongside any problems collected.
func analyzeAllPoolers(sa *ShardAnalysis, fn func(*ShardAnalysis, *store.Pooler) (*types.Problem, error)) ([]types.Problem, error) {
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
