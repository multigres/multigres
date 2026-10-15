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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/internal/snapshot"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// The helpers below derive analyzer-relevant judgments from a pooler's raw
// health (Health()). They replaced the digested PoolerAnalysis fields so the
// snapshot stays the single source of truth and there is no parallel cached copy.

// leaderEndpoint returns the leader's postgres host and port; host is "" when
// there is no leader snapshot or it hasn't reported a hostname.
func leaderEndpoint(leader *multiorchdatapb.PoolerHealthState) (host string, port int32) {
	return leader.GetMultipooler().GetHostname(), leader.GetMultipooler().GetPortMap()["postgres"]
}

// poolerID returns the pooler's ID from its health record.
func poolerID(p *multiorchdatapb.PoolerHealthState) *clustermetadatapb.ID {
	return p.GetMultipooler().GetId()
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
func freshAndInitialized(p *multiorchdatapb.PoolerHealthState, now time.Time, freshness time.Duration) bool {
	hs, ok := snapshot.HealthWithin(p, now, freshness)
	return ok && hs.GetStatus().GetIsInitialized()
}

// recruitable reports whether p is, as of now, usable as a target for
// recruitment or cohort admission. Each check here is also enforced
// synchronously server-side (Recruit/ValidateRevocation refuses the same
// cases with FAILED_PRECONDITION), so this doesn't itself guard correctness —
// it exists so callers' feasibility judgments (is a recruitment quorum
// reachable, is this pooler admissible) agree with what an actual Recruit
// attempt would do, instead of counting a member that would just be refused.
func recruitable(p *multiorchdatapb.PoolerHealthState, now time.Time, freshness time.Duration) bool {
	if !freshAndInitialized(p, now, freshness) {
		return false
	}
	hs, _ := snapshot.HealthWithin(p, now, freshness)
	// Draining (graceful shutdown or an admin-stopped WAL receiver).
	if types.PoolerIsCohortIneligible(hs.GetAvailabilityStatus()) {
		return false
	}
	// No cached position — e.g. a flapping pooler whose consensus status
	// hasn't completed a read yet. Without this, a failover could look
	// feasible forever while every real Recruit attempt is refused, so
	// collective backoff never engages (nothing ever persists).
	if hs.GetConsensusStatus().GetCurrentPosition() == nil {
		return false
	}
	// Hasn't caught back up from a pg_rewind yet.
	return hs.GetConsensusStatus().GetRecruitBlockedUntil() == nil
}

// walReplayNotPaused reports whether the standby's WAL replay is active. A
// pooler with no replication status returns false; callers that distinguish an
// unavailable observation from a negative one must check for nil first.
func walReplayNotPaused(p *multiorchdatapb.PoolerHealthState) bool {
	rs := p.GetStatus().GetReplicationStatus()
	if rs == nil {
		return false
	}
	return !rs.GetIsWalReplayPaused()
}

// primaryConnInfoHost returns the standby's configured primary host, or "" if
// replication is not configured.
func primaryConnInfoHost(p *multiorchdatapb.PoolerHealthState) string {
	return p.GetStatus().GetReplicationStatus().GetPrimaryConnInfo().GetHost()
}

// walReceiverActive reports whether the standby's WAL receiver is actively
// streaming or waiting. "waiting" is healthy: the receiver is connected but
// the primary has no new WAL to send. Any other status (including "") means
// the receiver is not running — this covers timeline divergence where the
// receiver connects, gets FATAL, and exits, leaving primary_conninfo on disk.
func walReceiverActive(p *multiorchdatapb.PoolerHealthState) bool {
	rs := p.GetStatus().GetReplicationStatus()
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
func walReceiverStreaming(p *multiorchdatapb.PoolerHealthState) bool {
	rs := p.GetStatus().GetReplicationStatus()
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

// compareLeaderTimeline compares two leader snapshots by rule position. LSN is
// intentionally excluded from the comparison (CompareRulePosition already
// stops at decision-then-proposal): for leaders, the coordinator term must be
// unique per promotion, so equal terms indicate a consensus bug rather than a
// resolvable tie.
func compareLeaderTimeline(a, b *multiorchdatapb.PoolerHealthState) int {
	return commonconsensus.CompareRulePosition(
		a.GetConsensusStatus().GetCurrentPosition().GetPosition(),
		b.GetConsensusStatus().GetCurrentPosition().GetPosition(),
	)
}

// analyzeAllPoolers runs fn against each pooler in sa, collecting all problems.
// Both the shard snapshot and the pooler's snapshot are passed so callbacks can
// access shard-level context alongside pooler-specific state.
// Errors are accumulated — the first error encountered is returned alongside any problems collected.
func analyzeAllPoolers(sa *snapshot.Shard, fn func(*snapshot.Shard, *multiorchdatapb.PoolerHealthState) (*types.Problem, error)) ([]types.Problem, error) {
	var problems []types.Problem
	var firstErr error
	for _, poolerAnalysis := range sa.Poolers {
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

// observationFresh reports whether the pooler's most recent successful health
// snapshot is recent enough (within maxAge, measured against now on the
// orchestrator clock) to be trusted as a live observation.
//
// A pooler that has never recorded a snapshot is NOT fresh: with no observation
// we have no evidence it is alive. Because LastSeen advances only on a
// successful snapshot (never on a disconnect), freshness alone captures
// "recently, successfully observed" — so callers need not separately consult a
// connection flag or a snapshot counter, and a brief stream interruption does
// not flip liveness until the observation genuinely ages out.
func observationFresh(h *multiorchdatapb.PoolerHealthState, now time.Time, maxAge time.Duration) bool {
	age, ok := snapshot.ObservationAge(h, now)
	if !ok {
		return false
	}
	return age <= maxAge
}
