// Copyright 2026 Supabase, Inc.
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

package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
	"github.com/multigres/multigres/go/tools/telemetry"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// highestKnownRule returns the highest-numbered rule this pooler knows of,
// combining the rule it has applied into its own position with the rule it was
// most recently told to follow via SetPrimary/Promote (consensusPromises's
// recorded ReplicationPrimary). A standby told of a newer leader before its own
// position advances still reads the newer rule. Returns nil if neither source
// carries a rule.
//
// "Known" is deliberately distinct from "committed": this ranks across both the
// committed position and the replication primary, so it can name a leader whose
// rule has not yet committed to local WAL.
//
// TODO: decisions that must be write-safe (e.g. admitting writes, advertising a
// rewind source) need the committed rule, not the highest *known* rule — add a
// highestCommittedRule companion that reads only the committed position's rule
// so those callers don't act on not-yet-committed leadership.
//
// Both sources arrive as a single ConsensusStatus from getCachedConsensusStatus
// (in-memory only — no postgres query, so this stays side-effect free for the
// monitor's decision path; the cache is refreshed by discoverPostgresState's
// ObservePosition earlier in the same iteration). Ranking is delegated to
// commonconsensus.HighestKnownRule — the same method the orchestrator uses to
// identify the shard leader — so the pooler and orch can never rank rules
// differently.
func (pm *MultipoolerManager) highestKnownPosition() *clustermetadatapb.RulePosition {
	return commonconsensus.HighestKnownRule([]*clustermetadatapb.ConsensusStatus{
		pm.consensusMgr.CachedConsensusStatus(),
	})
}

// staleStandbyDemoteTarget returns the leader this pooler should restart as a
// standby of, for the case where consensus has moved on but postgres is still
// running as a primary on a deposed term. Returns nil unless there is a recorded
// primary with usable contact info, the recorded rule strictly outranks our
// applied position, the recorded leader is not us, the rule is not revoked, and
// the recorded leader has advertised that it is rewind-ready — without a target
// that is safe to rewind against we wait rather than restart blind (restarting a
// diverged primary as a standby of a not-yet-checkpointed leader would FATAL on
// our own un-replicated WAL).
func (pm *MultipoolerManager) staleStandbyDemoteTarget() *clustermetadatapb.PoolerAddress {
	rp := pm.consensusMgr.GetReplicationPrimary()
	if rp == nil {
		return nil
	}
	target := rp.GetPrimary()
	if target == nil || target.GetHost() == "" || target.GetPostgresPort() == 0 {
		return nil
	}
	// Defer the demote until the recorded leader is rewind-ready (it has
	// checkpointed onto its current timeline, relayed via SetPrimary). Leaving the
	// node running as a deposed (queryable) primary meanwhile avoids both downtime
	// and the FATAL of rewinding against a stale-timeline source.
	if !rp.GetRewindReady() {
		return nil
	}
	// The recorded leader is us: not a "superseded by another leader" case, so
	// there is nothing to demote toward.
	if leader := commonconsensus.PossiblyUndecidedRule(rp.GetPosition()).GetLeaderId(); leader != nil && pm.serviceID != nil &&
		leader.GetCell() == pm.serviceID.GetCell() && leader.GetName() == pm.serviceID.GetName() {
		return nil
	}
	// Only act when the replication primary's recorded rule position outranks
	// this pooler's own last locally-recovered position — that's the sign
	// that rewinding against it might actually help. A lower or equal
	// recorded position is stale relative to us and must not trigger a demote.
	if commonconsensus.CompareRulePosition(rp.GetPosition(), pm.consensusMgr.Rules().CachedPosition().GetPosition()) <= 0 {
		return nil
	}
	// Don't race a mid-flight Recruit/Propose: skip a revoked rule.
	if commonconsensus.IsRuleRevoked(rp.GetPosition(), pm.consensusMgr.Promises().GetInconsistentRevocation()) {
		return nil
	}
	return target
}

// postgresState represents the state of PostgreSQL for monitoring
type postgresState struct {
	pgctldAvailable          bool
	dirInitialized           bool
	postgresRunning          bool
	backupsAvailable         bool
	pgMode                   pgmode.Mode
	bootstrapSentinelPresent bool
	// rewindSourceReady is true when this pooler is a primary whose last completed
	// checkpoint is on its current running timeline, so it is safe to pg_rewind
	// from. False on standbys and on a freshly promoted primary that has not yet
	// checkpointed onto its new timeline.
	rewindSourceReady bool
}

// postgresStateEqual reports whether two postgresState values are identical.
func postgresStateEqual(a, b postgresState) bool {
	return a.pgctldAvailable == b.pgctldAvailable &&
		a.dirInitialized == b.dirInitialized &&
		a.postgresRunning == b.postgresRunning &&
		a.backupsAvailable == b.backupsAvailable &&
		a.pgMode == b.pgMode &&
		a.bootstrapSentinelPresent == b.bootstrapSentinelPresent &&
		a.rewindSourceReady == b.rewindSourceReady
}

// remedialAction represents actions the postgres monitor can take
type remedialAction int

const (
	remedialActionNone remedialAction = iota
	remedialActionStartPostgres
	remedialActionRestoreFromBackup
	remedialActionCreateFirstBackup
	remedialActionReconcileGUC
	// remedialActionFixPrimaryConnInfo means postgres is in recovery and the
	// topology says REPLICA, but primary_conninfo doesn't match the primary
	// recorded in consensus.ConsensusPromises.ReplicationPrimary (the most recent
	// SetPrimary/Promote). Reconciles the GUC so this replica points at the right
	// primary regardless of how it got out of sync (failed SetPrimary apply,
	// hand edit, snapshot restore, etc.).
	remedialActionFixPrimaryConnInfo
	// remedialActionDemoteStalePrimary means consensus has named another leader
	// (the recorded rule outranks our applied position) but postgres is still
	// running as a primary on the deposed term. Restart as a standby of the
	// recorded leader, running pg_rewind to recover from any timeline divergence.
	remedialActionDemoteStalePrimary
	// remedialActionReconcileState means postgres agrees with the rule but the
	// StateManager's effective state has drifted from what we've observed: either
	// the pooler's role differs from the rule-derived role (or is still UNKNOWN at
	// boot), or the observed physical primary-ness differs from what components
	// last saw (postgres entered/left recovery without a role change). Reconcile
	// both in one Mutate: it transitions the serving components (query service,
	// replication tracker) and republishes the topology label + self-leadership.
	remedialActionReconcileState
	// remedialActionResignLeadership means the rule names us leader but postgres
	// is running as a standby. We do not self-promote; signal resignation so the
	// coordinator re-elects.
	remedialActionResignLeadership
	// remedialActionMarkRewindReady means this pooler is the non-resigned leader,
	// postgres has checkpointed onto its current timeline (rewindSourceReady), but
	// the published ReplicationPrimary has not yet advertised rewind_ready. Mark it
	// and broadcast so a diverged follower's recovery (orch's gated pg_rewind) can
	// proceed. Purely a state-publication step — no postgres mutation.
	remedialActionMarkRewindReady
)

// monitorPostgresIteration performs one iteration of PostgreSQL monitoring.
// Returns the discovered postgres state on success, or an error if the state
// could not be determined. The caller is responsible for transition detection
// and broadcasting health updates.
func (pm *MultipoolerManager) monitorPostgresIteration(ctx context.Context) (postgresState, error) {
	const (
		reasonPgctldUnavailable = "pgctld_unavailable"
		reasonPostgresRunning   = "postgres_running"
	)

	// Wait for manager to be ready
	if err := pm.checkReady(); err != nil {
		pm.logger.InfoContext(ctx, "MonitorPostgres: manager not ready yet")
		return postgresState{}, err
	}

	// Discover current state
	currentState, err := pm.discoverPostgresState(ctx)
	if err != nil {
		// Log and skip this tick; the next iteration will retry. A persistent
		// failure keeps the error loud rather than silently triggering the wrong
		// remediation. pgctld unavailability gets its dedicated reason code so
		// the monitor's log-dedup path behaves as before.
		if !currentState.pgctldAvailable {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: pgctld unavailable", "error", err)
			pm.pgMonitorLastLoggedReason = reasonPgctldUnavailable
		} else {
			// TODO: If we have errors detecting postgres state for long enough, maybe try restarting postgres
			// just in case? Could have been some kind of a fluke event like failing to create a socket file
			// that might be resolved by restarting.
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to discover state; skipping tick", "error", err)
		}
		return postgresState{}, err
	}

	// Determine what remediation is needed
	action := pm.determineRemedialAction(ctx, currentState)
	if action == remedialActionNone {
		// No action needed - just log status
		if currentState.postgresRunning {
			pm.setMonitorReason(ctx, reasonPostgresRunning, "MonitorPostgres: PostgreSQL is running")
		}
		return currentState, nil
	}

	// Acquire action lock before taking remedial action
	lockCtx, err := pm.actionLock.Acquire(ctx, "MonitorPostgres")
	if err != nil {
		pm.logger.InfoContext(ctx, "MonitorPostgres: failed to acquire action lock", "error", err)
		return postgresState{}, err
	}
	defer pm.actionLock.Release(lockCtx)

	// Re-verify state after acquiring lock (conditions may have changed)
	currentState, err = pm.discoverPostgresState(lockCtx)
	if err != nil {
		if !currentState.pgctldAvailable {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: pgctld unavailable after lock acquire", "error", err)
			pm.pgMonitorLastLoggedReason = reasonPgctldUnavailable
		} else {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to re-discover state after lock acquire; skipping tick", "error", err)
		}
		return postgresState{}, err
	}

	// Re-determine action based on current state
	action = pm.determineRemedialAction(lockCtx, currentState)

	// Take remedial action with lock held
	pm.takeRemedialAction(lockCtx, action, currentState)

	return currentState, nil
}

// discoverPostgresState discovers the current state of PostgreSQL. Returns an
// error only when a probe fails in a way that leaves the state genuinely
// ambiguous (e.g. a sentinel stat failing for reasons other than NotExist);
// callers must refuse to take remedial action in that case. pgctld being
// unavailable is not such a case — it is represented as pgctldAvailable=false.
func (pm *MultipoolerManager) discoverPostgresState(ctx context.Context) (postgresState, error) {
	state := postgresState{}

	// Check if pgctld client is available
	if pm.pgctldClient == nil {
		return state, nil // All fields remain false
	}
	state.pgctldAvailable = true

	// Get status from pgctld. On failure return both the state (with
	// pgctldAvailable=false) and the error so the caller can log the
	// underlying cause while still reading the unavailability flag.
	statusResp, err := pm.pgctldClient.Status(ctx, &pgctldpb.StatusRequest{})
	if err != nil {
		state.pgctldAvailable = false
		return state, fmt.Errorf("pgctld status: %w", err)
	}

	// Check if directory is initialized
	state.dirInitialized = (statusResp.Status != pgctldpb.ServerStatus_NOT_INITIALIZED)

	// Check if Postgres is running
	state.postgresRunning = (statusResp.Status == pgctldpb.ServerStatus_RUNNING)
	if state.postgresRunning {
		var err error
		state.pgMode, err = pm.postgresMode(ctx)
		if err != nil {
			// Postgres is running but we can't read its recovery mode, leaving the
			// role genuinely ambiguous. Acting on a guessed value is dangerous: a
			// guessed primary would make a healthy but momentarily-unqueryable replica
			// look like a stale primary and trigger a destructive demote (pg_rewind),
			// or flip the published writable bit on a guess. The probe returns
			// pgmode.Unknown on error, which never derives a writable role;
			// surface the error so the monitor skips remediation this tick and
			// retries, matching the bootstrap-sentinel handling below.
			return state, fmt.Errorf("determine recovery mode: %w", err)
		}
		// A primary is a rewind source only once it has checkpointed onto its
		// current timeline. Cheap to skip on standbys (rewindSourceReady would
		// return false anyway).
		if state.pgMode.OutOfRecovery() {
			ready, rrErr := pm.rewindSourceReady(ctx)
			if rrErr != nil {
				// Unlike isPrimary above, this is safe to leave best-effort: a failed
				// probe leaves rewindSourceReady=false, and that error defaults in the
				// fail-safe direction. The worst case is that other poolers don't yet
				// pick us as a pg_rewind source, which only delays their recovery — it
				// never triggers an action. So we warn and continue rather than failing
				// the whole tick (which would also block unrelated remediation).
				pm.logger.WarnContext(ctx, "Failed to determine rewind-source readiness", "error", rrErr)
			} else {
				state.rewindSourceReady = ready
			}
		}
		// Refresh the cached consensus position from postgres so this iteration's
		// role decisions (highestKnownRule / SelfConsensusRole read the cache)
		// converge on current state rather than lagging the periodic Status RPC that
		// otherwise refreshes it. If the position can't be read — postgres or the
		// multischema is unreadable (readCurrentRule errors when current_rule is
		// missing) — the role would rest on stale/absent consensus state, so surface
		// the error and let the monitor skip remediation this tick, matching the
		// isPrimary probe above.
		if _, err := pm.consensusMgr.Rules().ObservePosition(ctx); err != nil {
			return state, fmt.Errorf("refresh consensus position: %w", err)
		}
	}

	// Check if backups are available (only if directory not initialized)
	if !state.dirInitialized {
		state.backupsAvailable = pm.hasCompleteBackups(ctx)
	}

	sentinelPresent, err := pm.hasBootstrapSentinel()
	if err != nil {
		// We can't tell whether a stale sentinel needs handling. Surface as an
		// error so the monitor skips this tick; the operator should investigate.
		return state, fmt.Errorf("check bootstrap sentinel: %w", err)
	}
	state.bootstrapSentinelPresent = sentinelPresent

	return state, nil
}

// setMonitorReason sets the current monitor state reason and logs on state changes.
// This avoids log spam during repeated monitor iterations with the same state.
func (pm *MultipoolerManager) setMonitorReason(ctx context.Context, reason, message string) {
	if pm.pgMonitorLastLoggedReason != reason {
		pm.logger.InfoContext(ctx, message)
		pm.pgMonitorLastLoggedReason = reason
	}
}

// primaryConnInfoDiffersFromRecorded returns true when this pooler has been
// informed about a primary (via SetPrimary or Promote) and the live primary_conninfo
// in postgres doesn't match the recorded primary's contact info. Returns false
// when there's nothing to compare against, when we couldn't read the GUC, or
// when the recorded primary names this pooler itself.
//
// Returns false when the recorded primary's rule is revoked by our recorded
// revocation: a Recruit that's already advanced revoked_below_term has
// deliberately cleared primary_conninfo, and the cached "recorded primary" is
// stale until the next SetPrimary/Promote updates it. Without this gate, the
// monitor would race the Recruit/Promote flow by restoring conninfo to the
// just-revoked primary, which then causes Promote to refuse with
// "primary_conninfo is set".
//
// Used by the postgres monitor to decide whether to trigger
// remedialActionFixPrimaryConnInfo on each tick.
func (pm *MultipoolerManager) primaryConnInfoDiffersFromRecorded(ctx context.Context) bool {
	// Don't detect drift we can't fix: when StopReplication has set the
	// manual-stop flag, setPrimaryConnInfoLocked refuses every conninfo
	// rewrite until StartReplication clears it. Detecting drift anyway would
	// fire the reconciliation action on every tick and log a noisy
	// FAILED_PRECONDITION error each time.
	if pm.walReceiverManuallyStopped.Load() {
		return false
	}
	rp := pm.consensusMgr.GetReplicationPrimary()
	if rp == nil {
		return false
	}
	target := rp.GetPrimary()
	if target == nil {
		return false
	}
	// Skip if the recorded rule names this pooler as the leader — that's the
	// primary-side case, out of scope for replica-conninfo reconciliation.
	if leader := commonconsensus.PossiblyUndecidedRule(rp.GetPosition()).GetLeaderId(); leader != nil &&
		leader.GetCell() == pm.serviceID.GetCell() && leader.GetName() == pm.serviceID.GetName() {
		return false
	}
	// Skip if the recorded rule is revoked. The cached primary is from before
	// the current revocation took effect; restoring conninfo to it would race
	// the Recruit/Promote flow that's mid-flight (see function doc).
	if commonconsensus.IsRuleRevoked(rp.GetPosition(), pm.consensusMgr.Promises().GetInconsistentRevocation()) {
		return false
	}
	targetHost := target.GetHost()
	targetPort := target.GetPostgresPort()
	if targetHost == "" || targetPort == 0 {
		return false
	}

	// Use a short deadline so a slow query never blocks the monitor tick.
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	connInfoStr, err := pm.readPrimaryConnInfo(ctx)
	if err != nil {
		// Conservative: don't trigger reconciliation we can't verify.
		return false
	}
	if connInfoStr == "" {
		return true
	}
	parsed, err := parseAndRedactPrimaryConnInfo(connInfoStr)
	if err != nil || parsed == nil {
		// Unparsable conninfo is itself drift worth fixing.
		return true
	}
	return parsed.GetHost() != targetHost || parsed.GetPort() != targetPort
}

// reconcilePrimaryConnInfoToRecorded re-applies primary_conninfo so it points
// at the currently-recorded ReplicationPrimary. Callers detect drift via
// primaryConnInfoDiffersFromRecorded, which guarantees GetReplicationPrimary()
// has a valid target by the time this runs. logPrefix distinguishes the two
// call sites (the periodic monitor tick vs. an in-line SetPrimary reconcile)
// in logs.
//
// TODO: when suspectedDivergence=true (an unexpected demotion happened
// without a follow-up pg_rewind), just setting primary_conninfo is not
// enough — the WAL receiver will fail to start due to timeline divergence.
// Route through demoteStalePrimaryLocked instead, which runs pg_rewind
// dry-run (cheap when there's no divergence) before re-establishing
// replication. This would let both callers self-heal a stuck-replica
// scenario without waiting for orch's FixReplicationAction to issue a
// RewindToSource RPC.
func (pm *MultipoolerManager) reconcilePrimaryConnInfoToRecorded(ctx context.Context, logPrefix string) {
	target := pm.consensusMgr.GetReplicationPrimary().GetPrimary()
	pm.logger.InfoContext(ctx, logPrefix+": primary_conninfo drift detected; rewriting to recorded primary",
		"target_primary", target.GetId().GetName(),
		"target_host", target.GetHost(),
		"target_port", target.GetPostgresPort())
	if err := pm.setPrimaryConnInfoLocked(ctx, target.GetHost(), target.GetPostgresPort(),
		true /* stopReplicationBefore */, true /* startReplicationAfter */); err != nil {
		pm.logger.ErrorContext(ctx, logPrefix+": failed to reconcile primary_conninfo", "error", err)
	}
}

// determineRoleAction returns the action needed to align this pooler's role with
// the rule-derived intended role: demote a stale primary, resign when the rule
// names us leader but postgres is a standby, etc.
func (pm *MultipoolerManager) determineRoleAction(role commonconsensus.ConsensusRole, state postgresState) remedialAction {
	// Consensus role: not leader (follower/observer)
	// Postgres: PRIMARY
	// Diagnosis: Stale primary. We should restart as a replica, but we anticipate
	// having extra / phantom transactions so don't restart until we've been informed
	// the current leader to allow rewinding.
	if role != commonconsensus.ConsensusRoleLeader && state.pgMode.OutOfRecovery() {
		if pm.staleStandbyDemoteTarget() != nil {
			return remedialActionDemoteStalePrimary
		}
		return remedialActionNone
	}

	// Rule: LEADER
	// Postgres: STANDBY / RECOVERY MODE
	// Diagnosis: Non-functioning primary. We could either re-promote or resign leadership.
	// For now we resign by broadcasting to multiorch that we're not able to fulfill the
	// leadership role at this term, at which point it may choose to re-promote.
	//
	// TODO(leader-led-leader-changes): promote here instead of resigning (reuse
	// Promote's promoteStandbyToPrimary), and disambiguate resign (we lost our
	// postgres) from promote (newly elected). Embedding the leader host/port in the
	// WAL rule would also let replicas reconcile without waiting for SetPrimary.
	if role == commonconsensus.ConsensusRoleLeader && !state.pgMode.OutOfRecovery() {
		if pm.consensusMgr.ResignedLeaderAtTerm() == 0 {
			return remedialActionResignLeadership
		}
		return remedialActionNone
	}

	// Re-fan the effective state to components if the last-applied one is stale
	// vs. what we now observe (recovery + the live consensus snapshot -> routing
	// role, plus serving status). This is what propagates a routing-role flip (the
	// committed rule landing after pg_promote, or a revocation) to the query
	// server's write gate, and completes a transient DRAINING. hasDrift reads the
	// consensus snapshot itself, so it and fixDrift derive from the same inputs.
	if pm.stateManager.hasDrift(state.pgMode) {
		return remedialActionReconcileState
	}

	return remedialActionNone
}

// determineReplicationSettingsAction reconciles postgres replication settings to
// the recorded consensus state.
func (pm *MultipoolerManager) determineReplicationSettingsAction(ctx context.Context, state postgresState) remedialAction {
	// primary_conninfo is a standby concern: reconcile it to the recorded
	// ReplicationPrimary when it has drifted from what we've been told via
	// SetPrimary/Promote.
	if !state.pgMode.OutOfRecovery() {
		if pm.primaryConnInfoDiffersFromRecorded(ctx) {
			return remedialActionFixPrimaryConnInfo
		}
		// TODO: self-rewind detection. A replica may need pg_rewind even
		// when it was never primary — phantom transactions can leak in
		// (e.g. via a brief sync-replication ack that got rolled back on
		// the primary). The old consensus flow let orch re-issue an
		// explicit rewind whenever it suspected one; the new flow's
		// SetPrimary is idempotent and won't repeat work, so the monitor
		// needs to self-detect stuck replication. Check
		// pg_stat_wal_receiver.status: if not "streaming" for
		// >stuckThreshold AND we have a recorded primary, treat as
		// suspected divergence — set suspectedDivergence=true so the next
		// remedialActionFixPrimaryConnInfo iteration runs
		// pg_rewind dry-run before re-establishing replication. Cheap
		// when no divergence, conclusive when there is.
	}

	// synchronous_standby_names is a primary concern: it has no effect on a
	// standby, and setting it there leaks state.
	if state.pgMode.OutOfRecovery() && pm.consensusMgr.Rules().HasInconsistentGUC(ctx) {
		return remedialActionReconcileGUC
	}

	return remedialActionNone
}

// determineRemedialAction decides what action to take based on discovered state.
// This is pure decision logic with no side effects.
func (pm *MultipoolerManager) determineRemedialAction(ctx context.Context, currentState postgresState) remedialAction {
	// Pgctld unavailable: No action possible
	if !currentState.pgctldAvailable {
		return remedialActionNone
	}

	// Postgres is running: reconcile against the consensus rule. First align the
	// role (determineRoleAction), then when the role needs no action
	// reconcile the replication settings (determineReplicationSettingsAction).
	if currentState.postgresRunning {
		if pm.highestKnownPosition() == nil {
			// No rule-bearing position observed yet; wait rather than act on the
			// observed postgres state. This also defers DRAINING -> SERVING recovery:
			// a node only re-enables serving once it knows its rule-derived role,
			// matching the "healthy and role-aligned" contract on PoolerServingStatus.
			return remedialActionNone
		}
		role := commonconsensus.SelfConsensusRole(pm.consensusMgr.CachedConsensusStatus())
		if action := pm.determineRoleAction(role, currentState); action != remedialActionNone {
			return action
		}
		if action := pm.determineReplicationSettingsAction(ctx, currentState); action != remedialActionNone {
			return action
		}
		if pm.shouldMarkRewindReady(currentState, role) {
			return remedialActionMarkRewindReady
		}
		return remedialActionNone
	}

	// Postgres is not running: start it, restore from backup, or bootstrap.
	return determinePostgresNotRunningAction(currentState)
}

// shouldMarkRewindReady reports whether this pooler should advertise rewind
// readiness this tick: postgres has checkpointed onto its current timeline
// (rewindSourceReady), this pooler is the rule-named leader and has not resigned
// leadership, and the published ReplicationPrimary has not yet advertised it. The
// last check makes the resulting action a false->true edge, so its broadcast
// fires once per promotion rather than every tick.
func (pm *MultipoolerManager) shouldMarkRewindReady(state postgresState, role commonconsensus.ConsensusRole) bool {
	if !state.rewindSourceReady || role != commonconsensus.ConsensusRoleLeader {
		return false
	}
	if pm.consensusMgr.ResignedLeaderAtTerm() != 0 {
		return false
	}
	return !pm.consensusMgr.GetReplicationPrimary().GetRewindReady()
}

// determinePostgresNotRunningAction decides how to bring postgres up when it is
// not running.
func determinePostgresNotRunningAction(state postgresState) remedialAction {
	// A sentinel from a prior first-backup attempt means bootstrap crashed
	// mid-flight. Any on-disk pg_data is stale — force the create-first-backup
	// path so createFirstBackupAndInitializeLocked can clean up and retry.
	if state.bootstrapSentinelPresent {
		return remedialActionCreateFirstBackup
	}
	// Postgres not running: Try to start or restore
	if state.dirInitialized {
		return remedialActionStartPostgres
	}
	if state.backupsAvailable {
		return remedialActionRestoreFromBackup
	}
	// Directory not initialized and no backups: try to create the first backup.
	return remedialActionCreateFirstBackup
}

// takeRemedialAction executes the specified remedial action.
// Caller must hold the action lock.
func (pm *MultipoolerManager) takeRemedialAction(ctx context.Context, action remedialAction, state postgresState) {
	// Assert that the action lock is held
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		pm.logger.ErrorContext(ctx, "takeRemedialAction called without action lock", "error", err)
		return
	}

	const (
		reasonPostgresRunning            = "postgres_running"
		reasonStartingPostgres           = "starting_postgres"
		reasonRestoringFromBackup        = "restoring_from_backup"
		reasonCreatingFirstBackup        = "creating_first_backup"
		reasonWaitingForFirstBackupLease = "waiting_for_first_backup_lease"
	)

	switch action {
	case remedialActionNone:
		// No action to take
		return

	case remedialActionDemoteStalePrimary:
		// Re-check under the action lock: the decision was made on a lock-free
		// snapshot and may have raced a revocation or another demote path. This also
		// re-applies the rewind-ready gate (staleStandbyDemoteTarget returns nil
		// until the recorded leader has checkpointed onto its current timeline).
		target := pm.staleStandbyDemoteTarget()
		if target == nil {
			return
		}
		pm.setMonitorReason(ctx, reasonPostgresRunning, "MonitorPostgres: PostgreSQL is running")
		pm.logger.InfoContext(ctx, "MonitorPostgres: stale primary; consensus names another leader, restarting as standby",
			"target_primary", target.GetId().GetName(),
			"target_host", target.GetHost(),
			"target_port", target.GetPostgresPort())
		// postgres is a primary on a deposed term, so its timeline has likely
		// diverged from the new leader; restartAsStandbyLocked runs pg_rewind
		// (cheap when there's no divergence). The rewind-ready gate is enforced in
		// staleStandbyDemoteTarget above, so by here it is safe to rewind.
		if _, err := pm.consensusMgr.SetSuspectedDivergence(ctx, true); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to set suspected divergence", "error", err)
		}
		if _, err := pm.restartAsStandbyLocked(ctx, target.GetHost(), target.GetPostgresPort()); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to restart stale primary as standby", "error", err)
			return
		}
		// Sync physical primary-ness immediately so the gateway and stale-leader
		// analyzer stop treating us as a primary, rather than waiting a monitor cycle
		// for reconcile. We just restarted as a standby, so postgres is no longer
		// primary; that derives routing role REPLICA (clearing the PRIMARY label /
		// self-leadership and the writable signal). Serving status is unchanged (a
		// healthy standby keeps serving reads).
		if err := pm.stateManager.Mutate(ctx, func(s *servingStateMutation) {
			s.PostgresMode = pgmode.InRecovery
		}); err != nil {
			pm.logger.WarnContext(ctx, "MonitorPostgres: failed to apply role after demote", "error", err)
		}

	case remedialActionReconcileState:
		pm.setMonitorReason(ctx, reasonPostgresRunning, "MonitorPostgres: PostgreSQL is running")
		// The StateManager's effective state has drifted (routing role and/or a
		// transient drain). fixDrift re-derives the routing role from the freshly
		// observed recovery flag and the live consensus snapshot and re-fans it out;
		// the record's PoolerType / self-leadership follow. This is the propagation
		// path for a committed-rule landing after pg_promote or a revocation reaching
		// the query server's write gate. Serving is re-enabled only out of DRAINING;
		// a DISABLED pooler is left not-serving.
		pm.logger.InfoContext(ctx, "MonitorPostgres: reconciling drifted state", "postgres_mode", state.pgMode)
		if err := pm.stateManager.fixDrift(ctx, state.pgMode); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to reconcile drifted state", "error", err)
		}

	case remedialActionResignLeadership:
		pm.setMonitorReason(ctx, reasonPostgresRunning, "MonitorPostgres: PostgreSQL is running")
		// The rule names us leader but postgres is running as a standby. Signal
		// voluntary resignation at the highest-known rule's term so the coordinator
		// re-elects; this branch only fires when that rule names us, so the term is
		// current. We do not self-promote.
		highestPosition := pm.highestKnownPosition()
		pm.logger.InfoContext(ctx, "MonitorPostgres: rule names us leader but postgres is a standby; resigning",
			"position", commonconsensus.FormatRulePosition(highestPosition))
		if commonconsensus.PossiblyUndecidedRule(highestPosition).GetRuleNumber().GetCoordinatorTerm() != 0 {
			if err := pm.consensusMgr.SetResignedLeaderAtTerm(ctx, highestPosition); err != nil {
				pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to set resigned primary term", "error", err)
			}
		}
		// This branch preempts the reconcileState drift check, so sync the
		// writable state here: postgres is a standby, so stop the heartbeat
		// writer / LISTEN even though we remain the rule's leader. Without this
		// the writer keeps issuing INSERTs against a read-only standby every
		// interval until re-election.
		if err := pm.stateManager.Mutate(ctx, func(s *servingStateMutation) {
			s.PostgresMode = pgmode.InRecovery
		}); err != nil {
			pm.logger.WarnContext(ctx, "MonitorPostgres: failed to sync postgres primary status on resign", "error", err)
		}

	case remedialActionFixPrimaryConnInfo:
		pm.reconcilePrimaryConnInfoToRecorded(ctx, "MonitorPostgres")

	case remedialActionStartPostgres:
		// Honour the in-memory flag set by tests and demos to suppress auto-restart
		// during controlled failovers.
		if pm.postgresRestartsDisabled.Load() {
			pm.logger.InfoContext(ctx, "MonitorPostgres: skipping start, postgres restarts disabled")
			return
		}
		pm.setMonitorReason(ctx, reasonStartingPostgres, "MonitorPostgres: PostgreSQL initialized but not running, starting PostgreSQL")
		if err := pm.actionLock.SetAction(ctx, multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_STARTING); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to set action", "error", err)
		}
		if err := pm.startPostgres(ctx); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to start PostgreSQL, will retry", "error", err)
		}

	case remedialActionRestoreFromBackup:
		// Respect the in-memory flag set by tests and demos to suppress auto-restart
		if pm.postgresRestartsDisabled.Load() {
			pm.logger.InfoContext(ctx, "MonitorPostgres: skipping auto-restore, postgres restarts disabled")
			return
		}
		pm.setMonitorReason(ctx, reasonRestoringFromBackup, "MonitorPostgres: directory not initialized but backups available, restoring from backup")
		if err := pm.actionLock.SetAction(ctx, multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_RESTORING_FROM_BACKUP); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to set action", "error", err)
		}
		if err := pm.restoreAndStartPostgres(ctx); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to restore from backup, will retry", "error", err)
		}

	case remedialActionCreateFirstBackup:
		pm.setMonitorReason(ctx, reasonCreatingFirstBackup, "MonitorPostgres: no backup found, attempting to create one")
		if err := pm.actionLock.SetAction(ctx, multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_CREATING_FIRST_BACKUP); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to set action", "error", err)
		}
		busy, backupFound, err := pm.createFirstBackupAndInitializeLocked(ctx)
		if busy {
			pm.setMonitorReason(ctx, reasonWaitingForFirstBackupLease, "MonitorPostgres: backup lease held by another pooler, waiting")
		} else if err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to create first backup, will retry", "error", err)
		} else if backupFound {
			// Another pooler created the backup just before we acquired the lease.
			// Restore immediately rather than waiting for the next monitor iteration.
			pm.setMonitorReason(ctx, reasonRestoringFromBackup, "MonitorPostgres: first backup found; restoring")
			if err := pm.actionLock.SetAction(ctx, multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_RESTORING_FROM_BACKUP); err != nil {
				pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to set action", "error", err)
			}
			if err := pm.restoreAndStartPostgres(ctx); err != nil {
				pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to restore from backup, will retry", "error", err)
			}
		}

	case remedialActionReconcileGUC:
		pm.setMonitorReason(ctx, reasonPostgresRunning, "MonitorPostgres: PostgreSQL is running")
		pm.logger.InfoContext(ctx, "MonitorPostgres: re-applying stale GUC")
		if err := pm.consensusMgr.Rules().ReconcileGUC(ctx, !state.pgMode.OutOfRecovery()); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: GUC reconciliation failed", "error", err)
		}

	case remedialActionMarkRewindReady:
		pm.setMonitorReason(ctx, reasonPostgresRunning, "MonitorPostgres: PostgreSQL is running")
		// Advertise rewind-source readiness. MarkSelfRewindReady defends that the
		// record names us at the term we observed and that the flag was not already
		// set; broadcast on the false->true edge so a diverged follower's recovery
		// sees it without waiting for the next periodic snapshot.
		expectedPosition := pm.consensusMgr.GetReplicationPrimary().GetPosition()
		if pm.consensusMgr.MarkSelfRewindReady(pm.serviceID, expectedPosition) {
			pm.logger.InfoContext(ctx, "MonitorPostgres: checkpointed onto current timeline; advertising rewind-ready")
			pm.broadcastHealth()
		}
	}
}

// hasCompleteBackups checks if there are any complete backups available
func (pm *MultipoolerManager) hasCompleteBackups(ctx context.Context) bool {
	// Get list of backups
	backups, err := pm.backup.ListBackups(ctx)
	if err != nil {
		return false
	}

	// Filter to only complete backups
	for _, b := range backups {
		if b.Status == multipoolermanagerdatapb.BackupMetadata_COMPLETE {
			return true
		}
	}

	return false
}

// startPostgres starts PostgreSQL via pgctld
//
// TODO: preemptive-rewind safety. A monitor-driven restart can't know what
// happened between the previous run and now — postgres may have crashed
// mid-write as primary, or may have been killed externally. The safe default
// is to come back as a standby with suspectedDivergence=true so that the next
// SetPrimary/Promote/standby-conninfo path routes through demoteStalePrimaryLocked
// (which runs pg_rewind dry-run; cheap when there's no divergence) before
// trusting local WAL. This bears on the broader self-rewind plan:
//   - replicas with phantom transactions: orch sends an explicit
//     RewindToSource RPC today; a future change should let the local monitor
//     detect stuck replication and self-heal without needing orch in the loop.
//   - primaries demoted unexpectedly (crash, SIGKILL, external pg_demote):
//     the restart-as-standby helper should require callers to declare
//     "clean" vs "unexpected" so an unexpected transition can set
//     suspectedDivergence up front, increasing the odds of fast convergence once
//     a new leader is announced.
func (pm *MultipoolerManager) startPostgres(ctx context.Context) error {
	pm.logger.InfoContext(ctx, "MonitorPostgres: Attempting to restart PostgreSQL")
	if pm.pgctldClient == nil {
		return errors.New("pgctld client not available")
	}

	_, err := pm.pgctldClient.Start(ctx, &pgctldpb.StartRequest{})
	if err != nil {
		return fmt.Errorf("MonitorPostgres: failed to start PostgreSQL: %w", err)
	}

	pm.logger.InfoContext(ctx, "MonitorPostgres: PostgreSQL started successfully")

	// Reopen connections after postgres restart to replace stale socket FDs.
	// Only when connection pool is initialized (the manager may not have
	// connection infrastructure in unit tests with minimal setup).
	if pm.connPoolMgr != nil {
		pm.reopenConnections(ctx)

		// Wait for database connection to be ready
		if err := pm.waitForDatabaseConnection(ctx); err != nil {
			return fmt.Errorf("MonitorPostgres: database not ready after restart: %w", err)
		}
	}

	return nil
}

// restoreAndStartPostgres restores from backup and starts PostgreSQL.
// This is used by MonitorPostgres for auto-restore functionality.
// Caller must hold the action lock.
func (pm *MultipoolerManager) restoreAndStartPostgres(ctx context.Context) error {
	// Re-check status to ensure conditions haven't changed
	// (e.g., another process may have initialized or started postgres while we waited for lock)
	if pm.pgctldClient != nil {
		statusResp, err := pm.pgctldClient.Status(ctx, &pgctldpb.StatusRequest{})
		if err == nil {
			// If directory is now initialized, skip restore
			if statusResp.Status != pgctldpb.ServerStatus_NOT_INITIALIZED {
				pm.logger.InfoContext(ctx, "MonitorPostgres: directory became initialized after acquiring lock, skipping restore")
				return nil
			}
		}
		// If status check fails, continue with restore attempt
	}

	// Get the latest complete backup
	backups, err := pm.backup.ListBackups(ctx)
	if err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
	}

	// Filter to only complete backups
	var completeBackups []*multipoolermanagerdatapb.BackupMetadata
	for _, b := range backups {
		if b.Status == multipoolermanagerdatapb.BackupMetadata_COMPLETE {
			completeBackups = append(completeBackups, b)
		}
	}

	if len(completeBackups) == 0 {
		return errors.New("no complete backups available")
	}

	// Use the latest complete backup (last in the list)
	latestBackup := completeBackups[len(completeBackups)-1]

	pm.logger.InfoContext(ctx, "MonitorPostgres: restoring from backup",
		"backup_id", latestBackup.BackupId)

	// Perform the restore
	if err := telemetry.WithSpan(ctx, "monitor-postgres-restore", func(ctx context.Context) error {
		return pm.restoreFromBackupLocked(ctx, latestBackup.BackupId)
	}); err != nil {
		return fmt.Errorf("failed to restore from backup: %w", err)
	}

	pm.logger.InfoContext(ctx, "MonitorPostgres: successfully restored from backup",
		"backup_id", latestBackup.BackupId,
		"shard", pm.getShardID(),
		"position", commonconsensus.FormatRulePosition(pm.consensusMgr.Rules().CachedPosition().GetPosition()))

	return nil
}
