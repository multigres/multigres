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

	"google.golang.org/protobuf/proto"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/tools/telemetry"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// deriveTypeAndObs maps a consensus rule to the pooler role it implies and the
// self-leadership observation to publish. A nil rule (no rule known yet) yields
// UNKNOWN; a rule naming this pooler yields PRIMARY plus an observation; any
// other rule yields REPLICA with no observation. This is the single place the
// rule is translated into a role.
func deriveTypeAndObs(rule *clustermetadatapb.ShardRule, selfID *clustermetadatapb.ID) (clustermetadatapb.PoolerType, *clustermetadatapb.LeaderObservation) {
	if rule == nil {
		return clustermetadatapb.PoolerType_UNKNOWN, nil
	}
	if proto.Equal(rule.GetLeaderId(), selfID) {
		return clustermetadatapb.PoolerType_PRIMARY, &clustermetadatapb.LeaderObservation{
			LeaderId:         selfID,
			LeaderRuleNumber: rule.GetRuleNumber(),
		}
	}
	return clustermetadatapb.PoolerType_REPLICA, nil
}

// latestRule returns the highest-numbered rule this pooler knows of, combining
// the rule it has applied into its own position with the rule it was most
// recently told to follow via SetPrimary/Promote (consensusState's recorded
// ReplicationPrimary). A standby told of a newer leader before its own position
// advances still reads the newer rule. Returns nil if neither source carries a
// rule.
//
// Both sources arrive as a single ConsensusStatus from getCachedConsensusStatus
// (in-memory only — no postgres query, so this stays side-effect free for the
// monitor's decision path; the cache is refreshed by discoverPostgresState's
// ObservePosition earlier in the same iteration). Ranking is delegated to
// commonconsensus.HighestKnownRule — the same method the orchestrator uses to
// identify the shard leader — so the pooler and orch can never rank rules
// differently.
func (pm *MultiPoolerManager) latestRule() *clustermetadatapb.ShardRule {
	return commonconsensus.HighestKnownRule([]*clustermetadatapb.ConsensusStatus{
		pm.getCachedConsensusStatus(),
	})
}

// intendedRole is the pooler role implied by the highest rule it knows of — the
// consensus-authoritative source of truth for the published PoolerType,
// independent of the observed postgres recovery mode.
func (pm *MultiPoolerManager) intendedRole() clustermetadatapb.PoolerType {
	t, _ := deriveTypeAndObs(pm.latestRule(), pm.serviceID)
	return t
}

// staleStandbyDemoteTarget returns the leader this pooler should restart as a
// standby of, for the case where consensus has moved on but postgres is still
// running as a primary on a deposed term. Returns nil unless there is a recorded
// primary with usable contact info, the recorded rule strictly outranks our
// applied position, the recorded leader is not us, and the rule is not revoked —
// without a target to rewind against we wait rather than restart blind.
func (pm *MultiPoolerManager) staleStandbyDemoteTarget() *clustermetadatapb.PoolerAddress {
	rp := pm.consensusState.GetReplicationPrimary()
	if rp == nil {
		return nil
	}
	target := rp.GetPrimary()
	if target == nil || target.GetHost() == "" || target.GetPostgresPort() == 0 {
		return nil
	}
	// The recorded leader is us: not a "superseded by another leader" case, so
	// there is nothing to demote toward.
	if leader := rp.GetRule().GetLeaderId(); leader != nil && pm.serviceID != nil &&
		leader.GetCell() == pm.serviceID.GetCell() && leader.GetName() == pm.serviceID.GetName() {
		return nil
	}
	// Only act when the recorded rule outranks our applied position. A lower or
	// equal recorded rule is stale relative to us and must not trigger a demote.
	selfRuleNum := pm.rules.CachedPosition().GetRule().GetRuleNumber()
	if commonconsensus.CompareRuleNumbers(rp.GetRule().GetRuleNumber(), selfRuleNum) <= 0 {
		return nil
	}
	// Don't race a mid-flight Recruit/Propose: skip a revoked rule.
	if commonconsensus.IsRuleRevoked(rp.GetRule(), pm.consensusState.GetInconsistentRevocation()) {
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
	isPrimary                bool
	bootstrapSentinelPresent bool
	// primaryTerm is the coordinator term at which this pooler is the primary
	// per the highest known rule. 0 if we are not the primary for that rule.
	primaryTerm int64
}

// postgresStateEqual reports whether two postgresState values are identical.
func postgresStateEqual(a, b postgresState) bool {
	return a.pgctldAvailable == b.pgctldAvailable &&
		a.dirInitialized == b.dirInitialized &&
		a.postgresRunning == b.postgresRunning &&
		a.backupsAvailable == b.backupsAvailable &&
		a.isPrimary == b.isPrimary &&
		a.bootstrapSentinelPresent == b.bootstrapSentinelPresent &&
		a.primaryTerm == b.primaryTerm
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
	// recorded in consensus.ConsensusState.ReplicationPrimary (the most recent
	// SetPrimary/Promote). Reconciles the GUC so this replica points at the right
	// primary regardless of how it got out of sync (failed SetPrimary apply,
	// hand edit, snapshot restore, etc.).
	remedialActionFixPrimaryConnInfo
	// remedialActionDemoteStalePrimary means consensus has named another leader
	// (the recorded rule outranks our applied position) but postgres is still
	// running as a primary on the deposed term. Restart as a standby of the
	// recorded leader, running pg_rewind to recover from any timeline divergence.
	remedialActionDemoteStalePrimary
	// remedialActionReconcileRole means postgres agrees with the rule but the
	// pooler's role has drifted from the rule-derived role (or is still UNKNOWN at
	// boot). Apply the rule-derived role: SetState transitions the serving
	// components (query service, replication tracker) and republishes the
	// topology label + self-leadership observation.
	remedialActionReconcileRole
	// remedialActionResignLeadership means the rule names us leader but postgres
	// is running as a standby. We do not self-promote; signal resignation so the
	// coordinator re-elects.
	remedialActionResignLeadership
)

// monitorPostgresIteration performs one iteration of PostgreSQL monitoring.
// Returns the discovered postgres state on success, or an error if the state
// could not be determined. The caller is responsible for transition detection
// and broadcasting health updates.
func (pm *MultiPoolerManager) monitorPostgresIteration(ctx context.Context) (postgresState, error) {
	const (
		reasonPgctldUnavailable = "pgctld_unavailable"
		reasonPostgresRunning   = "postgres_running"
	)

	// Wait for manager to be ready
	if err := pm.checkReady(); err != nil {
		pm.logger.InfoContext(pm.ctx, "MonitorPostgres: manager not ready yet")
		return postgresState{}, err
	}

	// Skip all action while awaiting a rewind after emergency demotion.
	if pm.rewindPending.Load() {
		pm.logger.InfoContext(ctx, "MonitorPostgres: skipping, awaiting rewind after demotion")
		return postgresState{}, nil
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

	// Re-check rewindPending after acquiring the lock: Recruit sets this flag
	// while holding the lock, so we may have been waiting while it demoted the node.
	if pm.rewindPending.Load() {
		pm.logger.InfoContext(ctx, "MonitorPostgres: skipping after lock acquire, rewind now pending")
		return postgresState{}, nil
	}

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
func (pm *MultiPoolerManager) discoverPostgresState(ctx context.Context) (postgresState, error) {
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
		state.isPrimary, err = pm.isPrimary(ctx)
		if err != nil {
			pm.logger.ErrorContext(ctx, "Failed to determine primary status", "error", err)
		}
		// Lock-free first pass from the monitor: prefer a fresh read, fall back to
		// the cached rule when postgres is unreachable so we keep the last-known
		// primary term across postgres restarts and crashes.
		cs, err := pm.getInconsistentConsensusStatus(ctx)
		if err != nil {
			cs = pm.getCachedConsensusStatus()
		}
		state.primaryTerm = commonconsensus.LeaderTerm(cs)
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

// primaryTermLocked returns the coordinator term of the pooler's current
// committed rule if this pooler is the primary per that rule. Uses a
// consistent consensus status read and therefore requires the action lock.
// Returns (0, nil) when the rule does not name this pooler as primary.
// Returns (0, err) only when the consensus status cannot be read at all —
// callers that need to distinguish "confirmed not primary" from "unknown"
// must inspect the error.
//
// Callers that cannot hold the action lock should read
// getInconsistentConsensusStatus themselves and pass the result to
// consensus.LeaderTerm.
func (pm *MultiPoolerManager) primaryTermLocked(ctx context.Context) (int64, error) {
	cs, err := pm.getConsensusStatus(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to read consensus status: %w", err)
	}
	return commonconsensus.LeaderTerm(cs), nil
}

// setMonitorReason sets the current monitor state reason and logs on state changes.
// This avoids log spam during repeated monitor iterations with the same state.
func (pm *MultiPoolerManager) setMonitorReason(ctx context.Context, reason, message string) {
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
func (pm *MultiPoolerManager) primaryConnInfoDiffersFromRecorded(_ postgresState) bool {
	// Don't detect drift we can't fix: when StopReplication has set the
	// manual-stop flag, setPrimaryConnInfoLocked refuses every conninfo
	// rewrite until StartReplication clears it. Detecting drift anyway would
	// fire the reconciliation action on every tick and log a noisy
	// FAILED_PRECONDITION error each time.
	if pm.walReceiverManuallyStopped.Load() {
		return false
	}
	rp := pm.consensusState.GetReplicationPrimary()
	if rp == nil {
		return false
	}
	target := rp.GetPrimary()
	if target == nil {
		return false
	}
	// Skip if the recorded rule names this pooler as the leader — that's the
	// primary-side case, out of scope for replica-conninfo reconciliation.
	if leader := rp.GetRule().GetLeaderId(); leader != nil &&
		leader.GetCell() == pm.serviceID.GetCell() && leader.GetName() == pm.serviceID.GetName() {
		return false
	}
	// Skip if the recorded rule is revoked. The cached primary is from before
	// the current revocation took effect; restoring conninfo to it would race
	// the Recruit/Promote flow that's mid-flight (see function doc).
	if commonconsensus.IsRuleRevoked(rp.GetRule(), pm.consensusState.GetInconsistentRevocation()) {
		return false
	}
	targetHost := target.GetHost()
	targetPort := target.GetPostgresPort()
	if targetHost == "" || targetPort == 0 {
		return false
	}

	// Use a short deadline so a slow query never blocks the monitor tick.
	ctx, cancel := context.WithTimeout(pm.ctx, 500*time.Millisecond)
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

// determineRoleAction returns the action needed to align this pooler's role with
// the rule-derived intended role: demote a stale primary, resign when the rule
// names us leader but postgres is a standby, etc.
func (pm *MultiPoolerManager) determineRoleAction(intended clustermetadatapb.PoolerType, state postgresState) remedialAction {
	// Rule: FOLLOWER
	// Postgres: PRIMARY
	// Diagnosis: Stale primary. We should restart as a replica, but we anticipate
	// having extra / phantom transactions so don't restart until we've been informed
	// the current leader to allow rewinding.
	if intended == clustermetadatapb.PoolerType_REPLICA && state.isPrimary {
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
	if intended == clustermetadatapb.PoolerType_PRIMARY && !state.isPrimary {
		pm.mu.Lock()
		resigned := pm.resignedLeaderAtTerm
		pm.mu.Unlock()
		if resigned == 0 {
			return remedialActionResignLeadership
		}
		return remedialActionNone
	}

	// Above we were comparing Postgres state vs consensus role.
	// Here we're comparing our query serving state & health broadcast state
	// vs consensus role. This could help recover from something like an RPC
	// that timed out and failed to do something like starting or stopping
	// heartbeat writes, etc.
	if pm.getPoolerType() != intended {
		return remedialActionReconcileRole
	}

	return remedialActionNone
}

// determineReplicationSettingsAction reconciles postgres replication settings to
// the recorded consensus state.
func (pm *MultiPoolerManager) determineReplicationSettingsAction(ctx context.Context, state postgresState) remedialAction {
	// primary_conninfo is a standby concern: reconcile it to the recorded
	// ReplicationPrimary when it has drifted from what we've been told via
	// SetPrimary/Promote.
	if !state.isPrimary {
		if pm.primaryConnInfoDiffersFromRecorded(state) {
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
		// suspected divergence — set rewindPending=true so the next
		// remedialActionFixPrimaryConnInfo iteration runs
		// pg_rewind dry-run before re-establishing replication. Cheap
		// when no divergence, conclusive when there is.
	}

	// synchronous_standby_names is a primary concern: it has no effect on a
	// standby, and setting it there leaks state.
	if state.isPrimary && pm.rules.HasInconsistentGUC(ctx) {
		return remedialActionReconcileGUC
	}

	return remedialActionNone
}

// determineRemedialAction decides what action to take based on discovered state.
// This is pure decision logic with no side effects.
func (pm *MultiPoolerManager) determineRemedialAction(ctx context.Context, currentState postgresState) remedialAction {
	// Pgctld unavailable: No action possible
	if !currentState.pgctldAvailable {
		return remedialActionNone
	}

	// Postgres is running: reconcile against the consensus rule. First align the
	// role (determineRoleAction), then when the role needs no action
	// reconcile the replication settings (determineReplicationSettingsAction).
	if currentState.postgresRunning {
		intended := pm.intendedRole()
		if intended == clustermetadatapb.PoolerType_UNKNOWN {
			// No rule-bearing position observed yet; wait rather than act on the
			// observed postgres state.
			return remedialActionNone
		}
		if action := pm.determineRoleAction(intended, currentState); action != remedialActionNone {
			return action
		}
		return pm.determineReplicationSettingsAction(ctx, currentState)
	}

	// Postgres is not running: start it, restore from backup, or bootstrap.
	return determinePostgresNotRunningAction(currentState)
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
func (pm *MultiPoolerManager) takeRemedialAction(ctx context.Context, action remedialAction, state postgresState) {
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
		// snapshot and may have raced a revocation or another demote path.
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
		// (cheap when there's no divergence) when rewindPending is set.
		pm.rewindPending.Store(true)
		if _, err := pm.restartAsStandbyLocked(ctx, target.GetHost(), target.GetPostgresPort()); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to restart stale primary as standby", "error", err)
			return
		}
		// Apply the rule-derived role immediately (SetState transitions serving
		// state and republishes the label) so the gateway and stale-leader analyzer
		// stop treating us as a primary, rather than waiting a monitor cycle for
		// ReconcileRole. The rule named another leader (that is why we demoted), so
		// this resolves to REPLICA.
		intended, obs := deriveTypeAndObs(pm.latestRule(), pm.serviceID)
		if err := pm.servingState.SetState(ctx, intended, obs, clustermetadatapb.PoolerServingStatus_SERVING); err != nil {
			pm.logger.WarnContext(ctx, "MonitorPostgres: failed to apply role after demote", "error", err)
		}

	case remedialActionReconcileRole:
		pm.setMonitorReason(ctx, reasonPostgresRunning, "MonitorPostgres: PostgreSQL is running")
		// The pooler's role has drifted from the rule-derived role (or is still
		// UNKNOWN at boot). Apply it: SetState transitions the serving components
		// and republishes the label. A follow-up PR makes SetState derive the type
		// internally; for now the monitor passes the rule-derived type explicitly.
		intended, obs := deriveTypeAndObs(pm.latestRule(), pm.serviceID)
		pm.logger.InfoContext(ctx, "MonitorPostgres: applying consensus role from rule",
			"intended_role", intended.String())
		if err := pm.servingState.SetState(ctx, intended, obs, clustermetadatapb.PoolerServingStatus_SERVING); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to apply role from rule", "error", err)
		}

	case remedialActionResignLeadership:
		pm.setMonitorReason(ctx, reasonPostgresRunning, "MonitorPostgres: PostgreSQL is running")
		// The rule names us leader but postgres is running as a standby. Signal
		// voluntary resignation so the coordinator re-elects; use primary_term (the
		// term at which we were elected) so the coordinator knows the signal is
		// current. We do not self-promote.
		pm.logger.InfoContext(ctx, "MonitorPostgres: rule names us leader but postgres is a standby; resigning",
			"primary_term", state.primaryTerm)
		if state.primaryTerm != 0 {
			if err := pm.setResignedLeaderAtTerm(ctx, state.primaryTerm); err != nil {
				pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to set resigned primary term", "error", err)
			}
		}

	case remedialActionFixPrimaryConnInfo:
		rp := pm.consensusState.GetReplicationPrimary()
		target := rp.GetPrimary()
		targetHost := target.GetHost()
		targetPort := target.GetPostgresPort()
		pm.logger.InfoContext(ctx, "MonitorPostgres: primary_conninfo drift detected; rewriting to recorded primary",
			"target_primary", target.GetId().GetName(),
			"target_host", targetHost,
			"target_port", targetPort)
		// TODO: when rewindPending=true (an unexpected demotion happened
		// without a follow-up pg_rewind), just setting primary_conninfo is
		// not enough — the WAL receiver will fail to start due to timeline
		// divergence. Route through demoteStalePrimaryLocked instead, which
		// runs pg_rewind dry-run (cheap when there's no divergence) before
		// re-establishing replication. This would let the monitor self-heal
		// a stuck-replica scenario without waiting for orch's
		// FixReplicationAction to issue a RewindToSource RPC.
		if err := pm.setPrimaryConnInfoLocked(ctx, targetHost, targetPort,
			true /* stopReplicationBefore */, true /* startReplicationAfter */); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to reconcile primary_conninfo", "error", err)
		}

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
		if err := pm.rules.ReconcileGUC(ctx, !state.isPrimary); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: GUC reconciliation failed", "error", err)
		}
	}
}

// hasCompleteBackups checks if there are any complete backups available
func (pm *MultiPoolerManager) hasCompleteBackups(ctx context.Context) bool {
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
// is to come back as a standby with rewindPending=true so that the next
// SetPrimary/Promote/standby-conninfo path routes through demoteStalePrimaryLocked
// (which runs pg_rewind dry-run; cheap when there's no divergence) before
// trusting local WAL. This bears on the broader self-rewind plan:
//   - replicas with phantom transactions: orch sends an explicit
//     RewindToSource RPC today; a future change should let the local monitor
//     detect stuck replication and self-heal without needing orch in the loop.
//   - primaries demoted unexpectedly (crash, SIGKILL, external pg_demote):
//     the restart-as-standby helper should require callers to declare
//     "clean" vs "unexpected" so an unexpected transition can set
//     rewindPending up front, increasing the odds of fast convergence once
//     a new leader is announced.
func (pm *MultiPoolerManager) startPostgres(ctx context.Context) error {
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
func (pm *MultiPoolerManager) restoreAndStartPostgres(ctx context.Context) error {
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

	revokedBelowTerm, _ := pm.consensusState.GetInconsistentCurrentTermNumber()
	pm.logger.InfoContext(ctx, "MonitorPostgres: successfully restored from backup",
		"backup_id", latestBackup.BackupId,
		"shard", pm.getShardID(),
		"term", revokedBelowTerm)

	return nil
}
