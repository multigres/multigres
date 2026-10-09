// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These tests drive the coordinator's orchestration through the fakes in
// coordinator_fakes_test.go — the error and crash-recovery branches that need a
// failing dependency or a persisted transient phase, neither of which the real-PG
// e2e suite can produce deterministically.

func TestCreateMigration_InputValidation(t *testing.T) {
	tc := newTestCoord(t)
	ctx := context.Background()

	_, err := tc.c.CreateMigration(ctx, CreateParams{TargetDatabase: "app", Tables: []string{"t"}})
	require.ErrorContains(t, err, "source connection is required")

	_, err = tc.c.CreateMigration(ctx, CreateParams{ConnectionName: "src", Tables: []string{"t"}})
	require.ErrorContains(t, err, "target database is required")

	_, err = tc.c.CreateMigration(ctx, CreateParams{ConnectionName: "src", TargetDatabase: "app"})
	require.ErrorContains(t, err, "at least one table is required")
}

func TestCreateMigration_SourceUnreachable(t *testing.T) {
	tc := newTestCoord(t)
	tc.srcErr = errors.New("connection refused")
	_, err := tc.c.CreateMigration(context.Background(), CreateParams{
		ConnectionName: "src", TargetDatabase: "app", Tables: []string{"public.orders"},
	})
	require.ErrorContains(t, err, "connection refused")
}

func TestCreateMigration_ValidateError(t *testing.T) {
	tc := newTestCoord(t)
	tc.src.validateErr = errors.New("wal_level is replica")
	_, err := tc.c.CreateMigration(context.Background(), CreateParams{
		ConnectionName: "src", TargetDatabase: "app", Tables: []string{"public.orders"},
	})
	require.ErrorContains(t, err, "wal_level is replica")
}

func TestCreateMigration_DuplicateName(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{ID: 99, Name: "nightly", Phase: PhaseImporting})
	_, err := tc.c.CreateMigration(context.Background(), CreateParams{
		ConnectionName: "src", TargetDatabase: "app", Name: "nightly", Tables: []string{"public.orders"},
	})
	require.ErrorContains(t, err, "already exists")
}

func TestCreateMigration_InsertError(t *testing.T) {
	tc := newTestCoord(t)
	tc.store.insertErr = errors.New("disk full")
	_, err := tc.c.CreateMigration(context.Background(), CreateParams{
		ConnectionName: "src", TargetDatabase: "app", Tables: []string{"public.orders"},
	})
	require.ErrorContains(t, err, "disk full")
}

func TestCreateMigration_Success(t *testing.T) {
	tc := newTestCoord(t)
	tc.src.resolved = []string{"public.orders"}
	tc.src.validateInfo = &SourceInfo{ServerVersionNum: 170000, CanCreateSubscription: false} // warns, still ok
	proj, err := tc.c.CreateMigration(context.Background(), CreateParams{
		ConnectionName: "src", TargetDatabase: "app", Tables: []string{"public.orders"}, CopyData: true,
	})
	require.NoError(t, err)
	require.Equal(t, PhaseCreated, proj.Phase)
	require.Equal(t, []string{"public.orders"}, proj.Tables)
	require.Len(t, tc.store.migs, 1)
	require.Equal(t, 1, tc.src.closed, "the validation source connection must be closed")
}

func TestStartMigration_RunSetupErrorMarksFailed(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCreated})
	tc.tgt.createSubErr = errors.New("subscription blew up")

	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionImport, ActivateOptions{})
	require.ErrorContains(t, err, "subscription blew up")
	require.Equal(t, PhaseFailed, tc.store.migs[testMigID].Phase, "a runSetup failure must record FAILED")
	require.Contains(t, tc.store.migs[testMigID].LastError, "subscription blew up")
}

func TestStartMigration_SuccessReachesImporting(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCreated})
	tc.tgt.status = &SubscriptionStatus{TotalRelations: 1, ReadyRelations: 1} // caught up

	proj, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionImport, ActivateOptions{})
	require.NoError(t, err)
	require.Equal(t, PhaseImporting, proj.Phase, "a caught-up subscription must advance COPYING -> IMPORTING")
	// runSetup must have created the publication (source) and subscription (target).
	require.Contains(t, tc.log, "source.CreatePublication")
	require.Contains(t, tc.log, "target.CreateSubscription")
	require.Contains(t, tc.log, "target.DropTables") // schema copy path (not skipped)

	// DisableUserTriggers must run between the schema copy landing and the
	// subscription that would start applying rows under
	// session_replication_role=replica — see its doc comment for the attack
	// this closes (a source trigger otherwise firing, in any mode, with the
	// target admin's privileges). DropUserCheckConstraints and
	// DisableUserRewriteRules close the same vector for CHECK constraints and
	// rewrite rules respectively.
	applySchema := logIndex(tc.log, "target.ApplySchema")
	disableTriggers := logIndex(tc.log, "target.DisableUserTriggers")
	dropChecks := logIndex(tc.log, "target.DropUserCheckConstraints")
	disableRules := logIndex(tc.log, "target.DisableUserRewriteRules")
	createSub := logIndex(tc.log, "target.CreateSubscription")
	require.NotEqual(t, -1, disableTriggers, "triggers must be disabled after every schema copy")
	require.NotEqual(t, -1, dropChecks, "check constraints must be dropped after every schema copy")
	require.NotEqual(t, -1, disableRules, "rewrite rules must be disabled after every schema copy")
	require.Less(t, applySchema, disableTriggers, "triggers can only be disabled once the schema (and its triggers) exist")
	require.Less(t, applySchema, dropChecks, "check constraints can only be dropped once the schema (and its constraints) exist")
	require.Less(t, applySchema, disableRules, "rewrite rules can only be disabled once the schema (and its rules) exist")
	require.Less(t, disableTriggers, createSub, "triggers must be disabled before the subscription can apply any row")
	require.Less(t, dropChecks, createSub, "check constraints must be dropped before the subscription can apply any row")
	require.Less(t, disableRules, createSub, "rewrite rules must be disabled before the subscription can apply any row")
}

// TestStartMigration_ChecksDropPrivilegeBeforeDropTables is the regression
// test for the HIGH finding this ordering guards against: CheckDropPrivilege
// must run, and must run before the admin pool drops anything, so a caller
// who only passed the gateway's CanCreateMigration gate (database CREATE +
// pg_create_subscription, not ownership of these specific tables) cannot have
// an existing target table it does not own destroyed via DropTables.
func TestStartMigration_ChecksDropPrivilegeBeforeDropTables(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCreated})
	tc.tgt.status = &SubscriptionStatus{TotalRelations: 1, ReadyRelations: 1}

	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionImport,
		ActivateOptions{CallerRole: "app"})
	require.NoError(t, err)

	checkPriv := logIndex(tc.log, "target.CheckDropPrivilege")
	dropTables := logIndex(tc.log, "target.DropTables")
	require.NotEqual(t, -1, checkPriv, "CheckDropPrivilege must run")
	require.Less(t, checkPriv, dropTables, "the privilege check must run before DropTables, not after")
	require.Equal(t, "app", tc.tgt.lastCallerRole, "ActivateOptions.CallerRole must reach the check")
}

// TestStartMigration_DropPrivilegeErrorMarksFailed is the regression test for
// the HIGH finding this guards against: a CheckDropPrivilege rejection must
// abort setup before DropTables ever runs, not merely get logged or ignored.
func TestStartMigration_DropPrivilegeErrorMarksFailed(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCreated})
	tc.tgt.checkDropPrivilegeErr = errors.New("role \"app\" lacks DROP privilege on existing target table(s): public.orders")

	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionImport,
		ActivateOptions{CallerRole: "app"})
	require.ErrorContains(t, err, "lacks DROP privilege")
	require.NotContains(t, tc.log, "target.DropTables", "a rejected privilege check must never reach DropTables")
	require.Equal(t, PhaseFailed, tc.store.migs[testMigID].Phase)
}

func TestStartMigration_SkipSchemaCopy(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCreated, SkipSchemaCopy: true})
	tc.tgt.status = &SubscriptionStatus{TotalRelations: 1, ReadyRelations: 1}

	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionImport, ActivateOptions{CallerRole: "app"})
	require.NoError(t, err)
	require.NotContains(t, tc.log, "target.DropTables", "SkipSchemaCopy must bypass the drop/apply schema step")
	require.NotContains(t, tc.log, "target.ApplySchema")
	require.NotContains(t, tc.log, "target.DisableUserTriggers", "nothing was just copied in, so there is nothing to disable")
	require.NotContains(t, tc.log, "target.DropUserCheckConstraints", "nothing was just copied in, so there is nothing to drop")
	require.NotContains(t, tc.log, "target.DisableUserRewriteRules", "nothing was just copied in, so there is nothing to disable")
	require.Contains(t, tc.log, "target.CheckDropPrivilege", "the privilege check must still run: the subscription below writes into these tables via the admin pool regardless of whether schema copy ran")
}

// TestStartMigration_SkipSchemaCopyStillChecksDropPrivilege is the regression
// test for the HIGH finding this guards against: with SkipSchemaCopy=true,
// CheckDropPrivilege was previously skipped entirely (it lived inside the
// `if !m.SkipSchemaCopy` block alongside DropTables), letting a caller who
// only passed the gateway's CanCreateMigration gate — not ownership of these
// specific tables — have the subscription stream source writes into a
// pre-existing target table it does not own, via the superuser admin pool. A
// rejected privilege check must abort setup before CreateSubscription ever
// runs, exactly as it already does on the non-skip path.
func TestStartMigration_SkipSchemaCopyStillChecksDropPrivilege(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCreated, SkipSchemaCopy: true})
	tc.tgt.checkDropPrivilegeErr = errors.New("role \"app\" lacks DROP privilege on existing target table(s): public.orders")

	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionImport,
		ActivateOptions{CallerRole: "app"})
	require.ErrorContains(t, err, "lacks DROP privilege")
	require.NotContains(t, tc.log, "target.CreateSubscription", "a rejected privilege check must never reach CreateSubscription")
	require.Equal(t, PhaseFailed, tc.store.migs[testMigID].Phase)
}

func TestDrop_CompletingResume(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCompleting, Direction: DirectionImport})
	_, err := tc.c.DropMigration(context.Background(), Ref{ID: testMigID}, DropOptions{})
	require.NoError(t, err)
	require.Equal(t, []int64{testMigID}, tc.store.deletes, "a COMPLETING drop must finish teardown and delete the row")
	require.Contains(t, tc.log, "target.DropSubscription", "IMPORT teardown drops the target subscription")
}

func TestDrop_NotStartedRequiresForce(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCreated})
	_, err := tc.c.DropMigration(context.Background(), Ref{ID: testMigID}, DropOptions{})
	require.ErrorContains(t, err, "has not started")
}

func TestDrop_NotCaughtUpRequiresWait(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCopying})
	tc.tgt.status = &SubscriptionStatus{TotalRelations: 1, ReadyRelations: 0} // not caught up
	_, err := tc.c.DropMigration(context.Background(), Ref{ID: testMigID}, DropOptions{})
	require.ErrorContains(t, err, "not caught up")
}

func TestDrop_WaitTimeout(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCopying})
	tc.tgt.status = &SubscriptionStatus{TotalRelations: 1, ReadyRelations: 0} // never catches up
	_, err := tc.c.DropMigration(context.Background(), Ref{ID: testMigID}, DropOptions{Wait: true, WaitTimeout: 20 * time.Millisecond})
	require.ErrorContains(t, err, "timed out waiting")
}

func TestDrop_ForceSkipsDrain(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseImporting})
	_, err := tc.c.DropMigration(context.Background(), Ref{ID: testMigID}, DropOptions{Force: true})
	require.NoError(t, err)
	require.Equal(t, []int64{testMigID}, tc.store.deletes)
	require.NotContains(t, tc.log, "source.SetReadOnly(true)", "force must skip the drain barrier")
}

func TestDrop_GracefulImportSuccess(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseImporting})
	_, err := tc.c.DropMigration(context.Background(), Ref{ID: testMigID}, DropOptions{})
	require.NoError(t, err)
	require.Equal(t, []int64{testMigID}, tc.store.deletes)
	require.Contains(t, tc.log, "target.DropSubscription")
}

func TestDrop_GracefulImportSoftQuiesce(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseImporting, QuiesceRoles: []string{"app"}})
	_, err := tc.c.DropMigration(context.Background(), Ref{ID: testMigID}, DropOptions{})
	require.NoError(t, err)
	// A graceful drop leaves the source a standalone primary, so it uses the soft
	// quiesce: read-only only, never terminating or fencing the app's own backends.
	require.Contains(t, tc.log, "source.SetReadOnly(true)", "graceful drop still drains read-only")
	require.NotContains(t, tc.log, "source.TerminateClientBackends", "graceful drop must not cut the app's backends")
	require.NotContains(t, tc.log, "source.RevokeConnect", "graceful drop must not fence the app roles")
	require.NotContains(t, tc.log, "source.GrantConnect",
		"nothing was ever revoked for this migration, so teardown must not grant CONNECT a role may have been denied for an unrelated reason")
}

func TestDrop_GracefulDrainFailureRestoresPhase(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseExporting})
	tc.tgt.waitSlotErr = errors.New("slot never confirmed") // EXPORT drains via the target slot
	_, err := tc.c.DropMigration(context.Background(), Ref{ID: testMigID}, DropOptions{})
	require.ErrorContains(t, err, "drain before dropping")
	require.Equal(t, PhaseExporting, tc.store.migs[testMigID].Phase, "a failed drain must restore the streaming phase")
	require.Empty(t, tc.store.deletes, "a failed drain must not delete the migration")
}

func TestDrop_CompletingUpdateErrorPropagates(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseImporting})
	tc.store.updateErr = errors.New("update rejected") // the COMPLETING commit fails
	_, err := tc.c.DropMigration(context.Background(), Ref{ID: testMigID}, DropOptions{})
	require.ErrorContains(t, err, "update rejected")
}

func TestSetDirection_Invalid(t *testing.T) {
	tc := newTestCoord(t)
	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, Direction("SIDEWAYS"), ActivateOptions{})
	require.ErrorContains(t, err, "invalid direction")
}

func TestSetDirection_NoOp(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseImporting})
	proj, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionImport, ActivateOptions{})
	require.NoError(t, err)
	require.Equal(t, PhaseImporting, proj.Phase)
	require.Equal(t, 0, tc.store.updates, "a no-op direction set must not write")
}

func TestSetDirection_ConflictingSwitchInProgress(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseSwitchingToExport})
	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionImport, ActivateOptions{})
	require.ErrorContains(t, err, "already in progress")
}

func TestSetDirection_ExportNotConfigured(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.targetConnInfo = nil
	tc.seed(&Migration{Phase: PhaseImporting})
	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionExport, ActivateOptions{})
	require.ErrorContains(t, err, "EXPORT direction is not configured")
}

func TestSetDirection_ExportSourceCannotSubscribe(t *testing.T) {
	tc := newTestCoord(t)
	tc.src.info = &SourceInfo{ServerVersionNum: 150000, CanCreateSubscription: false}
	tc.seed(&Migration{Phase: PhaseImporting})
	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionExport, ActivateOptions{})
	require.ErrorContains(t, err, "cannot switch to EXPORT")
}

func TestSetDirection_NotStreaming(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCopying})
	tc.tgt.status = &SubscriptionStatus{TotalRelations: 1, ReadyRelations: 0}
	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionExport, ActivateOptions{})
	require.ErrorContains(t, err, "must be caught up")
}

func TestActivate_ImportToExportFullSwitch(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = time.Now // readiness gate needs a real advancing clock
	m := tc.seed(&Migration{Phase: PhaseImporting})
	tc.src.lagPresent = true
	tc.src.lag = 0          // ready immediately
	tc.tgt.subExists = true // the import subscription is live (drives currentLinkLive + drain)

	proj, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionExport, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
	require.NoError(t, err)
	require.Equal(t, PhaseExporting, proj.Phase)
	require.Contains(t, tc.log, "target.CreatePublication", "EXPORT makes the target the publisher")
	require.Contains(t, tc.log, "target.CreateLogicalSlot", "the reverse slot is pre-created on the target")
	require.Contains(t, tc.log, "source.CreateSubscription", "the reverse subscription is established on the source")
}

// logIndex returns the position of the first log entry equal to want, or -1.
func logIndex(log []string, want string) int {
	for i, e := range log {
		if e == want {
			return i
		}
	}
	return -1
}

func TestActivate_HardQuiesceFencesRolesAndTerminates(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = time.Now
	m := tc.seed(&Migration{Phase: PhaseImporting, QuiesceRoles: []string{"app"}})
	tc.src.lagPresent = true
	tc.src.lag = 0
	tc.tgt.subExists = true

	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionExport, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
	require.NoError(t, err)

	// The hard quiesce fences the named roles and cuts live client backends, and both
	// must happen before the switch tears the import link down (source.DropPublication).
	revoke := logIndex(tc.log, "source.RevokeConnect")
	terminate := logIndex(tc.log, "source.TerminateClientBackends")
	dropPub := logIndex(tc.log, "source.DropPublication")
	require.NotEqual(t, -1, revoke, "activate must revoke CONNECT from the fenced roles")
	require.NotEqual(t, -1, terminate, "activate must terminate live source client backends")
	require.NotEqual(t, -1, dropPub)
	require.Less(t, revoke, dropPub, "the fence must precede the switch")
	require.Less(t, terminate, dropPub, "the write-cut must precede the switch")
}

// TestActivate_RevokeQueriesAndPersistsPublicHadConnect covers the finding this
// test guards against: GrantConnect at a later rollback/teardown must only
// restore PUBLIC's CONNECT when PUBLIC genuinely had it before this
// migration's ACTIVATE cutover revoked it — never when an operator had
// already locked PUBLIC out as a hardening measure. That requires querying
// and persisting the source's actual PUBLIC CONNECT state at revoke time,
// before the revoke overwrites it.
func TestActivate_RevokeQueriesAndPersistsPublicHadConnect(t *testing.T) {
	t.Run("PUBLIC already lacked CONNECT", func(t *testing.T) {
		tc := newTestCoord(t)
		tc.c.now = time.Now
		m := tc.seed(&Migration{Phase: PhaseImporting, QuiesceRoles: []string{"app"}})
		tc.src.lagPresent = true
		tc.tgt.subExists = true
		tc.src.publicHasConnect = false

		_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionExport, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
		require.NoError(t, err)
		require.Contains(t, tc.log, "source.PublicHasConnect", "the hard quiesce must query PUBLIC's current state before revoking")
		require.False(t, m.PublicHadConnect, "must persist that PUBLIC never had CONNECT")
	})

	t.Run("PUBLIC had CONNECT", func(t *testing.T) {
		tc := newTestCoord(t)
		tc.c.now = time.Now
		m := tc.seed(&Migration{Phase: PhaseImporting, QuiesceRoles: []string{"app"}})
		tc.src.lagPresent = true
		tc.tgt.subExists = true
		tc.src.publicHasConnect = true

		_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionExport, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
		require.NoError(t, err)
		require.True(t, m.PublicHadConnect, "must persist that PUBLIC had CONNECT")
	})

	t.Run("no quiesce roles still queries and revokes PUBLIC", func(t *testing.T) {
		// Regression test for the finding this guards against: PUBLIC normally
		// holds CONNECT whether or not any QuiesceRoles are configured, and
		// default_transaction_read_only is a USERSET GUC a reconnecting client
		// can override with BEGIN READ WRITE — so an empty QuiesceRoles must
		// not leave the hard quiesce's barrier open to every other role.
		tc := newTestCoord(t)
		tc.c.now = time.Now
		m := tc.seed(&Migration{Phase: PhaseImporting}) // no QuiesceRoles
		tc.src.lagPresent = true
		tc.tgt.subExists = true
		tc.src.publicHasConnect = true

		_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionExport, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
		require.NoError(t, err)
		require.Contains(t, tc.log, "source.PublicHasConnect", "PUBLIC's state must still be queried before revoking it")
		require.Contains(t, tc.log, "source.RevokeConnect", "PUBLIC must still be revoked even with no named roles to fence")
		require.True(t, m.PublicHadConnect)
		require.True(t, m.QuiesceApplied, "a hard quiesce ran, regardless of QuiesceRoles being empty")
	})
}

// TestActivate_RevokeQueriesAndPersistsQuiesceRoleConnLimits covers the finding
// this guards against: REVOKE CONNECT alone does not fence a role that
// inherits CONNECT from an ancestor group role, so RevokeConnect additionally
// caps each quiesce role's CONNECTION LIMIT to 0 (enforced by Postgres at
// authentication time, independent of the privilege-inheritance chain). The
// role's original limit must be queried and persisted before it is zeroed, so
// an operator-set custom limit (not just the default -1/unlimited) is restored
// correctly later.
func TestActivate_RevokeQueriesAndPersistsQuiesceRoleConnLimits(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = time.Now
	m := tc.seed(&Migration{Phase: PhaseImporting, QuiesceRoles: []string{"app", "reporting"}})
	tc.src.lagPresent = true
	tc.tgt.subExists = true
	tc.src.roleConnLimits = map[string]int32{"app": 5, "reporting": -1}

	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionExport, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
	require.NoError(t, err)
	require.Contains(t, tc.log, "source.RoleConnLimits", "the hard quiesce must query each role's current limit before zeroing it")
	require.Equal(t, map[string]int32{"app": 5, "reporting": -1}, m.QuiesceRoleConnLimits, "must persist the pre-revoke limits, not assume -1/unlimited")
}

// TestDrainCurrent_PersistsRestoreStateBeforeRevoke is the regression test for
// the finding this guards against: PublicHadConnect/QuiesceRoleConnLimits used
// to live only on the in-memory Migration struct until the whole switch's own
// store.Update ran at the very end (after switchTo too) — even though
// RevokeConnect, a live and hard-to-reverse mutation on the source, runs
// immediately after they're captured. A crash in that window (revoke already
// applied for real, but before the switch's own final persist) would lose the
// only record of what GrantConnect's later rollback/teardown should restore:
// it would then re-grant PUBLIC incorrectly (or not at all) and reset each
// role's connection limit to a hardcoded default instead of its real prior
// value. The store must already reflect both fields the moment the hard
// quiesce captures them — before RevokeConnect runs, not after the switch.
func TestDrainCurrent_PersistsRestoreStateBeforeRevoke(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseImporting, QuiesceRoles: []string{"app"}})
	tc.src.publicHasConnect = true
	tc.src.roleConnLimits = map[string]int32{"app": 7}

	_, err := tc.c.drainCurrent(context.Background(), m, DirectionImport, true)
	require.NoError(t, err)

	// fakeStore.Update shares m's pointer with the store (no copy), so reading
	// the stored fields back would pass even without a real persist — asserting
	// on the update counter is what actually proves store.Update was called,
	// not just that the in-memory struct was mutated.
	require.GreaterOrEqual(t, tc.store.updates, 1,
		"drainCurrent's hard quiesce must call store.Update itself, before RevokeConnect runs — not rely on a later caller to persist")
	require.True(t, m.PublicHadConnect)
	require.Equal(t, map[string]int32{"app": 7}, m.QuiesceRoleConnLimits)
}

func TestActivate_HardQuiesceWithoutRolesStillTerminates(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = time.Now
	m := tc.seed(&Migration{Phase: PhaseImporting}) // no QuiesceRoles
	tc.src.lagPresent = true
	tc.src.lag = 0
	tc.tgt.subExists = true

	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionExport, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
	require.NoError(t, err)
	require.Contains(t, tc.log, "source.TerminateClientBackends", "terminate is unconditional at the ACTIVATE barrier")
	require.Contains(t, tc.log, "source.RevokeConnect", "RevokeConnect must still run to fence PUBLIC, even with no named roles")
	// PUBLIC's CONNECT must stay revoked all the way through the reverse
	// subscription attaching — nothing restores it until a later rollback or
	// teardown, so a successful ACTIVATE must never call GrantConnect.
	require.NotContains(t, tc.log, "source.GrantConnect", "CONNECT must not be restored mid-switch")
}

func TestActivate_TerminateErrorAbortsSwitch(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = time.Now
	m := tc.seed(&Migration{Phase: PhaseImporting})
	tc.src.lagPresent = true
	tc.src.lag = 0
	tc.tgt.subExists = true
	tc.src.terminateErr = errors.New("cannot signal backends")

	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionExport, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
	require.ErrorContains(t, err, "cannot signal backends")
	require.NotContains(t, tc.log, "source.DropPublication", "a failed write-cut must not proceed into the switch")
}

// TestActivate_PartialHardQuiesceFailureRestoresConnectOnTeardown is the
// regression test for the MEDIUM finding this guards against: a hard quiesce
// can fail partway through its drainCurrent call — REVOKE CONNECT (and the
// per-role connection-limit cap) already applied, then the very next step,
// SetReadOnly(true), fails (e.g. a non-superuser source account lacking the
// ALTER SYSTEM privilege it needs). The migration is marked FAILED, leaving
// the fenced app roles locked out of the source — unless a later forced drop
// restores them via the IMPORT-direction teardown, not just the
// EXPORT-direction one that only ever runs once ACTIVATE has fully succeeded.
func TestActivate_PartialHardQuiesceFailureRestoresConnectOnTeardown(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = time.Now
	m := tc.seed(&Migration{Phase: PhaseImporting, QuiesceRoles: []string{"app"}})
	tc.src.lagPresent = true
	tc.src.lag = 0
	tc.tgt.subExists = true
	tc.src.roleConnLimits = map[string]int32{"app": 5}
	tc.src.setROErr = errors.New("permission denied: must be superuser")

	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionExport, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
	require.ErrorContains(t, err, "must be superuser")
	require.Contains(t, tc.log, "source.RevokeConnect", "the revoke must have already run before the failing step")
	require.Equal(t, PhaseFailed, m.Phase)
	require.Equal(t, map[string]int32{"app": 5}, m.QuiesceRoleConnLimits, "the pre-revoke limits must still be persisted for the later restore to use")

	// Clear the injected failure and the call log, then force-drop the now-FAILED
	// migration (force is required: it never reached a caught-up streaming
	// state). teardown's IMPORT-direction branch must restore CONNECT using
	// exactly the persisted state captured above.
	tc.src.setROErr = nil
	tc.log = nil
	_, err = tc.c.DropMigration(context.Background(), Ref{ID: m.ID}, DropOptions{Force: true})
	require.NoError(t, err)
	require.Contains(t, tc.log, "source.GrantConnect", "CONNECT revoked by the failed hard quiesce must be restored on teardown")
}

// TestActivate_AlreadyActive_IsNoOp covers the unified verb's symmetric no-op
// case for EXPORT (TestSetDirection_NoOp covers IMPORT): setting a migration
// to the direction it is already in succeeds without error or a store write,
// rather than erroring — this used to be the separate Activate RPC's "already
// active" rejection, deliberately replaced per the consolidated
// SetMigrationDirection's documented behavior ("if the current direction is
// already correct, it is a no-op").
func TestActivate_AlreadyActive_IsNoOp(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseExporting})
	proj, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionExport, ActivateOptions{})
	require.NoError(t, err)
	require.Equal(t, PhaseExporting, proj.Phase)
	require.Equal(t, 0, tc.store.updates, "a no-op direction set must not write")
}

func TestActivate_NotReadyTimeout(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = time.Now
	tc.seed(&Migration{Phase: PhaseImporting})
	tc.src.lagPresent = false // slot not found -> never ready
	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionExport, ActivateOptions{MaxLagBytes: 1, WaitTimeout: 20 * time.Millisecond})
	require.ErrorIs(t, err, ErrNotReady)
}

func TestDeactivate_ExportToImportFullSwitch(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseExporting})
	tc.src.subExists = true // export subscription live on the source (currentLinkLive + reverse-link no-op)

	proj, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionImport, ActivateOptions{})
	require.NoError(t, err)
	require.Equal(t, PhaseImporting, proj.Phase)
	require.Contains(t, tc.log, "source.CreatePublication", "IMPORT makes the source the publisher again")
	require.Contains(t, tc.log, "target.DropLogicalSlot", "the reverse slot is dropped on the target")
	require.Contains(t, tc.log, "target.CreateSubscription", "the forward subscription is re-established on the target")
}

// TestDeactivate_GrantConnectAfterForwardSubscriptionExists is the regression
// test for the finding this guards against: EXPORT->IMPORT used to restore app
// CONNECT before the forward (source-publishes-again) subscription existed, so a
// reconnecting client could write to the source in the window before that
// subscription's slot anchored a start point to capture it — silently losing the
// write. GrantConnect must not run until target.CreateSubscription has
// succeeded. AdvanceSequences must also precede GrantConnect, so a reconnecting
// write cannot collide with an already-migrated sequence value.
func TestDeactivate_GrantConnectAfterForwardSubscriptionExists(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.drainForImport = func(context.Context) error { return nil }
	m := tc.seed(&Migration{Phase: PhaseExporting, QuiesceRoles: []string{"app"}})
	tc.src.subExists = true

	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionImport, ActivateOptions{})
	require.NoError(t, err)

	createSub := logIndex(tc.log, "target.CreateSubscription")
	advanceSeqs := logIndex(tc.log, "source.AdvanceSequences")
	grantConnect := logIndex(tc.log, "source.GrantConnect")
	require.NotEqual(t, -1, createSub, "the forward subscription must be (re-)established")
	require.NotEqual(t, -1, grantConnect, "CONNECT must be restored for the fenced app role")
	require.NotEqual(t, -1, advanceSeqs, "sequences must be advanced before writes resume")
	require.Less(t, createSub, grantConnect,
		"CONNECT must not be restored until the forward subscription exists, or a reconnecting write can land before the slot anchors a capture point and be lost")
	require.Less(t, advanceSeqs, grantConnect,
		"sequences must be advanced before CONNECT is restored, or a reconnecting write can collide with an already-migrated sequence value")
}

func TestDeactivate_DrainsTargetAndRestoresConnect(t *testing.T) {
	tc := newTestCoord(t)
	drained := 0
	tc.c.drainForImport = func(context.Context) error { drained++; return nil }
	m := tc.seed(&Migration{Phase: PhaseExporting, QuiesceRoles: []string{"app"}})
	tc.src.subExists = true

	proj, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionImport, ActivateOptions{})
	require.NoError(t, err)
	require.Equal(t, PhaseImporting, proj.Phase)
	require.Equal(t, 1, drained, "deactivate must drain the pooler to non-serving synchronously before the target becomes a subscriber")
	require.Contains(t, tc.log, "source.GrantConnect", "deactivate restores app CONNECT on the source-as-publisher")
}

// TestDeactivate_GrantConnectRestoresPublicOnlyWhenItHadConnect is the
// regression test for the finding: deactivate (rollback) must only restore
// PUBLIC's CONNECT when the prior ACTIVATE recorded that PUBLIC genuinely had
// it — never unconditionally, or an operator who had deliberately locked
// PUBLIC out of a hardened source would find it re-opened to every login role
// after a rollback.
func TestDeactivate_GrantConnectRestoresPublicOnlyWhenItHadConnect(t *testing.T) {
	t.Run("PUBLIC never had CONNECT: not restored", func(t *testing.T) {
		tc := newTestCoord(t)
		tc.c.drainForImport = func(context.Context) error { return nil }
		m := tc.seed(&Migration{Phase: PhaseExporting, QuiesceRoles: []string{"app"}, PublicHadConnect: false})
		tc.src.subExists = true

		_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionImport, ActivateOptions{})
		require.NoError(t, err)
		require.Contains(t, tc.log, "source.GrantConnect", "the named app role is still restored")
		require.False(t, tc.src.grantConnectRestorePublic, "PUBLIC must not be re-granted CONNECT it never had")
	})

	t.Run("PUBLIC had CONNECT: restored", func(t *testing.T) {
		tc := newTestCoord(t)
		tc.c.drainForImport = func(context.Context) error { return nil }
		m := tc.seed(&Migration{Phase: PhaseExporting, QuiesceRoles: []string{"app"}, PublicHadConnect: true})
		tc.src.subExists = true

		_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionImport, ActivateOptions{})
		require.NoError(t, err)
		require.True(t, tc.src.grantConnectRestorePublic, "PUBLIC must be restored since it had CONNECT before ACTIVATE revoked it")
	})
}

// TestDeactivate_GrantConnectRestoresOriginalConnLimits is the restore-side
// regression test for the group-role-inheritance finding: GrantConnect must
// set each quiesce role's CONNECTION LIMIT back to the value persisted at
// ACTIVATE time (Migration.QuiesceRoleConnLimits), not a hardcoded -1 that
// would silently discard an operator-set custom limit.
func TestDeactivate_GrantConnectRestoresOriginalConnLimits(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.drainForImport = func(context.Context) error { return nil }
	m := tc.seed(&Migration{
		Phase:                 PhaseExporting,
		QuiesceRoles:          []string{"app", "reporting"},
		QuiesceRoleConnLimits: map[string]int32{"app": 5, "reporting": -1},
	})
	tc.src.subExists = true

	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionImport, ActivateOptions{})
	require.NoError(t, err)
	require.Equal(t, map[string]int32{"app": 5, "reporting": -1}, tc.src.grantConnectLimits)
}

func TestDeactivate_DrainForImportErrorAborts(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.drainForImport = func(context.Context) error { return errors.New("still serving") }
	m := tc.seed(&Migration{Phase: PhaseExporting})
	tc.src.subExists = true

	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionImport, ActivateOptions{})
	require.ErrorContains(t, err, "still serving")
	require.NotContains(t, tc.log, "source.CreatePublication", "a failed non-serving drain must not proceed into the switch")
}

// TestDeactivate_StuckDrainFailsWithinCeiling covers the EXPORT-direction
// reverse-slot failover bug: when the slot the drain is waiting on never
// confirms (e.g. lost in a target-primary failover without
// --enable-slot-based-replication), the caller here passes context.Background()
// — no deadline of its own — so without DefaultDrainWaitTimeout the drain, and
// the c.mu it holds, would never return. It must instead fail within the
// ceiling, land the migration as FAILED with a diagnostic LastError (not leave
// it silently stuck in EXPORTING), and leave c.mu free for subsequent calls.
func TestDeactivate_StuckDrainFailsWithinCeiling(t *testing.T) {
	orig := DefaultDrainWaitTimeout
	DefaultDrainWaitTimeout = 50 * time.Millisecond
	t.Cleanup(func() { DefaultDrainWaitTimeout = orig })

	tc := newTestCoord(t)
	tc.tgt.waitSlotBlocks = true
	m := tc.seed(&Migration{Phase: PhaseExporting, QuiesceRoles: []string{"app"}})
	tc.src.subExists = true

	done := make(chan error, 1)
	go func() {
		_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionImport, ActivateOptions{})
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(2 * time.Second):
		t.Fatal("Deactivate did not return within DefaultDrainWaitTimeout")
	}

	// FAILED with a diagnostic, not silently stuck in EXPORTING.
	require.Equal(t, PhaseFailed, tc.store.migs[m.ID].Phase)
	require.Contains(t, tc.store.migs[m.ID].LastError, "context deadline exceeded")

	// c.mu must be free: a read that would have queued behind the stuck drain
	// succeeds immediately now that Deactivate has returned.
	proj, err := tc.c.GetMigration(context.Background(), Ref{ID: m.ID})
	require.NoError(t, err)
	require.Equal(t, PhaseFailed, proj.Phase)
	require.Contains(t, proj.LastError, "context deadline exceeded")
}

func TestActivate_ExportGrantsBackOnTeardown(t *testing.T) {
	tc := newTestCoord(t)
	// A fenced EXPORT migration torn down (COMPLETING resume) must restore CONNECT.
	tc.seed(&Migration{Phase: PhaseCompleting, Direction: DirectionExport, QuiesceRoles: []string{"app"}})
	require.NoError(t, tc.c.Reconcile(context.Background()))
	require.Contains(t, tc.log, "source.GrantConnect", "EXPORT teardown restores app CONNECT on the standalone source")
}

func TestReconcile_CopyingToImporting(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCopying})
	tc.tgt.status = &SubscriptionStatus{TotalRelations: 2, ReadyRelations: 2}
	require.NoError(t, tc.c.Reconcile(context.Background()))
	require.Equal(t, PhaseImporting, tc.store.migs[testMigID].Phase)
	require.NotNil(t, tc.store.migs[testMigID].StreamingSince)
}

func TestReconcile_CompletingFinishesTeardown(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCompleting, Direction: DirectionExport})
	require.NoError(t, tc.c.Reconcile(context.Background()))
	require.Equal(t, []int64{testMigID}, tc.store.deletes)
	require.Contains(t, tc.log, "target.DropPublication", "EXPORT teardown drops the target publication")
}

func TestReconcile_ExportingReestablishesReverseLink(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseExporting})
	tc.src.subExists = false // reverse subscription missing -> reconcile must recreate it
	tc.tgt.slotReady = true  // the slot itself is fine; only the attach is missing
	require.NoError(t, tc.c.Reconcile(context.Background()))
	require.Contains(t, tc.log, "source.CreateSubscription")
	require.NotContains(t, tc.log, "target.CreateLogicalSlot", "a ready slot must not be recreated, just attached to")
}

func TestReconcile_SwitchingToExportResumes(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseSwitchingToExport})
	tc.tgt.subExists = true // pre-switch import link still live
	require.NoError(t, tc.c.Reconcile(context.Background()))
	require.Equal(t, PhaseExporting, tc.store.migs[testMigID].Phase, "an interrupted switch must roll forward to EXPORTING")
}

func TestEnsureReverseExportLink_AlreadyPresentIsNoOp(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseExporting})
	tc.src.subExists = true
	tc.tgt.slotReady = true
	require.NoError(t, tc.c.ensureReverseExportLink(context.Background(), m))
	require.NotContains(t, tc.log, "source.CreateSubscription", "an existing, healthy reverse link must not be recreated")
	require.NotContains(t, tc.log, "source.DropSubscription", "a healthy link must not be torn down")
}

func TestEnsureReverseExportLink_TargetConnInfoError(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.targetConnInfo = func(string) (string, error) { return "", errors.New("no advertise host") }
	m := tc.seed(&Migration{Phase: PhaseExporting})
	err := tc.c.ensureReverseExportLink(context.Background(), m)
	require.ErrorContains(t, err, "no advertise host")
}

// TestEnsureReverseExportLink_DetectsDegradedSlotLost covers the EXPORT-direction
// reverse-slot failover bug: a target-primary failover can drop the reverse
// slot even though the external subscription row survives (its apply worker
// just errors forever). ensureReverseExportLink must notice the slot is
// unusable and report it — not repair it automatically, since the only
// correctness-preserving recovery (drop, recreate, and resubscribe with a full
// copy) is a large, unbounded operation that must never run unannounced as a
// side effect of a routine reconcile tick. See TestReseedReverseLink_* for the
// explicit, operator-triggered repair.
func TestEnsureReverseExportLink_DetectsDegradedSlotLost(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseExporting})
	tc.src.subExists = true
	tc.tgt.slotReady = false // lost in a target-primary failover

	err := tc.c.ensureReverseExportLink(context.Background(), m)
	require.ErrorContains(t, err, "reseed required")

	require.NotContains(t, tc.log, "source.DropSubscription", "detection must not repair anything")
	require.NotContains(t, tc.log, "target.DropLogicalSlot", "detection must not repair anything")
	require.NotContains(t, tc.log, "target.CreateLogicalSlot", "detection must not repair anything")
	require.NotContains(t, tc.log, "source.CreateSubscription", "detection must not repair anything")

	require.NotEmpty(t, tc.store.migs[m.ID].ReverseLinkError, "the degraded state must be persisted")
	events := tc.store.journalEvents(m.ID)
	require.Contains(t, events, JournalEventReverseLinkDegraded, "the detection must be recorded for audit visibility")
	require.NotContains(t, events, JournalEventReseed, "no repair happened, so no reseed event")
}

// TestEnsureReverseExportLink_DetectsDegradedNothingExists covers the narrower
// case where a prior attempt failed between creating the slot and the
// subscription ever attaching to it — same detection, minus any subscription
// to report as stale.
func TestEnsureReverseExportLink_DetectsDegradedNothingExists(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseExporting})
	tc.src.subExists = false
	tc.tgt.slotReady = false

	err := tc.c.ensureReverseExportLink(context.Background(), m)
	require.ErrorContains(t, err, "reseed required")
	require.NotContains(t, tc.log, "source.CreateSubscription", "detection must not repair anything")
	require.NotEmpty(t, tc.store.migs[m.ID].ReverseLinkError, "the degraded state must be persisted")
}

// TestEnsureReverseExportLink_DetectsDegradedOnlyOnce covers the journal-spam
// guard: a periodic reconcile tick re-detecting the same already-recorded
// degraded condition must keep failing (so Reconcile keeps logging), but must
// not re-journal or re-persist the same state on every tick.
func TestEnsureReverseExportLink_DetectsDegradedOnlyOnce(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseExporting})
	tc.src.subExists = true
	tc.tgt.slotReady = false

	require.Error(t, tc.c.ensureReverseExportLink(context.Background(), m))
	require.Error(t, tc.c.ensureReverseExportLink(context.Background(), m))

	events := tc.store.journalEvents(m.ID)
	count := 0
	for _, e := range events {
		if e == JournalEventReverseLinkDegraded {
			count++
		}
	}
	require.Equal(t, 1, count, "the detection must be journaled once, not on every re-check")
}

// TestEnsureReverseExportLink_UnquiescesBeforeAttaching covers the data-loss
// race this function closes: the source must stay cluster-wide read-only
// until the instant the reverse subscription actually attaches, and any
// client that reconnected during that window must be cut again right before
// the attach, not left to race it. This only applies to the healthy
// first-attach path (a ready slot with no subscription yet) — a degraded slot
// is reported, not attached to (see TestEnsureReverseExportLink_Detects*).
func TestEnsureReverseExportLink_UnquiescesBeforeAttaching(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseExporting})
	tc.src.subExists = false
	tc.tgt.slotReady = true

	require.NoError(t, tc.c.ensureReverseExportLink(context.Background(), m))

	unquiesce := logIndex(tc.log, "source.SetReadOnly(false)")
	terminate := logIndex(tc.log, "source.TerminateClientBackends")
	attach := logIndex(tc.log, "source.CreateSubscription")
	require.NotEqual(t, -1, unquiesce, "the cluster-wide barrier must lift once the reverse subscription is about to attach")
	require.NotEqual(t, -1, terminate, "a client that reconnected during the wait must be cut again before the attach")
	require.Less(t, unquiesce, attach, "the barrier must lift before, not after, the subscription attaches")
	require.Less(t, terminate, attach, "the final cut must happen before the subscription attaches")
}

// TestReseedReverseLink_RepairsDegradedLink covers the explicit,
// operator-triggered repair ensureReverseExportLink's detection defers to: a
// stale subscription must be dropped, the slot recreated fresh, and the
// subscription resubscribed with a full copy (copy_data=true), since there is
// no safe way to resume into a slot that lost its position in the failover.
func TestReseedReverseLink_RepairsDegradedLink(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseExporting, ReverseLinkError: "reverse export subscription exists but its slot is missing or unusable"})
	tc.src.subExists = true
	tc.tgt.slotReady = false

	_, err := tc.c.ReseedReverseLink(context.Background(), Ref{ID: m.ID})
	require.NoError(t, err)

	require.Contains(t, tc.log, "source.DropSubscription", "the stale subscription must be dropped before reseeding")
	require.Contains(t, tc.log, "target.DropLogicalSlot", "the unusable slot must be dropped before recreating")
	require.Contains(t, tc.log, "target.CreateLogicalSlot", "the slot must be recreated fresh")
	require.Contains(t, tc.log, "source.CreateSubscription", "the subscription must be recreated")

	require.Empty(t, tc.store.migs[m.ID].ReverseLinkError, "a successful reseed must clear the degraded marker")
	events := tc.store.journalEvents(m.ID)
	require.Contains(t, events, JournalEventReseed, "the repair must be recorded for audit visibility")
}

// TestReseedReverseLink_RecreatesWhenNothingExists covers the case where no
// subscription ever attached — same recreate path, minus the drop step since
// there is nothing stale to tear down first.
func TestReseedReverseLink_RecreatesWhenNothingExists(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseExporting})
	tc.src.subExists = false
	tc.tgt.slotReady = false

	_, err := tc.c.ReseedReverseLink(context.Background(), Ref{ID: m.ID})
	require.NoError(t, err)

	require.NotContains(t, tc.log, "source.DropSubscription", "nothing to drop when the subscription never existed")
	require.Contains(t, tc.log, "target.CreateLogicalSlot")
	require.Contains(t, tc.log, "source.CreateSubscription")
}

// TestReseedReverseLink_AlreadyHealthyIsNoOp covers calling the repair
// speculatively (an operator need not have perfectly diagnosed the state
// first): if the link is already healthy, nothing destructive happens, and a
// stale degraded marker (e.g. left over from a transient condition that has
// since resolved itself) is still cleared.
func TestReseedReverseLink_AlreadyHealthyIsNoOp(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseExporting, ReverseLinkError: "stale"})
	tc.src.subExists = true
	tc.tgt.slotReady = true

	_, err := tc.c.ReseedReverseLink(context.Background(), Ref{ID: m.ID})
	require.NoError(t, err)

	require.NotContains(t, tc.log, "source.DropSubscription")
	require.NotContains(t, tc.log, "target.DropLogicalSlot")
	require.NotContains(t, tc.log, "target.CreateLogicalSlot")
	require.NotContains(t, tc.log, "source.CreateSubscription")
	require.Empty(t, tc.store.migs[m.ID].ReverseLinkError, "a stale marker must still be cleared")
}

// TestReseedReverseLink_RejectsNonExportingPhase covers the guard: this is a
// narrow repair for the EXPORT reverse link specifically, not a general-purpose
// action, so it must refuse to run against a migration in any other phase.
func TestReseedReverseLink_RejectsNonExportingPhase(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseImporting})

	_, err := tc.c.ReseedReverseLink(context.Background(), Ref{ID: m.ID})
	require.ErrorContains(t, err, "only valid for an EXPORTING migration")
}

// TestActivate_SwitchToExportKeepsSourceReadOnly covers the corrected fix for
// the same race: switchTo's own DropPublication (a catalog change on its own
// connection) needs only a session-local read-only override, not a full lift
// of the cluster-wide barrier — the barrier itself must stay up until
// ensureReverseExportLink's reverse subscription attaches, well after the
// switch commits.
func TestActivate_SwitchToExportKeepsSourceReadOnly(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = time.Now
	m := tc.seed(&Migration{Phase: PhaseImporting})
	tc.src.lagPresent = true
	tc.src.lag = 0
	tc.tgt.subExists = true

	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: m.ID}, DirectionExport, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
	require.NoError(t, err)

	sessionUnlock := logIndex(tc.log, "source.setSessionReadOnly(false)")
	dropPub := logIndex(tc.log, "source.DropPublication")
	clusterUnlock := logIndex(tc.log, "source.SetReadOnly(false)")
	attach := logIndex(tc.log, "source.CreateSubscription")
	require.NotEqual(t, -1, sessionUnlock, "switchTo must use the session-local override for its own DropPublication")
	require.NotEqual(t, -1, clusterUnlock, "the cluster-wide barrier must eventually lift once the reverse subscription attaches")
	require.Less(t, sessionUnlock, dropPub, "the session override must be in place before the switch's own DDL runs")
	require.Greater(t, clusterUnlock, dropPub, "the cluster-wide barrier must still be up when the switch commits its own catalog change")
	require.Less(t, clusterUnlock, attach, "the cluster-wide barrier must lift before the reverse subscription attaches, not after")
}

func TestIsRetryableUnavailable(t *testing.T) {
	require.False(t, isRetryableUnavailable(nil))
	require.True(t, isRetryableUnavailable(errors.New("server temporarily unavailable")))
	require.True(t, isRetryableUnavailable(errors.New("SQLSTATE 57P03")))
	require.True(t, isRetryableUnavailable(errors.New("08006 connection failure")))
	require.False(t, isRetryableUnavailable(errors.New("relation does not exist")))
}

func TestCreateConnection_ValidatesAndAssignsID(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = func() time.Time { return time.Unix(1_800_000_000, 0) }

	require.ErrorContains(t, tc.c.CreateConnection(context.Background(), &Connection{DSN: "host=x"}), "name is required")
	require.ErrorContains(t, tc.c.CreateConnection(context.Background(), &Connection{Name: "src2"}), "dsn is required")

	conn := &Connection{Name: "src2", DSN: "host=src2 dbname=app"}
	require.NoError(t, tc.c.CreateConnection(context.Background(), conn))
	require.NotZero(t, conn.ID, "CreateConnection must assign an id, not accept one from the caller")

	stored, err := tc.c.GetConnection(context.Background(), Ref{ID: conn.ID})
	require.NoError(t, err)
	require.Equal(t, conn.DSN, stored.DSN)
}

func TestGetConnection_NotFound(t *testing.T) {
	tc := newTestCoord(t)
	_, err := tc.c.GetConnection(context.Background(), Ref{ID: 999})
	require.ErrorIs(t, err, ErrConnectionNotFound)
}

func TestListConnections(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = func() time.Time { return time.Unix(1_800_000_000, 0) }
	require.NoError(t, tc.c.CreateConnection(context.Background(), &Connection{Name: "extra", DSN: "host=extra"}))

	conns, err := tc.c.ListConnections(context.Background())
	require.NoError(t, err)
	names := make([]string, len(conns))
	for i, c := range conns {
		names[i] = c.Name
	}
	// newTestCoord already seeds the default "src" connection (defaultConnID).
	require.ElementsMatch(t, []string{"src", "extra"}, names)
}

func TestDropConnection(t *testing.T) {
	tc := newTestCoord(t)
	require.NoError(t, tc.c.DropConnection(context.Background(), defaultConnID))
	_, err := tc.c.GetConnection(context.Background(), Ref{ID: defaultConnID})
	require.ErrorIs(t, err, ErrConnectionNotFound)
}

func TestListMigrations(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseImporting})

	projs, err := tc.c.ListMigrations(context.Background())
	require.NoError(t, err)
	require.Len(t, projs, 1)
	require.Equal(t, testMigID, projs[0].ID)
}

func TestListMigrations_PropagatesStoreError(t *testing.T) {
	tc := newTestCoord(t)
	tc.store.listErr = errors.New("connection lost")
	_, err := tc.c.ListMigrations(context.Background())
	require.ErrorContains(t, err, "connection lost")
}

func TestEnsureSchema(t *testing.T) {
	tc := newTestCoord(t)
	require.NoError(t, tc.c.EnsureSchema(context.Background()))

	tc.store.ensureErr = errors.New("schema creation failed")
	require.ErrorContains(t, tc.c.EnsureSchema(context.Background()), "schema creation failed")
}
