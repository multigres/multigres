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
	require.ErrorContains(t, err, "source DSN is required")

	_, err = tc.c.CreateMigration(ctx, CreateParams{SourceDSN: "x", Tables: []string{"t"}})
	require.ErrorContains(t, err, "target database is required")

	_, err = tc.c.CreateMigration(ctx, CreateParams{SourceDSN: "x", TargetDatabase: "app"})
	require.ErrorContains(t, err, "at least one table is required")
}

func TestCreateMigration_SourceUnreachable(t *testing.T) {
	tc := newTestCoord(t)
	tc.srcErr = errors.New("connection refused")
	_, err := tc.c.CreateMigration(context.Background(), CreateParams{
		SourceDSN: "host=src dbname=app", TargetDatabase: "app", Tables: []string{"public.orders"},
	})
	require.ErrorContains(t, err, "connection refused")
}

func TestCreateMigration_ValidateError(t *testing.T) {
	tc := newTestCoord(t)
	tc.src.validateErr = errors.New("wal_level is replica")
	_, err := tc.c.CreateMigration(context.Background(), CreateParams{
		SourceDSN: "host=src dbname=app", TargetDatabase: "app", Tables: []string{"public.orders"},
	})
	require.ErrorContains(t, err, "wal_level is replica")
}

func TestCreateMigration_DuplicateName(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{ID: 99, Name: "nightly", Phase: PhaseImporting})
	_, err := tc.c.CreateMigration(context.Background(), CreateParams{
		SourceDSN: "host=src dbname=app", TargetDatabase: "app", Name: "nightly", Tables: []string{"public.orders"},
	})
	require.ErrorContains(t, err, "already exists")
}

func TestCreateMigration_InsertError(t *testing.T) {
	tc := newTestCoord(t)
	tc.store.insertErr = errors.New("disk full")
	_, err := tc.c.CreateMigration(context.Background(), CreateParams{
		SourceDSN: "host=src dbname=app", TargetDatabase: "app", Tables: []string{"public.orders"},
	})
	require.ErrorContains(t, err, "disk full")
}

func TestCreateMigration_Success(t *testing.T) {
	tc := newTestCoord(t)
	tc.src.resolved = []string{"public.orders"}
	tc.src.validateInfo = &SourceInfo{ServerVersionNum: 170000, CanCreateSubscription: false} // warns, still ok
	proj, err := tc.c.CreateMigration(context.Background(), CreateParams{
		SourceDSN: "host=src dbname=app", TargetDatabase: "app", Tables: []string{"public.orders"}, CopyData: true,
	})
	require.NoError(t, err)
	require.Equal(t, PhaseCreated, proj.Phase)
	require.Equal(t, []string{"public.orders"}, proj.Tables)
	require.Len(t, tc.store.migs, 1)
	require.Equal(t, 1, tc.src.closed, "the validation source connection must be closed")
}

func TestStartMigration_WrongDirection(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseExporting})
	_, err := tc.c.StartMigration(context.Background(), Ref{ID: testMigID})
	require.ErrorContains(t, err, "IMPORT direction")
}

func TestStartMigration_RunSetupErrorMarksFailed(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCreated})
	tc.tgt.createSubErr = errors.New("subscription blew up")

	_, err := tc.c.StartMigration(context.Background(), Ref{ID: testMigID})
	require.ErrorContains(t, err, "subscription blew up")
	require.Equal(t, PhaseFailed, tc.store.migs[testMigID].Phase, "a runSetup failure must record FAILED")
	require.Contains(t, tc.store.migs[testMigID].LastError, "subscription blew up")
}

func TestStartMigration_SuccessReachesImporting(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCreated})
	tc.tgt.status = &SubscriptionStatus{TotalRelations: 1, ReadyRelations: 1} // caught up

	proj, err := tc.c.StartMigration(context.Background(), Ref{ID: testMigID})
	require.NoError(t, err)
	require.Equal(t, PhaseImporting, proj.Phase, "a caught-up subscription must advance COPYING -> IMPORTING")
	// runSetup must have created the publication (source) and subscription (target).
	require.Contains(t, tc.log, "source.CreatePublication")
	require.Contains(t, tc.log, "target.CreateSubscription")
	require.Contains(t, tc.log, "target.DropTables") // schema copy path (not skipped)
}

func TestStartMigration_SkipSchemaCopy(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCreated, SkipSchemaCopy: true})
	tc.tgt.status = &SubscriptionStatus{TotalRelations: 1, ReadyRelations: 1}

	_, err := tc.c.StartMigration(context.Background(), Ref{ID: testMigID})
	require.NoError(t, err)
	require.NotContains(t, tc.log, "target.DropTables", "SkipSchemaCopy must bypass the drop/apply schema step")
	require.NotContains(t, tc.log, "target.ApplySchema")
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
	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, Direction("SIDEWAYS"))
	require.ErrorContains(t, err, "invalid direction")
}

func TestSetDirection_NoOp(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseImporting})
	proj, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionImport)
	require.NoError(t, err)
	require.Equal(t, PhaseImporting, proj.Phase)
	require.Equal(t, 0, tc.store.updates, "a no-op direction set must not write")
}

func TestSetDirection_ConflictingSwitchInProgress(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseSwitchingToExport})
	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionImport)
	require.ErrorContains(t, err, "already in progress")
}

func TestSetDirection_ExportNotConfigured(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.targetConnInfo = nil
	tc.seed(&Migration{Phase: PhaseImporting})
	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionExport)
	require.ErrorContains(t, err, "EXPORT direction is not configured")
}

func TestSetDirection_ExportSourceCannotSubscribe(t *testing.T) {
	tc := newTestCoord(t)
	tc.src.info = &SourceInfo{ServerVersionNum: 150000, CanCreateSubscription: false}
	tc.seed(&Migration{Phase: PhaseImporting})
	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionExport)
	require.ErrorContains(t, err, "cannot switch to EXPORT")
}

func TestSetDirection_NotStreaming(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCopying})
	tc.tgt.status = &SubscriptionStatus{TotalRelations: 1, ReadyRelations: 0}
	_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionExport)
	require.ErrorContains(t, err, "must be caught up")
}

func TestActivate_ImportToExportFullSwitch(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = time.Now // readiness gate needs a real advancing clock
	m := tc.seed(&Migration{Phase: PhaseImporting})
	tc.src.lagPresent = true
	tc.src.lag = 0          // ready immediately
	tc.tgt.subExists = true // the import subscription is live (drives currentLinkLive + drain)

	proj, err := tc.c.Activate(context.Background(), Ref{ID: m.ID}, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
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

	_, err := tc.c.Activate(context.Background(), Ref{ID: m.ID}, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
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

func TestActivate_HardQuiesceWithoutRolesStillTerminates(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = time.Now
	m := tc.seed(&Migration{Phase: PhaseImporting}) // no QuiesceRoles
	tc.src.lagPresent = true
	tc.src.lag = 0
	tc.tgt.subExists = true

	_, err := tc.c.Activate(context.Background(), Ref{ID: m.ID}, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
	require.NoError(t, err)
	require.Contains(t, tc.log, "source.TerminateClientBackends", "terminate is unconditional at the ACTIVATE barrier")
	require.NotContains(t, tc.log, "source.RevokeConnect", "no roles means no CONNECT fence")
}

func TestActivate_TerminateErrorAbortsSwitch(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = time.Now
	m := tc.seed(&Migration{Phase: PhaseImporting})
	tc.src.lagPresent = true
	tc.src.lag = 0
	tc.tgt.subExists = true
	tc.src.terminateErr = errors.New("cannot signal backends")

	_, err := tc.c.Activate(context.Background(), Ref{ID: m.ID}, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
	require.ErrorContains(t, err, "cannot signal backends")
	require.NotContains(t, tc.log, "source.DropPublication", "a failed write-cut must not proceed into the switch")
}

func TestActivate_AlreadyActive(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseExporting})
	_, err := tc.c.Activate(context.Background(), Ref{ID: testMigID}, ActivateOptions{})
	require.ErrorContains(t, err, "already active")
}

func TestActivate_NotReadyTimeout(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = time.Now
	tc.seed(&Migration{Phase: PhaseImporting})
	tc.src.lagPresent = false // slot not found -> never ready
	_, err := tc.c.Activate(context.Background(), Ref{ID: testMigID}, ActivateOptions{MaxLagBytes: 1, WaitTimeout: 20 * time.Millisecond})
	require.ErrorIs(t, err, ErrNotReady)
}

func TestDeactivate_ExportToImportFullSwitch(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseExporting})
	tc.src.subExists = true // export subscription live on the source (currentLinkLive + reverse-link no-op)

	proj, err := tc.c.Deactivate(context.Background(), Ref{ID: m.ID})
	require.NoError(t, err)
	require.Equal(t, PhaseImporting, proj.Phase)
	require.Contains(t, tc.log, "source.CreatePublication", "IMPORT makes the source the publisher again")
	require.Contains(t, tc.log, "target.DropLogicalSlot", "the reverse slot is dropped on the target")
	require.Contains(t, tc.log, "target.CreateSubscription", "the forward subscription is re-established on the target")
}

func TestDeactivate_DrainsTargetAndRestoresConnect(t *testing.T) {
	tc := newTestCoord(t)
	drained := 0
	tc.c.drainForImport = func(context.Context) error { drained++; return nil }
	m := tc.seed(&Migration{Phase: PhaseExporting, QuiesceRoles: []string{"app"}})
	tc.src.subExists = true

	proj, err := tc.c.Deactivate(context.Background(), Ref{ID: m.ID})
	require.NoError(t, err)
	require.Equal(t, PhaseImporting, proj.Phase)
	require.Equal(t, 1, drained, "deactivate must drain the pooler to non-serving synchronously before the target becomes a subscriber")
	require.Contains(t, tc.log, "source.GrantConnect", "deactivate restores app CONNECT on the source-as-publisher")
}

func TestDeactivate_DrainForImportErrorAborts(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.drainForImport = func(context.Context) error { return errors.New("still serving") }
	m := tc.seed(&Migration{Phase: PhaseExporting})
	tc.src.subExists = true

	_, err := tc.c.Deactivate(context.Background(), Ref{ID: m.ID})
	require.ErrorContains(t, err, "still serving")
	require.NotContains(t, tc.log, "source.CreatePublication", "a failed non-serving drain must not proceed into the switch")
}

func TestActivate_ExportGrantsBackOnTeardown(t *testing.T) {
	tc := newTestCoord(t)
	// A fenced EXPORT migration torn down (COMPLETING resume) must restore CONNECT.
	tc.seed(&Migration{Phase: PhaseCompleting, Direction: DirectionExport, QuiesceRoles: []string{"app"}})
	require.NoError(t, tc.c.Reconcile(context.Background()))
	require.Contains(t, tc.log, "source.GrantConnect", "EXPORT teardown restores app CONNECT on the standalone source")
}

func TestDeactivate_NotActive(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseImporting})
	_, err := tc.c.Deactivate(context.Background(), Ref{ID: testMigID})
	require.ErrorContains(t, err, "not active")
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
	require.NoError(t, tc.c.Reconcile(context.Background()))
	require.Contains(t, tc.log, "source.CreateSubscription")
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
	require.NoError(t, tc.c.ensureReverseExportLink(context.Background(), m))
	require.NotContains(t, tc.log, "source.CreateSubscription", "an existing reverse link must not be recreated")
}

func TestEnsureReverseExportLink_TargetConnInfoError(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.targetConnInfo = func(string) (string, error) { return "", errors.New("no advertise host") }
	m := tc.seed(&Migration{Phase: PhaseExporting})
	err := tc.c.ensureReverseExportLink(context.Background(), m)
	require.ErrorContains(t, err, "no advertise host")
}

func TestUpdateMigration_TablesFrozenAfterStart(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseImporting})
	tables := []string{"public.orders"}
	_, err := tc.c.UpdateMigration(context.Background(), Ref{ID: testMigID}, UpdateParams{Tables: &tables})
	require.ErrorContains(t, err, "CREATED")
}

func TestUpdateMigration_SourceDatabaseChangeRejected(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCreated, SourceDSN: "host=src dbname=app"})
	newDSN := "host=src dbname=other"
	_, err := tc.c.UpdateMigration(context.Background(), Ref{ID: testMigID}, UpdateParams{SourceDSN: &newDSN})
	require.ErrorContains(t, err, "source database change is not allowed")
}

func TestIsRetryableUnavailable(t *testing.T) {
	require.False(t, isRetryableUnavailable(nil))
	require.True(t, isRetryableUnavailable(errors.New("server temporarily unavailable")))
	require.True(t, isRetryableUnavailable(errors.New("SQLSTATE 57P03")))
	require.True(t, isRetryableUnavailable(errors.New("08006 connection failure")))
	require.False(t, isRetryableUnavailable(errors.New("relation does not exist")))
}
