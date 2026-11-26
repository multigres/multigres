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

package manager

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
)

func countLogLines(logs *syncBuffer, substrs ...string) int {
	n := 0
	for line := range strings.SplitSeq(logs.String(), "\n") {
		match := true
		for _, sub := range substrs {
			match = match && strings.Contains(line, sub)
		}
		if match {
			n++
		}
	}
	return n
}

func newReconcileLogger() (*migrationReconcileLog, *slog.Logger, *syncBuffer) {
	var logs syncBuffer
	return &migrationReconcileLog{}, slog.New(slog.NewTextHandler(&logs, nil)), &logs
}

func TestMigrationReconcileLog_PassErrors(t *testing.T) {
	ctx := context.Background()
	l, logger, logs := newReconcileLogger()
	const failed, recovered = "migration reconcile pass failed", "migration reconcile pass succeeded again"

	for range 4 {
		l.report(ctx, logger, nil, errors.New("boom"))
	}
	assert.Equal(t, 1, countLogLines(logs, failed, "level=WARN", "error=boom"), "a repeated failure logs once")

	l.report(ctx, logger, nil, errors.New("different"))
	assert.Equal(t, 2, countLogLines(logs, failed), "changed error text logs again")

	l.report(ctx, logger, nil, nil)
	l.report(ctx, logger, nil, nil)
	assert.Equal(t, 1, countLogLines(logs, recovered, "level=INFO"), "recovery logs once")

	l.report(ctx, logger, nil, errors.New("different"))
	assert.Equal(t, 3, countLogLines(logs, failed), "a failure after recovery logs again")
}

func TestMigrationReconcileLog_PerMigrationResults(t *testing.T) {
	ctx := context.Background()
	l, logger, logs := newReconcileLogger()
	const failed, recovered = "reconcile migration failed", "reconcile migration succeeded again"
	fail := func(phase migration.Phase, msg string) migration.ReconcileResult {
		return migration.ReconcileResult{Phase: phase, Err: errors.New(msg)}
	}
	ok := migration.ReconcileResult{Phase: migration.PhaseCopying}

	for range 4 {
		l.report(ctx, logger, map[int64]migration.ReconcileResult{1: fail(migration.PhaseCopying, "stuck"), 2: ok}, nil)
	}
	assert.Equal(t, 1, countLogLines(logs, failed), "a repeated failure logs once, and a healthy migration logs nothing")
	assert.Equal(t, 1, countLogLines(logs, failed, "migration=1", "phase=COPYING", "error=stuck", "level=WARN"))

	// The same error text in another phase is a new failure.
	l.report(ctx, logger, map[int64]migration.ReconcileResult{1: fail(migration.PhaseExporting, "stuck"), 2: ok}, nil)
	assert.Equal(t, 2, countLogLines(logs, failed))
	assert.Equal(t, 1, countLogLines(logs, failed, "phase=EXPORTING"))

	// Changed error text is a new failure, and a second migration is tracked on its own.
	l.report(ctx, logger, map[int64]migration.ReconcileResult{
		1: fail(migration.PhaseExporting, "still stuck"),
		2: fail(migration.PhaseCopying, "stuck"),
	}, nil)
	assert.Equal(t, 4, countLogLines(logs, failed))

	// A failed listing keeps the history.
	l.report(ctx, logger, nil, errors.New("list unavailable"))
	l.report(ctx, logger, map[int64]migration.ReconcileResult{
		1: fail(migration.PhaseExporting, "still stuck"),
		2: fail(migration.PhaseCopying, "stuck"),
	}, nil)
	assert.Equal(t, 4, countLogLines(logs, failed))

	// Recovery logs once, a migration that disappears logs nothing, and a later failure logs again.
	l.report(ctx, logger, map[int64]migration.ReconcileResult{1: {Phase: migration.PhaseImporting}}, nil)
	assert.Equal(t, 1, countLogLines(logs, recovered, "migration=1", "level=INFO"))
	assert.Zero(t, countLogLines(logs, recovered, "migration=2"), "disappearing is not recovering")
	l.report(ctx, logger, map[int64]migration.ReconcileResult{2: fail(migration.PhaseCopying, "stuck")}, nil)
	assert.Equal(t, 5, countLogLines(logs, failed), "a reappearing migration logs afresh")
}

func TestMigrationReconcileLog_SkippedTickKeepsHistory(t *testing.T) {
	ctx := context.Background()
	l, logger, logs := newReconcileLogger()
	l.report(ctx, logger, nil, errors.New("boom"))

	l.report(ctx, logger, nil, errMigrationCoordinatorNotReady)
	l.report(ctx, logger, nil, errors.New("boom"))
	assert.Equal(t, 1, countLogLines(logs, "migration reconcile pass failed"))
	assert.Zero(t, countLogLines(logs, "succeeded again"))
}

func TestMigrationReconcileLog_ResetStartsAfresh(t *testing.T) {
	ctx := context.Background()
	fail := map[int64]migration.ReconcileResult{1: {Phase: migration.PhaseCopying, Err: errors.New("stuck")}}
	for name, reset := range map[string]func(l *migrationReconcileLog, logger *slog.Logger){
		"role mismatch": func(l *migrationReconcileLog, logger *slog.Logger) {
			l.report(ctx, logger, nil, &roleMismatchError{message: "standby"})
		},
		"promotion": func(l *migrationReconcileLog, _ *slog.Logger) { l.reset() },
	} {
		t.Run(name, func(t *testing.T) {
			l, logger, logs := newReconcileLogger()
			l.report(ctx, logger, fail, errors.New("boom"))
			reset(l, logger)
			l.report(ctx, logger, fail, errors.New("boom"))
			assert.Equal(t, 2, countLogLines(logs, "migration reconcile pass failed"))
			assert.Equal(t, 2, countLogLines(logs, "reconcile migration failed"))
			assert.Zero(t, countLogLines(logs, "succeeded again"), "a reset is not a recovery")
		})
	}
}

func TestForgetMigrationState_ResetsReconcileLog(t *testing.T) {
	ctx := context.Background()
	pm, _ := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
	var logs syncBuffer
	pm.logger = slog.New(slog.NewTextHandler(&logs, nil))

	pm.migrationReconcileLog.report(ctx, pm.logger, nil, errors.New("boom"))
	pm.forgetMigrationState()
	pm.migrationReconcileLog.report(ctx, pm.logger, nil, errors.New("boom"))
	assert.Equal(t, 2, countLogLines(&logs, "migration reconcile pass failed"))
}

func TestEnsureMigrationSchema_ReturnsFailureWithoutLogging(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
	var logs syncBuffer
	pm.logger = slog.New(slog.NewTextHandler(&logs, nil))
	qs.AddQueryPatternWithError(connectionsProbePattern, errors.New("probe unavailable"))
	addShardRefEnsure(qs, nil)
	qs.AddQueryPattern(".*", mock.MakeQueryResult(nil, nil))

	for range 3 {
		err := pm.ensureMigrationSchema(context.Background(), pm.migrationCoordinator())
		require.ErrorContains(t, err, "probe unavailable")
		assert.ErrorIs(t, err, migration.ErrSchemaPrerequisite)
	}
	assert.Empty(t, logs.String())
	pm.mu.Lock()
	assert.False(t, pm.migrationSchemaEnsured, "a failed ensure is retried")
	pm.mu.Unlock()
}

func TestRunMigrationReconcileOnce(t *testing.T) {
	primary := func(qs *mock.QueryService) {
		qs.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{false}}))
	}

	t.Run("standby is a role mismatch", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		qs.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))
		results, err := pm.runMigrationReconcileOnce(t.Context())
		assert.True(t, isRoleMismatch(err))
		assert.Nil(t, results)
	})

	t.Run("prerequisite schema failure ends the tick", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		primary(qs)
		qs.AddQueryPatternWithError(connectionsProbePattern, errors.New("probe unavailable"))
		addShardRefEnsure(qs, nil)
		qs.AddQueryPattern(".*", mock.MakeQueryResult(nil, nil))
		results, err := pm.runMigrationReconcileOnce(t.Context())
		assert.ErrorIs(t, err, migration.ErrSchemaPrerequisite)
		assert.Nil(t, results, "migrations are not reconciled without their tables")
	})

	t.Run("other schema failure is reported and migrations still reconcile", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		primary(qs)
		qs.AddQueryPattern(connectionsProbePattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{true}}))
		qs.AddQueryPatternWithError("CREATE OR REPLACE VIEW", errors.New("view unavailable"))
		qs.AddQueryPatternWithError("FROM multigres.migration", errors.New("migration load unavailable"))
		addShardRefEnsure(qs, nil)
		qs.AddQueryPattern(".*", mock.MakeQueryResult(nil, nil))
		_, err := pm.runMigrationReconcileOnce(t.Context())
		require.Error(t, err)
		assert.NotErrorIs(t, err, migration.ErrSchemaPrerequisite)
		assert.ErrorContains(t, err, "view unavailable")
		assert.ErrorContains(t, err, "migration load unavailable", "the reconcile still ran")
	})
}

func TestRunMigrationReconcile_RepeatedFailureLogsOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, "default", "0-inf")
		t.Cleanup(func() { require.NoError(t, pm.topoClient.Close()) })
		var logs syncBuffer
		pm.logger = slog.New(slog.NewTextHandler(&logs, nil))
		pm.migrationSchemaEnsured = true
		qs.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{false}}))
		qs.AddQueryPatternWithError("FROM multigres.migration", errors.New("migration load unavailable"))
		ctx, cancel := context.WithCancel(t.Context())
		go pm.runMigrationReconcile(ctx, 10*time.Millisecond)
		time.Sleep(55 * time.Millisecond)
		cancel()
		synctest.Wait()

		assert.Equal(t, 1, countLogLines(&logs, "migration reconcile pass failed"), "five failing ticks log once")
	})
}
