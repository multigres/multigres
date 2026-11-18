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

func TestEnsureMigrationSchema_FailureLogsOncePerChange(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
	var logs syncBuffer
	pm.logger = slog.New(slog.NewTextHandler(&logs, nil))
	const failed, recovered = "ensure migration schema failed", "ensure migration schema succeeded again"

	for range 3 {
		qs.AddQueryPatternOnceWithError(connectionsProbePattern, errors.New("probe unavailable"))
	}
	qs.AddQueryPatternOnceWithError(connectionsProbePattern, errors.New("probe still unavailable"))
	qs.AddQueryPatternOnce(connectionsProbePattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{true}}))
	addShardRefEnsure(qs, nil)
	qs.AddQueryPattern(".*", mock.MakeQueryResult(nil, nil))

	ensure := func(n int) {
		for range n {
			pm.ensureMigrationSchema(context.Background(), pm.migrationCoordinator())
		}
	}

	// The same failure repeated logs once; changed text logs again.
	ensure(3)
	assert.Equal(t, 1, countLogLines(&logs, failed, "level=WARN"))
	ensure(1)
	assert.Equal(t, 2, countLogLines(&logs, failed))
	assert.Zero(t, countLogLines(&logs, recovered))

	// Recovery logs once at info.
	ensure(1)
	assert.Equal(t, 1, countLogLines(&logs, recovered, "level=INFO"))
	pm.mu.Lock()
	assert.True(t, pm.migrationSchemaEnsured)
	pm.mu.Unlock()
	ensure(2)
	assert.Equal(t, 1, countLogLines(&logs, recovered), "an ensured schema does not run again")
}

func TestForgetMigrationState_ResetsEnsureSchemaFailureLog(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
	var logs syncBuffer
	pm.logger = slog.New(slog.NewTextHandler(&logs, nil))
	qs.AddQueryPatternWithError(connectionsProbePattern, errors.New("probe unavailable"))

	for range 3 {
		pm.ensureMigrationSchema(context.Background(), pm.migrationCoordinator())
	}
	require.Equal(t, 1, countLogLines(&logs, "ensure migration schema failed"))

	pm.forgetMigrationState()
	for range 3 {
		pm.ensureMigrationSchema(context.Background(), pm.migrationCoordinator())
	}
	assert.Equal(t, 2, countLogLines(&logs, "ensure migration schema failed"), "a new primary tenure logs the standing failure afresh")
}

func TestRunMigrationReconcile_RepeatedFailuresLogOnce(t *testing.T) {
	for _, tt := range []struct {
		name       string
		probeFails bool
		failedMsg  string
	}{
		{name: "recovery probe", probeFails: true, failedMsg: "failed to check recovery status for migration reconcile"},
		{name: "reconcile", failedMsg: "migration reconcile failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pm, qs := newTestManagerWithMock(t, "default", "0-inf")
				t.Cleanup(func() { require.NoError(t, pm.topoClient.Close()) })
				var logs syncBuffer
				pm.logger = slog.New(slog.NewTextHandler(&logs, nil))
				pm.migrationSchemaEnsured = true
				if tt.probeFails {
					qs.AddQueryPatternWithError("SELECT pg_is_in_recovery", errors.New("recovery probe unavailable"))
				} else {
					qs.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{false}}))
					qs.AddQueryPatternWithError("FROM multigres.migration", errors.New("migration load unavailable"))
				}
				ctx, cancel := context.WithCancel(t.Context())
				go pm.runMigrationReconcile(ctx, 10*time.Millisecond)
				time.Sleep(55 * time.Millisecond)
				cancel()
				synctest.Wait()

				assert.Equal(t, 1, countLogLines(&logs, tt.failedMsg), "five failing ticks log once")
			})
		})
	}
}
