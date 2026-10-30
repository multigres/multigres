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

package queryserving

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

// An autocommit statement whose backend dies while it runs must not be sent
// again on a fresh backend: PostgreSQL may already have applied its effects.
// nextval makes a replay visible, since a sequence value is consumed even when
// the statement fails. Like a client connected directly to PostgreSQL, the
// client gets an error and the sequence advances once.
func TestAutocommitStatementNotReplayedAfterBackendLoss(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 2*time.Minute)

	for _, m := range []struct {
		name string
		mode pgx.QueryExecMode
	}{
		{"simple", pgx.QueryExecModeSimpleProtocol},
		{"exec_unnamed", pgx.QueryExecModeExec},
	} {
		t.Run(m.name, func(t *testing.T) {
			seq := "replay_seq_" + m.name
			admin := openReservationLossConn(t, ctx, setup, pgx.QueryExecModeSimpleProtocol)
			utils.RequirePgxExec(t, ctx, admin, "CREATE SEQUENCE "+seq)
			t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SEQUENCE IF EXISTS "+seq) })

			a := openReservationLossConn(t, ctx, setup, m.mode)
			stmt := fmt.Sprintf("SELECT nextval('%s'), pg_sleep(5)", seq)
			done := make(chan error, 1)
			start := time.Now()
			go func() { done <- utils.PgxExec(ctx, a, stmt) }()

			// Terminate the backend running the statement once it shows up.
			var pid int
			require.Eventually(t, func() bool {
				err := admin.QueryRow(ctx,
					"SELECT pid FROM pg_stat_activity WHERE query = $1 AND state = 'active'", stmt).Scan(&pid)
				return err == nil
			}, 10*time.Second, 50*time.Millisecond, "statement never became active")
			var terminated bool
			require.NoError(t, admin.QueryRow(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&terminated))
			require.True(t, terminated)

			err := <-done
			require.Error(t, err, "the statement's backend was terminated; it must not succeed on another")
			require.Less(t, time.Since(start), 5*time.Second, "the statement must not run to completion a second time")

			var lastValue int64
			require.NoError(t, admin.QueryRow(ctx, "SELECT last_value FROM "+seq).Scan(&lastValue))
			require.Equal(t, int64(1), lastValue, "the sequence must be consumed once")
		})
	}
}
