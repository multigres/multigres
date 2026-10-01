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

package minigressetup

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
	"github.com/multigres/multigres/go/test/utils"
)

var setupManager = clustersetup.NewSharedSetupManager(func(t *testing.T) *Setup {
	return New(t)
})

func TestMain(m *testing.M) {
	exitCode := clustersetup.RunTestMain(m)
	if exitCode != 0 {
		setupManager.DumpLogs()
	}
	setupManager.Cleanup()
	os.Exit(exitCode) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}

func skipIfShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("Skipping end-to-end test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries)")
	}
}

// TestMinigres_ServesQueries checks the harness end to end: the shared Minigres
// cluster is in its clean state, and both comparison targets answer, the proxy
// target through the minigres process.
func TestMinigres_ServesQueries(t *testing.T) {
	skipIfShort(t)
	setup := setupManager.Get(t)
	setup.SetupTest(t)

	targets := setup.ComparisonTargets(t)
	require.Len(t, targets, 2)
	for _, target := range targets {
		t.Run(target.Name, func(t *testing.T) {
			db, err := sql.Open("postgres", clustersetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5"))
			require.NoError(t, err)
			defer db.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var one int
			require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
			assert.Equal(t, 1, one)

			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, "CREATE TEMP TABLE harness_check (x int)")
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, "INSERT INTO harness_check VALUES (1), (2)")
			require.NoError(t, err)
			var sum int
			require.NoError(t, tx.QueryRowContext(ctx, "SELECT sum(x) FROM harness_check").Scan(&sum))
			assert.Equal(t, 3, sum)
			require.NoError(t, tx.Rollback())
		})
	}
	assert.Equal(t, setup.ClientPort(), targets[1].Port, "the proxy target is the minigres client port")
	assert.Equal(t, setup.GatewayLogFile(), setup.PoolerLogFile(t), "minigres writes both halves to one log")
}
