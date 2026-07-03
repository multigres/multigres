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

package queryserving

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/test/utils"
)

// TestCachedPlanReprepareAfterDDL is the regression for SQLSTATE 0A000
// "cached plan must not change result type" getting stuck through the gateway.
// After a DDL changes a query's result columns, a subsequent extended-protocol
// execution (re-Parsed under a fresh statement name, as PG drivers do after 0A000)
// must succeed with the new columns — the gateway must re-prepare on the backend
// rather than reuse the stale cached plan.
func TestCachedPlanReprepareAfterDDL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 60*time.Second)

	// collectFirstResult returns the first Result carrying field metadata.
	// Uses the fused Bind+Describe+Execute path so the portal's RowDescription
	// rides back through the callback (plain BindAndExecute never sends one).
	exec := func(t *testing.T, conn interface {
		BindDescribeAndExecute(context.Context, string, string, [][]byte, []int16, []int16, int32, func(context.Context, *sqltypes.Result) error) (bool, error)
	}, stmtName string,
	) *sqltypes.Result {
		t.Helper()
		var got *sqltypes.Result
		_, err := conn.BindDescribeAndExecute(ctx, "", stmtName, nil, nil, nil, 0,
			func(_ context.Context, r *sqltypes.Result) error {
				if got == nil && r.Fields != nil {
					got = r
				}
				return nil
			})
		require.NoError(t, err)
		return got
	}

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			conn := connectLowLevelToPort(t, ctx, target.Port)
			defer conn.Close()

			_, err := conn.Query(ctx, "DROP TABLE IF EXISTS cptest")
			require.NoError(t, err)
			_, err = conn.Query(ctx, "CREATE TABLE cptest (a int, b int)")
			require.NoError(t, err)
			_, err = conn.Query(ctx, "INSERT INTO cptest VALUES (1, 2)")
			require.NoError(t, err)
			t.Cleanup(func() {
				c := connectLowLevelToPort(t, context.Background(), target.Port)
				defer c.Close()
				_, _ = c.Query(context.Background(), "DROP TABLE IF EXISTS cptest")
			})

			// Warm a prepared statement (2 columns).
			require.NoError(t, conn.Parse(ctx, "cp_s1", "SELECT * FROM cptest", nil))
			r1 := exec(t, conn, "cp_s1")
			require.NotNil(t, r1)
			assert.Len(t, r1.Fields, 2, "before DDL: two columns")

			// DDL changes the result type.
			_, err = conn.Query(ctx, "ALTER TABLE cptest DROP COLUMN b")
			require.NoError(t, err)

			// Re-Parse under a FRESH client statement name (same text) and execute.
			// Direct PG: fresh plan → OK. Gateway pre-fix: maps to stale canonical → 0A000.
			require.NoError(t, conn.Parse(ctx, "cp_s2", "SELECT * FROM cptest", nil))
			r2 := exec(t, conn, "cp_s2")
			require.NotNil(t, r2, "re-execute after DDL must succeed (no stuck 0A000)")
			assert.Len(t, r2.Fields, 1, "after DDL: one column")
			assert.Equal(t, "a", r2.Fields[0].Name)

			require.NoError(t, conn.CloseStatement(ctx, "cp_s1"))
			require.NoError(t, conn.CloseStatement(ctx, "cp_s2"))
		})
	}
}
