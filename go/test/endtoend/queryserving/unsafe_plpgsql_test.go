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
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultigateway_UnsafePLpgSQLBodyRejection verifies Tier 1 body analysis:
// a DO block or CREATE FUNCTION whose PL/pgSQL or SQL body changes backend
// session state or reaches a blocklisted function is rejected at plan time with
// SQLSTATE 0A000 (feature_not_supported), while benign bodies pass through.
//
// This closes the vector where a procedural body changes session state
// invisibly to the gateway's session tracker — e.g.
// DO $$ BEGIN PERFORM set_config('work_mem','10GB',false); END $$.
func TestMultigateway_UnsafePLpgSQLBodyRejection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping unsafe PL/pgSQL body tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping unsafe PL/pgSQL body tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 60*time.Second)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// Bodies that reach an unsafe construct and must be rejected.
	t.Run("rejected bodies", func(t *testing.T) {
		tests := []struct {
			name    string
			sql     string
			wantMsg string
		}{
			{
				name:    "DO PERFORM set_config",
				sql:     "DO $$ BEGIN PERFORM set_config('work_mem','10GB',false); END $$",
				wantMsg: "set_config inside a PL/pgSQL body is not supported",
			},
			{
				name:    "DO literal SET",
				sql:     "DO $$ BEGIN SET work_mem = '10GB'; END $$",
				wantMsg: "SET/RESET inside a PL/pgSQL body is not supported",
			},
			{
				name:    "DO blocklisted dblink",
				sql:     "DO $$ BEGIN PERFORM dblink('host=x','SELECT 1'); END $$",
				wantMsg: "dblink is not supported",
			},
			{
				name:    "DO set_config in nested IF",
				sql:     "DO $$ BEGIN IF true THEN PERFORM set_config('work_mem','10GB',false); END IF; END $$",
				wantMsg: "set_config inside a PL/pgSQL body is not supported",
			},
			{
				name:    "DO set_config in exception handler",
				sql:     "DO $$ BEGIN NULL; EXCEPTION WHEN others THEN PERFORM set_config('work_mem','10GB',false); END $$",
				wantMsg: "set_config inside a PL/pgSQL body is not supported",
			},
			{
				name:    "DO dynamic EXECUTE literal SET",
				sql:     "DO $$ BEGIN EXECUTE 'SET work_mem = ''10GB'''; END $$",
				wantMsg: "SET/RESET inside a PL/pgSQL body is not supported",
			},
			{
				name:    "DO dynamic EXECUTE non-literal",
				sql:     "DO $$ DECLARE v text := '10GB'; BEGIN EXECUTE 'SET work_mem = ' || v; END $$",
				wantMsg: "EXECUTE of a runtime-built statement",
			},
			{
				name:    "CREATE FUNCTION plpgsql set_config",
				sql:     "CREATE FUNCTION _unsafe_f1() RETURNS void AS $$ BEGIN PERFORM set_config('work_mem','10GB',false); END $$ LANGUAGE plpgsql",
				wantMsg: "set_config inside a PL/pgSQL body is not supported",
			},
			{
				name:    "CREATE FUNCTION sql body blocklisted",
				sql:     "CREATE FUNCTION _unsafe_f2() RETURNS text AS $$ SELECT dblink('host=x','SELECT 1') $$ LANGUAGE sql",
				wantMsg: "dblink is not supported",
			},
			{
				name:    "CREATE FUNCTION opaque language",
				sql:     "CREATE FUNCTION _unsafe_f3() RETURNS void AS $$ pass $$ LANGUAGE plpython3u",
				wantMsg: "cannot be inspected by the connection pooler",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := conn.Exec(ctx, tt.sql)
				require.Error(t, err, "%s should be rejected", tt.name)

				var pgErr *pgconn.PgError
				require.True(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T", err)
				assert.Equal(t, "0A000", pgErr.Code, "SQLSTATE should be feature_not_supported")
				assert.Contains(t, pgErr.Message, tt.wantMsg)
				t.Logf("rejected %s: %s", tt.name, pgErr.Message)
			})
		}
	})

	// A Tier 1 rejection over the extended protocol too — both query paths run
	// the same analysis.
	t.Run("rejected via extended protocol", func(t *testing.T) {
		_, err := conn.Exec(ctx,
			"DO $$ BEGIN PERFORM set_config('work_mem','10GB',false); END $$",
			pgx.QueryExecModeDescribeExec)
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T", err)
		assert.Equal(t, "0A000", pgErr.Code)
		assert.Contains(t, pgErr.Message, "set_config inside a PL/pgSQL body is not supported")
	})

	// The connection is still usable after a run of rejections.
	t.Run("connection healthy after rejections", func(t *testing.T) {
		var result int
		err := conn.QueryRow(ctx, "SELECT 1").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, 1, result)
	})

	// Benign bodies pass through and execute normally.
	t.Run("benign bodies pass through", func(t *testing.T) {
		// A DO block that does real, safe work.
		_, err := conn.Exec(ctx, "DO $$ BEGIN PERFORM 1; END $$")
		require.NoError(t, err, "benign DO block should be allowed")

		// A benign function with a dynamic EXECUTE of a constant, plus a benign
		// PERFORM; created then dropped so the shared database stays clean.
		_, err = conn.Exec(ctx,
			"CREATE OR REPLACE FUNCTION _benign_body() RETURNS void AS $$ "+
				"BEGIN PERFORM count(*) FROM pg_class; EXECUTE 'SELECT 1'; END $$ LANGUAGE plpgsql")
		require.NoError(t, err, "benign function body should be allowed")
		_, err = conn.Exec(ctx, "DROP FUNCTION _benign_body()")
		require.NoError(t, err)
	})
}
