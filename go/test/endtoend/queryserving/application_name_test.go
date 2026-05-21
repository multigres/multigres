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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_ApplicationName covers the gateway-managed
// `application_name` GUC: SET/SHOW/RESET resolve at the gateway without a PG
// round-trip, the client-visible value tracks SET, and the multipooler-side
// `multigres_vpid:<id>` stamping on the backend application_name is
// preserved (visible only through pg_stat_activity, not SHOW).
func TestMultiGateway_ApplicationName(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping application_name test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping application_name tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 150*time.Second)

	t.Run("SET and SHOW", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SET application_name = 'myapp'")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "myapp", result, "SHOW must return the client-set value")
	})

	t.Run("SET to empty string is honoured", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SET application_name = ''")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "", result, "explicit SET to empty string overrides default")
	})

	t.Run("RESET reverts to PG default (empty string)", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SET application_name = 'myapp'")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "RESET application_name")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "", result, "RESET reverts to empty default when no startup param given")
	})

	t.Run("SET TO DEFAULT reverts to default", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SET application_name = 'myapp'")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "SET application_name TO DEFAULT")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "", result, "SET TO DEFAULT reverts to empty default")
	})

	t.Run("RESET ALL also resets application_name", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SET application_name = 'myapp'")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "RESET ALL")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "", result, "RESET ALL must revert application_name to default")
	})

	t.Run("SET LOCAL is reverted at COMMIT", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SET application_name = 'session'")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "BEGIN")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "SET LOCAL application_name = 'local'")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "local", result, "SET LOCAL effective during txn")

		_, err = conn.Exec(ctx, "COMMIT")
		require.NoError(t, err)

		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "session", result, "after COMMIT, session value restored")
	})

	t.Run("ROLLBACK reverts non-LOCAL SET", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SET application_name = 'pre-txn'")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "BEGIN")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "SET application_name = 'in-txn'")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "ROLLBACK")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "pre-txn", result, "ROLLBACK reverts non-LOCAL SET")
	})

	t.Run("savepoint rollback reverts application_name", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "BEGIN")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "SET application_name = 'outer'")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "SAVEPOINT sp")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "SET application_name = 'inner'")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "inner", result)

		_, err = conn.Exec(ctx, "ROLLBACK TO SAVEPOINT sp")
		require.NoError(t, err)

		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "outer", result, "ROLLBACK TO sp reverts to pre-savepoint value")

		_, err = conn.Exec(ctx, "COMMIT")
		require.NoError(t, err)
	})

	t.Run("client value never reaches PostgreSQL backend", func(t *testing.T) {
		// Documented limitation (and proof that the gateway intercepts SET):
		// `SHOW application_name` returns the client-set value, but
		// `current_setting('application_name')` is evaluated by PostgreSQL and
		// therefore reflects whatever the multipooler set on the backend — never
		// the client-supplied value. Asserting the divergence guards against a
		// regression that would accidentally forward SET to PostgreSQL.
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SET application_name = 'myapp-client-only'")
		require.NoError(t, err)

		var showResult string
		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&showResult)
		require.NoError(t, err)
		assert.Equal(t, "myapp-client-only", showResult)

		var setting string
		err = conn.QueryRow(ctx, "SELECT current_setting('application_name')").Scan(&setting)
		require.NoError(t, err)
		assert.NotEqual(t, "myapp-client-only", setting,
			"current_setting must never see the client-supplied value — "+
				"if this fires, SET application_name is leaking through to PostgreSQL")
	})
}

// TestMultiGateway_ApplicationNameStartupParam confirms the startup parameter
// path: pgx passes application_name through the StartupMessage, the gateway
// captures it as the default, and removes it from the params forwarded to PG
// so the multipooler stamp remains authoritative on the wire.
func TestMultiGateway_ApplicationNameStartupParam(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping application_name startup param test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping application_name startup param tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 150*time.Second)

	t.Run("startup param sets default", func(t *testing.T) {
		connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
		connCfg, err := pgx.ParseConfig(connStr)
		require.NoError(t, err)
		connCfg.RuntimeParams["application_name"] = "startup-app"

		conn, err := pgx.ConnectConfig(ctx, connCfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		var result string
		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "startup-app", result, "startup param sets the default for SHOW")
	})

	t.Run("SET overrides startup param", func(t *testing.T) {
		connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
		connCfg, err := pgx.ParseConfig(connStr)
		require.NoError(t, err)
		connCfg.RuntimeParams["application_name"] = "startup-app"

		conn, err := pgx.ConnectConfig(ctx, connCfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		_, err = conn.Exec(ctx, "SET application_name = 'override'")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "override", result, "SET overrides startup param")
	})

	t.Run("RESET reverts to startup param default", func(t *testing.T) {
		connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
		connCfg, err := pgx.ParseConfig(connStr)
		require.NoError(t, err)
		connCfg.RuntimeParams["application_name"] = "startup-app"

		conn, err := pgx.ConnectConfig(ctx, connCfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		_, err = conn.Exec(ctx, "SET application_name = 'override'")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "RESET application_name")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "startup-app", result,
			"RESET reverts to startup param default, not empty")
	})
}
