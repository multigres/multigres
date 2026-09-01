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

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// restrictedProbeSet is a SET the enforcing gateway rejects (synchronous_commit
// is cluster-managed) but PostgreSQL itself accepts.
const restrictedProbeSet = "SET synchronous_commit = off"

// TestUnsafeConnection covers multigres.unsafe_connection, the per-connection
// opt-out that suppresses the gateway's unsafe-statement rejections and, in
// exchange, pins and quarantines the connection's backend so any untracked
// session state it changes can never leak to another client through the shared
// pool. The subtests exercise the real gateway → pooler → postgres path:
//
//   - the connect-time option and the SET latch both enable it;
//   - it is a one-way latch (off / RESET / bad value are rejected);
//   - a bad Boolean at connect time is a FATAL startup error;
//   - untracked backend state set on an unsafe connection does not leak to a
//     later pooled client (the quarantine-and-discard guarantee).
//
// They share one cluster fixture: the subtests each open their own connections
// and either clean up after themselves or rely on the quarantine, so no state
// leaks between them. The probe throughout is `SET synchronous_commit = <v>`: a
// cluster-restricted GUC the enforcing gateway rejects, yet a valid USERSET GUC
// PostgreSQL applies per session and the gateway does not track — so it doubles
// as both the "rejection suppressed" signal and a genuine untracked-state probe.
func TestUnsafeConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries)")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
	param := constants.UnsafeConnectionParam

	// Baseline: the probe really is rejected on an ordinary (enforcing)
	// connection — otherwise the suppression subtests below would prove nothing.
	t.Run("enforcing connection rejects the probe", func(t *testing.T) {
		ctx := utils.WithTimeout(t, 60*time.Second)
		conn, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer conn.Close(ctx)

		_, err = conn.Exec(ctx, restrictedProbeSet)
		pgErr := utils.RequirePgError(t, err, "0A000")
		assert.Contains(t, pgErr.Message, "synchronous_commit")

		// The connection is still usable after the rejection.
		var one int
		require.NoError(t, conn.QueryRow(ctx, "SELECT 1").Scan(&one))
		assert.Equal(t, 1, one)
	})

	// The connect-time option (options=-c multigres.unsafe_connection=on, sent
	// here as a startup RuntimeParam) latches the connection so the otherwise
	// rejected probe is accepted, and the gateway-only param is stripped (never
	// forwarded to the backend, which would reject an unknown GUC).
	t.Run("connect-time option enables unsafe connection", func(t *testing.T) {
		ctx := utils.WithTimeout(t, 60*time.Second)
		cfg, err := pgx.ParseConfig(connStr)
		require.NoError(t, err)
		cfg.RuntimeParams[param] = "on"

		conn, err := pgx.ConnectConfig(ctx, cfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		// The restricted SET the enforcing path rejects now goes through to postgres.
		_, err = conn.Exec(ctx, restrictedProbeSet)
		require.NoError(t, err, "restricted SET must be accepted on an unsafe connection")

		var sc string
		require.NoError(t, conn.QueryRow(ctx, "SHOW synchronous_commit").Scan(&sc))
		assert.Equal(t, "off", sc, "the SET must have been applied on the backend")

		// The gateway-only param is stripped before reaching the backend, so a
		// plain query still works (postgres never saw it as a GUC).
		var one int
		require.NoError(t, conn.QueryRow(ctx, "SELECT 1").Scan(&one))
		assert.Equal(t, 1, one)
	})

	// A malformed Boolean value in the connect-time option fails the startup with
	// a FATAL, matching how PostgreSQL rejects a bad Boolean GUC.
	t.Run("bad boolean connect-time option is a FATAL startup error", func(t *testing.T) {
		ctx := utils.WithTimeout(t, 60*time.Second)
		cfg, err := pgx.ParseConfig(connStr)
		require.NoError(t, err)
		cfg.RuntimeParams[param] = "notabool"

		conn, err := pgx.ConnectConfig(ctx, cfg)
		if conn != nil {
			conn.Close(ctx)
		}
		pgErr := utils.RequirePgError(t, err, "22023")
		assert.Equal(t, "FATAL", pgErr.Severity)
		assert.Contains(t, pgErr.Message, param)
	})

	// `SET multigres.unsafe_connection = on` latches the connection mid-session:
	// it replies like a normal SET, and a probe rejected before the latch is
	// accepted after it.
	t.Run("SET latch enables unsafe connection mid-session", func(t *testing.T) {
		ctx := utils.WithTimeout(t, 60*time.Second)
		conn, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer conn.Close(ctx)

		// Rejected before the latch.
		_, err = conn.Exec(ctx, restrictedProbeSet)
		_ = utils.RequirePgError(t, err, "0A000")

		// Latch on. It replies with a bare SET command tag.
		tag, err := conn.Exec(ctx, "SET "+param+" = on")
		require.NoError(t, err)
		assert.Equal(t, "SET", tag.String())

		// Accepted after the latch.
		_, err = conn.Exec(ctx, restrictedProbeSet)
		require.NoError(t, err, "restricted SET must be accepted once the latch is on")

		var sc string
		require.NoError(t, conn.QueryRow(ctx, "SHOW synchronous_commit").Scan(&sc))
		assert.Equal(t, "off", sc)
	})

	// The deprecated multigres.direct_connection alias must keep working — both at
	// connect time and via SET — until all clients migrate to the new name.
	t.Run("deprecated direct_connection alias still enables", func(t *testing.T) {
		ctx := utils.WithTimeout(t, 60*time.Second)

		t.Run("connect-time option", func(t *testing.T) {
			cfg, err := pgx.ParseConfig(connStr)
			require.NoError(t, err)
			cfg.RuntimeParams[constants.DirectConnectionParam] = "on"

			conn, err := pgx.ConnectConfig(ctx, cfg)
			require.NoError(t, err)
			defer conn.Close(ctx)

			_, err = conn.Exec(ctx, restrictedProbeSet)
			require.NoError(t, err, "deprecated alias must enable unsafe connection at connect time")
		})

		t.Run("SET latch", func(t *testing.T) {
			conn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)
			defer conn.Close(ctx)

			tag, err := conn.Exec(ctx, "SET "+constants.DirectConnectionParam+" = on")
			require.NoError(t, err)
			assert.Equal(t, "SET", tag.String())

			_, err = conn.Exec(ctx, restrictedProbeSet)
			require.NoError(t, err, "deprecated alias must enable unsafe connection mid-session")
		})
	})

	// The latch can only be turned on: turning it off, resetting it, and a
	// non-Boolean value are all rejected, both before and after it is enabled.
	t.Run("one-way latch", func(t *testing.T) {
		ctx := utils.WithTimeout(t, 60*time.Second)

		t.Run("off rejected before enabling", func(t *testing.T) {
			conn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)
			defer conn.Close(ctx)

			_, err = conn.Exec(ctx, "SET "+param+" = off")
			pgErr := utils.RequirePgError(t, err, "0A000")
			assert.Contains(t, pgErr.Message, "cannot be turned off")
		})

		t.Run("reset rejected", func(t *testing.T) {
			conn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)
			defer conn.Close(ctx)

			_, err = conn.Exec(ctx, "RESET "+param)
			pgErr := utils.RequirePgError(t, err, "0A000")
			assert.Contains(t, pgErr.Message, "cannot be reset")
		})

		t.Run("non-boolean value rejected", func(t *testing.T) {
			conn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)
			defer conn.Close(ctx)

			_, err = conn.Exec(ctx, "SET "+param+" = maybe")
			pgErr := utils.RequirePgError(t, err, "22023")
			assert.Contains(t, pgErr.Message, "Boolean")
		})

		t.Run("stays on after enabling (off still rejected)", func(t *testing.T) {
			conn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)
			defer conn.Close(ctx)

			_, err = conn.Exec(ctx, "SET "+param+" = on")
			require.NoError(t, err)

			// The latch is one-way: attempting to turn it off is still an error, and
			// the connection remains an unsafe connection (probe still accepted).
			_, err = conn.Exec(ctx, "SET "+param+" = off")
			_ = utils.RequirePgError(t, err, "0A000")

			_, err = conn.Exec(ctx, restrictedProbeSet)
			require.NoError(t, err, "connection must remain an unsafe connection after a failed off attempt")
		})
	})

	// Quarantine, observed directly by backend PID. A unsafe connection draws a
	// dedicated *reserved* backend; when it closes, that backend must be closed
	// (quarantined), not recycled. Since the reserved pool otherwise recycles a
	// released backend to the next reserved borrower, the check is: the direct
	// connection's PID must never reappear on a later reserved borrow. The probe
	// is a plain transaction (BEGIN … pg_backend_pid() … ROLLBACK), a reserved
	// connection drawing from the same pool. Removing the quarantine (the
	// closeOnRelease taint on the unsafe-connection reason) makes this fail.
	t.Run("backend is discarded on teardown", func(t *testing.T) {
		ctx := utils.WithTimeout(t, 90*time.Second)

		// The unsafe connection's dedicated reserved backend PID.
		var directPID int
		func() {
			cfg, err := pgx.ParseConfig(connStr)
			require.NoError(t, err)
			cfg.RuntimeParams[param] = "on"
			conn, err := pgx.ConnectConfig(ctx, cfg)
			require.NoError(t, err)
			defer conn.Close(ctx)
			require.NoError(t, conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&directPID))
		}()
		require.NotZero(t, directPID)

		// Probe the reserved pool via transactions. A recycled (non-quarantined)
		// backend would reappear here; a discarded one cannot.
		for i := range 12 {
			pid := reservedBackendPID(t, ctx, connStr)
			require.NotEqualf(t, directPID, pid,
				"probe %d reused the unsafe connection's backend PID %d; it must have been discarded, not recycled",
				i, directPID)
		}
	})

	// The higher-level safety acceptance test. It exercises the canonical
	// untracked-state vector: a set_config(..., is_local => false) hidden inside a
	// SQL function body, which the gateway's top-level tracking cannot see, so the
	// change lives only on the backend that ran it — exactly the state the
	// enforcing gateway rejects creating such a function to prevent. A direct
	// connection is allowed to create and run it, mutating work_mem on its own
	// backend; when it closes, the backend must be discarded rather than recycled
	// to the next reserved borrower. The probe is a transaction (a reserved
	// connection sharing the pool); it must always see the baseline, never the
	// leaked value. Removing the quarantine makes this fail.
	t.Run("no untracked state leaks through the pool", func(t *testing.T) {
		ctx := utils.WithTimeout(t, 90*time.Second)

		const leakValue = "111MB"
		baseline := reservedWorkMem(t, ctx, connStr)
		require.NotEmpty(t, baseline)
		require.NotEqual(t, leakValue, baseline, "baseline unexpectedly equals the leak probe value")

		// Unsafe connection: create + run a function whose body hides an
		// is_local=false set_config the gateway cannot track, mutating work_mem on
		// this backend only.
		func() {
			cfg, err := pgx.ParseConfig(connStr)
			require.NoError(t, err)
			cfg.RuntimeParams[param] = "on"
			conn, err := pgx.ConnectConfig(ctx, cfg)
			require.NoError(t, err)
			defer conn.Close(ctx)

			// The CREATE (an is_local=false set_config in the body) is rejected on an
			// enforcing connection by body analysis; an unsafe connection accepts it.
			_, err = conn.Exec(ctx, `CREATE OR REPLACE FUNCTION mg_dc_hidden_leak() RETURNS text
				LANGUAGE sql AS $$SELECT set_config('work_mem', '`+leakValue+`', false)$$`)
			require.NoError(t, err, "hidden-set_config function must be creatable on an unsafe connection")

			_, err = conn.Exec(ctx, "SELECT mg_dc_hidden_leak()")
			require.NoError(t, err)

			var got string
			require.NoError(t, conn.QueryRow(ctx, "SHOW work_mem").Scan(&got))
			require.Equal(t, leakValue, got, "the untracked value must apply on the unsafe connection's own backend")
		}()
		// DROP is allowed on an enforcing connection; clean up the catalog object.
		defer func() {
			conn, err := pgx.Connect(ctx, connStr)
			if err != nil {
				return
			}
			defer conn.Close(ctx)
			_, _ = conn.Exec(ctx, "DROP FUNCTION IF EXISTS mg_dc_hidden_leak()")
		}()

		// Probe the reserved pool several times. If the tainted backend were
		// recycled instead of discarded, one of these probes would observe leakValue.
		for i := range 12 {
			got := reservedWorkMem(t, ctx, connStr)
			require.Equalf(t, baseline, got,
				"probe %d saw work_mem=%q; the unsafe connection's untracked work_mem=%q leaked through the pool",
				i, got, leakValue)
		}
	})
}

// reservedBackendPID opens a fresh connection, reads pg_backend_pid() inside a
// transaction (forcing a reserved backend), rolls back, and closes.
func reservedBackendPID(t *testing.T, ctx context.Context, connStr string) int {
	t.Helper()
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) //nolint:errcheck
	var pid int
	require.NoError(t, tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))
	return pid
}

// reservedWorkMem opens a fresh connection, reads work_mem inside a transaction
// (forcing a reserved backend), rolls back, and closes.
func reservedWorkMem(t *testing.T, ctx context.Context, connStr string) string {
	t.Helper()
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) //nolint:errcheck
	var v string
	require.NoError(t, tx.QueryRow(ctx, "SHOW work_mem").Scan(&v))
	return v
}
