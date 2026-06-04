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

	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_SetValidation verifies that the multigateway validates a SET
// against PostgreSQL at SET time (PostgreSQL parity), rather than accepting it
// locally and letting the error surface later on an unrelated query.
//
// A `SET var = value` is planned as Sequence[ValidateSetting, ApplySessionState]
// (see planner.planVariableSetStmt): the ValidateSetting step runs
// set_config(name, value, is_local := true) on a backend, so an invalid or
// out-of-range value raises its error immediately — and reverts, leaving the
// pooled backend untouched — while the setting is tracked only on success. This
// is what makes pgvector's tuning GUCs behave like vanilla PostgreSQL (e.g.
// SET hnsw.ef_search = 1001 errors at SET time instead of derailing a later
// statement and aborting an unrelated DROP TABLE).
//
// extra_float_digits is the stand-in: a plain (non gateway-managed) GUC with a
// validated integer range of -15..3, so PostgreSQL rejects an out-of-range SET
// with SQLSTATE 22023.
func TestMultiGateway_SetValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SET validation test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SET validation test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	// An out-of-range value errors at SET time, just like vanilla PostgreSQL —
	// not on a later, unrelated statement.
	t.Run("out-of-range SET errors immediately", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		// extra_float_digits valid range is -15..3.
		_, err := conn.Exec(ctx, "SET extra_float_digits = 100")
		require.Error(t, err, "out-of-range SET should error at SET time, like vanilla PostgreSQL")

		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "expected a PgError, got %T: %v", err, err)
		assert.Equal(t, "22023", pgErr.Code, "should be invalid_parameter_value")
	})

	// A valid value is accepted and applied: validation does not get in the way
	// of normal SETs, and the setting is visible to a subsequent SHOW.
	t.Run("valid SET succeeds and takes effect", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SET extra_float_digits = 2")
		require.NoError(t, err, "valid SET should succeed")

		var got string
		require.NoError(t, conn.QueryRow(ctx, "SHOW extra_float_digits").Scan(&got))
		assert.Equal(t, "2", got, "the valid setting should be applied")
	})

	// The extended protocol (Parse/Bind/Execute) must validate and track SET
	// identically. pgx.QueryExecModeExec forces it, the way clients like the
	// PostgreSQL JDBC driver always send statements — even parameterless ones.
	// Previously this path forwarded a raw SET to a pooled backend (no
	// validation, no tracking, and a later RESET could not undo it).
	t.Run("extended protocol: out-of-range SET errors immediately", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SET extra_float_digits = 100", pgx.QueryExecModeExec)
		require.Error(t, err, "out-of-range SET over the extended protocol should error at SET time")

		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "expected a PgError, got %T: %v", err, err)
		assert.Equal(t, "22023", pgErr.Code, "should be invalid_parameter_value")
	})

	// Tracking + RESET over the extended protocol: a valid SET is applied and a
	// subsequent RESET reverts it. This proves the extended path tracks the
	// setting (rather than orphaning it on a backend), so RESET works.
	t.Run("extended protocol: SET tracks and RESET reverts", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)
		const m = pgx.QueryExecModeExec

		_, err := conn.Exec(ctx, "SET search_path = 'custom_ext'", m)
		require.NoError(t, err, "valid SET over the extended protocol should succeed")

		var got string
		require.NoError(t, conn.QueryRow(ctx, "SHOW search_path", m).Scan(&got))
		assert.Equal(t, "custom_ext", got, "extended-protocol SET should be tracked and applied")

		_, err = conn.Exec(ctx, "RESET search_path", m)
		require.NoError(t, err, "RESET over the extended protocol should succeed")

		require.NoError(t, conn.QueryRow(ctx, "SHOW search_path", m).Scan(&got))
		assert.NotEqual(t, "custom_ext", got, "RESET must revert — the extended SET must not orphan backend state")
		assert.Contains(t, got, "public", "default search_path contains public")
	})
}
