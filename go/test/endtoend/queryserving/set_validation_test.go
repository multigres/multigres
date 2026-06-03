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

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_SetValidationIsDeferred pins down whether the multigateway
// validates a SET against PostgreSQL at SET time (PG parity) or accepts it
// locally and lets the error surface later.
//
// Today the gateway defers: SET is applied to local session state with a
// synthetic CommandComplete and only reaches a backend on the next query (see
// go/services/multigateway/engine/apply_session_state.go). That deferral is the
// root cause of the pgvector compatibility failures:
//   - hnsw_vector / ivfflat_vector: out-of-range SETs of the extension's GUCs
//     (e.g. SET hnsw.ef_search = 1001) don't raise the range error at SET time.
//   - ivfflat_bit (downstream): the deferred erroring SET is flushed on the
//     following statement and aborts a DROP TABLE, leaking a table into the
//     next test.
//
// extra_float_digits is used as a stand-in: it's a plain (non gateway-managed)
// GUC with a validated integer range of -15..3, so vanilla PostgreSQL rejects
// an out-of-range SET immediately with SQLSTATE 22023 — exactly the behaviour
// pgvector's GUCs expect.
func TestMultiGateway_SetValidationIsDeferred(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SET validation test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SET validation test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	// Out-of-range for extra_float_digits (valid -15..3). Vanilla PostgreSQL:
	//   ERROR:  100 is outside the valid range for parameter "extra_float_digits" (-15 .. 3)
	const badSet = "SET extra_float_digits = 100"

	// Desired behaviour (PostgreSQL parity): the bad SET errors at SET time.
	// This currently FAILS — the gateway accepts the SET locally without
	// touching a backend — and documents the gap to close.
	t.Run("out-of-range SET errors immediately", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, badSet)
		require.Error(t, err, "SET with an out-of-range value should error at SET time, like vanilla PostgreSQL")

		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "expected a PgError, got %T: %v", err, err)
		assert.Equal(t, "22023", pgErr.Code, "should be invalid_parameter_value")
	})

	// Characterises the current deferral: a wholly unrelated later statement
	// carries the SET's validation error. This is the mechanism that aborts the
	// next pgvector test's DROP TABLE. Once SET is validated eagerly this subtest
	// self-skips (the SET above already errored), so the test stays meaningful
	// after the fix.
	t.Run("deferred error attaches to a later unrelated statement", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		if _, err := conn.Exec(ctx, badSet); err != nil {
			t.Skipf("SET now errors eagerly (deferral fixed); nothing to characterise: %v", err)
		}

		_, err := conn.Exec(ctx, "SELECT 1")
		require.Error(t, err, "the deferred SET error should surface here, on an unrelated statement")

		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "expected a PgError, got %T: %v", err, err)
		assert.Equal(t, "22023", pgErr.Code, "the late error is the SET's range violation")
		t.Logf("deferred SET error surfaced on a later SELECT 1: %s (SQLSTATE %s)", pgErr.Message, pgErr.Code)
	})
}
