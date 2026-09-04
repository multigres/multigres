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
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultigateway_LogicalReplicationSlotFailoverAdmission verifies, against
// a real cluster with --enable-slot-based-replication on, that the gateway's
// replication-slot guard (rejectNonTemporaryReplicationSlot in
// go/services/multigateway/planner/unsafe_funccall.go) admits a
// non-temporary logical slot only when the call spells out a literal
// failover=true itself (positionally or by name) — and that the slot
// Postgres actually creates is really registered for failover in every
// admitted case, not just accepted by the gateway. A call that omits
// failover is rejected: admission is a predicate about what the client
// wrote, never a promise to inject the argument on their behalf.
func TestMultigateway_LogicalReplicationSlotFailoverAdmission(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication-slot admission test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping replication-slot admission tests")
	}

	setup := getSlotBasedReplicationSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	ctx := utils.WithTimeout(t, 60*time.Second)
	require.NoError(t, db.PingContext(ctx))

	dropSlot := func(t *testing.T, name string) {
		t.Helper()
		_, _ = db.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", name)
	}
	assertSlotState := func(t *testing.T, name string, wantTemporary, wantFailover bool) {
		t.Helper()
		var temporary, failover bool
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT temporary, failover FROM pg_replication_slots WHERE slot_name = $1", name).
			Scan(&temporary, &failover))
		assert.Equal(t, wantTemporary, temporary, "slot_name=%s temporary", name)
		assert.Equal(t, wantFailover, failover, "slot_name=%s failover", name)
	}
	assertSlotAbsent := func(t *testing.T, name string) {
		t.Helper()
		var count int
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", name).Scan(&count))
		assert.Zero(t, count, "rejected call must never reach postgres, slot_name=%s", name)
	}

	t.Run("positional failover=true admitted and really registered for failover", func(t *testing.T) {
		const slot = "e2e_failover_positional"
		t.Cleanup(func() { dropSlot(t, slot) })

		_, err := db.ExecContext(ctx,
			"SELECT pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)", slot)
		require.NoError(t, err)
		assertSlotState(t, slot, false, true)
	})

	t.Run("named failover => true admitted, temporary/twophase omitted", func(t *testing.T) {
		const slot = "e2e_failover_named"
		t.Cleanup(func() { dropSlot(t, slot) })

		_, err := db.ExecContext(ctx,
			"SELECT pg_create_logical_replication_slot($1, 'pgoutput', failover => true)", slot)
		require.NoError(t, err)
		assertSlotState(t, slot, false, true)
	})

	t.Run("named temporary => false, failover => true admitted", func(t *testing.T) {
		const slot = "e2e_failover_named_both"
		t.Cleanup(func() { dropSlot(t, slot) })

		_, err := db.ExecContext(ctx,
			"SELECT pg_create_logical_replication_slot($1, 'pgoutput', temporary => false, failover => true)", slot)
		require.NoError(t, err)
		assertSlotState(t, slot, false, true)
	})

	t.Run("temporary=true still admitted regardless of failover", func(t *testing.T) {
		const slot = "e2e_temporary_slot"
		// A real temporary slot only lives on the backend that created it and
		// disappears when this connection closes, so no cleanup/assert against
		// pg_replication_slots after the fact — just confirm the gateway lets it
		// through, on its own connection so it doesn't outlive the subtest.
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		_, err = conn.ExecContext(ctx,
			"SELECT pg_create_logical_replication_slot($1, 'pgoutput', true)", slot)
		require.NoError(t, err)
	})

	t.Run("explicit failover => false still rejected (deliberate opt-out)", func(t *testing.T) {
		const slot = "e2e_failover_optout"
		_, err := db.ExecContext(ctx,
			"SELECT pg_create_logical_replication_slot($1, 'pgoutput', failover => false)", slot)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires temporary=true")
		assertSlotAbsent(t, slot)
	})

	t.Run("omitted failover rejected (must be explicit)", func(t *testing.T) {
		const slot = "e2e_failover_omitted"
		_, err := db.ExecContext(ctx,
			"SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires temporary=true")
		assertSlotAbsent(t, slot)
	})

	t.Run("PREPARE/EXECUTE with explicit failover => true is admitted", func(t *testing.T) {
		// PREPARE/EXECUTE is session-scoped, so both must run on the same
		// backend connection — a pool connection, not the shared db handle.
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		const slot = "e2e_failover_prepared"
		t.Cleanup(func() { dropSlot(t, slot) })

		_, err = conn.ExecContext(ctx,
			"PREPARE mkslot(text) AS SELECT pg_create_logical_replication_slot($1, 'pgoutput', failover => true)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "EXECUTE mkslot('"+slot+"')")
		require.NoError(t, err)
		assertSlotState(t, slot, false, true)
	})

	t.Run("EXPLAIN ANALYZE EXECUTE with explicit failover => true is admitted", func(t *testing.T) {
		// PREPARE/EXECUTE is session-scoped, so both must run on the same
		// backend connection — a pool connection, not the shared db handle.
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		const slot = "e2e_failover_explain"
		t.Cleanup(func() { dropSlot(t, slot) })

		_, err = conn.ExecContext(ctx,
			"PREPARE mkslot_explain(text) AS SELECT pg_create_logical_replication_slot($1, 'pgoutput', failover => true)")
		require.NoError(t, err)
		// ANALYZE makes EXPLAIN actually run the plan (and its side effects),
		// unlike plain EXPLAIN which only describes it.
		_, err = conn.ExecContext(ctx, "EXPLAIN ANALYZE EXECUTE mkslot_explain('"+slot+"')")
		require.NoError(t, err)
		assertSlotState(t, slot, false, true)
	})

	t.Run("COPY (subquery) with explicit failover => true is admitted", func(t *testing.T) {
		const slot = "e2e_failover_copy"
		t.Cleanup(func() { dropSlot(t, slot) })

		connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
		copyConn, err := pgconn.Connect(ctx, connStr)
		require.NoError(t, err)
		defer copyConn.Close(context.Background())

		var buf bytes.Buffer
		_, err = copyConn.CopyTo(ctx, &buf,
			"COPY (SELECT pg_create_logical_replication_slot('"+slot+"', 'pgoutput', failover => true)) TO STDOUT")
		require.NoError(t, err)
		assertSlotState(t, slot, false, true)
	})

	t.Run("DECLARE CURSOR WITH HOLD never admits failover slot creation", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		const slot = "e2e_failover_cursor_hold"
		tx, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx,
			"DECLARE c_failover CURSOR WITH HOLD FOR SELECT pg_create_logical_replication_slot('"+slot+"', 'pgoutput', failover => true)")
		require.Error(t, err, "a cursor evaluates its query at COMMIT, after this admission check, so it must be rejected")
		assert.Contains(t, err.Error(), "requires temporary=true")
		_ = tx.Rollback()
		assertSlotAbsent(t, slot)
	})

	t.Run("bound failover argument rejected (fails closed)", func(t *testing.T) {
		const slot = "e2e_failover_bound"
		_, err := db.ExecContext(ctx,
			"SELECT pg_create_logical_replication_slot($1, 'pgoutput', false, false, $2)", slot, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires temporary=true")
		assertSlotAbsent(t, slot)
	})

	t.Run("physical slot: failover has no meaning, still rejected without temporary=true", func(t *testing.T) {
		const slot = "e2e_physical_failover"
		_, err := db.ExecContext(ctx,
			"SELECT pg_create_physical_replication_slot($1, false, false)", slot)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires temporary=true")
		assertSlotAbsent(t, slot)
	})

	t.Run("stored view definition never admits failover slot creation, even with the flag on", func(t *testing.T) {
		const slot = "e2e_failover_view"
		_, err := db.ExecContext(ctx,
			"CREATE VIEW e2e_failover_view_v AS SELECT pg_create_logical_replication_slot('"+slot+"', 'pgoutput', failover => true)")
		require.Error(t, err, "a view's body is re-evaluated on every future SELECT, invisible to this admission check, so it must never be allowed to freeze in a failover-admitted decision")
		assert.Contains(t, err.Error(), "requires temporary=true")
		assertSlotAbsent(t, slot)
		_, _ = db.ExecContext(ctx, "DROP VIEW IF EXISTS e2e_failover_view_v")
	})

	t.Run("stored column default never admits failover slot creation, even with the flag on", func(t *testing.T) {
		const slot = "e2e_failover_default"
		_, err := db.ExecContext(ctx,
			"CREATE TABLE e2e_failover_default_t (a text DEFAULT pg_create_logical_replication_slot('"+slot+"', 'pgoutput', failover => true))")
		require.Error(t, err, "a column DEFAULT is stored and re-evaluated on every future INSERT that omits it, invisible to this admission check, so it must never be allowed to freeze in a failover-admitted decision — the same class of gap the view case above closes, not a view-specific fix")
		assert.Contains(t, err.Error(), "requires temporary=true")
		assertSlotAbsent(t, slot)
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS e2e_failover_default_t")
	})
}

// TestMultigateway_LogicalReplicationSlotFailoverAdmission_FlagOff confirms
// the default cluster — slot-based replication off — still rejects a
// non-temporary logical slot even with an explicit failover=true: admission
// requires the feature flag, not just the argument.
func TestMultigateway_LogicalReplicationSlotFailoverAdmission_FlagOff(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication-slot admission test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping replication-slot admission tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	ctx := utils.WithTimeout(t, 60*time.Second)
	require.NoError(t, db.PingContext(ctx))

	_, err = db.ExecContext(ctx,
		"SELECT pg_create_logical_replication_slot('e2e_flag_off', 'pgoutput', false, false, true)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires temporary=true")

	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'e2e_flag_off'").Scan(&count))
	assert.Zero(t, count)
}
