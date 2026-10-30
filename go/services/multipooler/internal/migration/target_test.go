// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
)

func TestTablesValuesClause(t *testing.T) {
	require.Equal(t, "(VALUES ('public.orders')) AS t(tbl)", tablesValuesClause([]string{"public.orders"}))
	require.Equal(t, "(VALUES ('public.orders'), ('app.items')) AS t(tbl)",
		tablesValuesClause([]string{"public.orders", "app.items"}))
}

func TestDropTablesSQL(t *testing.T) {
	// No tables → no statement (DropTables no-ops on this).
	require.Equal(t, "", dropTablesSQL(nil))
	require.Equal(t, "", dropTablesSQL([]string{}))

	// Safe lowercase identifiers are not quoted; the drop is one IF EXISTS …
	// CASCADE over all tables so a re-run over pre-existing target tables (and any
	// dependents) succeeds.
	require.Equal(t, "DROP TABLE IF EXISTS public.orders CASCADE",
		dropTablesSQL([]string{"public.orders"}))
	require.Equal(t, "DROP TABLE IF EXISTS public.orders, app.items CASCADE",
		dropTablesSQL([]string{"public.orders", "app.items"}))

	// Identifiers needing quoting (mixed case / reserved) are quoted per part.
	got := dropTablesSQL([]string{"public.Order"})
	require.Contains(t, got, `"Order"`)
	require.True(t, strings.HasPrefix(got, "DROP TABLE IF EXISTS "))
	require.True(t, strings.HasSuffix(got, " CASCADE"))
}

// TestDisableUserTriggers is the regression test for the CRITICAL finding
// this guards against: a trigger function copied from the source schema is
// created under the target admin (ApplySchema's own privileges), so any
// trigger left enabled — in any fire mode, including the ordinary default
// ORIGIN — would fire with the target admin's privileges: during replicated
// apply if left in ENABLE ALWAYS/REPLICA mode, or on ordinary application
// writes once the migration completes, since ORIGIN is exactly the mode that
// fires for direct DML. Only disabling the trigger outright closes both. See
// the function's doc comment for the full history (this replaced an earlier
// fire-mode-reset approach, which itself replaced textually stripping the
// fire-mode statement from pg_dump's own output).
func TestDisableUserTriggers(t *testing.T) {
	qs := &fakeQS{}
	tg := newTarget(qs)

	require.NoError(t, tg.DisableUserTriggers(context.Background(), []string{"public.orders", "app.items"}))

	require.Equal(t, []string{
		"ALTER TABLE public.orders DISABLE TRIGGER USER",
		"ALTER TABLE app.items DISABLE TRIGGER USER",
	}, qs.adminQueries)
}

// TestDropUserCheckConstraints is the regression test for the HIGH finding
// this guards against: a CHECK constraint's expression can call a function —
// directly, or indirectly through an operator or cast it uses — and that
// function was itself created by ApplySchema running as target admin, so it
// is now owned by the target admin regardless of who wrote its body.
// PostgreSQL has no DISABLE for a CHECK constraint (unlike DisableUserTriggers
// for a trigger): it evaluates on every write unconditionally, including ones
// the import subscription's apply worker applies, so an adversarial source
// table owner could use a CHECK constraint calling a SECURITY DEFINER
// function to run admin-privileged code on every replicated row. DROP is the
// only fire-mode-independent way to close it, applied unconditionally to every
// CHECK constraint — not just ones a catalog query can identify as calling a
// non-builtin function — for the same reason DisableUserTriggers disables
// every trigger unconditionally (see that function's doc comment): there is
// no automatic way to distinguish a reviewed, trusted constraint from an
// adversarial one.
func TestDropUserCheckConstraints(t *testing.T) {
	t.Run("no tables is a no-op", func(t *testing.T) {
		qs := &fakeQS{}
		tg := newTarget(qs)
		require.NoError(t, tg.DropUserCheckConstraints(context.Background(), nil))
		require.Empty(t, qs.adminQueries)
	})

	const listSQL = "SELECT t.tbl, c.conname FROM (VALUES ('public.orders')) AS t(tbl) " +
		"JOIN pg_constraint c ON c.conrelid = to_regclass(t.tbl) AND c.contype = 'c'"

	t.Run("no check constraints found: nothing is dropped", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{listSQL: emptyResult()}}
		tg := newTarget(qs)

		require.NoError(t, tg.DropUserCheckConstraints(context.Background(), []string{"public.orders"}))
		require.Equal(t, []string{listSQL}, qs.adminQueries)
	})

	t.Run("every found check constraint is dropped, regardless of its expression", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
			listSQL: {Rows: []*sqltypes.Row{
				{Values: []sqltypes.Value{sqltypes.Value("public.orders"), sqltypes.Value("orders_price_check")}},
				{Values: []sqltypes.Value{sqltypes.Value("public.orders"), sqltypes.Value("orders_evil_check")}},
			}},
		}}
		tg := newTarget(qs)

		require.NoError(t, tg.DropUserCheckConstraints(context.Background(), []string{"public.orders"}))

		require.Equal(t, []string{
			listSQL,
			"ALTER TABLE public.orders DROP CONSTRAINT orders_price_check",
			"ALTER TABLE public.orders DROP CONSTRAINT orders_evil_check",
		}, qs.adminQueries)
	})
}

// TestDisableUserRewriteRules is the regression test for the HIGH finding
// this guards against: a rewrite rule's action is arbitrary DML (it is a
// query-rewrite, not a mere check), and the function itself (or any function
// it invokes) was created by ApplySchema running as target admin, so it is
// now owned by the target admin regardless of who wrote it. Unlike a CHECK
// constraint, PostgreSQL does support disabling a rule (ev_enabled uses the
// same O/D/R/A scheme as a trigger's tgenabled — ALTER TABLE's own docs say
// rule enable/disable semantics are "as for disabled/enabled triggers"), so
// DISABLE RULE — not DROP — is the right fix here, and it is unconditional:
// not qualified by session_replication_role the way ENABLE/ENABLE REPLICA/
// ENABLE ALWAYS are, so it never fires in any role once disabled — during the
// import subscription's apply worker, or on ordinary application DML once the
// migration completes.
func TestDisableUserRewriteRules(t *testing.T) {
	t.Run("no tables is a no-op", func(t *testing.T) {
		qs := &fakeQS{}
		tg := newTarget(qs)
		require.NoError(t, tg.DisableUserRewriteRules(context.Background(), nil))
		require.Empty(t, qs.adminQueries)
	})

	const listSQL = "SELECT t.tbl, r.rulename FROM (VALUES ('public.orders')) AS t(tbl) " +
		"JOIN pg_rewrite r ON r.ev_class = to_regclass(t.tbl) WHERE r.rulename <> '_RETURN'"

	t.Run("no rewrite rules found: nothing is disabled", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{listSQL: emptyResult()}}
		tg := newTarget(qs)

		require.NoError(t, tg.DisableUserRewriteRules(context.Background(), []string{"public.orders"}))
		require.Equal(t, []string{listSQL}, qs.adminQueries)
	})

	t.Run("every found rewrite rule is disabled, regardless of its action", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
			listSQL: {Rows: []*sqltypes.Row{
				{Values: []sqltypes.Value{sqltypes.Value("public.orders"), sqltypes.Value("orders_audit_rule")}},
				{Values: []sqltypes.Value{sqltypes.Value("public.orders"), sqltypes.Value("orders_evil_rule")}},
			}},
		}}
		tg := newTarget(qs)

		require.NoError(t, tg.DisableUserRewriteRules(context.Background(), []string{"public.orders"}))

		require.Equal(t, []string{
			listSQL,
			"ALTER TABLE public.orders DISABLE RULE orders_audit_rule",
			"ALTER TABLE public.orders DISABLE RULE orders_evil_rule",
		}, qs.adminQueries)
	})
}

// TestDropUserDomainChecks is the regression test for the HIGH finding this
// guards against: a domain's CHECK constraint can call a function that
// ApplySchema created as target admin, and — unlike a table-level CHECK
// constraint — it is invisible to a query scoped by conrelid, since a domain
// constraint's pg_constraint row carries contypid (the domain's own type oid)
// instead. Confirmed against a live server that a domain CHECK fires on every
// write to a column of that domain type, even under
// session_replication_role=replica and with user triggers disabled.
func TestDropUserDomainChecks(t *testing.T) {
	t.Run("no tables is a no-op", func(t *testing.T) {
		qs := &fakeQS{}
		tg := newTarget(qs)
		require.NoError(t, tg.DropUserDomainChecks(context.Background(), nil))
		require.Empty(t, qs.adminQueries)
	})

	const listSQL = "WITH migrated AS (SELECT to_regclass(t.tbl) AS tbl_oid FROM (VALUES ('public.orders')) AS t(tbl))" +
		" SELECT DISTINCT n.nspname, ty.typname, c.conname," +
		" EXISTS (" +
		"SELECT 1 FROM pg_attribute a2 JOIN pg_type bt2 ON bt2.oid = a2.atttypid" +
		" WHERE a2.attnum > 0 AND NOT a2.attisdropped" +
		" AND (CASE WHEN bt2.typcategory = 'A' THEN bt2.typelem ELSE bt2.oid END) = ty.oid" +
		" AND a2.attrelid NOT IN (SELECT tbl_oid FROM migrated WHERE tbl_oid IS NOT NULL)" +
		") AS shared_outside" +
		" FROM migrated m" +
		" JOIN pg_attribute a ON a.attrelid = m.tbl_oid AND a.attnum > 0 AND NOT a.attisdropped" +
		" JOIN pg_type bt ON bt.oid = a.atttypid" +
		" JOIN pg_type ty ON ty.oid = CASE WHEN bt.typcategory = 'A' THEN bt.typelem ELSE bt.oid END AND ty.typtype = 'd'" +
		" JOIN pg_namespace n ON n.oid = ty.typnamespace" +
		" JOIN pg_constraint c ON c.contypid = ty.oid AND c.contype = 'c'"

	t.Run("no domain check constraints found: nothing is dropped", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{listSQL: emptyResult()}}
		tg := newTarget(qs)

		require.NoError(t, tg.DropUserDomainChecks(context.Background(), []string{"public.orders"}))
		require.Equal(t, []string{listSQL}, qs.adminQueries)
	})

	t.Run("every found domain check constraint is dropped, regardless of its expression", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
			listSQL: {Rows: []*sqltypes.Row{
				{Values: []sqltypes.Value{sqltypes.Value("public"), sqltypes.Value("d_amount"), sqltypes.Value("d_amount_check"), sqltypes.Value("f")}},
				{Values: []sqltypes.Value{sqltypes.Value("public"), sqltypes.Value("d_evil"), sqltypes.Value("d_evil_check"), sqltypes.Value("f")}},
			}},
		}}
		tg := newTarget(qs)

		require.NoError(t, tg.DropUserDomainChecks(context.Background(), []string{"public.orders"}))

		require.Equal(t, []string{
			listSQL,
			"ALTER DOMAIN public.d_amount DROP CONSTRAINT d_amount_check",
			"ALTER DOMAIN public.d_evil DROP CONSTRAINT d_evil_check",
		}, qs.adminQueries)
	})

	// This subtest is the regression test for the MEDIUM finding it guards
	// against: ALTER DOMAIN ... DROP CONSTRAINT disables the CHECK for every
	// column using the domain, anywhere in the database — confirmed against a
	// live server that this silently let an invalid value through on an
	// unrelated, non-migrated table sharing the domain. The caller migrating
	// public.orders has no rights over that other table, so this must refuse
	// rather than drop.
	t.Run("a domain shared with an un-migrated table is refused instead of dropped", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
			listSQL: {Rows: []*sqltypes.Row{
				{Values: []sqltypes.Value{sqltypes.Value("public"), sqltypes.Value("d_shared"), sqltypes.Value("d_shared_check"), sqltypes.Value("t")}},
			}},
		}}
		tg := newTarget(qs)

		err := tg.DropUserDomainChecks(context.Background(), []string{"public.orders"})

		require.Error(t, err)
		require.Equal(t, "0A000", mterrors.ExtractSQLSTATE(err))
		require.ErrorContains(t, err, "public.d_shared")
		require.Equal(t, []string{listSQL}, qs.adminQueries, "must not drop anything once a shared domain is found")
	})

	// listSQL above already pins the exact SQL sent to Postgres, including the
	// WHERE tbl_oid IS NOT NULL guard — the regression coverage for the MEDIUM
	// finding this fixes: without it, a tables entry that does not exist yet
	// on the target (to_regclass returns NULL) poisons the NOT IN subquery, so
	// shared_outside evaluates to false for every domain regardless of real
	// sharing — confirmed against a live server.
}

// TestDropUserExpressionIndexes is the regression test for the HIGH finding
// this guards against: an expression or partial index's function can call a
// function ApplySchema created as target admin, and it evaluates on every
// write to the indexed table. PostgreSQL requires that function to be marked
// IMMUTABLE, which blocks an actual write from inside it at runtime, but not
// a read of other tables — confirmed against a live server that a
// SECURITY DEFINER, IMMUTABLE-declared function reading a PUBLIC-revoked
// table can still leak its contents back to the writer via an exception
// message, from both an expression and a partial-index predicate.
func TestDropUserExpressionIndexes(t *testing.T) {
	t.Run("no tables is a no-op", func(t *testing.T) {
		qs := &fakeQS{}
		tg := newTarget(qs)
		require.NoError(t, tg.DropUserExpressionIndexes(context.Background(), nil))
		require.Empty(t, qs.adminQueries)
	})

	const listSQL = "SELECT t.tbl, n.nspname, i.relname, con.conname FROM (VALUES ('public.orders')) AS t(tbl)" +
		" JOIN pg_index ix ON ix.indrelid = to_regclass(t.tbl)" +
		" JOIN pg_class i ON i.oid = ix.indexrelid" +
		" JOIN pg_namespace n ON n.oid = i.relnamespace" +
		" LEFT JOIN pg_constraint con ON con.conindid = ix.indexrelid" +
		" WHERE ix.indexprs IS NOT NULL OR ix.indpred IS NOT NULL"

	t.Run("no expression/predicate indexes found: nothing is dropped", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{listSQL: emptyResult()}}
		tg := newTarget(qs)

		require.NoError(t, tg.DropUserExpressionIndexes(context.Background(), []string{"public.orders"}))
		require.Equal(t, []string{listSQL}, qs.adminQueries)
	})

	t.Run("a plain expression index is dropped directly; a constraint-backing one drops the constraint instead", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
			listSQL: {Rows: []*sqltypes.Row{
				{Values: []sqltypes.Value{sqltypes.Value("public.orders"), sqltypes.Value("public"), sqltypes.Value("orders_evil_idx"), nil}},
				{Values: []sqltypes.Value{sqltypes.Value("public.orders"), sqltypes.Value("public"), sqltypes.Value("orders_evil_uniq_idx"), sqltypes.Value("orders_evil_uniq")}},
			}},
		}}
		tg := newTarget(qs)

		require.NoError(t, tg.DropUserExpressionIndexes(context.Background(), []string{"public.orders"}))

		require.Equal(t, []string{
			listSQL,
			"DROP INDEX public.orders_evil_idx",
			"ALTER TABLE public.orders DROP CONSTRAINT orders_evil_uniq",
		}, qs.adminQueries)
	})
}

// TestDropUserGeneratedColumns is the regression test for the MEDIUM finding
// this guards against: PostgreSQL does not replicate a generated column's
// value, so the subscriber recomputes its expression locally on every
// replicated row — confirmed against a live server that a SECURITY DEFINER
// function invoked from a generated expression can read a PUBLIC-revoked
// table and leak its contents back the same way an expression index can.
func TestDropUserGeneratedColumns(t *testing.T) {
	t.Run("no tables is a no-op", func(t *testing.T) {
		qs := &fakeQS{}
		tg := newTarget(qs)
		require.NoError(t, tg.DropUserGeneratedColumns(context.Background(), nil))
		require.Empty(t, qs.adminQueries)
	})

	const listSQL = "SELECT t.tbl, a.attname FROM (VALUES ('public.orders')) AS t(tbl)" +
		" JOIN pg_attribute a ON a.attrelid = to_regclass(t.tbl) AND a.attnum > 0 AND NOT a.attisdropped" +
		" WHERE a.attgenerated <> ''"

	t.Run("no generated columns found: nothing is dropped", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{listSQL: emptyResult()}}
		tg := newTarget(qs)

		require.NoError(t, tg.DropUserGeneratedColumns(context.Background(), []string{"public.orders"}))
		require.Equal(t, []string{listSQL}, qs.adminQueries)
	})

	t.Run("a generated column's expression is dropped", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
			listSQL: {Rows: []*sqltypes.Row{
				{Values: []sqltypes.Value{sqltypes.Value("public.orders"), sqltypes.Value("evil_generated")}},
			}},
		}}
		tg := newTarget(qs)

		require.NoError(t, tg.DropUserGeneratedColumns(context.Background(), []string{"public.orders"}))

		require.Equal(t, []string{
			listSQL,
			"ALTER TABLE public.orders ALTER COLUMN evil_generated DROP EXPRESSION",
		}, qs.adminQueries)
	})
}

// TestCheckDropPrivilege is the regression test for the HIGH finding this
// check guards against: the gateway's CanCreateMigration gate only confirms a
// caller could perform logical-replication setup in principle (database
// CREATE + pg_create_subscription), not that it owns the specific tables a
// migration names — without this check, such a caller could name an existing
// target table it does not own and have DropTables destroy it (CASCADE, so
// dependents too) via the admin pool, bypassing Postgres's own ownership
// enforcement entirely.
func TestCheckDropPrivilege(t *testing.T) {
	t.Run("empty callerRole skips the check (CLI/multiadmin path)", func(t *testing.T) {
		qs := &fakeQS{}
		tg := newTarget(qs)
		require.NoError(t, tg.CheckDropPrivilege(context.Background(), []string{"public.orders"}, ""))
		require.Empty(t, qs.adminArgs, "no query should run when callerRole is empty")
	})

	t.Run("no tables is a no-op", func(t *testing.T) {
		qs := &fakeQS{}
		tg := newTarget(qs)
		require.NoError(t, tg.CheckDropPrivilege(context.Background(), nil, "app"))
		require.Empty(t, qs.adminArgs)
	})

	const checkSQL = "SELECT t.tbl FROM (VALUES ('public.orders')) AS t(tbl)" +
		" JOIN pg_class c ON c.oid = to_regclass(t.tbl)" +
		" JOIN pg_namespace n ON n.oid = c.relnamespace" +
		" WHERE NOT (pg_has_role($1::name, c.relowner, 'USAGE') OR pg_has_role($1::name, n.nspowner, 'USAGE'))"

	t.Run("no forbidden rows: caller may drop every existing table", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{checkSQL: emptyResult()}}
		tg := newTarget(qs)

		require.NoError(t, tg.CheckDropPrivilege(context.Background(), []string{"public.orders"}, "app"))

		require.Len(t, qs.adminArgs, 1)
		require.Equal(t, []any{"app"}, qs.adminArgs[0].args)
	})

	t.Run("a table the caller may not drop rejects with insufficient_privilege", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
			checkSQL: {Rows: []*sqltypes.Row{{Values: []sqltypes.Value{sqltypes.Value("public.orders")}}}},
		}}
		tg := newTarget(qs)

		err := tg.CheckDropPrivilege(context.Background(), []string{"public.orders"}, "app")

		require.Error(t, err)
		require.Equal(t, "42501", mterrors.ExtractSQLSTATE(err))
		require.ErrorContains(t, err, "public.orders")
		require.ErrorContains(t, err, `"app"`)
	})
}

// TestCheckRoleOwnership is the regression test for the HIGH finding this
// check guards against: a named Connection or Migration carries no ACL of its
// own, so without it any role that passes the gateway's CanCreateMigration
// gate could name a resource it did not create and act on it with that
// resource's privileges or data. Callers never surface this function's own
// error message directly — each wraps a non-nil result in its own
// resource-appropriate message (e.g. CreateMigration's "connection %q does
// not exist") — so this function's message is free to name both roles.
func TestCheckRoleOwnership(t *testing.T) {
	const checkSQL = "SELECT 1 WHERE NOT pg_has_role($1::name, $2::name, 'USAGE')"

	t.Run("caller has usage on the owner role: allowed", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{checkSQL: emptyResult()}}
		tg := newTarget(qs)

		require.NoError(t, tg.CheckRoleOwnership(context.Background(), "alice", "alice"))

		require.Len(t, qs.adminArgs, 1)
		require.Equal(t, []any{"alice", "alice"}, qs.adminArgs[0].args)
	})

	t.Run("caller lacks usage on the owner role: rejected", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
			checkSQL: {Rows: []*sqltypes.Row{{Values: []sqltypes.Value{sqltypes.Value("1")}}}},
		}}
		tg := newTarget(qs)

		err := tg.CheckRoleOwnership(context.Background(), "mallory", "alice")

		require.Error(t, err)
		require.Equal(t, "42501", mterrors.ExtractSQLSTATE(err))
		require.ErrorContains(t, err, `"mallory"`)
	})
}

func TestCreateLogicalSlot_RequestsFailover(t *testing.T) {
	qs := &fakeQS{}
	tg := newTarget(qs)

	require.NoError(t, tg.CreateLogicalSlot(context.Background(), "mt_sub_1"))

	require.Len(t, qs.adminArgs, 1)
	require.Equal(t, "SELECT pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)",
		qs.adminArgs[0].sql)
	require.Equal(t, []any{"mt_sub_1"}, qs.adminArgs[0].args)
}

func TestWaitSlotConfirmed(t *testing.T) {
	const slotConfirmedSQL = "SELECT confirmed_flush_lsn >= $1::pg_lsn FROM pg_replication_slots WHERE slot_name = $2"

	t.Run("missing slot fails fast, not retried", func(t *testing.T) {
		// emptyResult() (0 rows) is what fakeQS returns for any un-scripted query —
		// exactly what pg_replication_slots reports once the slot is gone, so this
		// also doubles as the regression case for the old "no rows == not yet"
		// behavior that polled forever instead of erroring.
		qs := &fakeQS{}
		tg := newTarget(qs)

		err := tg.WaitSlotConfirmed(context.Background(), "mt_sub_1", "0/100")
		require.ErrorContains(t, err, `replication slot "mt_sub_1" is missing`)
	})

	t.Run("not yet confirmed keeps polling until ctx is done", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
			slotConfirmedSQL: {Rows: []*sqltypes.Row{{Values: []sqltypes.Value{sqltypes.Value("f")}}}},
		}}
		tg := newTarget(qs)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err := tg.WaitSlotConfirmed(ctx, "mt_sub_1", "0/100")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("confirmed returns nil", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
			slotConfirmedSQL: {Rows: []*sqltypes.Row{{Values: []sqltypes.Value{sqltypes.Value("t")}}}},
		}}
		tg := newTarget(qs)

		require.NoError(t, tg.WaitSlotConfirmed(context.Background(), "mt_sub_1", "0/100"))
	})
}

func TestSlotReady(t *testing.T) {
	const slotReadySQL = "SELECT NOT temporary AND invalidation_reason IS NULL FROM pg_replication_slots WHERE slot_name = $1"

	cases := []struct {
		name string
		rows []*sqltypes.Row
		want bool
	}{
		{"missing slot is not ready", nil, false},
		{"ready", []*sqltypes.Row{{Values: []sqltypes.Value{sqltypes.Value("t")}}}, true},
		{"not ready (still temporary or invalidated)", []*sqltypes.Row{{Values: []sqltypes.Value{sqltypes.Value("f")}}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
				slotReadySQL: {Rows: c.rows},
			}}
			tg := newTarget(qs)
			ready, err := tg.SlotReady(context.Background(), "mt_sub_1")
			require.NoError(t, err)
			require.Equal(t, c.want, ready)
		})
	}
}

func TestCreateSubscriptionSQL(t *testing.T) {
	dsn := "host=gw port=5432 dbname=postgres"

	// Default (slotName ""): the subscription creates its own slot. Safe
	// lowercase identifiers are not quoted.
	got := createSubscriptionSQL("mt_sub", dsn, "mt_pub", true, "")
	require.Contains(t, got, "CREATE SUBSCRIPTION mt_sub ")
	require.Contains(t, got, "PUBLICATION mt_pub ")
	require.Contains(t, got, "copy_data = true")
	require.NotContains(t, got, "create_slot")
	require.NotContains(t, got, "slot_name")

	// EXPORT reverse subscription: attaches to a pre-created slot, no copy.
	got = createSubscriptionSQL("mt_sub", dsn, "mt_pub", false, "mt_sub")
	require.Contains(t, got, "copy_data = false")
	require.Contains(t, got, "create_slot = false")
	require.Contains(t, got, "slot_name = mt_sub")
}

// TestCreateSubscription_UsesNoQueryTextPath is the regression test for the
// HIGH finding this guards against: the CONNECTION clause embeds conninfo
// as-is, password included, and QueryAdmin's blanket tracing policy
// (IncludeQueryText) would otherwise attach that plaintext password to a
// span as db.query.text. CreateSubscription must route through
// QueryAdminNoQueryText instead, never QueryAdmin.
func TestCreateSubscription_UsesNoQueryTextPath(t *testing.T) {
	qs := &fakeQS{}
	tg := newTarget(qs)

	dsn := "host=src port=5432 dbname=app user=app password=supersecret"
	require.NoError(t, tg.CreateSubscription(context.Background(), "mt_sub", dsn, "mt_pub", true))

	require.Empty(t, qs.adminQueries, "must not go through the traced QueryAdmin path")
	require.Len(t, qs.noQueryTextQueries, 1)
	require.Contains(t, qs.noQueryTextQueries[0], "supersecret", "sanity check: this is indeed the statement carrying the password")
}
