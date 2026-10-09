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
	"fmt"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// target runs the local (Multigres) Postgres side of a migration through the
// admin/superuser pool. CREATE/DROP SUBSCRIPTION and CREATE/DROP PUBLICATION
// cannot run inside a transaction block, so they use the single-statement
// autocommit admin path (QueryAdmin), and the admin pool authenticates as a
// true superuser, which CREATE SUBSCRIPTION requires.
type target struct {
	qs executor.InternalQueryService
}

func newTarget(qs executor.InternalQueryService) *target { return &target{qs: qs} }

// ApplySchema applies schema SQL (pg_dump --schema-only output, psql backslash
// meta-commands already stripped) on the local Postgres.
func (t *target) ApplySchema(ctx context.Context, schemaSQL string) error {
	if err := t.qs.QueryAdminMultiStatement(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

// CheckDropPrivilege verifies, for every table in tables that already exists
// on the target, that callerRole could drop it under PostgreSQL's own
// privilege model. DROP TABLE is not governed by a GRANT-able ACL privilege —
// 'DROP' is not one of has_table_privilege's privilege types, so checking it
// there errors outright ("unrecognized privilege type"), confirmed against a
// live server — it requires owning the table, owning its schema (schema
// ownership grants drop rights over tables inside it that the schema owner
// does not themselves own, confirmed empirically), or having the privileges
// of a role that does, which pg_has_role's USAGE check covers for either. A
// table that does not exist yet is skipped: DropTables is a no-op for it, so
// there is nothing to protect. An empty callerRole means the call did not
// come through the gateway's SQL identity-checked path and the check is
// skipped, unchanged from before this check existed.
//
// This guards the gap the gateway's CanCreateMigration gate deliberately
// leaves open: that gate only confirms the caller could perform
// logical-replication setup in principle, not that it owns the specific
// tables this migration names. Without this, such a role could name an
// existing table it does not own and have DropTables destroy it (CASCADE, so
// dependents too) via the admin pool, bypassing Postgres's own ownership
// check entirely.
func (t *target) CheckDropPrivilege(ctx context.Context, tables []string, callerRole string) error {
	if callerRole == "" || len(tables) == 0 {
		return nil
	}
	sql := "SELECT t.tbl FROM " + tablesValuesClause(tables) +
		" JOIN pg_class c ON c.oid = to_regclass(t.tbl)" +
		" JOIN pg_namespace n ON n.oid = c.relnamespace" +
		" WHERE NOT (pg_has_role($1::name, c.relowner, 'USAGE') OR pg_has_role($1::name, n.nspowner, 'USAGE'))"
	res, err := t.qs.QueryAdminArgs(ctx, sql, callerRole)
	if err != nil {
		return fmt.Errorf("check drop privilege: %w", err)
	}
	if res == nil || len(res.Rows) == 0 {
		return nil
	}
	forbidden := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		var tbl string
		if err := executor.ScanRow(row, &tbl); err != nil {
			return fmt.Errorf("scan drop-privilege check result: %w", err)
		}
		forbidden = append(forbidden, tbl)
	}
	return mterrors.NewPgError("ERROR", mterrors.PgSSInsufficientPrivilege,
		fmt.Sprintf("role %q lacks DROP privilege on existing target table(s): %s", callerRole, strings.Join(forbidden, ", ")), "")
}

// CheckRoleOwnership verifies that callerRole could act as ownerRole, using
// pg_has_role's USAGE check, which covers both the creator itself and any
// role whose privileges include it. Shared by every create-time ownership
// gate in this package: a named Connection or Migration carries no ACL of
// its own, so without this, a role that passes the gateway's
// CanCreateMigration gate could act on a resource it did not create.
// Callers skip this when either role is empty, and wrap a non-nil error in
// their own resource-appropriate message.
func (t *target) CheckRoleOwnership(ctx context.Context, callerRole, ownerRole string) error {
	sql := "SELECT 1 WHERE NOT pg_has_role($1::name, $2::name, 'USAGE')"
	res, err := t.qs.QueryAdminArgs(ctx, sql, callerRole, ownerRole)
	if err != nil {
		return fmt.Errorf("check role ownership: %w", err)
	}
	if res != nil && len(res.Rows) > 0 {
		return mterrors.NewPgError("ERROR", mterrors.PgSSInsufficientPrivilege,
			fmt.Sprintf("role %q lacks privilege to act as %q", callerRole, ownerRole), "")
	}
	return nil
}

// DropTables drops the migrated tables on the local Postgres before a schema
// copy, so a pre-existing target table does not fail the copy with "relation
// already exists". IF EXISTS makes it safe when they are absent; CASCADE
// removes dependent objects (indexes, FKs, views) that would otherwise block
// the drop. Never called when SkipSchemaCopy keeps an out-of-band-seeded
// target — but CheckDropPrivilege still runs unconditionally in that case,
// since the subscription created afterward writes into these tables
// regardless of whether schema copy ran.
func (t *target) DropTables(ctx context.Context, tables []string) error {
	stmt := dropTablesSQL(tables)
	if stmt == "" {
		return nil
	}
	if _, err := t.qs.QueryAdmin(ctx, stmt); err != nil {
		return fmt.Errorf("drop target tables before schema copy: %w", err)
	}
	return nil
}

// dropTablesSQL builds a single DROP TABLE IF EXISTS ... CASCADE over the given
// schema-qualified tables, or "" when there are none.
func dropTablesSQL(tables []string) string {
	if len(tables) == 0 {
		return ""
	}
	quoted := make([]string, len(tables))
	for i, tbl := range tables {
		quoted[i] = quoteQualifiedName(tbl)
	}
	return "DROP TABLE IF EXISTS " + strings.Join(quoted, ", ") + " CASCADE"
}

// DisableUserTriggers disables every user-defined trigger on each migrated
// table via ALTER TABLE ... DISABLE TRIGGER USER, so none of the schema
// copy's triggers can ever fire on the target. ApplySchema creates the
// trigger function as target admin, so a SECURITY DEFINER function with an
// arbitrary body is now owned by target admin regardless of fire mode —
// ENABLE ALWAYS/REPLICA fires during the subscription's apply worker, and the
// default ORIGIN mode fires on ordinary writes once the migration completes.
// Disabling outright is the only fire-mode-independent fix. Called right
// after ApplySchema, before the subscription exists.
//
// This supersedes an earlier attempt to strip the dangerous ALTER TABLE
// statement from pg_dump's own text: a trigger name with an embedded newline
// defeats any line-based filter, letting attacker-injected text past it.
// Disabling the trigger as Postgres already parsed it is immune to that.
// Unlike DISABLE TRIGGER ALL, USER leaves internal FK triggers alone.
//
// There is no way to distinguish a reviewed, trusted trigger from an
// adversarial one automatically, so the safe default is to disable every
// user trigger unconditionally; an operator can re-enable a specific one by
// name after reviewing it.
func (t *target) DisableUserTriggers(ctx context.Context, tables []string) error {
	for _, tbl := range tables {
		stmt := "ALTER TABLE " + quoteQualifiedName(tbl) + " DISABLE TRIGGER USER"
		if _, err := t.qs.QueryAdmin(ctx, stmt); err != nil {
			return fmt.Errorf("disable user triggers on %s: %w", tbl, err)
		}
	}
	return nil
}

// DropUserCheckConstraints drops every CHECK constraint on each migrated
// table. PostgreSQL has no DISABLE for a CHECK constraint, and it is evaluated
// on every INSERT/UPDATE unconditionally, including by the import
// subscription's apply worker — so DROP is the only way to close the same
// SECURITY DEFINER vector DisableUserTriggers closes for triggers: a CHECK
// expression can call a function that ApplySchema created as target admin,
// letting a source table owner run admin-privileged code on every replicated
// row via the values it writes.
//
// Dropped unconditionally, not just constraints a catalog query can identify
// as calling a risky function: there is no automatic way to distinguish a
// reviewed, trusted constraint from an adversarial one, and a catalog-based
// heuristic is as incomplete as the text-based one that already failed for
// triggers (see DisableUserTriggers). An operator can recreate a specific
// check by hand after reviewing it. Called right after DisableUserTriggers.
func (t *target) DropUserCheckConstraints(ctx context.Context, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	sql := "SELECT t.tbl, c.conname FROM " + tablesValuesClause(tables) +
		" JOIN pg_constraint c ON c.conrelid = to_regclass(t.tbl) AND c.contype = 'c'"
	res, err := t.qs.QueryAdmin(ctx, sql)
	if err != nil {
		return fmt.Errorf("list check constraints: %w", err)
	}
	if res == nil {
		return nil
	}
	for _, row := range res.Rows {
		var tbl, conname string
		if err := executor.ScanRow(row, &tbl, &conname); err != nil {
			return fmt.Errorf("scan check constraint: %w", err)
		}
		stmt := "ALTER TABLE " + quoteQualifiedName(tbl) + " DROP CONSTRAINT " + ast.QuoteIdentifier(conname)
		if _, err := t.qs.QueryAdmin(ctx, stmt); err != nil {
			return fmt.Errorf("drop check constraint %q on %s: %w", conname, tbl, err)
		}
	}
	return nil
}

// DisableUserRewriteRules disables every rewrite rule on each migrated table
// via ALTER TABLE ... DISABLE RULE, closing the same SECURITY DEFINER vector
// for rules: a rule's action can run arbitrary DML, and unlike a CHECK
// constraint, PostgreSQL does support disabling one — ev_enabled uses the
// same O/D/R/A scheme as a trigger's tgenabled, so a disabled rule never
// fires in any role, matching DisableUserTriggers' guarantee.
//
// Disabled unconditionally, like DisableUserTriggers and
// DropUserCheckConstraints: there is no automatic way to distinguish a
// reviewed, trusted rule from an adversarial one. An operator can re-enable a
// specific rule by name after reviewing it. '_RETURN' (the rule a view
// depends on, always firing regardless of enable state) is excluded — it
// cannot appear on a migrated table, only a view, but excluding it keeps this
// safe if that ever changes.
func (t *target) DisableUserRewriteRules(ctx context.Context, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	sql := "SELECT t.tbl, r.rulename FROM " + tablesValuesClause(tables) +
		" JOIN pg_rewrite r ON r.ev_class = to_regclass(t.tbl) WHERE r.rulename <> '_RETURN'"
	res, err := t.qs.QueryAdmin(ctx, sql)
	if err != nil {
		return fmt.Errorf("list rewrite rules: %w", err)
	}
	if res == nil {
		return nil
	}
	for _, row := range res.Rows {
		var tbl, rulename string
		if err := executor.ScanRow(row, &tbl, &rulename); err != nil {
			return fmt.Errorf("scan rewrite rule: %w", err)
		}
		stmt := "ALTER TABLE " + quoteQualifiedName(tbl) + " DISABLE RULE " + ast.QuoteIdentifier(rulename)
		if _, err := t.qs.QueryAdmin(ctx, stmt); err != nil {
			return fmt.Errorf("disable rewrite rule %q on %s: %w", rulename, tbl, err)
		}
	}
	return nil
}

// DropUserDomainChecks drops every CHECK constraint on a domain used by a
// column of any migrated table — including a domain[] array column, whose
// pg_attribute.atttypid is the array type rather than the domain itself,
// resolved here via typelem — closing the same SECURITY DEFINER vector
// DropUserCheckConstraints closes for table-level CHECK constraints: a
// domain's CHECK expression can call a function ApplySchema created as
// target admin, and it evaluates on every write to a column of that domain
// type, including replicated ones — confirmed against a live server, firing
// under session_replication_role=replica and with user triggers disabled.
// DropUserCheckConstraints does not catch this: a domain constraint's
// pg_constraint row has contypid set (the domain's own pg_type oid), not
// conrelid (a table oid), so it is invisible to a query scoped by table.
// PostgreSQL has no DISABLE for a domain constraint either, so DROP is the
// only fire-mode-independent fix.
//
// Unlike every other sanitizer in this file, a domain is not table-scoped: it
// is a type, and ALTER DOMAIN ... DROP CONSTRAINT disables its CHECK for
// every column using it, anywhere in the database — confirmed against a live
// server that this silently let an invalid value through on a completely
// unrelated, non-migrated table that happened to share the domain. Dropping
// it unconditionally, the way the table-scoped sanitizers do, would let a
// caller authorized to migrate one table degrade data integrity on tables it
// has no rights over. So this refuses instead of dropping whenever a found
// domain is also used by a column outside the migrated table set, with a
// FeatureNotSupported error — consistent with failing loudly rather than
// silently doing something unsafe — so an operator can resolve the conflict
// explicitly (e.g. migrate the other tables too) instead of losing an
// unrelated table's constraint without warning.
//
// The migrated CTE's tbl_oid is to_regclass(t.tbl), which is NULL for a named
// table that does not exist yet on the target (e.g. with SkipSchemaCopy and a
// typo, or a table not yet created out of band). A NULL in that NOT IN
// subquery poisons it for every row — confirmed against a live server that a
// single non-existent table in tables made shared_outside evaluate to false
// for every domain, including one genuinely shared with an unrelated table,
// because `x NOT IN (..., NULL)` is never true in SQL's three-valued logic.
// Filtering it out with WHERE tbl_oid IS NOT NULL restores the guard; a
// missing table otherwise still surfaces as a build failure later in
// runSetup, just not from this check.
func (t *target) DropUserDomainChecks(ctx context.Context, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	sql := "WITH migrated AS (SELECT to_regclass(t.tbl) AS tbl_oid FROM " + tablesValuesClause(tables) + ")" +
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
	res, err := t.qs.QueryAdmin(ctx, sql)
	if err != nil {
		return fmt.Errorf("list domain check constraints: %w", err)
	}
	if res == nil {
		return nil
	}
	type domainConstraint struct{ domain, conname string }
	var toDrop []domainConstraint
	var shared []string
	for _, row := range res.Rows {
		var nspname, typname, conname string
		var sharedOutside bool
		if err := executor.ScanRow(row, &nspname, &typname, &conname, &sharedOutside); err != nil {
			return fmt.Errorf("scan domain check constraint: %w", err)
		}
		domain := quoteQualifiedName(nspname + "." + typname)
		if sharedOutside {
			shared = append(shared, domain)
			continue
		}
		toDrop = append(toDrop, domainConstraint{domain: domain, conname: conname})
	}
	if len(shared) > 0 {
		return mterrors.NewFeatureNotSupported(
			fmt.Sprintf("domain(s) %s are used by column(s) outside the migrated tables; migrating a table whose domain is shared with un-migrated tables is not supported", strings.Join(shared, ", ")),
		)
	}
	for _, dc := range toDrop {
		stmt := "ALTER DOMAIN " + dc.domain + " DROP CONSTRAINT " + ast.QuoteIdentifier(dc.conname)
		if _, err := t.qs.QueryAdmin(ctx, stmt); err != nil {
			return fmt.Errorf("drop domain check constraint %q on %s: %w", dc.conname, dc.domain, err)
		}
	}
	return nil
}

// DropUserExpressionIndexes drops every expression or partial index on each
// migrated table, the same SECURITY DEFINER vector again for index
// maintenance: an index expression or predicate can call a function
// ApplySchema created as target admin, and it evaluates on every write to
// the indexed table. PostgreSQL requires an index expression/predicate
// function to be marked IMMUTABLE, which blocks an actual write from inside
// it at runtime — but not a read of other tables, so it can still leak
// privileged data back to the writer (confirmed against a live server: a
// SECURITY DEFINER, IMMUTABLE-declared function reading a PUBLIC-revoked
// table and re-raising its contents in an exception message, invoked from
// both an expression and a partial-index predicate). There is no DISABLE for
// an index, so DROP is the only fix, applied unconditionally like
// DropUserCheckConstraints. An expression/partial index backing a
// UNIQUE/PRIMARY KEY/EXCLUDE constraint cannot be dropped directly (DROP
// INDEX refuses it); ALTER TABLE ... DROP CONSTRAINT is used instead, which
// drops the backing index too — losing that constraint's enforcement is the
// accepted cost of closing the vector, like dropping a plain CHECK
// constraint. Called alongside DropUserCheckConstraints.
func (t *target) DropUserExpressionIndexes(ctx context.Context, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	sql := "SELECT t.tbl, n.nspname, i.relname, con.conname FROM " + tablesValuesClause(tables) +
		" JOIN pg_index ix ON ix.indrelid = to_regclass(t.tbl)" +
		" JOIN pg_class i ON i.oid = ix.indexrelid" +
		" JOIN pg_namespace n ON n.oid = i.relnamespace" +
		" LEFT JOIN pg_constraint con ON con.conindid = ix.indexrelid" +
		" WHERE ix.indexprs IS NOT NULL OR ix.indpred IS NOT NULL"
	res, err := t.qs.QueryAdmin(ctx, sql)
	if err != nil {
		return fmt.Errorf("list expression/predicate indexes: %w", err)
	}
	if res == nil {
		return nil
	}
	for _, row := range res.Rows {
		var tbl, nspname, idxname string
		var conname *string
		if err := executor.ScanRow(row, &tbl, &nspname, &idxname, &conname); err != nil {
			return fmt.Errorf("scan expression/predicate index: %w", err)
		}
		var stmt string
		if conname != nil && *conname != "" {
			stmt = "ALTER TABLE " + quoteQualifiedName(tbl) + " DROP CONSTRAINT " + ast.QuoteIdentifier(*conname)
		} else {
			stmt = "DROP INDEX " + quoteQualifiedName(nspname+"."+idxname)
		}
		if _, err := t.qs.QueryAdmin(ctx, stmt); err != nil {
			return fmt.Errorf("drop expression/predicate index %q on %s: %w", idxname, tbl, err)
		}
	}
	return nil
}

// DropUserGeneratedColumns converts every GENERATED ALWAYS AS (...) STORED
// column on each migrated table into a plain column via ALTER TABLE ... ALTER
// COLUMN ... DROP EXPRESSION, the same SECURITY DEFINER vector again: logical
// replication does not transmit a generated column's value, so the
// subscriber recomputes it locally on every replicated row — confirmed
// against a live server, a SECURITY DEFINER, IMMUTABLE-declared function
// reading a PUBLIC-revoked table from a generated expression leaked its
// contents the same way an expression index does. There is no DISABLE for a
// generated column's expression, so DROP EXPRESSION is the only fix. It
// leaves the column NULL for every row written afterward, including
// replicated ones — the same accepted-enforcement-loss cost as dropping a
// CHECK constraint; an operator can recompute and reinstate a trusted
// expression after reviewing it. Called alongside DropUserExpressionIndexes.
func (t *target) DropUserGeneratedColumns(ctx context.Context, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	sql := "SELECT t.tbl, a.attname FROM " + tablesValuesClause(tables) +
		" JOIN pg_attribute a ON a.attrelid = to_regclass(t.tbl) AND a.attnum > 0 AND NOT a.attisdropped" +
		" WHERE a.attgenerated <> ''"
	res, err := t.qs.QueryAdmin(ctx, sql)
	if err != nil {
		return fmt.Errorf("list generated columns: %w", err)
	}
	if res == nil {
		return nil
	}
	for _, row := range res.Rows {
		var tbl, attname string
		if err := executor.ScanRow(row, &tbl, &attname); err != nil {
			return fmt.Errorf("scan generated column: %w", err)
		}
		stmt := "ALTER TABLE " + quoteQualifiedName(tbl) + " ALTER COLUMN " + ast.QuoteIdentifier(attname) + " DROP EXPRESSION"
		if _, err := t.qs.QueryAdmin(ctx, stmt); err != nil {
			return fmt.Errorf("drop generated expression on %s.%s: %w", tbl, attname, err)
		}
	}
	return nil
}

// CreatePublication creates a publication FOR TABLE the given tables on the
// local Postgres. Used when this side is the publisher (EXPORT direction).
func (t *target) CreatePublication(ctx context.Context, name string, tables []string) error {
	if _, err := t.qs.QueryAdmin(ctx, createPublicationSQL(name, tables)); err != nil {
		return fmt.Errorf("create publication: %w", err)
	}
	return nil
}

// DropPublication drops a publication on the local Postgres. Idempotent.
func (t *target) DropPublication(ctx context.Context, name string) error {
	stmt := "DROP PUBLICATION IF EXISTS " + ast.QuoteIdentifier(name)
	if _, err := t.qs.QueryAdmin(ctx, stmt); err != nil {
		return fmt.Errorf("drop publication: %w", err)
	}
	return nil
}

// CreateSubscription creates a logical-replication subscription on the local
// Postgres. conninfo is the source DSN, evaluated by the local Postgres. Uses
// QueryAdminNoQueryText, not QueryAdmin: the statement's quoted CONNECTION
// clause carries conninfo as-is, password included, and QueryAdmin's blanket
// IncludeQueryText tracing would otherwise attach that plaintext password to
// a span as db.query.text — confirmed against the tracing layer's own
// semconv.DBQueryText call.
func (t *target) CreateSubscription(ctx context.Context, name, conninfo, publication string, copyData bool) error {
	// create_slot defaults on (slotName ""): the forward IMPORT subscription and
	// the EXPORT->IMPORT reverse subscription both make their own slot on the
	// external source they dial. Only the IMPORT->EXPORT reverse subscription,
	// which dials the target through the gateway, attaches to a pre-created slot.
	if _, err := t.qs.QueryAdminNoQueryText(ctx, createSubscriptionSQL(name, conninfo, publication, copyData, "")); err != nil {
		return fmt.Errorf("create subscription: %w", err)
	}
	return nil
}

// DropSubscription drops a subscription (releasing its slot on the source) on
// the local Postgres. Idempotent.
func (t *target) DropSubscription(ctx context.Context, name string) error {
	stmt := "DROP SUBSCRIPTION IF EXISTS " + ast.QuoteIdentifier(name)
	if _, err := t.qs.QueryAdmin(ctx, stmt); err != nil {
		return fmt.Errorf("drop subscription: %w", err)
	}
	return nil
}

// SubscriptionStatus is the derived view of a subscription's progress, read live
// from pg_subscription_rel and pg_stat_subscription (never persisted).
type SubscriptionStatus struct {
	TotalRelations int64
	ReadyRelations int64
	CaughtUp       bool
	ReceivedLSN    string
	LatestEndLSN   string
	// LagBytes / LagSeconds are the live replication lag measured on the current
	// publisher (source in IMPORT, target in EXPORT). They are NOT filled by
	// SubscriptionStatus (which is purely subscription-derived); Coordinator.liveStatus
	// populates them best-effort via ReplicationLag so the projection can surface them.
	LagBytes   uint64
	LagSeconds float64
}

const subscriptionStatusSQL = `SELECT count(r.*),
		        count(r.*) FILTER (WHERE r.srsubstate = 'r'),
		        coalesce(max(ss.received_lsn)::text, ''),
		        coalesce(max(ss.latest_end_lsn)::text, '')
		 FROM pg_subscription s
		 LEFT JOIN pg_subscription_rel r ON r.srsubid = s.oid
		 LEFT JOIN pg_stat_subscription ss ON ss.subid = s.oid AND ss.relid IS NULL
		 WHERE s.subname = $1`

// SubscriptionStatus reads a subscription's copy/stream progress via the admin
// pool in a single query.
//
// The per-relation counts come from pg_subscription_rel; the stream position
// comes from the main apply worker's pg_stat_subscription row (relid IS NULL —
// tablesync workers have relid set). That row is absent until the apply worker
// connects, so it is LEFT JOINed and the LSNs coalesce to the empty string.
// Because it is an aggregate with no GROUP BY, the query returns exactly one row
// even for a missing/empty subscription (zero counts, empty LSNs), so the caller
// never special-cases a missing row.
func (t *target) SubscriptionStatus(ctx context.Context, name string) (*SubscriptionStatus, error) {
	res, err := t.qs.QueryAdminArgs(ctx, subscriptionStatusSQL, name)
	if err != nil {
		return nil, fmt.Errorf("read subscription status: %w", err)
	}
	st := &SubscriptionStatus{}
	if err := executor.ScanSingleRow(res, &st.TotalRelations, &st.ReadyRelations, &st.ReceivedLSN, &st.LatestEndLSN); err != nil {
		return nil, fmt.Errorf("scan subscription status: %w", err)
	}
	st.CaughtUp = st.TotalRelations > 0 && st.ReadyRelations == st.TotalRelations
	return st, nil
}

// SubscriptionExists reports whether a subscription with the given name exists on
// the local Postgres. Used to make the direction switch idempotent on resume.
func (t *target) SubscriptionExists(ctx context.Context, name string) (bool, error) {
	return t.existsCount(ctx, "SELECT count(*) FROM pg_subscription WHERE subname = $1", name)
}

// PublicationExists reports whether a publication with the given name exists.
func (t *target) PublicationExists(ctx context.Context, name string) (bool, error) {
	return t.existsCount(ctx, "SELECT count(*) FROM pg_publication WHERE pubname = $1", name)
}

func (t *target) existsCount(ctx context.Context, sql, arg string) (bool, error) {
	res, err := t.qs.QueryAdminArgs(ctx, sql, arg)
	if err != nil {
		return false, err
	}
	var n int64
	if err := executor.ScanSingleRow(res, &n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// CurrentLSN returns the local Postgres's current WAL LSN (call on a
// publisher/primary, e.g. the target in EXPORT direction).
func (t *target) CurrentLSN(ctx context.Context) (string, error) {
	res, err := t.qs.QueryAdmin(ctx, "SELECT pg_current_wal_lsn()::text")
	if err != nil {
		return "", fmt.Errorf("read target LSN: %w", err)
	}
	var lsn string
	if err := executor.ScanSingleRow(res, &lsn); err != nil {
		return "", fmt.Errorf("scan target LSN: %w", err)
	}
	return lsn, nil
}

// SlotExists reports whether a replication slot of the given name exists locally.
func (t *target) SlotExists(ctx context.Context, name string) (bool, error) {
	return t.existsCount(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", name)
}

// CreateLogicalSlot creates a logical replication slot (pgoutput plugin) on the
// local Postgres. Used to pre-create the EXPORT reverse slot during the switch,
// before serving turns on, so it captures every subsequent target write; the
// reverse subscription later attaches to it with create_slot=false. Requests
// failover (the slot-sync flag): harmless without
// --enable-slot-based-replication, but what lets the slot survive a
// target-primary failover when that flag is on.
func (t *target) CreateLogicalSlot(ctx context.Context, name string) error {
	if _, err := t.qs.QueryAdminArgs(ctx,
		"SELECT pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)", name); err != nil {
		return fmt.Errorf("create logical slot %q: %w", name, err)
	}
	return nil
}

// AdvanceSlot moves the named logical slot forward to targetLSN (forward-only, per
// pg_replication_slot_advance), so it starts delivering exactly at the handoff
// point and skips the switch's own catalog WAL. Call while the slot is inactive.
func (t *target) AdvanceSlot(ctx context.Context, name, targetLSN string) error {
	if _, err := t.qs.QueryAdminArgs(ctx, "SELECT pg_replication_slot_advance($1, $2::pg_lsn)", name, targetLSN); err != nil {
		return fmt.Errorf("advance slot %q to %s: %w", name, targetLSN, err)
	}
	return nil
}

// DropLogicalSlot drops the named replication slot if it exists. Used to tear
// down the EXPORT reverse slot (create_slot=false means dropping the subscription
// does not drop it).
func (t *target) DropLogicalSlot(ctx context.Context, name string) error {
	if exists, err := t.SlotExists(ctx, name); err != nil {
		return err
	} else if !exists {
		return nil
	}
	if _, err := t.qs.QueryAdminArgs(ctx, "SELECT pg_drop_replication_slot($1)", name); err != nil {
		return fmt.Errorf("drop slot %q: %w", name, err)
	}
	return nil
}

// ReplicationLag returns the current replication lag for the named slot on
// the local (target) Postgres when it is the publisher: byte lag =
// pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) and time lag =
// the walsender's replay_lag in seconds. present is false when the slot does
// not exist. Mirrors source.ReplicationLag for the reverse direction.
func (t *target) ReplicationLag(ctx context.Context, slot string) (lagBytes uint64, lagSeconds float64, present bool, err error) {
	const lagSQL = `SELECT
		GREATEST(pg_wal_lsn_diff(pg_current_wal_lsn(), sl.confirmed_flush_lsn), 0)::bigint,
		COALESCE(EXTRACT(EPOCH FROM sr.replay_lag), 0)::float8
	FROM pg_replication_slots sl
	LEFT JOIN pg_stat_replication sr ON sr.pid = sl.active_pid
	WHERE sl.slot_name = $1`
	res, err := t.qs.QueryAdminArgs(ctx, lagSQL, slot)
	if err != nil {
		return 0, 0, false, fmt.Errorf("read target replication lag for slot %q: %w", slot, err)
	}
	if res == nil || len(res.Rows) == 0 {
		return 0, 0, false, nil
	}
	var b int64
	var secs float64
	if err := executor.ScanSingleRow(res, &b, &secs); err != nil {
		return 0, 0, false, fmt.Errorf("scan target replication lag: %w", err)
	}
	return uint64(b), secs, true, nil
}

// WaitSlotConfirmed blocks until the named local replication slot has
// confirmed_flush_lsn >= targetLSN, or ctx is done. Call on the publisher side
// (target in EXPORT direction).
func (t *target) WaitSlotConfirmed(ctx context.Context, slot, targetLSN string) error {
	return pollSlotConfirmed(ctx, func(c context.Context) (bool, error) {
		res, err := t.qs.QueryAdminArgs(c,
			"SELECT confirmed_flush_lsn >= $1::pg_lsn FROM pg_replication_slots WHERE slot_name = $2",
			targetLSN, slot)
		if err != nil {
			return false, err
		}
		if res == nil || len(res.Rows) == 0 {
			// Absent is terminal, not "not yet": a slot that once existed and is
			// now gone (e.g. lost in a target-primary failover without
			// --enable-slot-based-replication) will never reappear on its own, so
			// polling for it would just stall until the caller's deadline.
			return false, fmt.Errorf("replication slot %q is missing — it may have been lost in a "+
				"target-primary failover; enable --enable-slot-based-replication so the slot survives failover",
				slot)
		}
		var ok bool
		if err := executor.ScanSingleRow(res, &ok); err != nil {
			return false, err
		}
		return ok, nil
	})
}

// SlotReady reports whether the named replication slot exists and is usable —
// not temporary (synced-but-not-yet-persisted, see CreateLogicalSlot; PostgreSQL
// drops a still-temporary synced slot at promotion) and not invalidated (e.g.
// fell too far behind). Either failure mode leaves the slot equally unusable, so
// callers that find it not ready should drop and recreate it rather than try to
// repair it in place. Deliberately does not check the synced column: that is a
// standby-local concept, and this is called against whichever node is currently
// primary.
func (t *target) SlotReady(ctx context.Context, name string) (bool, error) {
	res, err := t.qs.QueryAdminArgs(ctx,
		"SELECT NOT temporary AND invalidation_reason IS NULL FROM pg_replication_slots WHERE slot_name = $1", name)
	if err != nil {
		return false, fmt.Errorf("check slot %q readiness: %w", name, err)
	}
	if res == nil || len(res.Rows) == 0 {
		return false, nil
	}
	var ready bool
	if err := executor.ScanSingleRow(res, &ready); err != nil {
		return false, err
	}
	return ready, nil
}

// AdvanceSequences sets each migrated table's owned sequences past their current
// max plus margin on the local Postgres, so the target does not collide with
// copied values when it becomes a writer.
func (t *target) AdvanceSequences(ctx context.Context, tables []string, margin int64) error {
	for _, tbl := range tables {
		res, err := t.qs.QueryAdminArgs(ctx, ownedSequencesSQL, tbl)
		if err != nil {
			return fmt.Errorf("list sequences for %q: %w", tbl, err)
		}
		if res == nil {
			continue
		}
		for _, row := range res.Rows {
			var col string
			var seq *string
			if err := executor.ScanRow(row, &col, &seq); err != nil {
				return fmt.Errorf("scan sequence for %q: %w", tbl, err)
			}
			if seq == nil || *seq == "" {
				continue
			}
			if _, err := t.qs.QueryAdmin(ctx, setvalSQL(*seq, col, tbl, margin)); err != nil {
				return fmt.Errorf("advance sequence %s: %w", *seq, err)
			}
		}
	}
	return nil
}

// createSubscriptionSQL builds a CREATE SUBSCRIPTION statement. conninfo is a
// string literal (no bind params for utility DDL), doubly quoted — as a libpq
// conninfo by the caller and as a SQL string literal here — and must never be
// logged. When slotName is non-empty, the subscription attaches to a
// pre-existing slot (create_slot = false) instead of creating its own — used
// by the EXPORT reverse subscription, whose slot is pre-created on the target
// during the switch so it captures every post-switch write.
func createSubscriptionSQL(name, conninfo, publication string, copyData bool, slotName string) string {
	opts := fmt.Sprintf("copy_data = %t", copyData)
	if slotName != "" {
		opts += ", create_slot = false, slot_name = " + ast.QuoteIdentifier(slotName)
	}
	return fmt.Sprintf(
		"CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s WITH (%s)",
		ast.QuoteIdentifier(name),
		ast.QuoteStringLiteral(conninfo),
		ast.QuoteIdentifier(publication),
		opts,
	)
}

// createPublicationSQL builds a CREATE PUBLICATION ... FOR TABLE statement,
// quoting each (optionally schema-qualified) table name.
func createPublicationSQL(name string, tables []string) string {
	quoted := make([]string, len(tables))
	for i, tbl := range tables {
		quoted[i] = quoteQualifiedName(tbl)
	}
	return fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
		ast.QuoteIdentifier(name), strings.Join(quoted, ", "))
}

// quoteQualifiedName quotes a canonical "schema.table" string, splitting on
// its single separating dot. A schema or table name containing a literal dot
// is part of one component, not a third qualifier.
func quoteQualifiedName(name string) string {
	schema, table, ok := strings.Cut(name, ".")
	if !ok {
		return ast.QuoteIdentifier(name)
	}
	return ast.QuoteIdentifier(schema) + "." + ast.QuoteIdentifier(table)
}

// tablesValuesClause builds a "(VALUES ('a'),('b')) AS t(tbl)" clause from a
// list of schema-qualified table names, for joining the caller's table list
// against a catalog table keyed by to_regclass(t.tbl).
func tablesValuesClause(tables []string) string {
	values := make([]string, len(tables))
	for i, tbl := range tables {
		values[i] = "(" + ast.QuoteStringLiteral(tbl) + ")"
	}
	return "(VALUES " + strings.Join(values, ", ") + ") AS t(tbl)"
}
