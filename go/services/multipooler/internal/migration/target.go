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
// privilege model — ownership, or an explicit DROP grant, which
// has_table_privilege's 'DROP' check covers in one call. A table that does not
// exist yet is skipped: DropTables itself is a no-op for it (IF EXISTS), so
// there is nothing to protect. An empty callerRole means the call did not come
// through the gateway's SQL identity-checked path (CLI/multiadmin — see
// ActivateOptions.CallerRole) and the check is skipped, unchanged from before
// this check existed.
//
// This guards the gap the gateway's CanCreateMigration gate deliberately
// leaves open: that gate only confirms the caller could perform
// logical-replication setup in principle (database CREATE +
// pg_create_subscription), not that it owns the specific tables this
// migration names. Without this, such a role could name an existing table it
// does not own — one it merely knows the qualified name of — and have
// DropTables destroy it (CASCADE, so dependents too) via the admin pool,
// which bypasses Postgres's own ownership check entirely.
func (t *target) CheckDropPrivilege(ctx context.Context, tables []string, callerRole string) error {
	if callerRole == "" || len(tables) == 0 {
		return nil
	}
	values := make([]string, len(tables))
	for i, tbl := range tables {
		values[i] = "(" + ast.QuoteStringLiteral(tbl) + ")"
	}
	sql := "SELECT t.tbl FROM (VALUES " + strings.Join(values, ", ") + ") AS t(tbl) " +
		"WHERE to_regclass(t.tbl) IS NOT NULL " +
		"AND NOT coalesce(has_table_privilege($1, to_regclass(t.tbl), 'DROP'), false)"
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

// DropTables drops the migrated tables on the local Postgres before a schema
// copy, so a pre-existing target table (a re-run after a partial migration, or a
// target that already had these tables) does not fail the copy with "relation
// already exists". IF EXISTS makes it safe when they are absent; CASCADE removes
// dependent objects (indexes, FKs, views) that would otherwise block the drop.
// Never called when SkipSchemaCopy keeps an out-of-band-seeded target — but
// CheckDropPrivilege still runs unconditionally in that case (runSetup calls it
// before the SkipSchemaCopy branch), since the subscription created afterward
// writes into these tables regardless of whether schema copy ran.
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
// table via ALTER TABLE ... DISABLE TRIGGER USER, so no trigger the schema
// copy just carried over from the source can ever fire on the target — in
// any fire mode, at any time. This is what keeps a source table owner's
// trigger from running with the target admin's privileges: the trigger
// FUNCTION body itself was created by ApplySchema running as target admin, so
// a SECURITY DEFINER function with an arbitrary body is now owned by the
// target admin regardless of the trigger's fire mode. ENABLE ALWAYS/REPLICA
// would let it fire during the import subscription's apply worker
// (session_replication_role=replica); even the default ORIGIN mode —
// unmodified, ordinary CREATE TRIGGER, no special ALTER needed — fires on
// every ordinary application write once the migration completes and the
// table takes direct traffic again. Disabling the trigger outright is the
// only fire-mode-independent way to close both. Called right after
// ApplySchema, before the subscription that would start applying rows
// exists.
//
// This supersedes an earlier approach of resetting fire modes back to
// ORIGIN (via ENABLE TRIGGER ALL), which closed only the replica-apply
// firing vector and not the ordinary-DML one, and before that an approach of
// scanning pg_dump's own --schema-only text for the
// "ALTER TABLE ... ENABLE ALWAYS|REPLICA TRIGGER" statement and stripping it
// line by line: PostgreSQL permits embedded newlines (and other
// statement-breaking characters) inside a quoted trigger name, which pg_dump
// then emits verbatim — letting a crafted trigger name split that statement
// so a line-based (or any fixed-lookahead) textual filter either misses it
// or, worse, treats attacker-injected text after the split as a separate,
// unfiltered statement that ApplySchema then executes. Disabling triggers
// here instead, against the trigger as Postgres already parsed and created
// it, is immune to however pg_dump chooses to render the identifier. It runs
// over the already-validated migrated table list
// (checkQualifiedNamePartsSupported), so it introduces no new identifier
// risk. Unlike ENABLE/DISABLE TRIGGER ALL, USER does not touch internal FK
// constraint triggers and needs no superuser privilege, though the admin
// pool has it anyway.
//
// An operator who wants a specific migrated trigger to keep running (e.g. an
// audit or updated_at trigger they've reviewed) can re-enable it by name
// after the migration completes; there is no way to distinguish a reviewed,
// trusted trigger from an adversarial one automatically, so the safe default
// is to disable every user trigger unconditionally.
func (t *target) DisableUserTriggers(ctx context.Context, tables []string) error {
	for _, tbl := range tables {
		stmt := "ALTER TABLE " + quoteQualifiedName(tbl) + " DISABLE TRIGGER USER"
		if _, err := t.qs.QueryAdmin(ctx, stmt); err != nil {
			return fmt.Errorf("disable user triggers on %s: %w", tbl, err)
		}
	}
	return nil
}

// DropUserCheckConstraints drops every CHECK constraint on each migrated table.
// PostgreSQL has no DISABLE for a CHECK constraint (unlike a trigger — see
// DisableUserTriggers): it is evaluated on every INSERT/UPDATE unconditionally,
// including ones applied by the import subscription's apply worker, so DROP is
// the only fire-mode-independent way to close the same SECURITY DEFINER vector
// DisableUserTriggers closes for triggers — a CHECK constraint's expression can
// call a function (directly, or indirectly through an operator or cast it
// uses), and that function was itself created by ApplySchema running as target
// admin, so it is now owned by the target admin regardless of who wrote its
// body. An adversarial source table owner could use that to run
// admin-privileged code on every replicated row, triggered purely by the
// values it chooses to write on the source.
//
// Dropped unconditionally, every CHECK constraint, not just ones a query
// against pg_constraint/pg_proc can identify as calling a non-builtin
// function: DisableUserTriggers' own history is the reason — an earlier
// attempt at this kind of threat scanned pg_dump's dumped text for the
// dangerous statement and stripped it, and a crafted identifier containing an
// embedded newline defeated that line-based filter. Any catalog-based
// "does this expression call something risky" heuristic is the same class of
// incomplete filter, just one level removed from text instead of free from
// it; there is no way to distinguish a reviewed, trusted CHECK constraint from
// an adversarial one automatically, so the safe default is to drop every CHECK
// constraint unconditionally, the same policy DisableUserTriggers already
// applies to triggers. An operator who wants a specific migrated check kept
// can recreate it by hand after reviewing it. Called right after
// DisableUserTriggers, before the subscription that would start applying rows
// exists.
func (t *target) DropUserCheckConstraints(ctx context.Context, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	values := make([]string, len(tables))
	for i, tbl := range tables {
		values[i] = "(" + ast.QuoteStringLiteral(tbl) + ")"
	}
	sql := "SELECT t.tbl, c.conname FROM (VALUES " + strings.Join(values, ", ") + ") AS t(tbl) " +
		"JOIN pg_constraint c ON c.conrelid = to_regclass(t.tbl) AND c.contype = 'c'"
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
// DisableUserTriggers closes for triggers, for rules instead: a rule's action
// can run arbitrary DML (it is a query-rewrite, not a mere check), and unlike
// a CHECK constraint, PostgreSQL does support disabling a rule — ev_enabled
// uses the identical O/D/R/A scheme as a trigger's tgenabled (PostgreSQL's own
// ALTER TABLE documentation: rule enable/disable "semantics are as for
// disabled/enabled triggers"), so DISABLE RULE is unconditional: it is not
// qualified by session_replication_role the way ENABLE/ENABLE REPLICA/ENABLE
// ALWAYS are, so a disabled rule never fires in any role — not during the
// import subscription's apply worker (session_replication_role=replica), and
// not on ordinary application DML once the migration completes and the table
// takes direct traffic again (the default, unmodified ORIGIN mode a CREATE
// RULE carries over from the source is exactly the mode that fires then).
// Dropped unconditionally, like DisableUserTriggers and
// DropUserCheckConstraints, rather than only for rules a catalog query can
// identify as dangerous: there is no automatic way to distinguish a reviewed,
// trusted rule from an adversarial one. An operator who wants a specific
// migrated rule kept can re-enable it by name after reviewing it. The
// '_RETURN' rule PostgreSQL's own view mechanism depends on is excluded (it
// always fires regardless of enable state, to keep a view queryable) — it
// cannot appear on a migrated table anyway, only a view, but excluding it
// keeps this function safe to call if that ever changes.
func (t *target) DisableUserRewriteRules(ctx context.Context, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	values := make([]string, len(tables))
	for i, tbl := range tables {
		values[i] = "(" + ast.QuoteStringLiteral(tbl) + ")"
	}
	sql := "SELECT t.tbl, r.rulename FROM (VALUES " + strings.Join(values, ", ") + ") AS t(tbl) " +
		"JOIN pg_rewrite r ON r.ev_class = to_regclass(t.tbl) WHERE r.rulename <> '_RETURN'"
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
// Postgres. conninfo is the source DSN, evaluated by the local Postgres.
func (t *target) CreateSubscription(ctx context.Context, name, conninfo, publication string, copyData bool) error {
	// create_slot defaults on (slotName ""): the forward IMPORT subscription and
	// the EXPORT->IMPORT reverse subscription both make their own slot on the
	// external source they dial. Only the IMPORT->EXPORT reverse subscription,
	// which dials the target through the gateway, attaches to a pre-created slot.
	if _, err := t.qs.QueryAdmin(ctx, createSubscriptionSQL(name, conninfo, publication, copyData, "")); err != nil {
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
// failover (the PostgreSQL slot-sync flag, mirroring manager.EnsureLogicalSlot):
// harmless when the shard does not have --enable-slot-based-replication (the
// slot just never gets synced, same as before), but it is what lets the slot
// survive a target-primary failover when that flag is on.
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

// WaitSlotConfirmed blocks until the named local replication slot has
// confirmed_flush_lsn >= targetLSN, or ctx is done. Call on the publisher side
// (target in EXPORT direction).
// ReplicationLag returns the current replication lag for the named slot on the local
// (target) Postgres when it is the publisher (EXPORT direction): byte lag =
// pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) and time lag = the
// walsender's replay_lag in seconds. present is false when the slot does not exist.
// Mirrors source.ReplicationLag for the reverse direction.
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

// createSubscriptionSQL builds a CREATE SUBSCRIPTION statement. The conninfo is
// a string literal (utility DDL takes no bind parameters); it is doubly quoted —
// as a libpq conninfo by the caller and as a SQL string literal here — and must
// never be logged.
// createSubscriptionSQL builds a CREATE SUBSCRIPTION. When slotName is non-empty
// the subscription attaches to a pre-existing slot (create_slot = false) instead
// of creating its own — used by the EXPORT reverse subscription, whose slot is
// created on the target during the switch (before serving) so it captures every
// post-switch write; see the coordinator's switchTo / ensureReverseExportLink.
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

// quoteQualifiedName quotes a canonical "schema.table" string (produced by the
// resolver as nspname || '.' || relname — see insertMigrationTables), splitting
// on its single separating dot. A schema or table name containing a literal
// dot is part of one component, not a third qualifier.
func quoteQualifiedName(name string) string {
	schema, table, ok := strings.Cut(name, ".")
	if !ok {
		return ast.QuoteIdentifier(name)
	}
	return ast.QuoteIdentifier(schema) + "." + ast.QuoteIdentifier(table)
}
