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

// DropTables drops the migrated tables on the local Postgres before a schema
// copy, so a pre-existing target table (a re-run after a partial migration, or a
// target that already had these tables) does not fail the copy with "relation
// already exists". IF EXISTS makes it safe when they are absent; CASCADE removes
// dependent objects (indexes, FKs, views) that would otherwise block the drop.
// Never called when SkipSchemaCopy keeps an out-of-band-seeded target.
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

// CreatePublication creates a publication on the local Postgres FOR TABLE the
// given tables plus this migration's row-filtered multigres.ddl_log (so captured
// DDL rides the same stream). Used when this side is the publisher (EXPORT
// direction).
func (t *target) CreatePublication(ctx context.Context, name string, tables []string, migrationID string) error {
	if _, err := t.qs.QueryAdmin(ctx, createPublicationWithDDLLogSQL(name, tables, migrationID)); err != nil {
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

// AlterSubscriptionConnection repoints a subscription's CONNECTION at a new
// conninfo (password rotation, source endpoint change). The apply worker
// restarts with the new conninfo.
//
// ALTER SUBSCRIPTION is a utility statement, which PostgreSQL does not allow bind
// parameters in (the parser rejects "$1" in the CONNECTION position), and the
// subscription name is an identifier, which can never be parameterized — so the
// statement is built by quoting each part rather than with placeholders.
func (t *target) AlterSubscriptionConnection(ctx context.Context, name, conninfo string) error {
	stmt := "ALTER SUBSCRIPTION " + ast.QuoteIdentifier(name) + " CONNECTION " + ast.QuoteStringLiteral(conninfo)
	if _, err := t.qs.QueryAdmin(ctx, stmt); err != nil {
		return fmt.Errorf("alter subscription connection: %w", err)
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
// reverse subscription later attaches to it with create_slot=false.
func (t *target) CreateLogicalSlot(ctx context.Context, name string) error {
	if _, err := t.qs.QueryAdminArgs(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", name); err != nil {
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
			return false, nil // slot not present yet
		}
		var ok bool
		if err := executor.ScanSingleRow(res, &ok); err != nil {
			return false, err
		}
		return ok, nil
	})
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

// quoteQualifiedName quotes a possibly schema-qualified identifier
// (schema.table), quoting each dotted part independently.
func quoteQualifiedName(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = ast.QuoteIdentifier(p)
	}
	return strings.Join(parts, ".")
}
