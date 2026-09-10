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
	if _, err := t.qs.QueryAdmin(ctx, createSubscriptionSQL(name, conninfo, publication, copyData)); err != nil {
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
func createSubscriptionSQL(name, conninfo, publication string, copyData bool) string {
	return fmt.Sprintf(
		"CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s WITH (copy_data = %t)",
		ast.QuoteIdentifier(name),
		ast.QuoteStringLiteral(conninfo),
		ast.QuoteIdentifier(publication),
		copyData,
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
