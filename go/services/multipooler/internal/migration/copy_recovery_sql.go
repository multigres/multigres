// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// ErrCopyLockBusy defers diagnosis while PostgreSQL finishes an active handoff.
var ErrCopyLockBusy = errors.New("table copy handoff is still active")

type CopyRelation struct {
	Name     string `json:"name"`
	OID      int64  `json:"oid"`
	State    string `json:"state"`
	Ordinary bool   `json:"ordinary"`
}

// CopySnapshot binds publisher slot names and recovery intent to this target.
type CopySnapshot struct {
	SubscriptionOID int64
	SystemID        string
	Publications    []string
	SlotName        string
	Enabled         bool
	Relations       []CopyRelation
}

const copySnapshotSQL = `SELECT s.oid::bigint,
	(pg_control_system()).system_identifier::text,
	array_to_json(s.subpublications)::text, COALESCE(s.subslotname, ''),
	s.subenabled,
	COALESCE((SELECT json_agg(json_build_object(
		'name', n.nspname || '.' || c.relname, 'oid', r.srrelid::bigint,
		'state', r.srsubstate,
		'ordinary', c.relkind = 'r' AND NOT c.relispartition AND NOT EXISTS
			(SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid OR i.inhparent = c.oid))
		ORDER BY n.nspname, c.relname)
		FROM pg_subscription_rel r JOIN pg_class c ON c.oid = r.srrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace WHERE r.srsubid = s.oid), '[]'::json)::text
	FROM pg_subscription s
	WHERE s.subname = $1 AND s.subdbid =
		(SELECT oid FROM pg_database WHERE datname = current_database())
	AND NOT pg_is_in_recovery()`

type copyQuery func(context.Context, string, ...any) (*sqltypes.Result, error)

func readCopySnapshot(ctx context.Context, query copyQuery, name string) (*CopySnapshot, error) {
	res, err := query(ctx, copySnapshotSQL, name)
	if err != nil {
		return nil, fmt.Errorf("read table copy state for subscription %q: %w", name, err)
	}
	if res == nil || len(res.Rows) != 1 {
		return nil, fmt.Errorf("subscription %q is absent from the writable target database", name)
	}
	s := &CopySnapshot{}
	var publications, relations string
	if err := executor.ScanSingleRow(res, &s.SubscriptionOID, &s.SystemID, &publications,
		&s.SlotName, &s.Enabled, &relations); err != nil {
		return nil, fmt.Errorf("decode table copy state: %w", err)
	}
	if err := json.Unmarshal([]byte(publications), &s.Publications); err != nil {
		return nil, fmt.Errorf("decode subscription publications: %w", err)
	}
	if err := json.Unmarshal([]byte(relations), &s.Relations); err != nil {
		return nil, fmt.Errorf("decode subscription tables: %w", err)
	}
	return s, nil
}

func (t *target) CopySnapshot(ctx context.Context, name string) (*CopySnapshot, error) {
	return readCopySnapshot(ctx, t.qs.QueryAdminArgs, name)
}

func rollbackCopyTx(ctx context.Context, tx executor.InternalTx, result *error) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := tx.Rollback(cleanup); err != nil {
		*result = errors.Join(*result, fmt.Errorf("release table copy transaction: %w", err))
	}
}

func isCopyLockBusy(err error) bool {
	if mterrors.ExtractSQLSTATE(err) == "55P03" {
		return true
	}
	var pgerr *pgconn.PgError
	return errors.As(err, &pgerr) && pgerr.Code == "55P03"
}

// WithCopyLock never commits its neutral ALTER. The subscription object lock
// conflicts with the tablesync worker's state update lock, so a fresh statement
// after acquiring it cannot mistake an uncommitted SYNCDONE for a broken copy.
func (t *target) WithCopyLock(ctx context.Context, name string, fn func(*CopySnapshot) error) (err error) {
	tx, err := t.qs.BeginAdmin(ctx)
	if err != nil {
		return fmt.Errorf("begin table copy diagnosis: %w", err)
	}
	defer rollbackCopyTx(ctx, tx, &err)
	for _, sql := range []string{
		"SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
		"SET LOCAL lock_timeout = '1s'",
		"ALTER SUBSCRIPTION " + ast.QuoteIdentifier(name) + " SET (synchronous_commit = on)",
	} {
		if _, err = tx.Query(ctx, sql); err != nil {
			if isCopyLockBusy(err) {
				return fmt.Errorf("%w: subscription %q", ErrCopyLockBusy, name)
			}
			return fmt.Errorf("lock table copy handoff for subscription %q: %w", name, err)
		}
	}
	s, err := readCopySnapshot(ctx, tx.QueryArgs, name)
	if err != nil {
		return err
	}
	return fn(s)
}

func checkCopyIdentity(s *CopySnapshot, recovery *CopyRecovery) error {
	if s == nil || recovery == nil || !s.Enabled || s.SubscriptionOID != recovery.SubscriptionOID || s.SystemID != recovery.SystemID {
		return errors.New("table copy recovery subscription identity changed or is disabled")
	}
	return nil
}

func (t *target) SetSubscriptionPublications(ctx context.Context, name, publication string, recovery *CopyRecovery) error {
	s, err := t.CopySnapshot(ctx, name)
	if err != nil {
		return err
	}
	if err := checkCopyIdentity(s, recovery); err != nil {
		return err
	}
	// PostgreSQL requires REFRESH with copy_data=true outside a transaction block.
	sql := "ALTER SUBSCRIPTION " + ast.QuoteIdentifier(name) + " SET PUBLICATION " +
		ast.QuoteIdentifier(publication) + " WITH (refresh = true, copy_data = true)"
	if _, err := t.qs.QueryAdmin(ctx, sql); err != nil {
		return fmt.Errorf("refresh table copy subscription %q: %w", name, err)
	}
	return nil
}

const copyTruncateTableSQL = `SELECT c.oid::bigint,
	c.relkind = 'r' AND NOT c.relispartition AND NOT EXISTS
		(SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid OR i.inhparent = c.oid),
	EXISTS (SELECT 1 FROM pg_subscription_rel r WHERE r.srrelid = c.oid)
	FROM pg_class c WHERE c.oid = to_regclass($1::text)`

func (t *target) TruncateCopyTables(ctx context.Context, name string, recovery *CopyRecovery) (err error) {
	if recovery == nil || recovery.Stage != CopyRecoveryDetach || len(recovery.Tables) == 0 {
		return errors.New("table copy truncation requires a nonempty detach-stage recovery")
	}
	tx, err := t.qs.BeginAdmin(ctx)
	if err != nil {
		return fmt.Errorf("begin table copy truncation: %w", err)
	}
	defer rollbackCopyTx(ctx, tx, &err)
	if _, err := tx.Query(ctx, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"); err != nil {
		return err
	}
	if _, err := tx.Query(ctx, "SET LOCAL lock_timeout = '1s'"); err != nil {
		return err
	}
	if _, err := tx.Query(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		return fmt.Errorf("require durable table copy truncation: %w", err)
	}
	// Recovery enforces the same synchronous subscription policy as new copies.
	if _, err := tx.Query(ctx, "ALTER SUBSCRIPTION "+ast.QuoteIdentifier(name)+
		" SET (synchronous_commit = on)"); err != nil {
		return fmt.Errorf("lock subscription before table copy truncation: %w", err)
	}
	quoted := make([]string, 0, len(recovery.Tables))
	seen := map[string]bool{}
	for _, table := range recovery.Tables {
		if table.Name == "" || table.OID <= 0 || seen[table.Name] {
			return errors.New("invalid or duplicate table copy recovery table")
		}
		seen[table.Name] = true
		quoted = append(quoted, "ONLY "+quoteQualifiedName(table.Name))
	}
	if _, err := tx.Query(ctx, "LOCK TABLE "+strings.Join(quoted, ", ")+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		return fmt.Errorf("lock table copy recovery tables: %w", err)
	}
	s, err := readCopySnapshot(ctx, tx.QueryArgs, name)
	if err != nil {
		return err
	}
	if err := checkCopyIdentity(s, recovery); err != nil {
		return err
	}
	for _, table := range recovery.Tables {
		res, err := tx.QueryArgs(ctx, copyTruncateTableSQL, quoteQualifiedName(table.Name))
		if err != nil {
			return fmt.Errorf("read table copy truncation guard for %q: %w", table.Name, err)
		}
		var oid int64
		var ordinary, subscribed bool
		if err := executor.ScanSingleRow(res, &oid, &ordinary, &subscribed); err != nil {
			return fmt.Errorf("read table copy truncation guard for %q: %w", table.Name, err)
		}
		if oid != table.OID || !ordinary || subscribed {
			return fmt.Errorf("refusing table copy truncation for %q: identity, table kind, or subscription membership changed", table.Name)
		}
	}
	if _, err := tx.Query(ctx, "TRUNCATE "+strings.Join(quoted, ", ")+" CONTINUE IDENTITY RESTRICT"); err != nil {
		return fmt.Errorf("truncate detached table copies: %w", err)
	}
	return tx.Commit(ctx)
}

const copySlotsSQL = `SELECT slot_name,
	slot_type = 'logical' AND plugin = 'pgoutput' AND database = current_database()
	AND NOT temporary
	AND restart_lsn IS NOT NULL
	AND COALESCE(to_jsonb(s)->>'wal_status', '') <> 'lost'
	AND (to_jsonb(s)->>'invalidation_reason') IS NULL,
	pg_is_in_recovery() FROM pg_replication_slots s WHERE slot_name = ANY($1::text[])`

func (s *source) ReadCopySlots(mainSlot string, names []string) (map[string]bool, error) {
	if mainSlot == "" {
		return nil, errors.New("table copy subscription has no main replication slot")
	}
	wanted := append([]string{mainSlot}, names...)
	rows, err := s.conn.Query(s.ctx, copySlotsSQL, wanted)
	if err != nil {
		return nil, fmt.Errorf("read source table copy slots: %w", err)
	}
	defer rows.Close()
	present := make(map[string]bool, len(wanted))
	mainUsable := false
	for rows.Next() {
		var name string
		var usable, standby bool
		if err := rows.Scan(&name, &usable, &standby); err != nil {
			return nil, fmt.Errorf("decode source table copy slot: %w", err)
		}
		present[name] = true
		if name == mainSlot {
			mainUsable = usable && !standby
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read source table copy slots: %w", err)
	}
	if !mainUsable {
		return nil, fmt.Errorf("main source replication slot %q is missing, invalid, or not on the publisher primary", mainSlot)
	}
	return present, nil
}

type copyPublicationTable struct {
	Name     string `json:"name"`
	Ordinary bool   `json:"ordinary"`
	Filtered bool   `json:"filtered"`
	Columns  bool   `json:"columns"`
}

const copyPublicationSchemasSQL = "EXISTS (SELECT 1 FROM pg_publication_namespace pn WHERE pn.pnpubid = p.oid)"

const copyPublicationSQL = `SELECT p.puballtables, p.pubviaroot,
	p.pubinsert AND p.pubupdate AND p.pubdelete AND p.pubtruncate,
	` + copyPublicationSchemasSQL + `,
	COALESCE(to_jsonb(p)->>'pubgencols', 'n') = 'n',
	COALESCE((SELECT json_agg(json_build_object(
		'name', n.nspname || '.' || c.relname,
		'ordinary', c.relkind = 'r' AND NOT c.relispartition AND NOT EXISTS
			(SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid OR i.inhparent = c.oid),
		'filtered', (to_jsonb(pr)->>'prqual') IS NOT NULL,
		'columns', (to_jsonb(pr)->>'prattrs') IS NOT NULL))
		FROM pg_publication_rel pr JOIN pg_class c ON c.oid = pr.prrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace WHERE pr.prpubid = p.oid), '[]'::json)::text
	FROM pg_publication p WHERE p.pubname = $1`

func validatePlainCopyPublication(allTables, viaRoot, allChanges, schemas, plainGenerated bool, tables []copyPublicationTable, expected []string) error {
	if allTables || viaRoot || !allChanges || schemas || !plainGenerated {
		return errors.New("table copy recovery requires an explicit publication of all changes without schema, partition, or generated-column options")
	}
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		if !table.Ordinary || table.Filtered || table.Columns {
			return fmt.Errorf("table copy recovery publication table %q has unsupported inheritance, partition, row filter, or column list", table.Name)
		}
		names = append(names, table.Name)
	}
	want := slices.Clone(expected)
	slices.Sort(names)
	slices.Sort(want)
	if !slices.Equal(names, want) {
		return errors.New("table copy recovery publication table set changed")
	}
	return nil
}

func copyPublicationSQLForVersion(version int) (string, error) {
	if version < 140000 {
		return "", errors.New("table copy recovery publication validation requires PostgreSQL 14 or newer")
	}
	if version < 150000 {
		// Publications of schemas, row filters, and column lists arrived in 15.
		// Optional row fields are read through to_jsonb above; the absent schema
		// catalog must not appear in a statement that PostgreSQL 14 parses.
		return strings.Replace(copyPublicationSQL, copyPublicationSchemasSQL, "false", 1), nil
	}
	return copyPublicationSQL, nil
}

func (s *source) copyPublicationQuery() (string, error) {
	var version int
	if err := s.conn.QueryRow(s.ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		return "", fmt.Errorf("read source version before table copy recovery: %w", err)
	}
	return copyPublicationSQLForVersion(version)
}

func (s *source) ValidateCopyPublication(name string, expected []string) error {
	query, err := s.copyPublicationQuery()
	if err != nil {
		return err
	}
	var allTables, viaRoot, allChanges, schemas, plainGenerated bool
	var encoded string
	if err := s.conn.QueryRow(s.ctx, query, name).Scan(
		&allTables, &viaRoot, &allChanges, &schemas, &plainGenerated, &encoded); err != nil {
		return fmt.Errorf("read table copy publication %q: %w", name, err)
	}
	var tables []copyPublicationTable
	if err := json.Unmarshal([]byte(encoded), &tables); err != nil {
		return fmt.Errorf("decode table copy publication %q: %w", name, err)
	}
	if err := validatePlainCopyPublication(allTables, viaRoot, allChanges, schemas, plainGenerated, tables, expected); err != nil {
		return fmt.Errorf("validate table copy publication %q: %w", name, err)
	}
	return nil
}

func (s *source) EnsureCopyPublication(name string, expected []string) error {
	if _, err := s.copyPublicationQuery(); err != nil {
		return err
	}
	exists, err := s.PublicationExists(name)
	if err != nil {
		return err
	}
	if !exists {
		// CREATE PUBLICATION without FOR TABLE is the valid empty-publication case.
		sql := "CREATE PUBLICATION " + ast.QuoteIdentifier(name)
		if len(expected) > 0 {
			sql = createPublicationSQL(name, expected)
		}
		if _, err := s.conn.Exec(s.ctx, sql); err != nil {
			var pgerr *pgconn.PgError
			if !errors.As(err, &pgerr) || pgerr.Code != "42710" {
				return fmt.Errorf("create table copy recovery publication %q: %w", name, err)
			}
		}
	}
	return s.ValidateCopyPublication(name, expected)
}
