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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// ErrNotFound is returned by Store.Get when no migration with the given id
// exists.
var ErrNotFound = errors.New("migration not found")

// Store persists migrations via the admin (true-superuser) pool. The migration
// row lives in multigres.migration; its table list is normalized into the child
// multigres.migration_tables (one row per schema.table) and joined back in on
// read. Writes that touch both (Insert/Update) run in a transaction.
type Store struct {
	qs executor.InternalQueryService

	// mu guards cache, serializing the read RPCs (Get/List) against the writes
	// that keep it warm. It is independent of the coordinator's lock.
	mu sync.Mutex
	// cache holds every migration keyed by id, loaded lazily on the first
	// Get/List (nil means "not loaded"). Writes keep it warm rather than dropping
	// it whole: Update and Delete write through (replace or remove the one entry),
	// while Insert invalidates (created_at is DB-generated, so the in-memory row
	// is not yet accurate — the next Get/List reloads it). A written-through entry
	// is an independent copy, never the caller's *Migration, so the cache is never
	// mutated in place by a caller that keeps working with its object. Get/List
	// return the cached *Migration directly (no copy), so callers MUST treat the
	// result as read-only — see the doc on Migration.
	cache map[int64]*Migration
}

// NewStore returns a Store backed by the given admin query service.
func NewStore(qs executor.InternalQueryService) *Store {
	return &Store{qs: qs}
}

// EnsureSchema creates the migration tables if they do not already exist. It is
// idempotent and safe to call on every coordinator start, covering shards
// bootstrapped before the tables were added to createSidecarSchema.
// shard_key is created first: migration.migration_target and
// stat_migration.migration_target both store/cast into it. migration_connection
// is created next: migration.connection_id carries a foreign key into it.
// migration_tables is created after migration (it references it).
func (s *Store) EnsureSchema(ctx context.Context) error {
	if err := ensureType(ctx, s.qs, "shard_key", CreateMigrationShardKeyTypeSQL); err != nil {
		return fmt.Errorf("ensure migration schema: %w", err)
	}
	sqlList := []string{
		CreateMigrationConnectionSQL, MigrationConnectionNameUniqueIndexSQL,
		CreateMigrationSQL, CreateMigrationTablesSQL, CreateMigrationJournalSQL,
		MigrationNameUniqueIndexSQL, MigrationJournalMigrationIndexSQL,
	}
	for _, sql := range sqlList {
		if _, err := s.qs.QueryAdmin(ctx, sql); err != nil {
			return fmt.Errorf("ensure migration schema: %w", err)
		}
	}
	if _, err := s.qs.QueryAdmin(ctx, CreateStatMigrationViewSQL); err != nil {
		return fmt.Errorf("ensure migration schema: create stat_migration view: %w", err)
	}
	if _, err := s.qs.QueryAdmin(ctx, GrantStatMigrationSQL); err != nil {
		return fmt.Errorf("ensure migration schema: grant stat_migration: %w", err)
	}
	return nil
}

// insertMigrationSQL's args $5-$7 are (TargetDatabase, TargetShard,
// TargetTableGroup) in Insert's own call order, but shard_key's declared
// field order is (database, table_group, shard) — ROW($5,$7,$6) maps $7
// (TargetTableGroup) into the table_group slot and $6 (TargetShard) into the
// shard slot, matching the type correctly despite the Go args and the
// composite's fields not sharing one order.
const insertMigrationSQL = `INSERT INTO multigres.migration
	(migration_id, migration_phase, migration_name, connection_id, migration_target,
	 sequence_margin, copy_data, skip_schema_copy, direction)
	VALUES ($1,$2,$3,$4,ROW($5,$7,$6)::multigres.shard_key,$8,$9,$10,$11)`

// Insert writes a new migration row and its table list atomically. created_at
// defaults to now(); streaming_since starts NULL.
func (s *Store) Insert(ctx context.Context, m *Migration) error {
	tx, err := s.qs.BeginAdmin(ctx)
	if err != nil {
		return fmt.Errorf("begin insert migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.QueryArgs(
		ctx, insertMigrationSQL,
		m.ID, string(m.Phase), nullableString(m.Name), m.ConnectionID,
		m.TargetDatabase, m.TargetShard, m.TargetTableGroup, m.SequenceMargin, m.CopyData, m.SkipSchemaCopy,
		string(m.effectiveDirection()),
	); err != nil {
		return fmt.Errorf("insert migration: %w", err)
	}
	if err := insertMigrationTables(ctx, tx, m.ID, m.Tables); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit insert migration: %w", err)
	}
	s.invalidateCache()
	return nil
}

// updateMigrationSQL updates the mutable fields of a migration row. It does not
// touch the table list (which is replaced separately in Update) or the
// immutable migration_target.
const (
	updateMigrationSQL = `UPDATE multigres.migration SET
	migration_phase=$2, connection_id=$3,
	sequence_margin=$4, last_error=$5, streaming_since=$6, direction=$7, reverse_link_error=$8
	WHERE migration_id=$1`

	deleteMigrationTablesSQL = `DELETE FROM multigres.migration_tables WHERE migration_id=$1`
)

// Update writes back the mutable fields of a migration and replaces its table
// list, atomically. The id and migration_target are immutable and not updated.
func (s *Store) Update(ctx context.Context, m *Migration) error {
	var streamingSince any
	if m.StreamingSince != nil {
		streamingSince = *m.StreamingSince
	}
	tx, err := s.qs.BeginAdmin(ctx)
	if err != nil {
		return fmt.Errorf("begin update migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.QueryArgs(
		ctx, updateMigrationSQL,
		m.ID, string(m.Phase), m.ConnectionID,
		m.SequenceMargin, m.LastError, streamingSince, string(m.effectiveDirection()), m.ReverseLinkError,
	); err != nil {
		return fmt.Errorf("update migration: %w", err)
	}
	// Replace the table set. Only update-migration (while CREATED) changes it, but
	// it is rewritten unconditionally for simplicity — safe because the DELETE
	// runs before the INSERTs within this transaction, so no PK conflict with the
	// rows being removed.
	if _, err := tx.QueryArgs(ctx, deleteMigrationTablesSQL, m.ID); err != nil {
		return fmt.Errorf("clear migration tables: %w", err)
	}
	if err := insertMigrationTables(ctx, tx, m.ID, m.Tables); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit update migration: %w", err)
	}
	s.cacheStore(m)
	return nil
}

const deleteMigrationSQL = `DELETE FROM multigres.migration WHERE migration_id=$1`

// Delete removes a migration row; migration_tables rows cascade. Idempotent.
func (s *Store) Delete(ctx context.Context, id int64) error {
	if _, err := s.qs.QueryAdminArgs(ctx, deleteMigrationSQL, id); err != nil {
		return fmt.Errorf("delete migration: %w", err)
	}
	s.cacheDelete(id)
	return nil
}

const insertJournalSQL = `INSERT INTO multigres.migration_journal
		(migration_id, migration_name, event, phase, direction, from_lsn, to_lsn, last_error, detail)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`

// InsertJournal appends one row to the append-only migration journal. seq and
// created_at are DB-generated (BIGSERIAL / now() defaults) and are not read back
// here — the entry is fire-and-forget from the caller's perspective. The journal
// is a separate concern from the cached migration state, so this does not touch
// the read-cache.
func (s *Store) InsertJournal(ctx context.Context, e *JournalEntry) error {
	if _, err := s.qs.QueryAdminArgs(
		ctx, insertJournalSQL,
		e.MigrationID, e.MigrationName, string(e.Event), string(e.Phase),
		string(e.Direction), e.FromLSN, e.ToLSN, e.LastError, e.Detail,
	); err != nil {
		return fmt.Errorf("insert migration journal: %w", err)
	}
	return nil
}

// ListJournal returns the journal entries for a migration in seq order (oldest
// first). It reads the table directly by migration_id, independent of the
// migration read-cache, so it also returns entries for a migration whose row has
// already been dropped (the journal is retained for audit after a drop).
func (s *Store) ListJournal(ctx context.Context, migrationID int64) ([]*JournalEntry, error) {
	res, err := s.qs.QueryAdminArgs(ctx, selectJournalSQL, migrationID)
	if err != nil {
		return nil, fmt.Errorf("list migration journal: %w", err)
	}
	out := make([]*JournalEntry, 0)
	if res != nil {
		for _, row := range res.Rows {
			e, err := scanJournalEntry(row)
			if err != nil {
				return nil, err
			}
			out = append(out, e)
		}
	}
	return out, nil
}

// selectJournalSQL reads one migration's journal entries in seq order. It filters
// by migration_id directly (not via the migration read-cache), so it also returns
// entries for a migration whose row has been dropped.
const selectJournalSQL = `SELECT seq, migration_id, migration_name, event, phase,
	direction, from_lsn, to_lsn, last_error, detail, created_at
	FROM multigres.migration_journal
	WHERE migration_id=$1
	ORDER BY seq`

// scanJournalEntry decodes a result row (in selectJournalSQL column order) into a
// JournalEntry.
func scanJournalEntry(row *sqltypes.Row) (*JournalEntry, error) {
	var (
		e         JournalEntry
		event     string
		phase     string
		direction string
	)
	err := executor.ScanRow(row, &e.Seq, &e.MigrationID, &e.MigrationName, &event, &phase,
		&direction, &e.FromLSN, &e.ToLSN, &e.LastError, &e.Detail, &e.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("scan migration journal entry: %w", err)
	}
	e.Event = JournalEvent(event)
	e.Phase = Phase(phase)
	e.Direction = Direction(direction)
	return &e, nil
}

// selectMigrationSQL reads every migration and joins in its table list,
// aggregated to a JSON array of "schema.table" (order unspecified). It is used
// only to (re)load the read-cache — per-id lookup and ordering happen in memory.
// The plain LEFT JOIN yields one all-NULL row for a migration with no tables, so
// the CASE returns '[]' (rather than '[null]') when the group has no table rows.
const selectMigrationSQL = `SELECT m.migration_id, m.migration_phase, COALESCE(m.migration_name, ''),
	m.connection_id, (m.migration_target).database, (m.migration_target).shard, (m.migration_target).table_group,
	m.sequence_margin,
	m.copy_data, m.skip_schema_copy, m.direction,
	m.last_error, m.reverse_link_error,
	m.created_at, m.streaming_since,
	CASE WHEN count(t.table_name) = 0 THEN '[]'
	     ELSE json_agg(t.schema_name || '.' || t.table_name)::text
	END AS tables
	FROM multigres.migration m
	LEFT JOIN multigres.migration_tables t USING (migration_id)
	GROUP BY m.migration_id`

// loadCacheLocked (re)populates the cache with every migration in one query. The
// caller must hold s.mu.
func (s *Store) loadCacheLocked(ctx context.Context) error {
	res, err := s.qs.QueryAdmin(ctx, selectMigrationSQL)
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	cache := make(map[int64]*Migration)
	if res != nil {
		for _, row := range res.Rows {
			m, err := scanMigration(row)
			if err != nil {
				return err
			}
			cache[m.ID] = m
		}
	}
	s.cache = cache
	return nil
}

// invalidateCache drops the cache so the next Get/List reloads from the database.
// Used by Insert, where created_at is DB-generated and not yet known in memory.
func (s *Store) invalidateCache() {
	s.mu.Lock()
	s.cache = nil
	s.mu.Unlock()
}

// cacheStore write-through-updates the entry for m after a successful Update, so
// the cache stays warm (no reload on the next Get/List). It stores an
// independent copy — never the caller's *Migration — so a caller that keeps
// mutating its object cannot corrupt the cache. No-op when the cache is not
// loaded; the next Get/List loads everything.
func (s *Store) cacheStore(m *Migration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cache == nil {
		return
	}
	cp := *m
	cp.Tables = append([]string(nil), m.Tables...)
	s.cache[m.ID] = &cp
}

// cacheDelete drops one entry after a successful Delete, keeping the rest of the
// cache warm. No-op when the cache is not loaded.
func (s *Store) cacheDelete(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cache != nil {
		delete(s.cache, id)
	}
}

// Get returns the migration with the given id, or ErrNotFound. The returned
// *Migration is the cached entry itself, not a copy — callers MUST treat it as
// read-only (see the doc on Migration).
func (s *Store) Get(ctx context.Context, id int64) (*Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cache == nil {
		if err := s.loadCacheLocked(ctx); err != nil {
			return nil, err
		}
	}
	m, ok := s.cache[id]
	if !ok {
		return nil, ErrNotFound
	}
	return m, nil
}

// GetByRef returns the migration addressed by ref: an exact id match takes
// precedence, then a unique name match. Returns ErrNotFound if neither matches.
// An empty ref never matches (an unnamed migration is not addressable by name).
// The returned *Migration is the cached entry itself, not a copy — callers MUST
// treat it as read-only (see the doc on Migration).
func (s *Store) GetByRef(ctx context.Context, ref Ref) (*Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cache == nil {
		if err := s.loadCacheLocked(ctx); err != nil {
			return nil, err
		}
	}
	if ref.ID != 0 {
		if m, ok := s.cache[ref.ID]; ok {
			return m, nil
		}
	} else if ref.Name != "" {
		for _, m := range s.cache {
			if m.Name == ref.Name {
				return m, nil
			}
		}
	}
	return nil, ErrNotFound
}

// List returns all migrations ordered by creation time. The returned *Migration
// values are the cached entries themselves, not copies — callers MUST treat them
// as read-only (see the doc on Migration).
func (s *Store) List(ctx context.Context) ([]*Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cache == nil {
		if err := s.loadCacheLocked(ctx); err != nil {
			return nil, err
		}
	}
	out := make([]*Migration, 0, len(s.cache))
	for _, m := range s.cache {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// ErrConnectionNotFound is returned by connection lookups when no connection
// with the given ref matches.
var ErrConnectionNotFound = errors.New("connection not found")

const insertConnectionSQL = `INSERT INTO multigres.migration_connection
	(connection_id, name, dsn)
	VALUES ($1,$2,$3)`

// InsertConnection writes a new connection row. created_at defaults to now().
// Connections are read rarely compared to migrations (which are read every
// reconcile tick), so — unlike Migration — there is no cache: every read goes
// straight to the database. This is load-bearing, not an oversight: a cached,
// stale Connection would silently defeat the point of migrations referencing
// connections live (see Migration.ConnectionID).
func (s *Store) InsertConnection(ctx context.Context, c *Connection) error {
	if _, err := s.qs.QueryAdminArgs(
		ctx, insertConnectionSQL,
		c.ID, c.Name, c.DSN,
	); err != nil {
		return fmt.Errorf("insert connection: %w", err)
	}
	return nil
}

const deleteConnectionSQL = `DELETE FROM multigres.migration_connection WHERE connection_id=$1`

// DeleteConnection removes a connection row. Fails with a foreign-key-violation
// error if a migration still references it (migration.connection_id has no ON
// DELETE clause, so Postgres applies its default RESTRICT).
func (s *Store) DeleteConnection(ctx context.Context, id int64) error {
	if _, err := s.qs.QueryAdminArgs(ctx, deleteConnectionSQL, id); err != nil {
		return fmt.Errorf("delete connection: %w", err)
	}
	return nil
}

const selectConnectionsSQL = `SELECT connection_id, name, dsn, created_at
	FROM multigres.migration_connection`

const selectConnectionByIDSQL = `SELECT connection_id, name, dsn, created_at
	FROM multigres.migration_connection WHERE connection_id = $1`

const selectConnectionByNameSQL = `SELECT connection_id, name, dsn, created_at
	FROM multigres.migration_connection WHERE name = $1`

// GetConnectionByRef returns the connection addressed by ref: an exact id match
// takes precedence, then a unique name match. Returns ErrConnectionNotFound if
// neither matches. A single indexed row lookup (connection_id is the primary
// key, name has a unique index — see CreateMigrationConnectionSQL and
// MigrationConnectionNameUniqueIndexSQL), not a full-table scan: this runs on
// every resolveSourceDSN call, i.e. close to every coordinator action.
func (s *Store) GetConnectionByRef(ctx context.Context, ref Ref) (*Connection, error) {
	var (
		res *sqltypes.Result
		err error
	)
	switch {
	case ref.ID != 0:
		res, err = s.qs.QueryAdminArgs(ctx, selectConnectionByIDSQL, ref.ID)
	case ref.Name != "":
		res, err = s.qs.QueryAdminArgs(ctx, selectConnectionByNameSQL, ref.Name)
	default:
		return nil, ErrConnectionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
	}
	if res == nil || len(res.Rows) == 0 {
		return nil, ErrConnectionNotFound
	}
	return scanConnection(res.Rows[0])
}

// ListConnections returns every stored connection, unordered by id for a
// deterministic listing.
func (s *Store) ListConnections(ctx context.Context) ([]*Connection, error) {
	res, err := s.qs.QueryAdmin(ctx, selectConnectionsSQL)
	if err != nil {
		return nil, fmt.Errorf("list connections: %w", err)
	}
	out := make([]*Connection, 0)
	if res != nil {
		for _, row := range res.Rows {
			c, err := scanConnection(row)
			if err != nil {
				return nil, err
			}
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// scanConnection decodes a result row (in selectConnectionsSQL order) into a
// Connection.
func scanConnection(row *sqltypes.Row) (*Connection, error) {
	var c Connection
	if err := executor.ScanRow(row, &c.ID, &c.Name, &c.DSN, &c.CreatedAt); err != nil {
		return nil, fmt.Errorf("scan connection: %w", err)
	}
	return &c, nil
}

// scanMigration decodes a result row (in selectMigrationSQL order) into a
// Migration. The trailing tables column is a JSON array of "schema.table".
func scanMigration(row *sqltypes.Row) (*Migration, error) {
	var (
		m          Migration
		phase      string
		direction  string
		streaming  *time.Time
		tablesJSON string
	)
	err := executor.ScanRow(row, &m.ID, &phase, &m.Name, &m.ConnectionID, &m.TargetDatabase, &m.TargetShard, &m.TargetTableGroup, &m.SequenceMargin, &m.CopyData, &m.SkipSchemaCopy, &direction, &m.LastError, &m.ReverseLinkError, &m.CreatedAt, &streaming, &tablesJSON)
	if err != nil {
		return nil, fmt.Errorf("scan migration: %w", err)
	}
	m.Phase = Phase(phase)
	m.Direction = Direction(direction)
	m.StreamingSince = streaming
	if tablesJSON != "" {
		if err := json.Unmarshal([]byte(tablesJSON), &m.Tables); err != nil {
			return nil, fmt.Errorf("unmarshal tables: %w", err)
		}
	}
	return &m, nil
}

// nullableString maps an empty string to a SQL NULL — used for migration.name,
// so the partial-unique index on name admits multiple unnamed migrations; a
// non-empty string is stored verbatim.
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// insertMigrationTables inserts one multigres.migration_tables row per table
// (small N) inside the caller's transaction. Each entry is a canonical
// "schema.table" (produced by the resolver as nspname || '.' || relname), split
// on its single dot.
func insertMigrationTables(ctx context.Context, tx executor.InternalTx, id int64, tables []string) error {
	for _, qualified := range tables {
		schema, table, ok := strings.Cut(qualified, ".")
		if !ok || schema == "" || table == "" {
			return fmt.Errorf("invalid table name %q: expected schema.table", qualified)
		}
		if _, err := tx.QueryArgs(ctx,
			`INSERT INTO multigres.migration_tables (migration_id, schema_name, table_name) VALUES ($1,$2,$3)`,
			id, schema, table); err != nil {
			return fmt.Errorf("insert migration table %q: %w", qualified, err)
		}
	}
	return nil
}
