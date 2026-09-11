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
	cache map[string]*Migration
}

// NewStore returns a Store backed by the given admin query service.
func NewStore(qs executor.InternalQueryService) *Store {
	return &Store{qs: qs}
}

// EnsureSchema creates the migration tables if they do not already exist. It is
// idempotent and safe to call on every coordinator start, covering shards
// bootstrapped before the tables were added to createSidecarSchema.
// migration_tables is created after migration (it references it).
func (s *Store) EnsureSchema(ctx context.Context) error {
	sqlList := []string{CreateMigrationSQL, CreateMigrationTablesSQL}
	for _, sql := range sqlList {
		if _, err := s.qs.QueryAdmin(ctx, sql); err != nil {
			return fmt.Errorf("ensure migration schema: %w", err)
		}
	}
	return nil
}

// Insert writes a new migration row and its table list atomically. created_at
// defaults to now(); streaming_since starts NULL.
func (s *Store) Insert(ctx context.Context, m *Migration) error {
	const insertMigrationSQL = `INSERT INTO multigres.migration
		(migration_id, phase, active_direction, source_dsn, target_database, target_shard, sequence_margin)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`
	tx, err := s.qs.BeginAdmin(ctx)
	if err != nil {
		return fmt.Errorf("begin insert migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.QueryArgs(
		ctx, insertMigrationSQL,
		m.ID, string(m.Phase), string(m.ActiveDirection), m.SourceDSN,
		m.TargetDatabase, m.TargetShard, m.SequenceMargin,
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

// Update writes back the mutable fields of a migration and replaces its table
// list, atomically. The id, target_database, and target_shard are immutable and
// not updated.
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
		ctx, `UPDATE multigres.migration SET
		phase=$2, active_direction=$3, source_dsn=$4,
		sequence_margin=$5, last_error=$6, streaming_since=$7
		WHERE migration_id=$1`,
		m.ID, string(m.Phase), string(m.ActiveDirection), m.SourceDSN,
		m.SequenceMargin, m.LastError, streamingSince,
	); err != nil {
		return fmt.Errorf("update migration: %w", err)
	}
	// Replace the table set. Only update-migration (while CREATED) changes it, but
	// it is rewritten unconditionally for simplicity — safe because the DELETE
	// runs before the INSERTs within this transaction, so no PK conflict with the
	// rows being removed.
	const deleteMigrationTablesSQL = `DELETE FROM multigres.migration_tables WHERE migration_id=$1`
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

// Delete removes a migration row; migration_tables rows cascade. Idempotent.
func (s *Store) Delete(ctx context.Context, id string) error {
	const deleteMigrationSQL = `DELETE FROM multigres.migration WHERE migration_id=$1`
	if _, err := s.qs.QueryAdminArgs(ctx, deleteMigrationSQL, id); err != nil {
		return fmt.Errorf("delete migration: %w", err)
	}
	s.cacheDelete(id)
	return nil
}

// selectMigrationSQL reads every migration and joins in its table list,
// aggregated to a JSON array of "schema.table" (order unspecified). It is used
// only to (re)load the read-cache — per-id lookup and ordering happen in memory.
// The plain LEFT JOIN yields one all-NULL row for a migration with no tables, so
// the CASE returns '[]' (rather than '[null]') when the group has no table rows.
const selectMigrationSQL = `SELECT m.migration_id, m.phase, m.active_direction, m.source_dsn,
	m.target_database, m.target_shard, m.sequence_margin, m.last_error,
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
	cache := make(map[string]*Migration)
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
func (s *Store) cacheDelete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cache != nil {
		delete(s.cache, id)
	}
}

// Get returns the migration with the given id, or ErrNotFound. The returned
// *Migration is the cached entry itself, not a copy — callers MUST treat it as
// read-only (see the doc on Migration).
func (s *Store) Get(ctx context.Context, id string) (*Migration, error) {
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
	err := executor.ScanRow(row, &m.ID, &phase, &direction, &m.SourceDSN, &m.TargetDatabase, &m.TargetShard, &m.SequenceMargin, &m.LastError, &m.CreatedAt, &streaming, &tablesJSON)
	if err != nil {
		return nil, fmt.Errorf("scan migration: %w", err)
	}
	m.Phase = Phase(phase)
	m.ActiveDirection = Direction(direction)
	m.StreamingSince = streaming
	if tablesJSON != "" {
		if err := json.Unmarshal([]byte(tablesJSON), &m.Tables); err != nil {
			return nil, fmt.Errorf("unmarshal tables: %w", err)
		}
	}
	return &m, nil
}

// insertMigrationTables inserts one multigres.migration_tables row per table
// (small N) inside the caller's transaction. Each entry is a canonical
// "schema.table" (produced by the resolver as nspname || '.' || relname), split
// on its single dot.
func insertMigrationTables(ctx context.Context, tx executor.InternalTx, id string, tables []string) error {
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
