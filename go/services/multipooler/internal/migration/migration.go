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

// Package migration implements the Multigres Migrator table-migration coordinator that
// lives inside multipooler. It drives logical-replication migrations of tables
// from an external PostgreSQL source into this shard (and, once switched, back
// out), using stock PostgreSQL logical replication (Strategy S).
//
// The coordinator is active only on the shard primary and persists its state in
// a replicated sidecar table (multigres.migration), so the migration row and
// its subscription share one failover fate: a promoted standby already carries
// both and resumes coordination without re-reading any external store.
//
// This package does its target-side SQL through multipooler's admin/superuser
// connection (an Executor) and its source-side SQL over the operator-supplied
// DSN. It must not import the manager package; the manager constructs the
// coordinator and passes it an Executor.
package migration

import "time"

// Phase is the coordinator-owned lifecycle state of a migration. PostgreSQL has
// no notion of this multi-step workflow, so it is persisted in the migration
// row (unlike copy/stream progress, which is derived live from the catalogs).
type Phase string

const (
	// PhaseCreated: the row exists; no database changes yet.
	PhaseCreated Phase = "CREATED"
	// PhaseValidating: checking source reachability, wal_level, and replica identity.
	PhaseValidating Phase = "VALIDATING"
	// PhaseSchemaCopy: applying the one-shot pg_dump --schema-only on the target.
	PhaseSchemaCopy Phase = "SCHEMA_COPY"
	// PhaseCreatePublication: creating the publication on the current source.
	PhaseCreatePublication Phase = "CREATE_PUBLICATION"
	// PhaseCopying: subscription created; initial COPY in progress.
	PhaseCopying Phase = "COPYING"
	// PhaseStreaming: caught up; steady-state logical replication.
	PhaseStreaming Phase = "STREAMING"
	// PhaseSwitching: transient during a set-direction flip.
	PhaseSwitching Phase = "SWITCHING"
	// PhaseCompleting: transient during teardown (drain + detach).
	PhaseCompleting Phase = "COMPLETING"
	// PhaseFailed: a phase errored; last_error carries the reason.
	PhaseFailed Phase = "FAILED"
)

// Direction is which side is currently the publisher.
type Direction string

const (
	// DirectionImport: the external old database is the source (old DB -> Multigres).
	DirectionImport Direction = "IMPORT"
	// DirectionExport: Multigres is the source (Multigres -> old DB), for fail-back.
	DirectionExport Direction = "EXPORT"
)

// Migration is the persisted (coordinator-owned) state of one migration — the
// intent, inputs, and history. Live copy/stream status (relation counts, lag,
// caught-up) is NOT stored here; it is derived from pg_subscription_rel and
// pg_stat_subscription when a projection is built.
//
// READ-ONLY: a *Migration returned by Store.Get or Store.List is the shared
// read-cache entry, not a copy. Callers MUST NOT modify it — mutating a returned
// *Migration corrupts the cache and races concurrent readers. To change a
// migration, pass a Migration you own to Store.Insert/Update (which persist it
// and invalidate the cache).
type Migration struct {
	ID              string
	Phase           Phase
	ActiveDirection Direction

	// SourceDSN is the full libpq conninfo for the external database, password
	// included. It is stored in the sidecar schema (superuser-only, the same
	// protection class as pg_subscription.subconninfo) and is never returned in
	// a projection or logged.
	SourceDSN string

	TargetDatabase string
	TargetShard    string

	Tables         []string
	SequenceMargin int64

	LastError string

	CreatedAt      time.Time
	StreamingSince *time.Time
}

// publication/subscription object names are derived by convention from the id
// rather than stored, keeping the row minimal.

// PublicationName is the publication object name for a migration.
func (m *Migration) PublicationName() string { return "mt_pub_" + m.ID }

// SubscriptionName is the subscription object name for a migration.
func (m *Migration) SubscriptionName() string { return "mt_sub_" + m.ID }

// CreateMigrationSQL is the DDL for the sidecar migration table. It is idempotent
// (IF NOT EXISTS) so it can run both at shard bootstrap (createSidecarSchema)
// and on first use against an already-bootstrapped shard. The table is created
// through the admin pool and gets no PUBLIC grant, so only the true superuser
// (the admin connection) can read it — migration rows carry the source DSN.
const CreateMigrationSQL = `CREATE TABLE IF NOT EXISTS multigres.migration (
	migration_id TEXT PRIMARY KEY,
	phase TEXT NOT NULL,
	active_direction TEXT NOT NULL DEFAULT 'IMPORT',
	source_dsn TEXT NOT NULL,
	target_database TEXT NOT NULL,
	target_shard TEXT NOT NULL,
	sequence_margin BIGINT NOT NULL DEFAULT 0,
	last_error TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	streaming_since TIMESTAMPTZ NULL
)`

// CreateMigrationTablesSQL is the DDL for multigres.migration_tables, the normalized
// per-migration table list — one row per (schema, table) instead of a JSONB
// array on the migration row. Idempotent (IF NOT EXISTS); rows cascade-delete
// with their migration. Created after CreateMigrationSQL (it references it).
const CreateMigrationTablesSQL = `CREATE TABLE IF NOT EXISTS multigres.migration_tables (
	migration_id TEXT NOT NULL REFERENCES multigres.migration(migration_id) ON DELETE CASCADE,
	schema_name TEXT NOT NULL,
	table_name TEXT NOT NULL,
	PRIMARY KEY (migration_id, schema_name, table_name)
)`
