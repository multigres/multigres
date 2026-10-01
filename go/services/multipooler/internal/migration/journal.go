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

import "time"

// The migration journal is a durable, append-only audit log of a migration's
// lifecycle: each notable action (create, start, phase advance, direction
// switch, drop, failure) appends one row to multigres.migration_journal. It is
// deliberately separate from the single-writer multigres.migration state row:
// the migration row records the migration's *current* intent (one row,
// UPDATE-in-place, the crash-safe write-ahead log for phase/direction), while
// the journal records *history* (many rows, INSERT-only, never updated or
// deleted). Journal rows carry no foreign key to the migration row, so they are
// retained for audit after the migration is dropped (which deletes the row).
//
// The journal is internal: it lives in the multigres sidecar schema with no
// PUBLIC grant, so — like multigres.migration — it is readable only through the
// admin (superuser) pool or by a database user with the corresponding
// privileges. It never stores the source DSN or any credentials.

// JournalEvent names the kind of lifecycle action a journal entry records.
type JournalEvent string

const (
	// JournalEventCreate: the migration was created (CREATED; no DB changes yet).
	JournalEventCreate JournalEvent = "CREATE"
	// JournalEventStart: start was invoked and setup began.
	JournalEventStart JournalEvent = "START"
	// JournalEventPhase: a lifecycle phase advanced (Detail carries "from->to").
	JournalEventPhase JournalEvent = "PHASE"
	// JournalEventActivate: the go-live direction switch (IMPORT->EXPORT). Carries
	// the handoff LSNs (FromLSN = drained-to on the old writer, ToLSN = start on
	// the new writer).
	JournalEventActivate JournalEvent = "ACTIVATE"
	// JournalEventDeactivate: the roll-back direction switch (EXPORT->IMPORT).
	// Carries the handoff LSNs, like JournalEventActivate.
	JournalEventDeactivate JournalEvent = "DEACTIVATE"
	// JournalEventDrop: the migration was dropped (FromLSN = the drain LSN for a
	// graceful drop; empty for a forced drop).
	JournalEventDrop JournalEvent = "DROP"
	// JournalEventFailed: a phase errored; LastError carries the reason.
	JournalEventFailed JournalEvent = "FAILED"
)

// JournalEntry is one append-only record in the migration journal. Seq and
// CreatedAt are database-generated on insert (they are only populated on rows
// read back via ListJournal).
type JournalEntry struct {
	// Seq is the global monotonic audit order (DB-generated).
	Seq int64
	// MigrationID is the migration this entry belongs to. It carries no foreign
	// key to multigres.migration, so the entry survives the migration's drop.
	MigrationID int64
	// MigrationName is the migration's optional name, denormalized at write time
	// so the journal stays readable after the migration row is gone.
	MigrationName string
	Event         JournalEvent
	// Phase is the migration phase in effect at (or resulting from) the action.
	Phase Phase
	// Direction is the active replication direction at the action.
	Direction Direction
	// FromLSN is the drained-to / quiesce LSN on the old writer (set for a
	// direction switch and for a graceful drop); empty otherwise.
	FromLSN string
	// ToLSN is the start LSN on the new writer (set for a direction switch);
	// empty otherwise.
	ToLSN string
	// LastError is the failure reason (set for JournalEventFailed).
	LastError string
	// Detail is free-form context, e.g. "COPYING->IMPORTING" for a phase advance.
	Detail    string
	CreatedAt time.Time
}

// CreateMigrationJournalSQL is the DDL for the append-only journal table. It is
// idempotent (IF NOT EXISTS) so it can run both at shard bootstrap
// (createSidecarSchema) and on first use against an already-bootstrapped shard.
// Created through the admin pool with no PUBLIC grant, matching multigres.migration.
//
// migration_id is intentionally a plain BIGINT with NO foreign key to
// multigres.migration: journal rows must NOT cascade-delete when the migration
// row is dropped — the design retains the journal for audit after a drop. seq is
// a BIGSERIAL (replicates fine under physical replication, sharing the
// migration's failover fate).
const CreateMigrationJournalSQL = `CREATE TABLE IF NOT EXISTS multigres.migration_journal (
	seq BIGSERIAL PRIMARY KEY,
	migration_id BIGINT NOT NULL,
	migration_name TEXT NOT NULL DEFAULT '',
	event TEXT NOT NULL,
	phase TEXT NOT NULL,
	direction TEXT NOT NULL,
	from_lsn TEXT NOT NULL DEFAULT '',
	to_lsn TEXT NOT NULL DEFAULT '',
	last_error TEXT NOT NULL DEFAULT '',
	detail TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// MigrationJournalMigrationIndexSQL speeds the per-migration journal read
// (ListJournal filters by migration_id and orders by seq). Idempotent.
const MigrationJournalMigrationIndexSQL = `CREATE INDEX IF NOT EXISTS migration_journal_migration_id_idx
	ON multigres.migration_journal (migration_id, seq)`
