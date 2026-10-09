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

import (
	"strconv"
	"time"
)

// Ref addresses a migration by numeric id, or by name when ID == 0.
type Ref struct {
	ID   int64
	Name string
}

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
	// PhaseImporting: caught up, streaming from the external source into Multigres
	// (IMPORT direction). Steady state; the target does not serve client queries.
	PhaseImporting Phase = "IMPORTING"
	// PhaseExporting: caught up, streaming from Multigres out to the external
	// database (EXPORT direction). Steady state; the target serves — this is live.
	PhaseExporting Phase = "EXPORTING"
	// PhaseSwitchingToExport: transient go-live cutover (IMPORTING -> EXPORTING).
	// Persisted before the switch acts, so a resumed primary knows to roll forward.
	PhaseSwitchingToExport Phase = "SWITCHING_TO_EXPORT"
	// PhaseSwitchingToImport: transient roll-back (EXPORTING -> IMPORTING).
	PhaseSwitchingToImport Phase = "SWITCHING_TO_IMPORT"
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

// directionOf reports which side is currently the publisher for a phase — the
// single source of truth for direction, derived from the phase rather than
// stored separately. During a switch the "current" side is the one being drained:
// SWITCHING_TO_EXPORT is still importing, SWITCHING_TO_IMPORT is still exporting.
func directionOf(p Phase) Direction {
	switch p {
	case PhaseExporting, PhaseSwitchingToImport:
		return DirectionExport
	default:
		return DirectionImport
	}
}

// isStreaming reports whether a phase is a caught-up steady state (either
// direction) — the states from which a direction switch or a drain-and-drop may
// begin.
func isStreaming(p Phase) bool { return p == PhaseImporting || p == PhaseExporting }

// effectiveDirection is the migration's active direction, preferring the persisted
// Direction and falling back to directionOf(Phase) when it is empty (a row written
// before the direction column existed, or any non-completing phase where the phase
// alone is authoritative).
func (m *Migration) effectiveDirection() Direction {
	if m.Direction != "" {
		return m.Direction
	}
	return directionOf(m.Phase)
}

// switchingPhase is the transient phase recorded before switching toward target.
func switchingPhase(target Direction) Phase {
	if target == DirectionExport {
		return PhaseSwitchingToExport
	}
	return PhaseSwitchingToImport
}

// streamingPhase is the steady state a completed switch toward target settles in.
func streamingPhase(target Direction) Phase {
	if target == DirectionExport {
		return PhaseExporting
	}
	return PhaseImporting
}

// switchTarget is the direction an in-flight switching phase is heading toward.
// For a non-switching phase it returns the current direction (harmless).
func switchTarget(p Phase) Direction {
	switch p {
	case PhaseSwitchingToExport:
		return DirectionExport
	case PhaseSwitchingToImport:
		return DirectionImport
	default:
		return directionOf(p)
	}
}

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
	ID    int64
	Phase Phase

	// Direction is the active replication direction (which side is the publisher).
	// It is normally derivable from Phase via directionOf, but PhaseCompleting — the
	// transient teardown phase — carries no direction of its own, so the direction
	// in effect at drop time is persisted here. reconcileLocked and teardown read it
	// to finish an interrupted drop correctly (an EXPORT drop must not be torn down
	// as an IMPORT). Empty on rows written before this column existed; treat empty as
	// directionOf(Phase) — see effectiveDirection.
	Direction Direction

	// Name is an optional, human-friendly identifier, unique per target database
	// (empty for unnamed migrations). The ID remains the stable internal key; Name
	// is an alternative address for lookups.
	Name string

	// ConnectionID is a live, required reference to a multigres.migration_connection
	// row: the migration's source DSN is resolved from it fresh on every use,
	// never copied in at create time. Set once at create time and not currently
	// changeable afterward; Connections themselves are immutable once created too.
	ConnectionID int64

	TargetDatabase string
	TargetShard    string
	// TargetTableGroup is the tablegroup the target shard belongs to. A
	// database can have multiple tablegroups, each with its own shard
	// namespace (e.g. every tablegroup has its own shard "0-inf" in the MVP),
	// so TargetDatabase+TargetShard alone do not uniquely identify a shard.
	TargetTableGroup string

	Tables         []string
	SequenceMargin int64

	// OwnerRole is the PostgreSQL role the gateway's SQL client was connected
	// as when this migration was created. Empty when created through the
	// CLI/multiadmin path, which has no SQL-authenticated identity to stamp it
	// from; DropMigration's ownership check is skipped in that case. Without
	// it, any caller passing the gateway's CanCreateMigration gate could abort
	// another operator's migration.
	OwnerRole string

	// QuiesceRoles is the optional set of application role names whose CONNECT
	// privilege on the source is revoked at the ACTIVATE cutover and restored
	// on a deactivate rollback or teardown, so they cannot reconnect and write
	// once the source becomes a subscriber. Empty means the hard quiesce still
	// freezes and terminates client backends but does not fence by role.
	// Immutable after create; validated at create time. Persisted as a JSON
	// array.
	QuiesceRoles []string

	// PublicHadConnect records whether the PUBLIC pseudo-role held CONNECT on
	// the source database immediately before the ACTIVATE cutover's hard
	// quiesce revoked it. GrantConnect reads this back at rollback or teardown
	// so it only restores PUBLIC's CONNECT when PUBLIC genuinely had it.
	// Defaults to true, Postgres's own default ACL state, for a migration that
	// never reaches a hard quiesce.
	PublicHadConnect bool

	// QuiesceApplied records whether this migration's ACTIVATE cutover
	// actually ran a hard quiesce. QuiesceRoleConnLimits alone cannot prove
	// this, since it stays empty whenever QuiesceRoles is empty even though
	// PUBLIC is still revoked unconditionally, so teardown and a forced drop
	// need this flag to know whether GrantConnect must run.
	QuiesceApplied bool

	// QuiesceRoleConnLimits records each quiesce role's pg_roles.rolconnlimit,
	// where -1 means no limit, immediately before the hard quiesce caps it to
	// 0. GrantConnect's later restore sets each role back to its original
	// limit instead of assuming unlimited. Capping CONNECTION LIMIT fences a
	// role regardless of how it derives CONNECT. Keyed by role name; empty
	// when QuiesceRoles is.
	QuiesceRoleConnLimits map[string]int32

	// QuiesceRolesHadConnect records, per quiesce role, whether it held its own
	// direct CONNECT grant on the source database immediately before the hard
	// quiesce revoked it. A role named in QuiesceRoles may only have been
	// reachable via PUBLIC's CONNECT, so GrantConnect's later restore must only
	// re-grant roles this map marks true. Keyed by role name; empty when
	// QuiesceRoles is.
	QuiesceRolesHadConnect map[string]bool

	// CopyData is the initial-copy choice recorded at create time: true (the
	// default) runs the stock tablesync initial COPY at subscription setup; false
	// subscribes without a copy (target seeded out-of-band).
	CopyData bool
	// SkipSchemaCopy skips the pg_dump --schema-only step during setup (the target
	// schema already exists).
	SkipSchemaCopy bool

	LastError string

	// ReverseLinkError is set when ensureReverseExportLink finds the EXPORT
	// reverse link unusable (missing, still temporary, or invalidated slot) and
	// cleared once Coordinator.ReseedReverseLink repairs it. Empty means the
	// link is healthy (or the migration has never been EXPORTING). Distinct
	// from LastError/PhaseFailed: a degraded reverse link does not fail the
	// migration — it is still EXPORTING and serving, only its rollback path is
	// broken — so this must never drive a phase transition.
	ReverseLinkError string

	CreatedAt      time.Time
	StreamingSince *time.Time
}

// publication/subscription object names are derived by convention from the id
// rather than stored, keeping the row minimal.

// PublicationName is the publication object name for a migration.
func (m *Migration) PublicationName() string { return "mt_pub_" + strconv.FormatInt(m.ID, 10) }

// SubscriptionName is the subscription object name for a migration.
func (m *Migration) SubscriptionName() string { return "mt_sub_" + strconv.FormatInt(m.ID, 10) }

// quiesceRolesToRestoreConnect narrows QuiesceRoles down to the roles
// GrantConnect should actually re-grant: those QuiesceRolesHadConnect
// marks as having held a direct CONNECT grant before the hard quiesce
// revoked it. A migration that never reached a hard quiesce has an empty
// map, so this returns none.
func (m *Migration) quiesceRolesToRestoreConnect() []string {
	if len(m.QuiesceRoles) == 0 {
		return nil
	}
	restore := make([]string, 0, len(m.QuiesceRoles))
	for _, r := range m.QuiesceRoles {
		if m.QuiesceRolesHadConnect[r] {
			restore = append(restore, r)
		}
	}
	return restore
}


// CreateMigrationSQL is the DDL for the sidecar migration table, idempotent so
// it can run both at shard bootstrap and on first use against an
// already-bootstrapped shard. Created through the admin pool with no PUBLIC
// grant, so only the superuser admin connection can read it. connection_id has
// no ON DELETE clause, so Postgres applies its default (RESTRICT): dropping a
// connection still referenced by a migration fails with a
// foreign-key-violation error rather than silently orphaning the migration.
const CreateMigrationSQL = `CREATE TABLE IF NOT EXISTS multigres.migration (
	migration_id BIGINT PRIMARY KEY,
	migration_name TEXT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	migration_phase TEXT NOT NULL,
	migration_target multigres.shard_ref NOT NULL,
	connection_id BIGINT NOT NULL REFERENCES multigres.migration_connection(connection_id),
	owner_role TEXT NOT NULL DEFAULT '',
	sequence_margin BIGINT NOT NULL DEFAULT 0,
	copy_data BOOLEAN NOT NULL DEFAULT true,
	skip_schema_copy BOOLEAN NOT NULL DEFAULT false,
	direction TEXT NOT NULL DEFAULT 'IMPORT',
	quiesce_roles TEXT NOT NULL DEFAULT '[]',
	public_had_connect BOOLEAN NOT NULL DEFAULT true,
	quiesce_applied BOOLEAN NOT NULL DEFAULT false,
	quiesce_role_conn_limits TEXT NOT NULL DEFAULT '{}',
	quiesce_roles_had_connect TEXT NOT NULL DEFAULT '{}',
	last_error TEXT NOT NULL DEFAULT '',
	reverse_link_error TEXT NOT NULL DEFAULT '',
	streaming_since TIMESTAMPTZ NULL
)`

// MigrationNameUniqueIndexSQL enforces per-database uniqueness of the optional
// migration name. It is a partial unique index so multiple unnamed (NULL)
// migrations coexist. Created idempotently by EnsureSchema after the table.
const MigrationNameUniqueIndexSQL = `CREATE UNIQUE INDEX IF NOT EXISTS migration_name_key
	ON multigres.migration (migration_name) WHERE migration_name IS NOT NULL`

// CreateMigrationTablesSQL is the DDL for multigres.migration_tables, the normalized
// per-migration table list — one row per (schema, table) instead of a JSONB
// array on the migration row. Idempotent (IF NOT EXISTS); rows cascade-delete
// with their migration. Created after CreateMigrationSQL (it references it).
const CreateMigrationTablesSQL = `CREATE TABLE IF NOT EXISTS multigres.migration_tables (
	migration_id BIGINT NOT NULL REFERENCES multigres.migration(migration_id) ON DELETE CASCADE,
	schema_name TEXT NOT NULL,
	table_name TEXT NOT NULL,
	PRIMARY KEY (migration_id, schema_name, table_name)
)`
