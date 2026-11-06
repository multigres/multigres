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

// multigres.stat_migration is a real Postgres view exposing migration status —
// identity, target, phase/direction, copy/stream progress, and replication lag
// — without the source credential multigres.migration carries, so it is safe
// to grant to PUBLIC and query directly against the target Postgres, not just
// through the gateway's gRPC path.
//
// migration_target is stored as a genuine structured composite
// (multigres.shard_ref, mirroring clustermetadata.ShardRef), so this view
// passes it through unchanged.
// migration_phase/active_direction stay plain text rather than native Postgres
// ENUMs: enums have real friction for an evolving value set (can't remove or
// reorder a value without drop+recreate), and this vocabulary has already been
// revised multiple times during this design.

// CreateStatMigrationViewSQL defines multigres.stat_migration. CREATE OR
// REPLACE VIEW is itself idempotent; shard_ref, which migration_target is
// stored as, is not and must exist before multigres.migration itself does. It
// joins multigres.connections, which exists on the catalog shard only, so
// Store.EnsureSchema creates the view only where that table exists.
//
// total_relations/ready_relations and lag_bytes/lag_seconds are both read from
// whichever side of the replication link is locally visible to this target
// Postgres, keyed by the migration's conventional subscription/slot name
// ("mt_sub_<id>", shared across both directions):
//   - During IMPORT, the target holds the local subscription, so
//     pg_subscription_rel gives the real copy/stream progress; the local
//     catalogs have no visibility into the source's publisher-side lag, so
//     lag_bytes/lag_seconds read 0 here (the gateway's RPC path is the
//     authoritative way to read lag during IMPORT).
//   - During EXPORT, the target holds the local replication slot as publisher,
//     so lag_bytes/lag_seconds are the real local measurement; there is no
//     local subscription for this migration, so total_relations/ready_relations
//     read 0 here instead.
const CreateStatMigrationViewSQL = `CREATE OR REPLACE VIEW multigres.stat_migration AS
SELECT
	m.migration_id,
	m.migration_name,
	c.name::name AS connection_name,
	m.migration_target,
	m.migration_phase,
	m.direction AS active_direction,
	count(r.*) AS total_relations,
	count(r.*) FILTER (WHERE r.srsubstate = 'r') AS ready_relations,
	COALESCE(lag.lag_bytes, 0) AS lag_bytes,
	COALESCE(lag.lag_seconds, 0) AS lag_seconds,
	m.last_error
FROM multigres.migration m
JOIN multigres.connections c USING (connection_id)
LEFT JOIN pg_subscription ps ON ps.subname = 'mt_sub_' || m.migration_id::text
LEFT JOIN pg_subscription_rel r ON r.srsubid = ps.oid
LEFT JOIN LATERAL (
	SELECT
		GREATEST(pg_wal_lsn_diff(pg_current_wal_lsn(), sl.confirmed_flush_lsn), 0)::bigint AS lag_bytes,
		COALESCE(EXTRACT(EPOCH FROM sr.replay_lag), 0)::float8 AS lag_seconds
	FROM pg_replication_slots sl
	LEFT JOIN pg_stat_replication sr ON sr.pid = sl.active_pid
	WHERE sl.slot_name = 'mt_sub_' || m.migration_id::text
) lag ON true
GROUP BY m.migration_id, m.migration_name, c.name, m.migration_target,
	 m.migration_phase, m.direction, m.last_error, lag.lag_bytes, lag.lag_seconds`

// GrantStatMigrationSQL makes the view readable by any role, unlike
// multigres.migration/connections/migration_journal, which carry a
// source DSN/password and deliberately get no PUBLIC grant: stat_migration
// exposes only connection_name, never the DSN, so it is safe for the gateway's
// own serving connection and any operator to query directly. GRANT is
// idempotent on its own.
const GrantStatMigrationSQL = `GRANT SELECT ON multigres.stat_migration TO PUBLIC`
