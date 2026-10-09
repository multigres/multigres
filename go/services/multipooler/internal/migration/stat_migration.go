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

	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// multigres.stat_migration is a real Postgres view exposing migration status —
// identity, target, phase/direction, copy/stream progress, and replication lag
// — without the source credential multigres.migration carries, so (unlike that
// table) it is safe to grant to PUBLIC and query directly against the target
// Postgres, not just through the gateway's gRPC path. The gateway's own
// "pseudo-view" SQL interface (recognizing a supported SELECT shape against
// this name and answering it via the Migrator RPC instead, so status stays
// visible even while the target is NOT_SERVING) is a separate mechanism built
// on top of — this file is only the real, queryable-from-anywhere object it
// falls back to for any query outside that supported shape.
//
// migration_target is a genuine structured composite (clustermetadata.ShardKey)
// and is stored directly as multigres.migration.migration_target's own type
// (see CreateMigrationShardKeyTypeSQL in migration.go), so this view just
// passes it through — a client querying the view directly gets the same
// structured value the RPC surface uses. migration_phase/active_direction
// stay plain text rather than native Postgres ENUMs, deliberately: Postgres's
// own catalogs never use CREATE TYPE ... AS ENUM for a state/kind
// discriminator column (pg_class.relkind is char,
// pg_stat_activity.state/pg_stat_replication.state are both text), and native
// enums have real friction for an evolving value set — can't remove a value,
// can't reorder without drop+recreate, and ALTER TYPE ... ADD VALUE can't run
// in the same transaction that added it even on modern Postgres. The
// phase/direction vocabulary here has already been revised multiple times
// during this design, so that friction is not hypothetical.

// CreateStatMigrationViewSQL defines multigres.stat_migration. CREATE OR
// REPLACE VIEW is itself idempotent; shard_key, which migration_target is
// stored as, is not (see ensureType in Store.EnsureSchema) and must exist
// before multigres.migration itself does.
//
// total_relations/ready_relations and lag_bytes/lag_seconds are both read from
// whichever side of the replication link is locally visible to this target
// Postgres, keyed by the migration's conventional subscription/slot name
// ("mt_sub_<id>", shared across both directions — see Migration.SubscriptionName):
//   - During IMPORT, the target holds the local subscription, so
//     pg_subscription_rel gives the real copy/stream progress; the local
//     catalogs have no visibility into the external source's publisher-side
//     lag, so lag_bytes/lag_seconds read 0 here (the gateway's RPC path, which
//     does reach the source, is the authoritative way to read lag during
//     IMPORT — this view is the fallback, not the primary path).
//   - During EXPORT, the target holds the local replication slot as publisher,
//     so lag_bytes/lag_seconds are the real local measurement (mirroring
//     target.ReplicationLag's query); there is no local subscription for this
//     migration, so total_relations/ready_relations read 0 here instead.
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
JOIN multigres.migration_connection c USING (connection_id)
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
// multigres.migration/migration_connection/migration_journal (which carry a
// source DSN/password and so deliberately get no PUBLIC grant): stat_migration
// exposes only connection_name, never the DSN, so it is safe for the gateway's
// own serving connection and any operator to query directly. GRANT is
// idempotent on its own (re-granting is a no-op).
const GrantStatMigrationSQL = `GRANT SELECT ON multigres.stat_migration TO PUBLIC`

// ensureType creates a type in the multigres schema if it does not already
// exist. Unlike CREATE TABLE/INDEX/VIEW, CREATE TYPE has no IF NOT EXISTS
// clause, so EnsureSchema (idempotent, run on every coordinator start) must
// check pg_type itself rather than rely on the statement being naturally
// idempotent.
func ensureType(ctx context.Context, qs executor.InternalQueryService, typeName, createSQL string) error {
	res, err := qs.QueryAdminArgs(ctx,
		`SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
		 WHERE n.nspname = 'multigres' AND t.typname = $1`, typeName)
	if err != nil {
		return fmt.Errorf("check type %q exists: %w", typeName, err)
	}
	if res != nil && len(res.Rows) > 0 {
		return nil
	}
	if _, err := qs.QueryAdmin(ctx, createSQL); err != nil {
		return fmt.Errorf("create type %q: %w", typeName, err)
	}
	return nil
}
