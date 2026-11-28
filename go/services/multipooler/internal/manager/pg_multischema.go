// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/heartbeat"
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
	"github.com/multigres/multigres/go/services/multipooler/internal/shardref"
)

// ============================================================================
// Multigres Schema Operations
//
// This file contains methods for managing the multigres sidecar schema and
// its tables. These are operations that set up and maintain the multigres
// metadata within PostgreSQL.
// ============================================================================

// ----------------------------------------------------------------------------
// Schema Creation
// ----------------------------------------------------------------------------

// createSidecarSchema creates the multigres sidecar schema and all its tables.
//
// Every shard gets the shard-local tables (heartbeat, pgBackRest repos,
// consensus rules, the migration coordinator's state, and the
// ensureSidecarSchemas tables). The catalog shard
// (see constants.IsCatalogShard) also gets the sharding catalog, created by
// ensureSidecarSchemas's ensureCatalogSchema step rather than here, so a fresh
// shard and one bootstrapped by an older binary go through the same upgrade
// path.
func (pm *MultipoolerManager) createSidecarSchema(ctx context.Context, policy *clustermetadatapb.DurabilityPolicy) error {
	pm.logger.InfoContext(ctx, "creating multigres sidecar schema")

	// Functions to create the schema and its tables. Each function is
	// responsible for creating a specific part of the schema.
	createFuncs := []func(context.Context) error{
		pm.createSchema,
		pm.createHeartbeatTable,
		pm.createPgBackRestReposTable,
		func(ctx context.Context) error {
			return pm.consensusMgr.Rules().CreateRuleTables(ctx, policy, pm.serviceID)
		},
	}
	// migration is the Multigres Migrator table-migration coordinator's state table.
	// Created here so standbys inherit it via restore and post-failover primaries
	// already have it; the coordinator also ensures it on first use to cover
	// shards bootstrapped before this table existed. Every shard gets it, not
	// just the catalog shard: a migration can target any shard.
	createFuncs = append(createFuncs, pm.createMigrationTable)
	// ensureSidecarSchemas goes last, deliberately: it also runs alone
	// (without the rest of createSidecarSchema) each time a pooler becomes
	// the serving primary of an already-bootstrapped shard, so nothing
	// created here may ever depend on a table it creates. Keeping it last
	// turns a violation of that into an ordering failure here - a step
	// above it trying to depend on backend_vpid (or a future sidecar
	// table added here) would fail since ensureSidecarSchemas hasn't run
	// yet - rather than a convention someone has to remember.
	//
	// It passes no rule: the rule only labels a catalog version publish, and
	// bootstrap publishes none, since it creates catalog_state at the latest
	// format.
	createFuncs = append(createFuncs, func(ctx context.Context) error {
		return pm.ensureSidecarSchemas(ctx, nil)
	})

	for _, createFunc := range createFuncs {
		if err := createFunc(ctx); err != nil {
			return err
		}
	}

	pm.logger.InfoContext(ctx, "successfully created multigres sidecar schema")
	return nil
}

// isCatalogShard reports whether this pooler serves the shard that hosts the
// sharding catalog.
func (pm *MultipoolerManager) isCatalogShard() bool {
	return constants.IsCatalogShard(pm.record.ShardRef().GetTableGroup(), pm.record.ShardRef().GetShard())
}

// ensureSidecarSchemas idempotently (re-)creates the sidecar tables that a
// shard bootstrapped by an older pooler version may be missing, because
// createSidecarSchema only ever runs once, at genuine shard bootstrap. A shard
// bootstrapped before a given table existed in the code never goes through that
// path again, so it is never created — even after the pooler binary is upgraded
// — unless something re-ensures it afterward.
//
// Called from two locations:
//
//   - createSidecarSchema, so a freshly bootstrapping primary already has every
//     such table before the first backup.
//
//   - beforeServingPrimary, the StateManager hook that runs on every
//     transition to serving primary (open, resume, promotion, the release of a
//     serving hold), so a pooler never serves as the primary of a shard that
//     lacks them. A failure keeps it from serving as the primary until a later
//     transition succeeds.
//
// Every table created here must use CREATE TABLE IF NOT EXISTS (or equivalent):
// unlike createSidecarSchema's other steps, this one is expected to run
// repeatedly against an already-initialized schema. Because it gates serving,
// it must also write nothing when everything is already in place: a write
// assigns a transaction id, and under synchronous replication its commit
// waits for a standby. Each step checks before it writes.
//
// rule is the consensus rule this pooler is the primary under (see
// ensureCatalogSchema). It comes from the serving transition that calls this,
// which has not yet made this pooler the primary in the StateManager.
//
// Currently this is the shared multigres.shard_ref type, backend_vpid and the
// heartbeat table's newer columns and the probe role's read grant on it on every
// shard, plus, on the catalog shard, the versioned sharding catalog
// (ensureCatalogSchema, which is idempotent via its format version rather than
// IF NOT EXISTS) and the migrator's schema that depends on it
// (ensureCatalogMigrationSchema), both skipped by ensureCatalogShard when
// catalog_state records that this binary already applied them; add future
// late-added sidecar tables here as they arise.
func (pm *MultipoolerManager) ensureSidecarSchemas(ctx context.Context, rule *clustermetadatapb.RuleNumber) error {
	// shard_ref goes first: ensuring it renames a legacy multigres.shard_key
	// composite, which must happen before the catalog ensure creates its
	// multigres.shard_key table on the catalog shard.
	if err := pm.ensureShardRefType(ctx); err != nil {
		return err
	}

	// backend_vpid maps live backend pids to gateway virtual pids.
	if err := pm.createBackendVpidTable(ctx); err != nil {
		return err
	}

	// The heartbeat table is created once at bootstrap; add any columns this release
	// needs that an older one did not create. Reports rather than fails (see
	// pg_heartbeat_schema.go).
	pm.ensureHeartbeatSchema(ctx)

	// The probe role can be created after bootstrap, so grant on every
	// ensure. The probe is optional, so a failure must not stop the steps
	// below.
	if err := pm.grantHeartbeatToProbe(ctx); err != nil {
		pm.logger.WarnContext(ctx, "failed to grant heartbeat read access to the probe role", "error", err)
	}

	if pm.isCatalogShard() {
		if err := pm.ensureCatalogShard(ctx, rule); err != nil {
			return err
		}
	}

	return nil
}

// beforeServingPrimary is the StateManager's hook for a transition to serving
// primary (see StateManager.SetBeforeServingPrimary): it ensures the sidecar
// schemas under the rule the pooler is about to serve as the primary under,
// so a pooler never serves as the primary of a shard that lacks them. An
// unmanaged pooler fronts an external postgres whose schema is not ours to
// keep, so it does nothing there.
func (pm *MultipoolerManager) beforeServingPrimary(ctx context.Context, target servingstate.State) error {
	if pm.IsUnmanaged() {
		return nil
	}
	err := pm.ensureSidecarSchemas(ctx, target.Routing.Rule)
	pm.metrics.recordServingPrimaryEnsure(ctx, err)
	return err
}

// ensureCatalogMigrationSchema runs the migration store's EnsureSchema on the
// catalog shard, which adds the foreign key from multigres.migration into
// multigres.connections and the stat_migration view. ensureCatalogShard runs
// it after the catalog, at bootstrap, which puts both in the first backup the
// bootstrapping pooler restores from, and on serving transitions whose
// catalog definition hash differs.
func (pm *MultipoolerManager) ensureCatalogMigrationSchema(ctx context.Context) error {
	queryService := pm.internalQueryService()
	if queryService == nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "internal query service unavailable for migration schema")
	}
	execCtx, cancel := context.WithTimeout(ctx, migrationSchemaTimeout)
	defer cancel()
	if err := migration.NewStore(queryService).EnsureSchema(execCtx); err != nil {
		return mterrors.Wrap(err, "failed to ensure migration schema")
	}
	return nil
}

// migrationSchemaTimeout bounds ensureCatalogMigrationSchema.
const migrationSchemaTimeout = 5 * time.Second

// createSchema creates the multigres sidecar schema under the admin
// (true-superuser) connection, so the schema is owned by the true superuser and
// customer roles cannot drop or alter it. It grants only USAGE on the schema to
// PUBLIC — the minimum that lets customer roles resolve the one object they are
// permitted to read (multigres.backend_vpid, whose SELECT grant is applied when
// that table is created). No object privileges are implied by USAGE, so every
// other sidecar table remains reachable only through the admin pool.
func (pm *MultipoolerManager) createSchema(ctx context.Context) error {
	queryService := pm.internalQueryService()
	if queryService == nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "internal query service unavailable for multigres schema creation")
	}
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := queryService.QueryAdminMultiStatement(execCtx, `CREATE SCHEMA multigres;
GRANT USAGE ON SCHEMA multigres TO PUBLIC`); err != nil {
		return mterrors.Wrap(err, "failed to create multigres schema")
	}
	return nil
}

// ----------------------------------------------------------------------------
// Table Creation
// ----------------------------------------------------------------------------

// createHeartbeatTable creates the heartbeat table for leader election
func (pm *MultipoolerManager) createHeartbeatTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.adminExec(execCtx, heartbeat.CreateTableSQL); err != nil {
		return mterrors.Wrap(err, "failed to create heartbeat table")
	}
	return nil
}

// grantHeartbeatToProbe lets the probe role read the heartbeat table, if the
// role exists. Idempotent.
func (pm *MultipoolerManager) grantHeartbeatToProbe(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.adminExec(execCtx, heartbeat.GrantProbeReadSQL); err != nil {
		return mterrors.Wrap(err, "failed to grant heartbeat read access to the probe role")
	}
	return nil
}

// backendVpidInPlaceSQL reports whether multigres.backend_vpid exists with the
// storage options and the PUBLIC grant createBackendVpidTable gives it, and
// returns no row when the table is missing. A table created by an older
// binary, without them, reads false.
const backendVpidInPlaceSQL = `SELECT coalesce(
	c.reloptions @> ARRAY['autovacuum_vacuum_scale_factor=0', 'autovacuum_vacuum_threshold=100',
		'autovacuum_analyze_scale_factor=0', 'autovacuum_analyze_threshold=100']
	AND (SELECT array_agg(a.privilege_type ORDER BY a.privilege_type)
		FROM aclexplode(c.relacl) a WHERE a.grantee = 0) = ARRAY['SELECT'],
	false)
FROM pg_class c WHERE c.oid = to_regclass('multigres.backend_vpid')`

// createBackendVpidTable creates multigres.backend_vpid (the gateway-vpid →
// backend-pid mapping read by lock-wait probes). It reads first and writes
// nothing when the table is already in place: the ALTER, REVOKE and GRANT
// below rewrite the table's catalog row even when they change nothing.
func (pm *MultipoolerManager) createBackendVpidTable(ctx context.Context) error {
	queryService := pm.internalQueryService()
	if queryService == nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "internal query service unavailable for backend_vpid provisioning")
	}
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	res, err := queryService.QueryAdmin(execCtx, backendVpidInPlaceSQL)
	if err != nil {
		return mterrors.Wrap(err, "failed to check the backend_vpid table")
	}
	if res != nil && len(res.Rows) > 0 {
		var inPlace bool
		if err := executor.ScanSingleRow(res, &inPlace); err != nil {
			return mterrors.Wrap(err, "failed to check the backend_vpid table")
		}
		if inPlace {
			return nil
		}
	}
	// backend_vpid is the one sidecar table customer roles may read: writes go
	// through the admin pool, so PUBLIC gets only SELECT (USAGE on the schema is
	// granted in createSchema). The REVOKE ALL first line is belt-and-suspenders
	// against any inherited default privileges before the narrow SELECT grant.
	if err := queryService.QueryAdminMultiStatement(execCtx, `CREATE UNLOGGED TABLE IF NOT EXISTS multigres.backend_vpid (
	backend_pid integer PRIMARY KEY,
	vpid bigint NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE multigres.backend_vpid SET (
	autovacuum_vacuum_scale_factor = 0,
	autovacuum_vacuum_threshold = 100,
	autovacuum_analyze_scale_factor = 0,
	autovacuum_analyze_threshold = 100
);
REVOKE ALL PRIVILEGES ON TABLE multigres.backend_vpid FROM PUBLIC;
GRANT SELECT ON TABLE multigres.backend_vpid TO PUBLIC`); err != nil {
		return mterrors.Wrap(err, "failed to create backend_vpid table")
	}
	return nil
}

// createMigrationTable creates the Multigres Migrator coordinator's state
// tables: migration, migration_tables, and migration_journal. Idempotent.
// No PUBLIC grant, so they stay readable only through the admin pool.
//
// The migrator's connections are not here: they live in multigres.connections,
// a catalog table on the catalog shard (see ensureCatalogSchema). The migrator's
// own schema ensure adds the foreign key from multigres.migration into it where it
// exists (see migration.AddConnectionForeignKeySQL); this definition has none.
// On the catalog shard, ensureCatalogMigrationSchema runs that ensure after the
// catalog.
func (pm *MultipoolerManager) createMigrationTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	// migration_target is typed multigres.shard_ref. ensureSidecarSchemas
	// also ensures it, but runs after this step, so ensure it here too.
	if err := pm.ensureShardRefType(execCtx); err != nil {
		return err
	}

	// Order matters: each statement depends on types or tables created by the
	// one before it. This runs once on a genuinely fresh shard, so no
	// existence check is needed.
	stmts := []struct {
		label string
		sql   string
	}{
		{"migration table", migration.CreateMigrationSQL},
		{"migration_tables table", migration.CreateMigrationTablesSQL},
		{"migration_journal table", migration.CreateMigrationJournalSQL},
		{"migration_journal index", migration.MigrationJournalMigrationIndexSQL},
	}
	for _, s := range stmts {
		if err := pm.adminExec(execCtx, s.sql); err != nil {
			return mterrors.Wrap(err, "failed to create "+s.label)
		}
	}
	return nil
}

// ensureShardRefType ensures the shared multigres.shard_ref composite type
// (see package shardref), renaming the legacy multigres.shard_key composite
// that clusters built from main between the migration-coordinator wiring and
// the rename to shard_ref may have. It is idempotent.
func (pm *MultipoolerManager) ensureShardRefType(ctx context.Context) error {
	queryService := pm.internalQueryService()
	if queryService == nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "internal query service unavailable for shard_ref type")
	}
	legacy, err := shardref.Ensure(ctx, queryService)
	switch legacy {
	case shardref.LegacyRenamed:
		pm.logger.InfoContext(ctx, "renamed legacy type multigres.shard_key to multigres.shard_ref")
	case shardref.LegacyConflict:
		pm.logger.WarnContext(ctx, "legacy type multigres.shard_key and multigres.shard_ref both exist; leaving them unchanged")
	}
	if err != nil {
		return mterrors.Wrap(err, "failed to ensure shard_ref type")
	}
	return nil
}
