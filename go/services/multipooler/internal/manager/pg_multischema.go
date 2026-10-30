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
	"github.com/multigres/multigres/go/services/multipooler/internal/heartbeat"
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
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
	// (without the rest of createSidecarSchema) from openLocked's
	// version-skew catch-up on an already-bootstrapped shard, so nothing
	// created here may ever depend on a table it creates. Keeping it last
	// turns a violation of that into an ordering failure here - a step
	// above it trying to depend on backend_vpid (or a future sidecar
	// table added here) would fail since ensureSidecarSchemas hasn't run
	// yet - rather than a convention someone has to remember.
	createFuncs = append(createFuncs, pm.ensureSidecarSchemas)

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
//   - openLocked, so every process start and pause-resume cycle re-ensures these
//     tables on an already-bootstrapped shard too, not just genuine bootstrap or
//     promotion).
//
// Every table created here must use CREATE TABLE IF NOT EXISTS (or equivalent):
// unlike createSidecarSchema's other steps, this one is expected to run
// repeatedly against an already-initialized schema.
//
// Currently this is the shared multigres.shard_ref type, backend_vpid and
// pgbackrest_repos_meta on every shard, plus the versioned sharding catalog on the catalog shard
// (ensureCatalogSchema, which is idempotent via its format version rather than
// IF NOT EXISTS); add future late-added sidecar tables here as they arise.
func (pm *MultipoolerManager) ensureSidecarSchemas(ctx context.Context) error {
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

	// pgbackrest_repos_meta versions the generation list; shards bootstrapped
	// before it existed have the repos table but not the revision.
	if err := pm.ensurePgBackRestReposMeta(ctx); err != nil {
		return err
	}

	if pm.isCatalogShard() {
		if err := pm.ensureCatalogSchema(ctx); err != nil {
			return err
		}
	}

	return nil
}

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

// createBackendVpidTable creates multigres.backend_vpid (the gateway-vpid →
// backend-pid mapping read by lock-wait probes).
func (pm *MultipoolerManager) createBackendVpidTable(ctx context.Context) error {
	queryService := pm.internalQueryService()
	if queryService == nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "internal query service unavailable for backend_vpid provisioning")
	}
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
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
// tables: migration_connection, migration, and migration_tables. Idempotent.
// No PUBLIC grant since migration_connection rows hold the source DSN, so
// they stay readable only through the admin pool.
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
		{"migration_connection table", migration.CreateMigrationConnectionSQL},
		{"migration_connection name index", migration.MigrationConnectionNameUniqueIndexSQL},
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
