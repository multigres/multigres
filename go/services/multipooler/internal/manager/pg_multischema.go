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
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
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
// MVP Limitation: Currently, we only support the default tablegroup. This function
// validates that the multipooler is configured for the default tablegroup and will
// return an error otherwise.
//
// For the default tablegroup, this function also creates the multischema global
// tables (tablegroup, tablegroup_table, shard).
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
		func(ctx context.Context) error {
			// Create multischema global tables for the default tablegroup
			pm.logger.InfoContext(ctx, "creating multischema global tables for default tablegroup")
			return pm.createTablegroup(ctx)
		},
		pm.createTablegroupTable,
		pm.createShard,
		// migration is the Multigres Migrator table-migration coordinator's state table.
		// Created here so standbys inherit it via restore and post-failover primaries
		// already have it; the coordinator also ensures it on first use to cover
		// shards bootstrapped before this table existed.
		pm.createMigrationTable,
		// ensureSidecarSchemas goes last, deliberately: it also runs alone
		// (without the rest of createSidecarSchema) from openLocked's
		// version-skew catch-up on an already-bootstrapped shard, so nothing
		// created here may ever depend on a table it creates. Keeping it last
		// turns a violation of that into an ordering failure here - a step
		// above it trying to depend on backend_vpid (or a future sidecar
		// table added here) would fail since ensureSidecarSchemas hasn't run
		// yet - rather than a convention someone has to remember.
		pm.ensureSidecarSchemas,
	}

	for _, createFunc := range createFuncs {
		if err := createFunc(ctx); err != nil {
			return err
		}
	}

	pm.logger.InfoContext(ctx, "successfully created multigres sidecar schema")
	return nil
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
// Currently this is just backend_vpid; add future late-added sidecar tables
// here as they arise.
func (pm *MultipoolerManager) ensureSidecarSchemas(ctx context.Context) error {
	// backend_vpid maps live backend pids to gateway virtual pids.
	if err := pm.createBackendVpidTable(ctx); err != nil {
		return err
	}
	return nil
}

// initializeMultischemaData inserts the initial tablegroup and shard records.
//
// MVP Limitation: Currently, we only support the default tablegroup with shard "0-inf".
// This function validates these constraints and returns an error otherwise.
//
// TODO: In the future, tablegroup and shard insertion should be done via a dedicated
// RPC, and the bootstrap code should insert the tablegroup in the default primary
// pooler. For simplicity in the MVP, we do this as part of InitializePrimary since
// we only support a single tablegroup/shard for now.
func (pm *MultipoolerManager) initializeMultischemaData(ctx context.Context) error {
	tableGroup := pm.record.ShardKey().GetTableGroup()
	shard := pm.record.ShardKey().GetShard()

	// MVP validation: only default tablegroup with shard 0-inf is supported
	// This is an extra guardrail. Multipoolers shouldn't start unless they
	// are in the default tablegroup. However, we shouldn't be calling this function
	// by the time we support multiple tablegroups/shards.
	// This will ensure we make sure to remove this code when we get to that point.
	if err := constants.ValidateMVPTableGroupAndShard(tableGroup, shard); err != nil {
		return mterrors.Wrap(err, "MVP validation failed in initializeMultischemaData")
	}

	pm.logger.InfoContext(ctx, "initializing multischema data",
		"tablegroup", tableGroup, "shard", shard)

	if err := pm.insertTablegroup(ctx, tableGroup); err != nil {
		return err
	}

	if err := pm.insertShard(ctx, tableGroup, shard); err != nil {
		return err
	}

	pm.logger.InfoContext(ctx, "successfully initialized multischema data")
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
	if err := pm.adminExec(execCtx, `CREATE TABLE multigres.heartbeat (
		shard_id BYTEA PRIMARY KEY,
		leader_id TEXT NOT NULL,
		ts BIGINT NOT NULL,
		quorum_commit_lsn pg_lsn,
		quorum_commit_ts TIMESTAMPTZ
	)`); err != nil {
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

// ----------------------------------------------------------------------------
// Multischema Global Tables (default tablegroup only)
// ----------------------------------------------------------------------------

// createTablegroup creates the tablegroup table for tracking table groups
func (pm *MultipoolerManager) createTablegroup(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.adminExec(execCtx, `CREATE TABLE multigres.tablegroup (
		oid BIGSERIAL PRIMARY KEY,
		name TEXT NOT NULL UNIQUE,
		type TEXT NOT NULL
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create tablegroup table")
	}
	return nil
}

// createTablegroupTable creates the tablegroup_table table for tracking tables within tablegroups
func (pm *MultipoolerManager) createTablegroupTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.adminExec(execCtx, `CREATE TABLE multigres.tablegroup_table (
		oid BIGSERIAL PRIMARY KEY,
		tablegroup_oid BIGINT NOT NULL REFERENCES multigres.tablegroup(oid),
		name TEXT NOT NULL,
		UNIQUE (tablegroup_oid, name)
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create tablegroup_table table")
	}
	return nil
}

// createShard creates the shard table for tracking shards within tablegroups
func (pm *MultipoolerManager) createShard(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.adminExec(execCtx, `CREATE TABLE multigres.shard (
		oid BIGSERIAL PRIMARY KEY,
		tablegroup_oid BIGINT NOT NULL REFERENCES multigres.tablegroup(oid),
		shard_name TEXT NOT NULL,
		key_range_start BYTEA NULL,
		key_range_end BYTEA NULL,
		UNIQUE (tablegroup_oid, shard_name)
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create shard table")
	}
	return nil
}

// createMigrationTable creates multigres.migration_connection, multigres.migration,
// and migration's child multigres.migration_tables (the normalized per-migration
// table list) — the Multigres Migrator coordinator's state. Idempotent (IF NOT
// EXISTS); no PUBLIC grant, so they stay readable only through the admin
// (superuser) pool — rows hold the source DSN. The DDL lives with the
// coordinator (migration.CreateMigrationConnectionSQL / CreateMigrationSQL /
// CreateMigrationTablesSQL); migration_connection is created first (migration's
// connection_id column carries a foreign key into it), migration_tables second
// (it references migration).
func (pm *MultipoolerManager) createMigrationTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	// Order matters: shard_key (migration's migration_target column is typed
	// with it) before migration_connection, migration_connection (and its name
	// index) before migration (connection_id's foreign key target), migration
	// before migration_tables (its foreign key target), migration_journal's
	// index after its table. This runs exactly once, on a genuinely fresh
	// shard — unlike Store.EnsureSchema (which also creates shard_key, for the
	// fallback case of a shard bootstrapped before this table existed), there
	// is no need for an idempotent existence check here: the type cannot
	// already exist the first time this ever runs.
	stmts := []struct {
		label string
		sql   string
	}{
		{"shard_key type", migration.CreateMigrationShardKeyTypeSQL},
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

// ----------------------------------------------------------------------------
// Data Operations
// ----------------------------------------------------------------------------

// insertTablegroup inserts a tablegroup record into the tablegroup table.
// Uses ON CONFLICT DO NOTHING to handle concurrent insertions gracefully.
// The type is hardcoded to "unsharded" for the MVP.
func (pm *MultipoolerManager) insertTablegroup(ctx context.Context, name string) error {
	pm.logger.InfoContext(ctx, "inserting tablegroup", "name", name)
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	err := pm.adminExecArgs(execCtx, `INSERT INTO multigres.tablegroup (name, type)
		VALUES ($1, 'unsharded')
		ON CONFLICT (name) DO NOTHING`, name)
	if err != nil {
		return mterrors.Wrap(err, "failed to insert tablegroup")
	}
	return nil
}

// insertShard inserts a shard record into the shard table.
// Returns an error if the tablegroup doesn't exist.
// Uses ON CONFLICT DO NOTHING on (tablegroup_oid, shard_name) to handle concurrent insertions gracefully.
func (pm *MultipoolerManager) insertShard(ctx context.Context, tablegroupName string, shardName string) error {
	pm.logger.InfoContext(ctx, "inserting shard", "tablegroup", tablegroupName, "shard", shardName)

	// First, fetch the tablegroup oid
	queryCtx, queryCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer queryCancel()
	result, err := pm.adminQueryArgs(queryCtx, "SELECT oid FROM multigres.tablegroup WHERE name = $1", tablegroupName)
	if err != nil {
		return mterrors.Wrap(err, "failed to find tablegroup: "+tablegroupName)
	}

	var tablegroupOid int64
	if err := executor.ScanSingleRow(result, &tablegroupOid); err != nil {
		return mterrors.Wrap(err, "failed to find tablegroup: "+tablegroupName)
	}

	// Insert the shard
	execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer execCancel()
	err = pm.adminExecArgs(execCtx, `INSERT INTO multigres.shard (tablegroup_oid, shard_name)
		VALUES ($1, $2)
		ON CONFLICT (tablegroup_oid, shard_name) DO NOTHING`, tablegroupOid, shardName)
	if err != nil {
		return mterrors.Wrap(err, "failed to insert shard")
	}

	return nil
}
