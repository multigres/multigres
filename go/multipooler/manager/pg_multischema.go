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

	"github.com/multigres/multigres/go/common/mterrors"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
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

// defaultTableGroup is the tablegroup value that indicates this multipooler
// serves the default database where multischema global tables should be created.
const defaultTableGroup = "default"

// defaultShard is the only shard supported for the default tablegroup in the MVP.
const defaultShard = "0-inf"

// createSidecarSchema creates the multigres sidecar schema and all its tables.
//
// MVP Limitation: Currently, we only support the default tablegroup. This function
// validates that the multipooler is configured for the default tablegroup and will
// return an error otherwise.
//
// For the default tablegroup, this function also creates the multischema global
// tables (tablegroup, table, shard).
func (pm *MultiPoolerManager) createSidecarSchema(ctx context.Context) error {
	pm.logger.InfoContext(ctx, "Creating multigres sidecar schema")

	tableGroup := pm.getTableGroup()

	// MVP validation: only default tablegroup is supported
	if tableGroup != defaultTableGroup {
		pm.logger.ErrorContext(ctx, "Only default tablegroup is supported in MVP",
			"tablegroup", tableGroup)
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"only default tablegroup is supported, got: "+tableGroup)
	}

	if err := pm.createSchema(ctx); err != nil {
		return err
	}

	if err := pm.createHeartbeatTable(ctx); err != nil {
		return err
	}

	if err := pm.createDurabilityPolicyTable(ctx); err != nil {
		return err
	}

	// Create multischema global tables for the default tablegroup
	pm.logger.InfoContext(ctx, "Creating multischema global tables for default tablegroup")

	if err := pm.createTablegroups(ctx); err != nil {
		return err
	}

	if err := pm.createTables(ctx); err != nil {
		return err
	}

	if err := pm.createShards(ctx); err != nil {
		return err
	}

	pm.logger.InfoContext(ctx, "Successfully created multigres sidecar schema")
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
func (pm *MultiPoolerManager) initializeMultischemaData(ctx context.Context) error {
	tableGroup := pm.getTableGroup()
	shard := pm.getShard()

	// MVP validation: only default tablegroup with shard 0-inf is supported
	if tableGroup != defaultTableGroup {
		pm.logger.ErrorContext(ctx, "Only default tablegroup is supported in MVP",
			"tablegroup", tableGroup)
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"only default tablegroup is supported, got: "+tableGroup)
	}
	if shard != defaultShard {
		pm.logger.ErrorContext(ctx, "Only shard 0-inf is supported for default tablegroup in MVP",
			"shard", shard)
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"only shard 0-inf is supported for default tablegroup, got: "+shard)
	}

	pm.logger.InfoContext(ctx, "Initializing multischema data",
		"tablegroup", tableGroup, "shard", shard)

	if err := pm.insertTablegroup(ctx, tableGroup); err != nil {
		return err
	}

	if err := pm.insertShard(ctx, tableGroup, shard); err != nil {
		return err
	}

	pm.logger.InfoContext(ctx, "Successfully initialized multischema data")
	return nil
}

// createSchema creates the multigres schema if it doesn't exist
func (pm *MultiPoolerManager) createSchema(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err := pm.db.ExecContext(execCtx, "CREATE SCHEMA IF NOT EXISTS multigres")
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to create multigres schema", "error", err)
		return mterrors.Wrap(err, "failed to create multigres schema")
	}

	return nil
}

// ----------------------------------------------------------------------------
// Table Creation
// ----------------------------------------------------------------------------

// createHeartbeatTable creates the heartbeat table for leader election
func (pm *MultiPoolerManager) createHeartbeatTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err := pm.db.ExecContext(execCtx, `
		CREATE TABLE IF NOT EXISTS multigres.heartbeat (
			shard_id BYTEA PRIMARY KEY,
			leader_id TEXT NOT NULL,
			ts BIGINT NOT NULL
		)
	`)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to create heartbeat table", "error", err)
		return mterrors.Wrap(err, "failed to create heartbeat table")
	}

	return nil
}

// createDurabilityPolicyTable creates the durability_policy table and its indexes
func (pm *MultiPoolerManager) createDurabilityPolicyTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err := pm.db.ExecContext(execCtx, `
		CREATE TABLE IF NOT EXISTS multigres.durability_policy (
			id BIGSERIAL PRIMARY KEY,
			policy_name TEXT NOT NULL,
			policy_version BIGINT NOT NULL,
			quorum_rule JSONB NOT NULL,
			is_active BOOLEAN NOT NULL DEFAULT true,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (policy_name, policy_version),
			CONSTRAINT quorum_rule_required_count_check CHECK (
				(quorum_rule->>'required_count')::int >= 1
			)
		)
	`)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to create durability_policy table", "error", err)
		return mterrors.Wrap(err, "failed to create durability_policy table")
	}

	// Create index on is_active for efficient active policy lookups
	execCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err = pm.db.ExecContext(execCtx, `
		CREATE INDEX IF NOT EXISTS idx_durability_policy_active
		ON multigres.durability_policy(is_active)
		WHERE is_active = true
	`)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to create durability_policy index", "error", err)
		return mterrors.Wrap(err, "failed to create durability_policy index")
	}

	return nil
}

// ----------------------------------------------------------------------------
// Multischema Global Tables (default tablegroup only)
// ----------------------------------------------------------------------------

// createTablegroups creates the tablegroup table for tracking table groups
func (pm *MultiPoolerManager) createTablegroups(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err := pm.db.ExecContext(execCtx, `
		CREATE TABLE IF NOT EXISTS multigres.tablegroup (
			oid BIGSERIAL PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			type TEXT NOT NULL
		)
	`)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to create tablegroup table", "error", err)
		return mterrors.Wrap(err, "failed to create tablegroup table")
	}

	return nil
}

// createTables creates the table table for tracking tables within tablegroups
func (pm *MultiPoolerManager) createTables(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err := pm.db.ExecContext(execCtx, `
		CREATE TABLE IF NOT EXISTS multigres.table (
			oid BIGSERIAL PRIMARY KEY,
			tablegroup_oid BIGINT NOT NULL REFERENCES multigres.tablegroup(oid),
			name TEXT NOT NULL,
			UNIQUE (tablegroup_oid, name)
		)
	`)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to create table table", "error", err)
		return mterrors.Wrap(err, "failed to create table table")
	}

	return nil
}

// createShards creates the shard table for tracking shards within tablegroups
func (pm *MultiPoolerManager) createShards(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err := pm.db.ExecContext(execCtx, `
		CREATE TABLE IF NOT EXISTS multigres.shard (
			oid BIGSERIAL PRIMARY KEY,
			tablegroup_oid BIGINT NOT NULL REFERENCES multigres.tablegroup(oid),
			shard_name TEXT NOT NULL,
			key_range_start BYTEA NULL,
			key_range_end BYTEA NULL,
			UNIQUE (tablegroup_oid, shard_name)
		)
	`)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to create shard table", "error", err)
		return mterrors.Wrap(err, "failed to create shard table")
	}

	return nil
}

// ----------------------------------------------------------------------------
// Data Operations
// ----------------------------------------------------------------------------

// insertTablegroup inserts a tablegroup record into the tablegroup table.
// Uses ON CONFLICT DO NOTHING to handle concurrent insertions gracefully.
// The type is hardcoded to "unsharded" for the MVP.
func (pm *MultiPoolerManager) insertTablegroup(ctx context.Context, name string) error {
	pm.logger.InfoContext(ctx, "Inserting tablegroup", "name", name)

	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err := pm.db.ExecContext(execCtx, `
		INSERT INTO multigres.tablegroup (name, type)
		VALUES ($1, 'unsharded')
		ON CONFLICT (name) DO NOTHING
	`, name)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to insert tablegroup", "name", name, "error", err)
		return mterrors.Wrap(err, "failed to insert tablegroup")
	}

	return nil
}

// insertShard inserts a shard record into the shard table.
// Returns an error if the tablegroup doesn't exist.
// Uses ON CONFLICT DO NOTHING on (tablegroup_oid, shard_name) to handle concurrent insertions gracefully.
func (pm *MultiPoolerManager) insertShard(ctx context.Context, tablegroupName string, shardName string) error {
	pm.logger.InfoContext(ctx, "Inserting shard", "tablegroup", tablegroupName, "shard", shardName)

	// First, fetch the tablegroup oid
	queryCtx, queryCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer queryCancel()

	var tablegroupOid int64
	err := pm.db.QueryRowContext(queryCtx,
		"SELECT oid FROM multigres.tablegroup WHERE name = $1", tablegroupName).Scan(&tablegroupOid)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to find tablegroup", "tablegroup", tablegroupName, "error", err)
		return mterrors.Wrap(err, "failed to find tablegroup: "+tablegroupName)
	}

	// Insert the shard
	execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer execCancel()

	_, err = pm.db.ExecContext(execCtx, `
		INSERT INTO multigres.shard (tablegroup_oid, shard_name)
		VALUES ($1, $2)
		ON CONFLICT (tablegroup_oid, shard_name) DO NOTHING
	`, tablegroupOid, shardName)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to insert shard", "tablegroup", tablegroupName, "shard", shardName, "error", err)
		return mterrors.Wrap(err, "failed to insert shard")
	}

	return nil
}

// insertDurabilityPolicy inserts a durability policy into the durability_policy table.
// Uses ON CONFLICT DO NOTHING to handle concurrent insertions gracefully.
func (pm *MultiPoolerManager) insertDurabilityPolicy(ctx context.Context, policyName string, quorumRuleJSON []byte) error {
	pm.logger.InfoContext(ctx, "Inserting durability policy", "policy_name", policyName)

	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err := pm.db.ExecContext(execCtx, `
		INSERT INTO multigres.durability_policy (policy_name, policy_version, quorum_rule, is_active, created_at, updated_at)
		VALUES ($1, 1, $2::jsonb, true, NOW(), NOW())
		ON CONFLICT (policy_name, policy_version) DO NOTHING
	`, policyName, quorumRuleJSON)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to insert durability policy", "policy_name", policyName, "error", err)
		return mterrors.Wrap(err, "failed to insert durability policy")
	}

	pm.logger.InfoContext(ctx, "Successfully inserted durability policy", "policy_name", policyName)
	return nil
}
