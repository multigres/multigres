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
	"github.com/multigres/multigres/go/multipooler/executor"
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
func (pm *MultiPoolerManager) createSidecarSchema(ctx context.Context) error {
	pm.logger.InfoContext(ctx, "Creating multigres sidecar schema")

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

	if err := pm.createTablegroup(ctx); err != nil {
		return err
	}

	if err := pm.createTablegroupTable(ctx); err != nil {
		return err
	}

	if err := pm.createShard(ctx); err != nil {
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
	// This is an extra guardrail. Multipoolers shouldn't start unless they
	// are in the default tablegroup. However, we shouldn't be calling this function
	// by the time we support multiple tablegroups/shards.
	// This will ensure we make sure to remove this code when we get to that point.
	if err := constants.ValidateMVPTableGroupAndShard(tableGroup, shard); err != nil {
		return mterrors.Wrap(err, "MVP validation failed in initializeMultischemaData")
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
	if err := pm.exec(execCtx, "CREATE SCHEMA IF NOT EXISTS multigres"); err != nil {
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
	sql := `CREATE TABLE IF NOT EXISTS multigres.heartbeat (
		shard_id BYTEA PRIMARY KEY,
		leader_id TEXT NOT NULL,
		ts BIGINT NOT NULL
	)`
	if err := pm.exec(execCtx, sql); err != nil {
		return mterrors.Wrap(err, "failed to create heartbeat table")
	}
	return nil
}

// createDurabilityPolicyTable creates the durability_policy table and its indexes
func (pm *MultiPoolerManager) createDurabilityPolicyTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	tableSql := `CREATE TABLE IF NOT EXISTS multigres.durability_policy (
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
	)`
	if err := pm.exec(execCtx, tableSql); err != nil {
		return mterrors.Wrap(err, "failed to create durability_policy table")
	}
	execCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	// Create index on is_active for efficient active policy lookups
	indexSql := `CREATE INDEX IF NOT EXISTS idx_durability_policy_active
		ON multigres.durability_policy(is_active)
		WHERE is_active = true`
	if err := pm.exec(execCtx, indexSql); err != nil {
		return mterrors.Wrap(err, "failed to create durability_policy index")
	}

	return nil
}

// ----------------------------------------------------------------------------
// Multischema Global Tables (default tablegroup only)
// ----------------------------------------------------------------------------

// createTablegroup creates the tablegroup table for tracking table groups
func (pm *MultiPoolerManager) createTablegroup(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	sql := `CREATE TABLE IF NOT EXISTS multigres.tablegroup (
		oid BIGSERIAL PRIMARY KEY,
		name TEXT NOT NULL UNIQUE,
		type TEXT NOT NULL
	)`
	if err := pm.exec(execCtx, sql); err != nil {
		return mterrors.Wrap(err, "failed to create tablegroup table")
	}
	return nil
}

// createTablegroupTable creates the tablegroup_table table for tracking tables within tablegroups
func (pm *MultiPoolerManager) createTablegroupTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	sql := `CREATE TABLE IF NOT EXISTS multigres.tablegroup_table (
		oid BIGSERIAL PRIMARY KEY,
		tablegroup_oid BIGINT NOT NULL REFERENCES multigres.tablegroup(oid),
		name TEXT NOT NULL,
		UNIQUE (tablegroup_oid, name)
	)`
	if err := pm.exec(execCtx, sql); err != nil {
		return mterrors.Wrap(err, "failed to create tablegroup_table table")
	}
	return nil
}

// createShard creates the shard table for tracking shards within tablegroups
func (pm *MultiPoolerManager) createShard(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	sql := `CREATE TABLE IF NOT EXISTS multigres.shard (
		oid BIGSERIAL PRIMARY KEY,
		tablegroup_oid BIGINT NOT NULL REFERENCES multigres.tablegroup(oid),
		shard_name TEXT NOT NULL,
		key_range_start BYTEA NULL,
		key_range_end BYTEA NULL,
		UNIQUE (tablegroup_oid, shard_name)
	)`
	if err := pm.exec(execCtx, sql); err != nil {
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
	err := pm.execArgs(execCtx, `INSERT INTO multigres.tablegroup (name, type)
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
func (pm *MultiPoolerManager) insertShard(ctx context.Context, tablegroupName string, shardName string) error {
	pm.logger.InfoContext(ctx, "Inserting shard", "tablegroup", tablegroupName, "shard", shardName)

	// First, fetch the tablegroup oid
	queryCtx, queryCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer queryCancel()
	result, err := pm.queryArgs(queryCtx, "SELECT oid FROM multigres.tablegroup WHERE name = $1", tablegroupName)
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
	err = pm.execArgs(execCtx, `INSERT INTO multigres.shard (tablegroup_oid, shard_name)
		VALUES ($1, $2)
		ON CONFLICT (tablegroup_oid, shard_name) DO NOTHING`, tablegroupOid, shardName)
	if err != nil {
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
	err := pm.execArgs(execCtx, `INSERT INTO multigres.durability_policy (policy_name, policy_version, quorum_rule, is_active, created_at, updated_at)
		VALUES ($1, 1, $2::jsonb, true, NOW(), NOW())
		ON CONFLICT (policy_name, policy_version) DO NOTHING`, policyName, string(quorumRuleJSON))
	if err != nil {
		return mterrors.Wrap(err, "failed to insert durability policy")
	}

	pm.logger.InfoContext(ctx, "Successfully inserted durability policy", "policy_name", policyName)
	return nil
}
