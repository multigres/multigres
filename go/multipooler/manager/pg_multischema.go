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

// createSidecarSchema creates the multigres sidecar schema and all its tables
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

	pm.logger.InfoContext(ctx, "Successfully created multigres sidecar schema")
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
// Data Operations
// ----------------------------------------------------------------------------

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
