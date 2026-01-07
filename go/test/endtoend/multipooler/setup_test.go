// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file provides backward-compatible wrappers around shardsetup.ShardSetup.
//
// The wrappers here (MultipoolerTestSetup, setupPoolerTest, cleanupOption, etc.) exist
// to minimize changes to existing tests in this PR. In a follow-up, we can migrate tests
// to use shardsetup directly and remove most of this file.
//
// What can be removed in a follow-up:
//   - MultipoolerTestSetup wrapper → use shardsetup.ShardSetup directly
//   - setupPoolerTest → use shardsetup.SetupTest (add WithDropTables to shardsetup if needed)
//   - cleanupOption/cleanupConfig → use shardsetup.SetupTestOption directly
//   - waitForManagerReady → use shardsetup.WaitForManagerReady
//
// What should stay (test-specific helpers):
//   - getPrimaryStatusFromClient, waitForSyncConfigConvergenceWithClient, containsStandbyIDInConfig

package multipooler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// testPostgresPassword is exposed from shardsetup for backward compatibility.
const testPostgresPassword = shardsetup.TestPostgresPassword

// ProcessInstance is an alias to shardsetup.ProcessInstance for backward compatibility.
type ProcessInstance = shardsetup.ProcessInstance

// MultipoolerTestSetup provides backward-compatible access to the shared test infrastructure.
// It wraps shardsetup.ShardSetup and exposes the old field-based access pattern.
type MultipoolerTestSetup struct {
	*shardsetup.ShardSetup

	// Backward-compatible fields (these are derived from ShardSetup.Multipoolers map)
	PrimaryMultipooler *ProcessInstance
	StandbyMultipooler *ProcessInstance
	PrimaryPgctld      *ProcessInstance
	StandbyPgctld      *ProcessInstance
}

// newMultipoolerTestSetup wraps a ShardSetup in a MultipoolerTestSetup for backward compatibility.
func newMultipoolerTestSetup(setup *shardsetup.ShardSetup) *MultipoolerTestSetup {
	mts := &MultipoolerTestSetup{
		ShardSetup: setup,
	}

	// Map the new structure to old fields using dynamic primary name
	if setup.PrimaryName != "" {
		if primary := setup.GetMultipoolerInstance(setup.PrimaryName); primary != nil {
			mts.PrimaryMultipooler = primary.Multipooler
			mts.PrimaryPgctld = primary.Pgctld
		}
	}

	// Pick first standby for backward-compatible StandbyMultipooler field
	standbys := setup.GetStandbys()
	if len(standbys) > 0 {
		mts.StandbyMultipooler = standbys[0].Multipooler
		mts.StandbyPgctld = standbys[0].Pgctld
	}

	return mts
}

// getSharedTestSetup returns the shared test infrastructure with backward-compatible field access.
func getSharedTestSetup(t *testing.T) *MultipoolerTestSetup {
	t.Helper()
	setup := newMultipoolerTestSetup(getSharedSetup(t))

	// Multipooler tests expect exactly 2 nodes (1 primary + 1 standby).
	// Fail early if this assumption is violated.
	standbys := setup.GetStandbys()
	if len(standbys) != 1 {
		t.Fatalf("getSharedTestSetup: expected exactly 1 standby, got %d. "+
			"Use shardsetup directly for multi-standby tests.", len(standbys))
	}

	// Fail early with a clear error if primary is not available
	if setup.PrimaryMultipooler == nil {
		t.Fatalf("getSharedTestSetup: PrimaryMultipooler is nil (PrimaryName=%q). "+
			"Cluster may not have bootstrapped correctly.", setup.PrimaryName)
	}

	return setup
}

// waitForManagerReady waits for the manager to be in ready state.
// Deprecated: Use shardsetup.WaitForManagerReady directly.
func waitForManagerReady(t *testing.T, _ *MultipoolerTestSetup, manager *ProcessInstance) {
	t.Helper()
	shardsetup.WaitForManagerReady(t, manager)
}

// cleanupOption is a function that configures cleanup behavior.
type cleanupOption func(*cleanupConfig)

// cleanupConfig holds the configuration for test cleanup.
type cleanupConfig struct {
	noReplication    bool
	pauseReplication bool
	tablesToDrop     []string
	gucsToReset      []string
}

// WithoutReplication returns a cleanup option that explicitly disables replication setup.
func WithoutReplication() cleanupOption {
	return func(c *cleanupConfig) {
		c.noReplication = true
	}
}

// WithPausedReplication returns a cleanup option that starts replication but pauses WAL replay.
func WithPausedReplication() cleanupOption {
	return func(c *cleanupConfig) {
		c.pauseReplication = true
	}
}

// WithDropTables returns a cleanup option that registers tables to drop on cleanup.
func WithDropTables(tables ...string) cleanupOption {
	return func(c *cleanupConfig) {
		c.tablesToDrop = append(c.tablesToDrop, tables...)
	}
}

// WithResetGuc returns a cleanup option that saves and restores specific GUC settings.
func WithResetGuc(gucs ...string) cleanupOption {
	return func(c *cleanupConfig) {
		c.gucsToReset = append(c.gucsToReset, gucs...)
	}
}

// setupPoolerTest provides test isolation by validating clean state, optionally configuring
// replication, and automatically restoring any state changes at test cleanup.
//
// This is a backward-compatible wrapper around shardsetup.ShardSetup.SetupTest().
func setupPoolerTest(t *testing.T, setup *MultipoolerTestSetup, opts ...cleanupOption) {
	t.Helper()

	// Parse options
	config := &cleanupConfig{}
	for _, opt := range opts {
		opt(config)
	}

	// Map to shardsetup options
	var shardOpts []shardsetup.SetupTestOption
	if config.noReplication {
		shardOpts = append(shardOpts, shardsetup.WithoutReplication())
	}
	if config.pauseReplication {
		shardOpts = append(shardOpts, shardsetup.WithPausedReplication())
	}
	if len(config.gucsToReset) > 0 {
		shardOpts = append(shardOpts, shardsetup.WithResetGuc(config.gucsToReset...))
	}

	// Register table cleanup BEFORE SetupTest so it runs AFTER GUC restoration (LIFO order).
	// This is important because if a test breaks replication but sync replication is still
	// configured, DROP TABLE will hang waiting for disconnected standbys.
	// By running after GUC restoration, synchronous_standby_names is already cleared.
	if len(config.tablesToDrop) > 0 {
		t.Cleanup(func() {
			// Use a short timeout - if processes are dead, don't hang
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			primaryClient := setup.NewPrimaryClient(t)
			defer primaryClient.Close()

			for _, table := range config.tablesToDrop {
				_, err := primaryClient.Pooler.ExecuteQuery(ctx, "DROP TABLE IF EXISTS "+table, 0)
				if err != nil {
					t.Logf("Warning: Failed to drop table %s in cleanup: %v", table, err)
				}
			}
		})
	}

	// Delegate to the shared setup (registers GUC restoration cleanup)
	setup.SetupTest(t, shardOpts...)
}

// makeMultipoolerID creates a multipooler ID for testing.
func makeMultipoolerID(cell, name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	}
}

// Helper function to get PrimaryStatus from a manager client.
func getPrimaryStatusFromClient(t *testing.T, client multipoolermanagerpb.MultiPoolerManagerClient) *multipoolermanagerdatapb.PrimaryStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	statusResp, err := client.PrimaryStatus(ctx, &multipoolermanagerdatapb.PrimaryStatusRequest{})
	require.NoError(t, err, "PrimaryStatus should succeed")
	require.NotNil(t, statusResp.Status, "Status should not be nil")
	return statusResp.Status
}

// Helper function to wait for synchronous replication config to converge to expected value.
func waitForSyncConfigConvergenceWithClient(t *testing.T, client multipoolermanagerpb.MultiPoolerManagerClient, checkFunc func(*multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool, message string) {
	t.Helper()
	require.Eventually(t, func() bool {
		status := getPrimaryStatusFromClient(t, client)
		return checkFunc(status.SyncReplicationConfig)
	}, 5*time.Second, 200*time.Millisecond, message)
}

// Helper function to check if a standby ID is in the config.
func containsStandbyIDInConfig(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration, cell, name string) bool {
	if config == nil {
		return false
	}
	for _, id := range config.StandbyIds {
		if id.Cell == cell && id.Name == name {
			return true
		}
	}
	return false
}
