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

// Package endtoend contains integration tests for multigres components.
//
// Bootstrap sync replication test:
//   - TestBootstrapConfiguresSyncReplication: Verifies that after bootstrap completes,
//     the primary has synchronous_standby_names configured according to the durability policy.
package endtoend

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

// TestBootstrapConfiguresSyncReplication verifies that after bootstrap completes,
// the primary has synchronous_standby_names configured according to the durability policy.
func TestBootstrapConfiguresSyncReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping test without postgres")
	}

	// Setup 3-node cluster with ANY_2 durability policy
	env := setupMultiOrchTestEnv(t, testEnvConfig{
		tempDirPrefix:    "bsrt*",
		cellName:         "test-cell",
		database:         "postgres",
		shardID:          "sync-repl-test",
		tableGroup:       "test",
		durabilityPolicy: "ANY_2",
		stanzaName:       "sync-repl-test",
	})

	nodes := env.createNodes(3)
	env.setupPgBackRest()
	env.registerNodes()

	// Start multiorch and wait for bootstrap
	multiOrchCmd := env.startMultiOrch()
	defer terminateProcess(t, multiOrchCmd, "multiorch", 5*time.Second)

	primaryNode := waitForShardPrimary(t, nodes, 90*time.Second)
	require.NotNil(t, primaryNode, "should have a primary after bootstrap")

	// Wait for at least 1 standby to initialize (may have issues with all standbys in test environment)
	// For sync replication, we need at least 1 standby to verify configuration
	waitForStandbysInitialized(t, nodes, primaryNode.name, 1, 90*time.Second)

	// Verify synchronous_standby_names is configured on primary
	t.Run("verifies_sync_standby_names_configured", func(t *testing.T) {
		socketDir := filepath.Join(primaryNode.dataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, primaryNode.pgPort)
		defer db.Close()

		var syncStandbyNames string
		err := db.QueryRow("SHOW synchronous_standby_names").Scan(&syncStandbyNames)
		require.NoError(t, err, "should query synchronous_standby_names")

		assert.NotEmpty(t, syncStandbyNames,
			"synchronous_standby_names should be configured for ANY_2 policy")
		t.Logf("synchronous_standby_names = %s", syncStandbyNames)

		// Verify it contains ANY keyword (for ANY_2 policy)
		assert.Contains(t, strings.ToUpper(syncStandbyNames), "ANY",
			"should use ANY method for ANY_2 policy")
	})
}
