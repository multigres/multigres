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

package queryserving

import (
	"os"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
	"github.com/multigres/multigres/go/test/endtoend/minigressetup"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// The default, TLS and require-SSL clusters run on either topology, chosen by
// MULTIGRES_E2E_TOPOLOGY (see clustersetup.IsMinigres). The replica and
// slot-based replication clusters need Multigres; their tests skip under
// Minigres.

// setupManager manages the shared test setup for tests in this package.
var setupManager = clustersetup.NewSharedSetupManager(func(t *testing.T) shardsetup.Cluster {
	if clustersetup.IsMinigres() {
		return minigressetup.New(t)
	}
	// Create a 2-node cluster for testing (primary + standby)
	// We only use the primary for transaction tests, but shardsetup requires 2 nodes for bootstrap
	return shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby
		shardsetup.WithMultigateway(),      // enable multigateway
	)
})

// replicaSetupManager manages a shared setup with the multigateway replica port enabled.
var replicaSetupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	return shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultigatewayReplicaPort(), // enable replica-reads port
	)
})

// tlsSetupManager manages a separate shared setup with TLS-enabled multigateway.
// SSL tests need their own cluster because the multigateway must be started with TLS certificates.
var tlsSetupManager = clustersetup.NewSharedSetupManager(func(t *testing.T) shardsetup.Cluster {
	if clustersetup.IsMinigres() {
		return minigressetup.New(t, minigressetup.WithTLS())
	}
	return shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultigatewayTLS(), // enable multigateway with TLS
	)
})

// requireSSLSetupManager manages a shared setup with --pg-require-ssl=true.
// Plaintext StartupMessage is rejected; only TLS-negotiated clients succeed.
var requireSSLSetupManager = clustersetup.NewSharedSetupManager(func(t *testing.T) shardsetup.Cluster {
	if clustersetup.IsMinigres() {
		return minigressetup.New(t, minigressetup.WithRequireSSL())
	}
	return shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultigatewayRequireSSL(),
	)
})

// slotBasedReplicationSetupManager manages a shared setup with
// --enable-slot-based-replication on for both multigateway and multipooler.
// Needs its own cluster because the flag changes what the gateway admits.
var slotBasedReplicationSetupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	return shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultigatewayExtraArgs("--enable-slot-based-replication=true"),
		shardsetup.WithMultipoolerExtraArgs("--enable-slot-based-replication=true"),
	)
})

// TestMain sets the path and cleans up after all tests.
func TestMain(m *testing.M) {
	exitCode := shardsetup.RunTestMain(m)
	if exitCode != 0 {
		setupManager.DumpLogs()
		replicaSetupManager.DumpLogs()
		tlsSetupManager.DumpLogs()
		requireSSLSetupManager.DumpLogs()
		slotBasedReplicationSetupManager.DumpLogs()
	}
	setupManager.Cleanup()
	replicaSetupManager.Cleanup()
	tlsSetupManager.Cleanup()
	requireSSLSetupManager.Cleanup()
	slotBasedReplicationSetupManager.Cleanup()
	os.Exit(exitCode) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}

// getSharedSetup returns the shared setup for tests.
func getSharedSetup(t *testing.T) shardsetup.Cluster {
	t.Helper()
	return setupManager.Get(t)
}

// getTLSSharedSetup returns the shared setup with TLS-enabled multigateway for SSL tests.
func getTLSSharedSetup(t *testing.T) shardsetup.Cluster {
	t.Helper()
	return tlsSetupManager.Get(t)
}

// getRequireSSLSharedSetup returns the shared setup with --pg-require-ssl=true.
func getRequireSSLSharedSetup(t *testing.T) shardsetup.Cluster {
	t.Helper()
	return requireSSLSetupManager.Get(t)
}

// getSlotBasedReplicationSharedSetup returns the shared setup with
// --enable-slot-based-replication=true on multigateway and multipooler.
func getSlotBasedReplicationSharedSetup(t *testing.T) *shardsetup.ShardSetup {
	t.Helper()
	clustersetup.RequireMultigresTopology(t, "slot-based replication to replicas")
	return slotBasedReplicationSetupManager.Get(t)
}

// newIsolatedCluster starts a cluster owned by one test, on the topology chosen
// by MULTIGRES_E2E_TOPOLOGY: a Multigres shard (two poolers and a gateway) or a
// single Minigres process. poolerArgs are passed to the pooler half.
func newIsolatedCluster(t *testing.T, poolerArgs ...string) (shardsetup.Cluster, func()) {
	t.Helper()
	if clustersetup.IsMinigres() {
		return minigressetup.NewIsolated(t, minigressetup.WithExtraArgs(poolerArgs...))
	}
	return shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby (bootstrap needs 2)
		shardsetup.WithMultigateway(),
		shardsetup.WithMultipoolerExtraArgs(poolerArgs...),
	)
}
