# MultiOrch Recovery Integration Tests Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add end-to-end integration tests that verify MultiOrch automatically detects and recovers from shard problems (uninitialized shards, missing primary) without manual intervention.

**Architecture:** Convert existing TestBootstrapInitialization to use MultiOrch for automatic bootstrap detection and execution. Add two new tests for leader reelection scenarios. Tests start multiorch as a separate process, create problem scenarios, and verify multiorch detects and fixes them automatically.

**Tech Stack:** Go testing, exec.Command for process management, etcd for topology, PostgreSQL for data, gRPC for RPC calls

---

## Task 1: Refactor TestBootstrapInitialization to use MultiOrch

**Files:**
- Modify: `go/test/endtoend/bootstrap_test.go:61-296`
- Test: `go test ./go/test/endtoend -run TestBootstrapInitialization -v`

**Step 1: Add helper function to start multiorch process**

Add after `setupPgBackRestForBootstrap` function (line 596):

```go
// startMultiOrch starts a multiorch process with the given configuration
func startMultiOrch(t *testing.T, baseDir, cell string, etcdAddr string, watchTargets []string) *exec.Cmd {
	t.Helper()

	orchDataDir := filepath.Join(baseDir, "multiorch")
	require.NoError(t, os.MkdirAll(orchDataDir, 0o755), "Failed to create multiorch data dir")

	grpcPort := utils.GetFreePort(t)
	httpPort := utils.GetFreePort(t)

	args := []string{
		"--cell", cell,
		"--watch-targets", strings.Join(watchTargets, ","),
		"--topo-global-server-addresses", etcdAddr,
		"--topo-global-root", "/multigres/global",
		"--topo-implementation", "etcd2",
		"--grpc-port", fmt.Sprintf("%d", grpcPort),
		"--http-port", fmt.Sprintf("%d", httpPort),
		"--bookkeeping-interval", "2s",
		"--cluster-metadata-refresh-interval", "2s",
	}

	multiOrchCmd := exec.Command("multiorch", args...)
	multiOrchCmd.Dir = orchDataDir

	logFile := filepath.Join(orchDataDir, "multiorch.log")
	logF, err := os.Create(logFile)
	require.NoError(t, err, "Failed to create multiorch log file")
	multiOrchCmd.Stdout = logF
	multiOrchCmd.Stderr = logF

	require.NoError(t, multiOrchCmd.Start(), "Failed to start multiorch")
	t.Logf("Started multiorch (pid: %d, grpc: %d, http: %d, log: %s)",
		multiOrchCmd.Process.Pid, grpcPort, httpPort, logFile)

	// Wait for multiorch to be ready
	waitForProcessReady(t, "multiorch", grpcPort, 15*time.Second)
	t.Logf("MultiOrch is ready")

	return multiOrchCmd
}
```

**Step 2: Add helper to wait for shard bootstrap completion**

Add after `startMultiOrch` function:

```go
// waitForShardBootstrapped polls multipooler nodes until at least one is initialized as primary
func waitForShardBootstrapped(t *testing.T, nodes []*nodeInstance, timeout time.Duration) *nodeInstance {
	t.Helper()

	deadline := time.Now().Add(timeout)
	checkInterval := 2 * time.Second

	for time.Now().Before(deadline) {
		for _, node := range nodes {
			status := checkInitializationStatus(t, node)
			if status.IsInitialized && status.Role == "primary" {
				t.Logf("Shard bootstrapped: primary is %s", node.name)
				return node
			}
		}
		t.Logf("Waiting for shard bootstrap... (sleeping %v)", checkInterval)
		time.Sleep(checkInterval)
	}

	t.Fatalf("Timeout: shard did not bootstrap within %v", timeout)
	return nil
}
```

**Step 3: Refactor TestBootstrapInitialization to remove direct action call**

Replace lines 154-181 (from "// Setup pgbackrest..." to "// Wait for initialization...") with:

```go
	// Setup pgbackrest configuration for all nodes before bootstrap
	setupPgBackRestForBootstrap(t, tempDir, nodes, pgBackRestStanza)

	// Register nodes in topology so multiorch can discover them
	for _, node := range nodes {
		pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      node.cell,
				Name:      node.name,
			},
			Hostname: "localhost",
			PortMap: map[string]int32{
				"grpc": int32(node.grpcPort),
			},
			Shard:      shardID,
			Database:   database,
			TableGroup: "test",
			Type:       clustermetadatapb.PoolerType_PRIMARY, // All start as PRIMARY candidates
		}
		err = ts.RegisterMultiPooler(ctx, pooler, true /* overwrite */)
		require.NoError(t, err, "Failed to register pooler %s in topology", node.name)
		t.Logf("Registered pooler %s in topology", node.name)
	}

	// Start multiorch to watch this shard
	watchTarget := fmt.Sprintf("%s/test/%s", database, shardID)
	multiOrchCmd := startMultiOrch(t, tempDir, cellName, etcdClientAddr, []string{watchTarget})
	defer terminateProcess(t, multiOrchCmd, "multiorch", 5*time.Second)

	// Wait for multiorch to detect uninitialized shard and bootstrap it automatically
	t.Logf("Waiting for multiorch to detect and bootstrap the shard...")
	primaryNode := waitForShardBootstrapped(t, nodes, 60*time.Second)
	require.NotNil(t, primaryNode, "Expected multiorch to bootstrap shard automatically")
```

**Step 4: Add missing imports at top of file**

Add to import block (after line 29):

```go
	"strconv"
	"strings"
	"syscall"
```

**Step 5: Run test to verify it works**

Run: `go test ./go/test/endtoend -run TestBootstrapInitialization -v -timeout=5m`

Expected: Test passes, multiorch detects uninitialized shard and bootstraps it automatically

**Step 6: Commit**

```bash
git add go/test/endtoend/bootstrap_test.go
git commit -m "test(multiorch): refactor bootstrap test to use multiorch automatic detection

Replace direct bootstrap action call with multiorch process that automatically
detects uninitialized shard and bootstraps it. This tests the full recovery
pipeline: detection -> analysis -> action -> execution."
```

---

## Task 2: Add test for automatic leader reelection after primary failure

**Files:**
- Modify: `go/test/endtoend/bootstrap_test.go` (add new test after TestBootstrapInitialization)
- Test: `go test ./go/test/endtoend -run TestMultiOrchLeaderReelection -v`

**Step 1: Add test function skeleton**

Add after TestBootstrapInitialization (around line 296):

```go
func TestMultiOrchLeaderReelection(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end leader reelection test (short mode or no postgres binaries)")
	}

	_, err := exec.LookPath("etcd")
	require.NoError(t, err, "etcd binary must be available in PATH")

	ctx := t.Context()

	// Setup test directory
	tempDir, err := os.MkdirTemp("/tmp", "lrtest*")
	require.NoError(t, err, "Failed to create temp directory")
	t.Logf("Leader reelection test directory: %s", tempDir)

	t.Cleanup(func() {
		if os.Getenv("KEEP_TEMP_DIRS") != "" {
			t.Logf("Keeping test directory for debugging: %s", tempDir)
			return
		}
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("Warning: failed to remove temp directory %s: %v", tempDir, err)
		} else {
			t.Logf("Cleaned up test directory: %s", tempDir)
		}
	})

	// Start etcd
	etcdDataDir := filepath.Join(tempDir, "etcd_data")
	require.NoError(t, os.MkdirAll(etcdDataDir, 0o755))

	etcdClientAddr, _ := etcdtopo.StartEtcdWithOptions(t, etcdtopo.EtcdOptions{
		ClientPort: utils.GetFreePort(t),
		PeerPort:   utils.GetFreePort(t),
		DataDir:    etcdDataDir,
	})

	t.Logf("Started etcd at %s", etcdClientAddr)

	// Create topology
	testRoot := "/multigres"
	globalRoot := filepath.Join(testRoot, "global")
	cellName := "test-cell"
	cellRoot := filepath.Join(testRoot, cellName)

	ts, err := topo.OpenServer("etcd2", globalRoot, []string{etcdClientAddr})
	require.NoError(t, err, "Failed to open topology server")
	defer ts.Close()

	err = ts.CreateCell(ctx, cellName, &clustermetadatapb.Cell{
		ServerAddresses: []string{etcdClientAddr},
		Root:            cellRoot,
	})
	require.NoError(t, err, "Failed to create cell")

	database := "postgres"
	backupLocation := filepath.Join(tempDir, "pgbackrest-repo")
	err = ts.CreateDatabase(ctx, database, &clustermetadatapb.Database{
		Name:             database,
		BackupLocation:   backupLocation,
		DurabilityPolicy: "ANY_2",
	})
	require.NoError(t, err, "Failed to create database in topology")

	shardID := "test-shard-reelect"
	pgBackRestStanza := "reelect-test"

	// Create 3 nodes
	nodes := make([]*nodeInstance, 3)
	for i := range 3 {
		nodes[i] = createEmptyNode(t, tempDir, cellName, shardID, database, i, etcdClientAddr, pgBackRestStanza)
		defer cleanupNode(t, nodes[i])
	}

	t.Logf("Created 3 empty nodes")

	// Setup pgbackrest
	setupPgBackRestForBootstrap(t, tempDir, nodes, pgBackRestStanza)

	// Register all nodes in topology
	for _, node := range nodes {
		pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      node.cell,
				Name:      node.name,
			},
			Hostname: "localhost",
			PortMap: map[string]int32{
				"grpc": int32(node.grpcPort),
			},
			Shard:      shardID,
			Database:   database,
			TableGroup: "test",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
		}
		err = ts.RegisterMultiPooler(ctx, pooler, true)
		require.NoError(t, err, "Failed to register pooler %s", node.name)
	}

	// Start multiorch
	watchTarget := fmt.Sprintf("%s/test/%s", database, shardID)
	multiOrchCmd := startMultiOrch(t, tempDir, cellName, etcdClientAddr, []string{watchTarget})
	defer terminateProcess(t, multiOrchCmd, "multiorch", 5*time.Second)

	// Wait for initial bootstrap
	t.Logf("Waiting for initial bootstrap...")
	primaryNode := waitForShardBootstrapped(t, nodes, 60*time.Second)
	require.NotNil(t, primaryNode)
	t.Logf("Initial primary: %s", primaryNode.name)

	// Wait for standbys to catch up
	time.Sleep(5 * time.Second)

	// Kill postgres on the primary (multipooler stays running to report unhealthy status)
	t.Logf("Killing postgres on primary node %s to simulate database crash", primaryNode.name)
	killPostgres(t, primaryNode)

	// Wait for multiorch to detect failure and elect new primary
	t.Logf("Waiting for multiorch to detect primary failure and elect new leader...")

	newPrimaryNode := waitForNewPrimaryElected(t, nodes, primaryNode.name, 60*time.Second)
	require.NotNil(t, newPrimaryNode, "Expected multiorch to elect new primary automatically")
	t.Logf("New primary elected: %s", newPrimaryNode.name)

	// Verify new primary is functional
	t.Run("verify new primary is functional", func(t *testing.T) {
		status := checkInitializationStatus(t, newPrimaryNode)
		require.True(t, status.IsInitialized, "New primary should be initialized")
		require.Equal(t, "primary", status.Role, "New leader should have primary role")

		// Verify we can connect and query
		socketDir := filepath.Join(newPrimaryNode.dataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, newPrimaryNode.pgPort)
		defer db.Close()

		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		require.NoError(t, err, "Should be able to query new primary")
		assert.Equal(t, 1, result)
	})
}
```

**Step 2: Add helper functions for postgres process management and primary election**

Add after `waitForShardBootstrapped`:

```go
// getPostgresPid reads the postgres PID from postmaster.pid file
func getPostgresPid(t *testing.T, node *nodeInstance) int {
	t.Helper()

	pidFile := filepath.Join(node.dataDir, "pg_data", "postmaster.pid")
	data, err := os.ReadFile(pidFile)
	require.NoError(t, err, "Failed to read postgres PID file for %s", node.name)

	lines := strings.Split(string(data), "\n")
	require.Greater(t, len(lines), 0, "PID file should have at least one line")

	pid, err := strconv.Atoi(strings.TrimSpace(lines[0]))
	require.NoError(t, err, "Failed to parse PID from postmaster.pid")

	return pid
}

// killPostgres terminates the postgres process for a node (simulates database crash)
func killPostgres(t *testing.T, node *nodeInstance) {
	t.Helper()

	pgPid := getPostgresPid(t, node)
	t.Logf("Killing postgres (PID %d) on node %s", pgPid, node.name)

	err := syscall.Kill(pgPid, syscall.SIGKILL)
	require.NoError(t, err, "Failed to kill postgres process")

	t.Logf("Postgres killed on %s - multipooler should detect failure", node.name)
}

// waitForNewPrimaryElected polls nodes until a new primary (different from oldPrimaryName) is elected
func waitForNewPrimaryElected(t *testing.T, nodes []*nodeInstance, oldPrimaryName string, timeout time.Duration) *nodeInstance {
	t.Helper()

	deadline := time.Now().Add(timeout)
	checkInterval := 2 * time.Second

	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node.name == oldPrimaryName {
				continue // Skip the old primary
			}
			status := checkInitializationStatus(t, node)
			if status.IsInitialized && status.Role == "primary" {
				t.Logf("New primary elected: %s", node.name)
				return node
			}
		}
		t.Logf("Waiting for new primary election... (sleeping %v)", checkInterval)
		time.Sleep(checkInterval)
	}

	t.Fatalf("Timeout: new primary not elected within %v", timeout)
	return nil
}
```

**Step 3: Run test to verify reelection works**

Run: `go test ./go/test/endtoend -run TestMultiOrchLeaderReelection -v -timeout=5m`

Expected: Test passes, multiorch detects failed primary and elects new leader automatically

**Step 4: Commit**

```bash
git add go/test/endtoend/bootstrap_test.go
git commit -m "test(multiorch): add automatic leader reelection integration test

Test verifies multiorch detects primary failure and automatically elects
a new leader from remaining standbys. Covers the ShardHasNoPrimary analyzer
and AppointLeader recovery action."
```

---

## Task 3: Add test for leader reelection in mixed initialization scenario

**Files:**
- Modify: `go/test/endtoend/bootstrap_test.go` (add new test)
- Test: `go test ./go/test/endtoend -run TestMultiOrchMixedInitialization -v`

**Step 1: Add test for mixed initialized/uninitialized scenario**

Add after TestMultiOrchLeaderReelection:

```go
func TestMultiOrchMixedInitializationRepair(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end mixed initialization test (short mode or no postgres binaries)")
	}

	_, err := exec.LookPath("etcd")
	require.NoError(t, err, "etcd binary must be available in PATH")

	ctx := t.Context()

	// Setup test directory
	tempDir, err := os.MkdirTemp("/tmp", "mixtest*")
	require.NoError(t, err, "Failed to create temp directory")
	t.Logf("Mixed initialization test directory: %s", tempDir)

	t.Cleanup(func() {
		if os.Getenv("KEEP_TEMP_DIRS") != "" {
			t.Logf("Keeping test directory for debugging: %s", tempDir)
			return
		}
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("Warning: failed to remove temp directory %s: %v", tempDir, err)
		} else {
			t.Logf("Cleaned up test directory: %s", tempDir)
		}
	})

	// Start etcd
	etcdDataDir := filepath.Join(tempDir, "etcd_data")
	require.NoError(t, os.MkdirAll(etcdDataDir, 0o755))

	etcdClientAddr, _ := etcdtopo.StartEtcdWithOptions(t, etcdtopo.EtcdOptions{
		ClientPort: utils.GetFreePort(t),
		PeerPort:   utils.GetFreePort(t),
		DataDir:    etcdDataDir,
	})

	// Create topology
	testRoot := "/multigres"
	globalRoot := filepath.Join(testRoot, "global")
	cellName := "test-cell"
	cellRoot := filepath.Join(testRoot, cellName)

	ts, err := topo.OpenServer("etcd2", globalRoot, []string{etcdClientAddr})
	require.NoError(t, err)
	defer ts.Close()

	err = ts.CreateCell(ctx, cellName, &clustermetadatapb.Cell{
		ServerAddresses: []string{etcdClientAddr},
		Root:            cellRoot,
	})
	require.NoError(t, err)

	database := "postgres"
	backupLocation := filepath.Join(tempDir, "pgbackrest-repo")
	err = ts.CreateDatabase(ctx, database, &clustermetadatapb.Database{
		Name:             database,
		BackupLocation:   backupLocation,
		DurabilityPolicy: "ANY_2",
	})
	require.NoError(t, err)

	shardID := "test-shard-mixed"
	pgBackRestStanza := "mixed-test"

	// Create 3 nodes
	nodes := make([]*nodeInstance, 3)
	for i := range 3 {
		nodes[i] = createEmptyNode(t, tempDir, cellName, shardID, database, i, etcdClientAddr, pgBackRestStanza)
		defer cleanupNode(t, nodes[i])
	}

	setupPgBackRestForBootstrap(t, tempDir, nodes, pgBackRestStanza)

	// Bootstrap first 2 nodes manually (simulate partial initialization)
	t.Logf("Manually bootstrapping first 2 nodes to create mixed scenario...")

	rpcClient := rpcclient.NewClient(10)
	defer rpcClient.Close()

	coordNodes := make([]*store.PoolerHealth, 2)
	for i := 0; i < 2; i++ {
		node := nodes[i]
		pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      node.cell,
				Name:      node.name,
			},
			Hostname: "localhost",
			PortMap: map[string]int32{
				"grpc": int32(node.grpcPort),
			},
			Shard:    shardID,
			Database: database,
		}
		coordNodes[i] = store.NewPoolerHealthFromMultiPooler(pooler)
	}

	logger := slog.Default()
	bootstrapAction := actions.NewBootstrapShardAction(rpcClient, ts, logger)
	err = bootstrapAction.Execute(ctx, shardID, database, coordNodes)
	require.NoError(t, err, "Manual bootstrap of first 2 nodes should succeed")

	time.Sleep(3 * time.Second) // Wait for bootstrap to complete

	// Verify first 2 nodes are initialized
	for i := 0; i < 2; i++ {
		status := checkInitializationStatus(t, nodes[i])
		require.True(t, status.IsInitialized, "Node %d should be initialized", i)
		t.Logf("Node %d initialized as %s", i, status.Role)
	}

	// Kill the primary to create a mixed scenario: 1 initialized standby + 1 uninitialized
	var initialPrimary *nodeInstance
	var initialStandby *nodeInstance
	for i := 0; i < 2; i++ {
		status := checkInitializationStatus(t, nodes[i])
		if status.Role == "primary" {
			initialPrimary = nodes[i]
		} else {
			initialStandby = nodes[i]
		}
	}
	require.NotNil(t, initialPrimary, "Should have identified initial primary")
	require.NotNil(t, initialStandby, "Should have identified initial standby")

	t.Logf("Killing postgres on initial primary %s to create repair scenario", initialPrimary.name)
	killPostgres(t, initialPrimary)

	// Register all nodes in topology (including the uninitialized node3)
	for _, node := range nodes {
		if node.name == initialPrimary.name {
			continue // Don't register the killed primary
		}
		pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      node.cell,
				Name:      node.name,
			},
			Hostname: "localhost",
			PortMap: map[string]int32{
				"grpc": int32(node.grpcPort),
			},
			Shard:      shardID,
			Database:   database,
			TableGroup: "test",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
		}
		err = ts.RegisterMultiPooler(ctx, pooler, true)
		require.NoError(t, err, "Failed to register pooler %s", node.name)
		t.Logf("Registered %s (initialized: %v)", node.name, node.name != nodes[2].name)
	}

	// Start multiorch to watch and repair
	watchTarget := fmt.Sprintf("%s/test/%s", database, shardID)
	multiOrchCmd := startMultiOrch(t, tempDir, cellName, etcdClientAddr, []string{watchTarget})
	defer terminateProcess(t, multiOrchCmd, "multiorch", 5*time.Second)

	// Wait for multiorch to detect mixed state and appoint the initialized standby as primary
	t.Logf("Waiting for multiorch to detect mixed initialization and appoint new leader...")

	newPrimary := waitForShardBootstrapped(t, []*nodeInstance{initialStandby, nodes[2]}, 60*time.Second)
	require.NotNil(t, newPrimary, "Expected multiorch to appoint new primary")

	// Should prefer the initialized standby over uninitialized node
	assert.Equal(t, initialStandby.name, newPrimary.name,
		"MultiOrch should elect the initialized standby as primary")

	t.Logf("MultiOrch successfully repaired mixed initialization scenario: %s is new primary", newPrimary.name)
}
```

**Step 2: Run test to verify mixed scenario handling**

Run: `go test ./go/test/endtoend -run TestMultiOrchMixedInitialization -v -timeout=5m`

Expected: Test passes, multiorch detects mixed state and elects the initialized node as primary

**Step 3: Commit**

```bash
git add go/test/endtoend/bootstrap_test.go
git commit -m "test(multiorch): add mixed initialization repair integration test

Test verifies multiorch handles mixed scenarios where some nodes are initialized
and others are not. After primary failure, multiorch should prefer promoting
the initialized standby over bootstrapping uninitialized nodes."
```

---

## Task 4: Run all integration tests together

**Step 1: Run all three tests**

Run: `go test ./go/test/endtoend -run "TestBootstrapInitialization|TestMultiOrchLeaderReelection|TestMultiOrchMixedInitialization" -v -timeout=15m`

Expected: All 3 tests pass, demonstrating full recovery pipeline coverage

**Step 2: Verify tests can run individually**

Run each test separately:
```bash
go test ./go/test/endtoend -run TestBootstrapInitialization -v -timeout=5m
go test ./go/test/endtoend -run TestMultiOrchLeaderReelection -v -timeout=5m
go test ./go/test/endtoend -run TestMultiOrchMixedInitialization -v -timeout=5m
```

Expected: Each test passes independently

**Step 3: Final commit with documentation**

Add comment at top of bootstrap_test.go explaining the test suite:

```go
// Package endtoend contains integration tests for multigres components.
//
// Bootstrap and recovery tests:
// - TestBootstrapInitialization: Verifies multiorch automatically detects and bootstraps
//   uninitialized shards without manual intervention.
// - TestMultiOrchLeaderReelection: Verifies multiorch detects primary failure and
//   automatically elects a new leader from remaining standbys.
// - TestMultiOrchMixedInitializationRepair: Verifies multiorch handles mixed scenarios
//   where some nodes are initialized and others are not, preferring to promote
//   initialized standbys over bootstrapping new nodes.
```

```bash
git add go/test/endtoend/bootstrap_test.go
git commit -m "docs(test): add documentation for multiorch recovery integration tests

Document the three integration test scenarios:
- Automatic bootstrap detection and execution
- Automatic leader reelection after primary failure
- Mixed initialization repair scenarios

These tests provide end-to-end coverage of the recovery pipeline:
detection -> analysis -> action -> execution"
```

---

## Verification Checklist

After completing all tasks, verify:

- [ ] All 3 integration tests pass independently
- [ ] Tests can run together without interference
- [ ] Tests properly clean up processes and temp directories
- [ ] MultiOrch process starts and stops cleanly in tests
- [ ] Tests verify actual database state (not just process state)
- [ ] Test logs clearly show detection and recovery flow
- [ ] No flaky timeouts (tests have reasonable timeout values)
- [ ] Tests skip appropriately in short mode or without postgres binaries

## Notes

- **TDD Not Applicable:** These are integration tests that test existing functionality end-to-end. The code being tested (recovery pipeline) is already implemented.
- **Process Management:** All tests use `t.Cleanup()` and `defer` to ensure processes are terminated even on test failure.
- **Timing:** Tests use polling with reasonable intervals (2s) and timeouts (60s for bootstrap, reelection) to avoid flakiness.
- **Isolation:** Each test uses separate temp directory and unique shard IDs to avoid interference.
