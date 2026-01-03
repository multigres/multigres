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

package shardsetup

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/etcdtopo"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	// Register topo plugins
	_ "github.com/multigres/multigres/go/common/plugins/topo"
)

// SetupConfig holds the configuration for creating a ShardSetup.
type SetupConfig struct {
	MultipoolerCount   int
	MultiOrchCount     int
	Database           string
	TableGroup         string
	Shard              string
	CellName           string
	DurabilityPolicy   string // Durability policy (e.g., "ANY_2")
	SkipInitialization bool   // Start processes but don't initialize postgres (for bootstrap tests)
}

// SetupOption is a function that configures setup creation.
type SetupOption func(*SetupConfig)

// WithMultipoolerCount sets the number of multipooler instances to create.
// Default is 2 (primary + standby).
func WithMultipoolerCount(count int) SetupOption {
	return func(c *SetupConfig) {
		c.MultipoolerCount = count
	}
}

// WithMultiOrchCount sets the number of multiorch instances to create.
// Default is 0.
func WithMultiOrchCount(count int) SetupOption {
	return func(c *SetupConfig) {
		c.MultiOrchCount = count
	}
}

// WithDatabase sets the database name for the topology.
func WithDatabase(db string) SetupOption {
	return func(c *SetupConfig) {
		c.Database = db
	}
}

// WithCellName sets the cell name for the topology.
func WithCellName(cell string) SetupOption {
	return func(c *SetupConfig) {
		c.CellName = cell
	}
}

// WithDurabilityPolicy sets the durability policy for the database.
// Default is "ANY_2".
func WithDurabilityPolicy(policy string) SetupOption {
	return func(c *SetupConfig) {
		c.DurabilityPolicy = policy
	}
}

// WithoutInitialization skips postgres initialization and leaves nodes uninitialized.
// Use this for bootstrap tests where multiorch will initialize the shard.
// Processes (pgctld, multipooler) are started but postgres is not initialized.
func WithoutInitialization() SetupOption {
	return func(c *SetupConfig) {
		c.SkipInitialization = true
	}
}

// SetupTestConfig holds configuration for SetupTest.
type SetupTestConfig struct {
	NoReplication    bool     // Don't configure replication
	PauseReplication bool     // Configure replication but pause WAL replay
	GucsToReset      []string // GUCs to save before test and restore after
}

// SetupTestOption is a function that configures SetupTest behavior.
type SetupTestOption func(*SetupTestConfig)

// WithoutReplication returns an option that actively breaks replication.
// Clears primary_conninfo and synchronous_standby_names, so tests can set up replication from scratch.
func WithoutReplication() SetupTestOption {
	return func(c *SetupTestConfig) {
		c.NoReplication = true
	}
}

// WithPausedReplication returns an option that pauses WAL replay on standbys.
// Replication is already configured from bootstrap; this just pauses WAL application.
// Use this for tests that need to test pg_wal_replay_resume().
func WithPausedReplication() SetupTestOption {
	return func(c *SetupTestConfig) {
		c.PauseReplication = true
	}
}

// WithResetGuc returns an option that saves and restores specific GUC settings.
func WithResetGuc(gucNames ...string) SetupTestOption {
	return func(c *SetupTestConfig) {
		c.GucsToReset = append(c.GucsToReset, gucNames...)
	}
}

// multipoolerName returns the name for a multipooler instance by index.
// Uses generic names like "pooler-1", "pooler-2" since multiorch decides which becomes primary.
func multipoolerName(index int) string {
	return fmt.Sprintf("pooler-%d", index+1)
}

// multiOrchName returns the name for a multiorch instance by index.
func multiOrchName(index int) string {
	if index == 0 {
		return "multiorch"
	}
	return fmt.Sprintf("multiorch%d", index)
}

// NewIsolated creates a new isolated ShardSetup for a single test and returns a cleanup function.
// Use this instead of a shared setup when tests need to kill primaries or perform other
// destructive operations that can't be cleanly restored.
//
// Example:
//
//	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(3))
//	defer cleanup()
//	// ... test code that kills primaries, etc.
//
// The cleanup function stops all processes, removes the temp directory, etc.
// If the test failed, it dumps service logs before cleanup to aid debugging.
// Unlike shared setups, this shard is completely isolated and won't affect other tests.
func NewIsolated(t *testing.T, opts ...SetupOption) (*ShardSetup, func()) {
	t.Helper()

	setup := New(t, opts...)
	cleanup := func() {
		if t.Failed() {
			setup.DumpServiceLogs()
		}
		setup.Cleanup()
	}
	return setup, cleanup
}

// New creates a new ShardSetup with the specified configuration.
// This follows the pattern from multipooler/setup_test.go:getSharedTestSetup.
func New(t *testing.T, opts ...SetupOption) *ShardSetup {
	t.Helper()

	// Default configuration
	config := &SetupConfig{
		MultipoolerCount: 2, // primary + standby
		MultiOrchCount:   0,
		Database:         "postgres",
		TableGroup:       constants.DefaultTableGroup,
		Shard:            constants.DefaultShard,
		CellName:         "test-cell",
		DurabilityPolicy: "ANY_2",
	}

	// Apply options
	for _, opt := range opts {
		opt(config)
	}

	if config.MultipoolerCount < 1 {
		t.Fatalf("MultipoolerCount must be at least 1, got %d", config.MultipoolerCount)
	}

	// Verify TestMain set up PATH correctly (our binaries should be available)
	for _, binary := range []string{"multipooler", "pgctld"} {
		if _, err := exec.LookPath(binary); err != nil {
			t.Fatalf("%s binary not found in PATH - ensure TestMain calls pathutil.PrependBinToPath()", binary)
		}
	}

	// Check if PostgreSQL binaries are available
	if !utils.HasPostgreSQLBinaries() {
		t.Fatalf("PostgreSQL binaries not found, make sure to install PostgreSQL and add it to the PATH")
	}

	tempDir, tempDirCleanup := testutil.TempDir(t, "shardsetup_test")

	// Start etcd for topology
	t.Logf("Starting etcd for topology...")

	etcdDataDir := filepath.Join(tempDir, "etcd_data")
	if err := os.MkdirAll(etcdDataDir, 0o755); err != nil {
		t.Fatalf("failed to create etcd data directory: %v", err)
	}
	etcdClientAddr, etcdCmd, err := startEtcd(t, etcdDataDir)
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}

	// Create topology server and cell
	testRoot := "/multigres"
	globalRoot := path.Join(testRoot, "global")
	cellRoot := path.Join(testRoot, config.CellName)

	ts, err := topoclient.OpenServer(topoclient.DefaultTopoImplementation, globalRoot, []string{etcdClientAddr}, topoclient.NewDefaultTopoConfig())
	if err != nil {
		t.Fatalf("failed to open topology server: %v", err)
	}

	// Create the cell
	err = ts.CreateCell(context.Background(), config.CellName, &clustermetadatapb.Cell{
		ServerAddresses: []string{etcdClientAddr},
		Root:            cellRoot,
	})
	if err != nil {
		t.Fatalf("failed to create cell: %v", err)
	}

	t.Logf("Created topology cell '%s' at etcd %s", config.CellName, etcdClientAddr)

	// Create the database entry in topology with backup_location
	backupLocation := filepath.Join(tempDir, "backup-repo")
	err = ts.CreateDatabase(context.Background(), config.Database, &clustermetadatapb.Database{
		Name:             config.Database,
		BackupLocation:   backupLocation,
		DurabilityPolicy: config.DurabilityPolicy,
	})
	if err != nil {
		t.Fatalf("failed to create database in topology: %v", err)
	}
	t.Logf("Created database '%s' in topology with backup_location=%s", config.Database, backupLocation)

	setup := &ShardSetup{
		TempDir:            tempDir,
		TempDirCleanup:     tempDirCleanup,
		EtcdClientAddr:     etcdClientAddr,
		EtcdCmd:            etcdCmd,
		TopoServer:         ts,
		CellName:           config.CellName,
		Multipoolers:       make(map[string]*MultipoolerInstance),
		MultiOrchInstances: make(map[string]*ProcessInstance),
	}

	// Create all multipooler instances (but don't start yet)
	var multipoolerInstances []*MultipoolerInstance
	for i := 0; i < config.MultipoolerCount; i++ {
		name := multipoolerName(i)
		grpcPort := utils.GetFreePort(t)
		pgPort := utils.GetFreePort(t)
		multipoolerPort := utils.GetFreePort(t)

		inst := setup.CreateMultipoolerInstance(t, name, grpcPort, pgPort, multipoolerPort)
		multipoolerInstances = append(multipoolerInstances, inst)

		t.Logf("Created multipooler instance '%s': pgctld gRPC=%d, PG=%d, multipooler gRPC=%d",
			name, grpcPort, pgPort, multipoolerPort)
	}

	// Start all processes (pgctld, multipooler, pgbackrest) for all nodes
	startProcessesWithoutInit(t, setup, multipoolerInstances, config)

	// Create multiorch instances (if any requested by the test)
	setup.createMultiOrchInstances(t, config)

	// For uninitialized mode (bootstrap tests), we're done - leave nodes uninitialized
	if config.SkipInitialization {
		t.Logf("Shard setup complete (uninitialized): %d multipoolers, %d multiorchs",
			config.MultipoolerCount, config.MultiOrchCount)
		return setup
	}

	// Use multiorch to bootstrap the shard organically
	initializeWithMultiOrch(t, setup, config)

	// Start pgBackRest servers after initialization completes
	// (multipooler generates config files during initialization)
	setup.startPgBackRestServers(t)

	t.Logf("Shard setup complete: %d multipoolers, %d multiorchs",
		config.MultipoolerCount, config.MultiOrchCount)

	return setup
}

// createMultiOrchInstances creates multiorch instances (but doesn't start them).
func (s *ShardSetup) createMultiOrchInstances(t *testing.T, config *SetupConfig) {
	t.Helper()
	if config.MultiOrchCount == 0 {
		return
	}
	watchTargets := []string{fmt.Sprintf("%s/%s/%s", config.Database, config.TableGroup, config.Shard)}
	for i := 0; i < config.MultiOrchCount; i++ {
		name := multiOrchName(i)
		s.CreateMultiOrchInstance(t, name, config.CellName, watchTargets)
		t.Logf("Created multiorch '%s' (will start after replication is configured)", name)
	}
}

// StartMultiOrchs starts all multiorch instances.
// Use this for tests that need multiorch running from the get-go.
func (s *ShardSetup) StartMultiOrchs(t *testing.T) {
	t.Helper()
	for name, mo := range s.MultiOrchInstances {
		if mo.IsRunning() {
			continue
		}
		if err := mo.Start(t); err != nil {
			t.Fatalf("StartMultiOrchs: failed to start multiorch %s: %v", name, err)
		}
		t.Cleanup(mo.CleanupFunc(t))
		t.Logf("StartMultiOrchs: Started multiorch '%s': gRPC=%d, HTTP=%d", name, mo.GrpcPort, mo.HttpPort)
	}
}

// initializeWithMultiOrch uses multiorch to bootstrap the shard organically.
// It starts a single multiorch (temporary if none configured), waits for it to
// initialize the shard, then stops it (clean state = multiorch not running).
func initializeWithMultiOrch(t *testing.T, setup *ShardSetup, config *SetupConfig) {
	t.Helper()

	var mo *ProcessInstance
	var moName string
	var isTemporary bool
	var moCleanup func()

	// Use existing multiorch or create a temporary one
	if len(setup.MultiOrchInstances) > 0 {
		// Use the first multiorch instance
		for name, inst := range setup.MultiOrchInstances {
			mo = inst
			moName = name
			moCleanup = inst.CleanupFunc(t)
			break
		}
	} else {
		// Create a temporary multiorch for initialization
		watchTargets := []string{fmt.Sprintf("%s/%s/%s", config.Database, config.TableGroup, config.Shard)}
		mo, moCleanup = setup.CreateMultiOrchInstance(t, "temp-multiorch", config.CellName, watchTargets)
		moName = "temp-multiorch"
		isTemporary = true
		t.Logf("Created temporary multiorch for initialization")
	}

	// Start multiorch
	if err := mo.Start(t); err != nil {
		t.Fatalf("failed to start multiorch %s: %v", moName, err)
	}
	t.Logf("Started multiorch '%s' for shard bootstrap", moName)

	// Wait for multiorch to bootstrap the shard (elect a primary)
	primaryName, err := waitForShardBootstrap(t, setup)
	if err != nil {
		t.Fatalf("failed to bootstrap shard: %v", err)
	}
	setup.PrimaryName = primaryName
	t.Logf("Primary elected: %s", primaryName)

	// Stop multiorch (clean state = multiorch not running)
	moCleanup()
	t.Logf("Stopped multiorch '%s' after bootstrap", moName)

	// Remove temporary multiorch from the map
	if isTemporary {
		delete(setup.MultiOrchInstances, "temp-multiorch")
	}

	// Save the current GUC values as the baseline "clean state".
	// After bootstrap, replication is configured, so the baseline includes:
	// - Primary: synchronous_standby_names with standby list, synchronous_commit=on
	// - Replicas: primary_conninfo pointing to primary
	// ValidateCleanState and cleanup will restore to these values.
	setup.saveBaselineGucs(t)

	t.Log("Shard initialized via multiorch bootstrap")
}

// waitForShardBootstrap waits for multiorch to bootstrap the shard by electing a primary
// and initializing all standbys. Returns the name of the elected primary or an error.
func waitForShardBootstrap(t *testing.T, setup *ShardSetup) (string, error) {
	t.Helper()

	ctx := utils.WithTimeout(t, 60*time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timeout waiting for shard bootstrap after 60s")
		case <-ticker.C:
			primaryName, allInitialized := checkBootstrapStatus(t, setup)
			if primaryName != "" && allInitialized {
				t.Logf("waitForShardBootstrap: primary=%s, all nodes initialized", primaryName)
				return primaryName, nil
			}
		}
	}
}

// checkBootstrapStatus checks if all nodes are initialized and returns the primary name.
// A node is considered initialized only if it can be queried AND has an explicit type (PRIMARY or REPLICA).
// Additionally checks that:
// - PRIMARY has sync replication configured with the expected number of standbys
// - REPLICA has primary_conn_info configured
func checkBootstrapStatus(t *testing.T, setup *ShardSetup) (string, bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var primaryName string
	var initializedCount int
	expectedReplicaCount := len(setup.Multipoolers) - 1

	for name, inst := range setup.Multipoolers {
		client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
		if err != nil {
			t.Logf("checkBootstrapStatus: failed to connect to %s: %v", name, err)
			continue
		}

		// Check if node is initialized (can query postgres)
		_, err = QueryStringValue(ctx, client.Pooler, "SELECT 1")
		if err != nil {
			client.Close()
			continue
		}

		// Check pooler type via Status RPC - must be explicit (not UNKNOWN)
		statusResp, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
		if err != nil || statusResp.Status == nil {
			client.Close()
			continue
		}

		status := statusResp.Status
		isFullyInitialized := false

		switch status.PoolerType {
		case clustermetadatapb.PoolerType_PRIMARY:
			// Check that sync replication is configured with expected standbys
			if status.PrimaryStatus == nil ||
				status.PrimaryStatus.SyncReplicationConfig == nil ||
				len(status.PrimaryStatus.SyncReplicationConfig.StandbyIds) < expectedReplicaCount {
				standbyCount := 0
				if status.PrimaryStatus != nil && status.PrimaryStatus.SyncReplicationConfig != nil {
					standbyCount = len(status.PrimaryStatus.SyncReplicationConfig.StandbyIds)
				}
				t.Logf("checkBootstrapStatus: %s is PRIMARY but sync replication not ready (standbys=%d, expected=%d)",
					name, standbyCount, expectedReplicaCount)
			} else {
				primaryName = name
				isFullyInitialized = true
			}

		case clustermetadatapb.PoolerType_REPLICA:
			// Check that primary_conn_info is configured
			hasReplicationStatus := status.ReplicationStatus != nil
			hasPrimaryConnInfo := hasReplicationStatus && status.ReplicationStatus.PrimaryConnInfo != nil
			hasHost := hasPrimaryConnInfo && status.ReplicationStatus.PrimaryConnInfo.Host != ""

			t.Logf("checkBootstrapStatus: %s is REPLICA - hasReplicationStatus=%v, hasPrimaryConnInfo=%v, hasHost=%v",
				name, hasReplicationStatus, hasPrimaryConnInfo, hasHost)

			if hasPrimaryConnInfo {
				t.Logf("checkBootstrapStatus: %s PrimaryConnInfo.Host=%q, Port=%d",
					name, status.ReplicationStatus.PrimaryConnInfo.Host, status.ReplicationStatus.PrimaryConnInfo.Port)
			}

			if !hasHost {
				t.Logf("checkBootstrapStatus: %s is REPLICA but primary_conn_info not configured", name)
			} else {
				isFullyInitialized = true
			}
			// UNKNOWN type means not fully initialized yet - don't count
		}

		client.Close()

		if isFullyInitialized {
			initializedCount++
		}
	}

	allInitialized := initializedCount == len(setup.Multipoolers)
	t.Logf("checkBootstrapStatus: primary=%s, initialized=%d/%d", primaryName, initializedCount, len(setup.Multipoolers))
	return primaryName, allInitialized
}

// startProcessesWithoutInit starts pgctld and multipooler processes without initializing postgres.
// Use this for bootstrap tests where multiorch will initialize the shard.
// pgBackRest servers are started later via startPgBackRestServers() after initialization.
//
// TODO: Consider parallelizing Start() calls using a WaitGroup for faster startup.
// Currently processes are started sequentially which adds latency.
func startProcessesWithoutInit(t *testing.T, setup *ShardSetup, instances []*MultipoolerInstance, config *SetupConfig) {
	t.Helper()

	for _, inst := range instances {
		pgctld := inst.Pgctld
		multipooler := inst.Multipooler

		// Start pgctld (postgres will be initialized later, or by multiorch for bootstrap)
		if err := pgctld.Start(t); err != nil {
			t.Fatalf("failed to start pgctld for %s: %v", inst.Name, err)
		}
		t.Logf("Started pgctld for %s (gRPC=%d, PG=%d)", inst.Name, pgctld.GrpcPort, pgctld.PgPort)

		// Start multipooler
		if err := multipooler.Start(t); err != nil {
			t.Fatalf("failed to start multipooler for %s: %v", inst.Name, err)
		}

		// Wait for multipooler to be ready
		WaitForManagerReady(t, multipooler)
		t.Logf("Multipooler %s is ready (uninitialized)", inst.Name)
	}

	t.Logf("Started %d processes without initialization (ready for bootstrap)", len(instances))
}

// startPgBackRestServers starts pgBackRest servers for all multipooler instances.
// This must be called AFTER PostgreSQL initialization is complete, because multipooler
// generates the pgbackrest config files during initialization (in configureArchiveMode).
func (s *ShardSetup) startPgBackRestServers(t *testing.T) {
	t.Helper()

	for name, inst := range s.Multipoolers {
		pgbackrest := s.startPgBackRestServer(t, name, inst.Pgctld.DataDir, inst.Multipooler.PgBackRestPort)
		inst.PgBackRest = pgbackrest
		t.Logf("Started pgBackRest for %s (port=%d)", name, pgbackrest.Port)
	}

	t.Logf("Started %d pgBackRest servers", len(s.Multipoolers))
}

// startEtcd starts etcd without registering t.Cleanup() handlers
// since cleanup is handled manually by TestMain via Cleanup().
// Follows the pattern from multipooler/setup_test.go:startEtcdForSharedSetup.
func startEtcd(t *testing.T, dataDir string) (string, *exec.Cmd, error) {
	t.Helper()

	// Check if etcd is available in PATH
	_, err := exec.LookPath("etcd")
	if err != nil {
		return "", nil, fmt.Errorf("etcd not found in PATH: %w", err)
	}

	// Get ports for etcd (client and peer)
	clientPort := utils.GetFreePort(t)
	peerPort := utils.GetFreePort(t)

	name := "shardsetup_test"
	clientAddr := fmt.Sprintf("http://localhost:%v", clientPort)
	peerAddr := fmt.Sprintf("http://localhost:%v", peerPort)
	initialCluster := fmt.Sprintf("%v=%v", name, peerAddr)

	// Wrap etcd with run_in_test to ensure cleanup if test process dies
	cmd := exec.Command("run_in_test.sh", "etcd",
		"-name", name,
		"-advertise-client-urls", clientAddr,
		"-initial-advertise-peer-urls", peerAddr,
		"-listen-client-urls", clientAddr,
		"-listen-peer-urls", peerAddr,
		"-initial-cluster", initialCluster,
		"-data-dir", dataDir)

	// Set MULTIGRES_TESTDATA_DIR for directory-deletion triggered cleanup
	cmd.Env = append(os.Environ(),
		"MULTIGRES_TESTDATA_DIR="+dataDir,
	)

	if err := cmd.Start(); err != nil {
		return "", nil, fmt.Errorf("failed to start etcd: %w", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := etcdtopo.WaitForReady(ctx, clientAddr); err != nil {
		_ = cmd.Process.Kill()
		return "", nil, err
	}

	return clientAddr, cmd, nil
}

// ValidateCleanState checks that all multipoolers are in the expected clean state.
// Clean state is defined by the baseline GUCs captured after bootstrap:
//   - Primary: not in recovery, GUCs match baseline, term=1, type=PRIMARY
//   - Standbys: in recovery, GUCs match baseline, wal_replay not paused, term=1, type=REPLICA
//   - MultiOrch: NOT running (multiorch starts in SetupTest and stops in cleanup)
//
// Returns an error if state is not clean.
func (s *ShardSetup) ValidateCleanState() error {
	if s == nil {
		return nil
	}

	// Require primary to be set (happens after bootstrap)
	if s.PrimaryName == "" {
		return fmt.Errorf("no primary has been elected (PrimaryName not set)")
	}
	if s.GetMultipoolerInstance(s.PrimaryName) == nil {
		return fmt.Errorf("primary instance %s not found", s.PrimaryName)
	}

	// Verify multiorch instances are NOT running (clean state = no orchestration)
	for name, mo := range s.MultiOrchInstances {
		if mo.IsRunning() {
			return fmt.Errorf("multiorch %s is running (clean state = not running)", name)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for name, inst := range s.Multipoolers {
		client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
		if err != nil {
			return fmt.Errorf("failed to connect to %s: %w", name, err)
		}
		defer client.Close()

		isPrimary := name == s.PrimaryName

		// Check recovery mode
		inRecovery, err := QueryStringValue(ctx, client.Pooler, "SELECT pg_is_in_recovery()")
		if err != nil {
			return fmt.Errorf("%s failed to query pg_is_in_recovery: %w", name, err)
		}

		if isPrimary {
			if inRecovery != "f" {
				return fmt.Errorf("%s pg_is_in_recovery=%s (expected f)", name, inRecovery)
			}
			// Validate pooler type is PRIMARY
			if err := ValidatePoolerType(ctx, client.Manager, clustermetadatapb.PoolerType_PRIMARY, name); err != nil {
				return err
			}
		} else {
			if inRecovery != "t" {
				return fmt.Errorf("%s pg_is_in_recovery=%s (expected t)", name, inRecovery)
			}

			// Verify WAL replay not paused
			isPaused, err := QueryStringValue(ctx, client.Pooler, "SELECT pg_is_wal_replay_paused()")
			if err != nil {
				return fmt.Errorf("%s failed to query pg_is_wal_replay_paused: %w", name, err)
			}
			if isPaused != "f" {
				return fmt.Errorf("%s pg_is_wal_replay_paused=%s (expected f)", name, isPaused)
			}

			// Validate pooler type is REPLICA
			if err := ValidatePoolerType(ctx, client.Manager, clustermetadatapb.PoolerType_REPLICA, name); err != nil {
				return err
			}
		}

		// Validate GUCs match baseline values
		if baselineGucs, ok := s.BaselineGucs[name]; ok {
			for gucName, expectedValue := range baselineGucs {
				if err := ValidateGUCValue(ctx, client.Pooler, gucName, expectedValue, name); err != nil {
					return err
				}
			}
		}

		// Validate term is 1 for all nodes
		if err := ValidateTerm(ctx, client.Consensus, 1, name); err != nil {
			return err
		}
	}

	return nil
}

// ResetToCleanState resets all multipoolers to the baseline clean state.
// This restores GUCs to baseline values, resets terms to 1, pooler types to PRIMARY/REPLICA,
// resumes WAL replay, and stops multiorch instances.
func (s *ShardSetup) ResetToCleanState(t *testing.T) {
	t.Helper()

	if s == nil {
		return
	}

	// Stop multiorch instances first (clean state = not running)
	for name, mo := range s.MultiOrchInstances {
		if mo.IsRunning() {
			mo.TerminateGracefully(t, 5*time.Second)
			t.Logf("Reset: Stopped multiorch %s", name)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for name, inst := range s.Multipoolers {
		client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
		if err != nil {
			t.Logf("Reset: Failed to connect to %s: %v", name, err)
			continue
		}

		isPrimary := name == s.PrimaryName

		// Check if primary was demoted and restore if needed
		if isPrimary {
			inRecovery, err := QueryStringValue(ctx, client.Pooler, "SELECT pg_is_in_recovery()")
			if err != nil {
				t.Logf("Reset: Failed to check if %s is in recovery: %v", name, err)
			} else if inRecovery == "t" {
				t.Logf("Reset: %s was demoted, restoring to primary state...", name)
				if err := RestorePrimaryAfterDemotion(ctx, client.Manager); err != nil {
					t.Logf("Reset: Failed to restore %s after demotion: %v", name, err)
				}
			}
		}

		// Restore GUCs to baseline values
		if baselineGucs, ok := s.BaselineGucs[name]; ok && len(baselineGucs) > 0 {
			RestoreGUCs(ctx, t, client.Pooler, baselineGucs, name)
		}

		// Resume WAL replay if paused (for standbys)
		if !isPrimary {
			_, _ = client.Pooler.ExecuteQuery(ctx, "SELECT pg_wal_replay_resume()", 0)
		}

		// Reset pooler type
		expectedType := clustermetadatapb.PoolerType_REPLICA
		if isPrimary {
			expectedType = clustermetadatapb.PoolerType_PRIMARY
		}
		if err := SetPoolerType(ctx, client.Manager, expectedType); err != nil {
			t.Logf("Reset: Failed to set pooler type on %s: %v", name, err)
		}

		// Reset term
		if err := ResetTerm(ctx, client.Manager); err != nil {
			t.Logf("Reset: Failed to reset term on %s: %v", name, err)
		}

		client.Close()
	}
}

// SetupTest provides test isolation by validating clean state and automatically
// restoring baseline state at test cleanup.
//
// DEFAULT BEHAVIOR (no options):
//   - Validates clean state before test (GUCs match baseline from bootstrap)
//   - Replication is already configured from bootstrap
//   - Registers cleanup to restore baseline GUCs and reset state after test
//
// WithoutReplication():
//   - Actively breaks replication: clears primary_conninfo and synchronous_standby_names
//   - Use for tests that need to set up replication from scratch
//   - Cleanup restores baseline (re-enables replication)
//
// WithPausedReplication():
//   - Pauses WAL replay on standbys (replication already configured)
//   - Use for tests that need to test pg_wal_replay_resume()
//
// WithResetGuc(gucNames...):
//   - Adds additional GUCs to save/restore beyond baseline
//
// Follows the pattern from multipooler/setup_test.go:setupPoolerTest.
func (s *ShardSetup) SetupTest(t *testing.T, opts ...SetupTestOption) {
	t.Helper()

	config := &SetupTestConfig{}
	for _, opt := range opts {
		opt(config)
	}

	// Fail fast if shared processes died
	s.CheckSharedProcesses(t)

	// Validate that settings are in the expected clean state (GUCs match baseline)
	if err := s.ValidateCleanState(); err != nil {
		t.Fatalf("SetupTest: %v. Previous test leaked state.", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// If WithoutReplication is set, actively break replication
	if config.NoReplication {
		s.breakReplication(t, ctx)
	}

	// If WithPausedReplication is set, pause WAL replay on standbys
	if config.PauseReplication {
		s.pauseReplicationOnStandbys(t, ctx)
	}

	// Start multiorch instances
	// TODO (@rafa): once we have a way to disable multiorch on a shard, we don't need
	// this big hammer of stopping / starting on each test.
	for name, mo := range s.MultiOrchInstances {
		if err := mo.Start(t); err != nil {
			t.Fatalf("SetupTest: failed to start multiorch %s: %v", name, err)
		}
		t.Logf("SetupTest: Started multiorch '%s': gRPC=%d, HTTP=%d", name, mo.GrpcPort, mo.HttpPort)
	}

	// Register cleanup handler to restore to baseline state
	t.Cleanup(func() {
		// Stop multiorch instances first (clean state = multiorch not running)
		for name, mo := range s.MultiOrchInstances {
			if mo.IsRunning() {
				mo.TerminateGracefully(t, 5*time.Second)
				t.Logf("Cleanup: Stopped multiorch %s", name)
			}
		}

		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()

		for name, inst := range s.Multipoolers {
			client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if err != nil {
				t.Logf("Cleanup: failed to connect to %s: %v", name, err)
				continue
			}

			isPrimary := name == s.PrimaryName

			// Check if primary was demoted and restore if needed
			if isPrimary {
				inRecovery, err := QueryStringValue(cleanupCtx, client.Pooler, "SELECT pg_is_in_recovery()")
				if err != nil {
					t.Logf("Cleanup: failed to check if %s is in recovery: %v", name, err)
				} else if inRecovery == "t" {
					t.Logf("Cleanup: %s was demoted, restoring to primary state...", name)
					if err := RestorePrimaryAfterDemotion(cleanupCtx, client.Manager); err != nil {
						t.Logf("Cleanup: failed to restore %s after demotion: %v", name, err)
					}
				}
			}

			// Restore GUCs to baseline values
			if baselineGucs, ok := s.BaselineGucs[name]; ok && len(baselineGucs) > 0 {
				RestoreGUCs(cleanupCtx, t, client.Pooler, baselineGucs, name)
			}

			// Always resume WAL replay (must be after GUC restoration)
			// This ensures we leave the system in a good state even if tests paused replay.
			if !isPrimary {
				_, _ = client.Pooler.ExecuteQuery(cleanupCtx, "SELECT pg_wal_replay_resume()", 0)
			}

			// Reset pooler type
			expectedType := clustermetadatapb.PoolerType_REPLICA
			if isPrimary {
				expectedType = clustermetadatapb.PoolerType_PRIMARY
			}
			if err := SetPoolerType(cleanupCtx, client.Manager, expectedType); err != nil {
				t.Logf("Cleanup: failed to set pooler type on %s: %v", name, err)
			}

			// Reset term
			if err := ResetTerm(cleanupCtx, client.Manager); err != nil {
				t.Logf("Cleanup: failed to reset term on %s: %v", name, err)
			}

			client.Close()
		}

		// Validate cleanup worked
		require.Eventually(t, func() bool {
			return s.ValidateCleanState() == nil
		}, 2*time.Second, 50*time.Millisecond, "Test cleanup failed: state did not return to clean state")
	})
}

// breakReplication clears replication configuration on all nodes.
// Use this for tests that need to set up replication from scratch.
func (s *ShardSetup) breakReplication(t *testing.T, ctx context.Context) {
	t.Helper()

	// Clear synchronous_standby_names on primary
	primary := s.GetMultipoolerInstance(s.PrimaryName)
	if primary != nil {
		client, err := NewMultipoolerClient(primary.Multipooler.GrpcPort)
		if err == nil {
			_, _ = client.Pooler.ExecuteQuery(ctx, "ALTER SYSTEM RESET synchronous_standby_names", 0)
			_, _ = client.Pooler.ExecuteQuery(ctx, "ALTER SYSTEM RESET synchronous_commit", 0)
			_, _ = client.Pooler.ExecuteQuery(ctx, "SELECT pg_reload_conf()", 0)
			client.Close()
			t.Logf("SetupTest: Cleared synchronous_standby_names on primary %s", s.PrimaryName)
		}
	}

	// Clear primary_conninfo on standbys
	for name, inst := range s.Multipoolers {
		if name == s.PrimaryName {
			continue
		}

		client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
		if err != nil {
			t.Logf("SetupTest: failed to connect to %s: %v", name, err)
			continue
		}

		_, _ = client.Pooler.ExecuteQuery(ctx, "ALTER SYSTEM RESET primary_conninfo", 0)
		_, _ = client.Pooler.ExecuteQuery(ctx, "SELECT pg_reload_conf()", 0)
		client.Close()
		t.Logf("SetupTest: Cleared primary_conninfo on standby %s", name)
	}
}

// pauseReplicationOnStandbys pauses WAL replay on all standbys.
func (s *ShardSetup) pauseReplicationOnStandbys(t *testing.T, ctx context.Context) {
	t.Helper()

	for name, inst := range s.Multipoolers {
		if name == s.PrimaryName {
			continue
		}

		client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
		if err != nil {
			t.Logf("SetupTest: failed to connect to %s: %v", name, err)
			continue
		}

		_, err = client.Pooler.ExecuteQuery(ctx, "SELECT pg_wal_replay_pause()", 0)
		client.Close()
		if err != nil {
			t.Logf("SetupTest: Failed to pause WAL replay on %s: %v", name, err)
		} else {
			t.Logf("SetupTest: Paused WAL replay on %s", name)
		}
	}
}

// DemotePrimary demotes the primary by putting it into standby mode.
// This is used to test failover scenarios and then reset the cluster.
func (s *ShardSetup) DemotePrimary(t *testing.T) {
	t.Helper()

	primary := s.GetMultipoolerInstance(s.PrimaryName)
	if primary == nil {
		t.Fatal("primary not found")
		return // unreachable, but needed for linter
	}

	client, err := NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err, "failed to connect to primary")
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Demote using the Demote RPC with term 1 (clean state starts at term 1)
	_, err = client.Manager.Demote(ctx, &multipoolermanagerdatapb.DemoteRequest{
		ConsensusTerm: 1,
	})
	require.NoError(t, err, "failed to demote primary")

	// Wait for primary to be in recovery mode
	require.Eventually(t, func() bool {
		queryCtx, queryCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer queryCancel()
		inRecovery, err := QueryStringValue(queryCtx, client.Pooler, "SELECT pg_is_in_recovery()")
		return err == nil && inRecovery == "t"
	}, 10*time.Second, 100*time.Millisecond, "primary should be in recovery mode after demotion")

	t.Log("Primary demoted successfully")
}

// NewClient returns a new MultipoolerClient for the specified multipooler instance.
// The caller is responsible for closing the client.
func (s *ShardSetup) NewClient(t *testing.T, name string) *MultipoolerClient {
	t.Helper()

	inst := s.GetMultipoolerInstance(name)
	if inst == nil {
		t.Fatalf("multipooler %s not found", name)
		return nil // unreachable, but needed for linter
	}

	client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
	require.NoError(t, err, "failed to connect to %s", name)

	return client
}

// NewPrimaryClient returns a new MultipoolerClient for the primary instance.
// The caller is responsible for closing the client.
func (s *ShardSetup) NewPrimaryClient(t *testing.T) *MultipoolerClient {
	return s.NewClient(t, s.PrimaryName)
}

// makeMultipoolerID creates a multipooler ID for testing.
func makeMultipoolerID(cell, name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	}
}

// GetMultipoolerID returns the multipooler ID for the named instance.
func (s *ShardSetup) GetMultipoolerID(name string) *clustermetadatapb.ID {
	inst := s.GetMultipoolerInstance(name)
	if inst == nil {
		return nil
	}
	return makeMultipoolerID(inst.Multipooler.Cell, inst.Multipooler.Name)
}

// KillPostgres terminates the postgres process for a node (simulates database crash).
// This sends SIGKILL directly to the postgres process, bypassing any graceful shutdown.
// The multipooler stays running to report the unhealthy status to multiorch.
func (s *ShardSetup) KillPostgres(t *testing.T, name string) {
	t.Helper()

	inst := s.GetMultipoolerInstance(name)
	if inst == nil {
		t.Fatalf("node %s not found", name)
		return // unreachable, but needed for linter
	}

	// Read the postgres PID from postmaster.pid
	pidFile := filepath.Join(inst.Pgctld.DataDir, "pg_data", "postmaster.pid")
	data, err := os.ReadFile(pidFile)
	require.NoError(t, err, "Failed to read postgres PID file for %s", name)

	lines := strings.Split(string(data), "\n")
	require.Greater(t, len(lines), 0, "PID file should have at least one line")

	pid, err := strconv.Atoi(strings.TrimSpace(lines[0]))
	require.NoError(t, err, "Failed to parse PID from postmaster.pid")

	t.Logf("Killing postgres (PID %d) on node %s", pid, name)

	err = syscall.Kill(pid, syscall.SIGKILL)
	require.NoError(t, err, "Failed to kill postgres process")

	t.Logf("Postgres killed on %s - multipooler should detect failure", name)
}

// baselineGucNames returns the GUC names to save/restore for baseline state.
var baselineGucNames = []string{
	"synchronous_standby_names",
	"synchronous_commit",
	"primary_conninfo",
}

// saveBaselineGucs captures the current GUC values from all nodes as the baseline "clean state".
// This is called after bootstrap completes, so the baseline includes replication configuration.
func (s *ShardSetup) saveBaselineGucs(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.BaselineGucs = make(map[string]map[string]string)

	for name, inst := range s.Multipoolers {
		client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
		if err != nil {
			t.Logf("saveBaselineGucs: failed to connect to %s: %v", name, err)
			continue
		}

		gucs := SaveGUCs(ctx, client.Pooler, baselineGucNames)
		s.BaselineGucs[name] = gucs

		t.Logf("saveBaselineGucs: saved GUCs for %s: %v", name, gucs)
		client.Close()
	}
}
