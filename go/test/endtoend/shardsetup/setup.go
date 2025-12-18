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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/etcdtopo"
	"github.com/multigres/multigres/go/provisioner/local/pgbackrest"
	"github.com/multigres/multigres/go/test/endtoend"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/pathutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/pb/pgctldservice"

	// Register topo plugins
	_ "github.com/multigres/multigres/go/common/plugins/topo"
)

// SetupConfig holds the configuration for creating a ShardSetup.
type SetupConfig struct {
	MultipoolerCount int
	MultiOrchCount   int
	Database         string
	TableGroup       string
	Shard            string
	CellName         string
	DurabilityPolicy string // Durability policy (e.g., "ANY_2")
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

// SetupTestConfig holds configuration for SetupTest.
type SetupTestConfig struct {
	NoReplication    bool     // Don't configure replication
	PauseReplication bool     // Configure replication but pause WAL replay
	Cleanup          bool     // Register cleanup handlers (default: true for shared setups)
	GucsToReset      []string // GUCs to save before test and restore after
}

// SetupTestOption is a function that configures SetupTest behavior.
type SetupTestOption func(*SetupTestConfig)

// WithoutReplication returns an option that disables replication setup.
// Use this for tests that need to set up replication from scratch.
// Saves and restores synchronous replication settings and primary_conninfo.
func WithoutReplication() SetupTestOption {
	return func(c *SetupTestConfig) {
		c.NoReplication = true
		c.GucsToReset = append(c.GucsToReset, "synchronous_standby_names", "synchronous_commit", "primary_conninfo")
	}
}

// WithPausedReplication returns an option that starts replication but pauses WAL replay.
// Use this for tests that need to test pg_wal_replay_resume().
// Saves and restores synchronous replication settings and primary_conninfo.
func WithPausedReplication() SetupTestOption {
	return func(c *SetupTestConfig) {
		c.PauseReplication = true
		c.GucsToReset = append(c.GucsToReset, "synchronous_standby_names", "synchronous_commit", "primary_conninfo")
	}
}

// WithoutCleanup returns an option that skips cleanup handler registration.
// Use this for isolated shards where cleanup is handled by defer cleanup().
func WithoutCleanup() SetupTestOption {
	return func(c *SetupTestConfig) {
		c.Cleanup = false
	}
}

// WithResetGuc returns an option that saves and restores specific GUC settings.
func WithResetGuc(gucNames ...string) SetupTestOption {
	return func(c *SetupTestConfig) {
		c.GucsToReset = append(c.GucsToReset, gucNames...)
	}
}

// stanzaName derives the pgBackRest stanza name from database, tablegroup, and shard.
func stanzaName(database, tableGroup, shard string) string {
	// Replace special characters with underscores for valid stanza name
	shard = strings.ReplaceAll(shard, "-", "_")
	return fmt.Sprintf("%s_%s_%s", database, tableGroup, shard)
}

// multipoolerName returns the name for a multipooler instance by index.
// Index 0 is "primary", index 1+ are "standby", "standby2", "standby3", etc.
func multipoolerName(index int) string {
	if index == 0 {
		return "primary"
	}
	if index == 1 {
		return "standby"
	}
	return fmt.Sprintf("standby%d", index)
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

	// Set the PATH so our binaries can be found
	if err := pathutil.PrependBinToPath(); err != nil {
		t.Fatalf("failed to add bin to PATH: %v", err)
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

	ts, err := topoclient.OpenServer("etcd2", globalRoot, []string{etcdClientAddr}, topoclient.NewDefaultTopoConfig())
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
	backupLocation := filepath.Join(tempDir, "backup-repo", config.Database, config.TableGroup, config.Shard)
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

		inst := setup.CreateMultipoolerInstance(t, name, grpcPort, pgPort, multipoolerPort, stanzaName(config.Database, config.TableGroup, config.Shard))
		multipoolerInstances = append(multipoolerInstances, inst)

		t.Logf("Created multipooler instance '%s': pgctld gRPC=%d, PG=%d, multipooler gRPC=%d",
			name, grpcPort, pgPort, multipoolerPort)
	}

	// Create standby data directories before initializing primary
	// This is needed for symmetric pgBackRest configuration
	for i := 1; i < len(multipoolerInstances); i++ {
		standbyDataDir := multipoolerInstances[i].Pgctld.DataDir
		if err := os.MkdirAll(filepath.Join(standbyDataDir, "pg_data"), 0o755); err != nil {
			t.Fatalf("failed to create standby pg_data dir: %v", err)
		}
		if err := os.MkdirAll(filepath.Join(standbyDataDir, "pg_sockets"), 0o755); err != nil {
			t.Fatalf("failed to create standby pg_sockets dir: %v", err)
		}
	}

	// Initialize primary
	primary := multipoolerInstances[0]
	standbys := multipoolerInstances[1:]
	initializePrimary(t, tempDir, primary, standbys, config)

	// Initialize standbys
	for _, standby := range standbys {
		initializeStandby(t, tempDir, primary, standby, config)
	}

	// Create multiorch instances (but don't start yet - SetupTest starts them after replication)
	if config.MultiOrchCount > 0 {
		watchTargets := []string{fmt.Sprintf("%s/%s/%s", config.Database, config.TableGroup, config.Shard)}
		for i := 0; i < config.MultiOrchCount; i++ {
			name := multiOrchName(i)
			setup.CreateMultiOrchInstance(t, name, config.CellName, watchTargets)
			t.Logf("Created multiorch '%s' (will start after replication is configured)", name)
		}
	}

	t.Logf("Shard setup complete: %d multipoolers, %d multiorchs",
		config.MultipoolerCount, config.MultiOrchCount)

	return setup
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

// initializePrimary sets up the primary pgctld, PostgreSQL, pgbackrest, consensus term, and multipooler.
// Follows the pattern from multipooler/setup_test.go:initializePrimary.
func initializePrimary(t *testing.T, baseDir string, primary *MultipoolerInstance, standbys []*MultipoolerInstance, config *SetupConfig) {
	t.Helper()

	pgctld := primary.Pgctld
	multipooler := primary.Multipooler

	// Start primary pgctld server
	if err := pgctld.Start(t); err != nil {
		t.Fatalf("failed to start primary pgctld: %v", err)
	}

	// Initialize PostgreSQL data directory (but don't start yet)
	primaryGrpcAddr := fmt.Sprintf("localhost:%d", pgctld.GrpcPort)
	if err := endtoend.InitPostgreSQLDataDir(t, primaryGrpcAddr); err != nil {
		t.Fatalf("failed to init PostgreSQL data dir: %v", err)
	}

	// Create pgbackrest configuration
	repoPath := filepath.Join(baseDir, "backup-repo", config.Database, config.TableGroup, config.Shard)
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		t.Fatalf("failed to create backup repo: %v", err)
	}

	logPath := filepath.Join(baseDir, "logs", "pgbackrest")
	if err := os.MkdirAll(logPath, 0o755); err != nil {
		t.Fatalf("failed to create pgbackrest log dir: %v", err)
	}

	spoolPath := filepath.Join(baseDir, "pgbackrest-spool")
	if err := os.MkdirAll(spoolPath, 0o755); err != nil {
		t.Fatalf("failed to create pgbackrest spool dir: %v", err)
	}

	lockPath := filepath.Join(baseDir, "pgbackrest-lock")
	if err := os.MkdirAll(lockPath, 0o755); err != nil {
		t.Fatalf("failed to create pgbackrest lock dir: %v", err)
	}

	// Create symmetric pgbackrest configuration with all standbys as additional hosts
	configPath := filepath.Join(pgctld.DataDir, "pgbackrest.conf")
	backupCfg := pgbackrest.Config{
		StanzaName:      stanzaName(config.Database, config.TableGroup, config.Shard),
		PgDataPath:      filepath.Join(pgctld.DataDir, "pg_data"),
		PgPort:          pgctld.PgPort,
		PgSocketDir:     filepath.Join(pgctld.DataDir, "pg_sockets"),
		PgUser:          "postgres",
		PgDatabase:      "postgres",
		AdditionalHosts: make([]pgbackrest.PgHost, 0, len(standbys)),
		LogPath:         logPath,
		SpoolPath:       spoolPath,
		LockPath:        lockPath,
		RetentionFull:   2,
	}

	for _, standby := range standbys {
		backupCfg.AdditionalHosts = append(backupCfg.AdditionalHosts, pgbackrest.PgHost{
			DataPath:  filepath.Join(standby.Pgctld.DataDir, "pg_data"),
			SocketDir: filepath.Join(standby.Pgctld.DataDir, "pg_sockets"),
			Port:      standby.Pgctld.PgPort,
			User:      "postgres",
			Database:  "postgres",
		})
	}

	if err := pgbackrest.WriteConfigFile(configPath, backupCfg); err != nil {
		t.Fatalf("failed to write pgbackrest config: %v", err)
	}
	t.Logf("Created symmetric pgbackrest config at %s (stanza: %s)", configPath, stanzaName(config.Database, config.TableGroup, config.Shard))

	// Configure archive_mode in postgresql.auto.conf BEFORE starting PostgreSQL
	pgDataDir := filepath.Join(pgctld.DataDir, "pg_data")
	autoConfPath := filepath.Join(pgDataDir, "postgresql.auto.conf")
	archiveConfig := fmt.Sprintf(`
# Archive mode for pgbackrest backups
archive_mode = on
archive_command = 'pgbackrest --stanza=%s --config=%s --repo1-path=%s archive-push %%p'
`, stanzaName(config.Database, config.TableGroup, config.Shard), configPath, repoPath)
	f, err := os.OpenFile(autoConfPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("failed to open postgresql.auto.conf: %v", err)
	}
	if _, err := f.WriteString(archiveConfig); err != nil {
		f.Close()
		t.Fatalf("failed to write archive config: %v", err)
	}
	f.Close()
	t.Log("Configured archive_mode in postgresql.auto.conf")

	// Start PostgreSQL with archive mode enabled
	if err := endtoend.StartPostgreSQL(t, primaryGrpcAddr); err != nil {
		t.Fatalf("failed to start PostgreSQL: %v", err)
	}
	t.Log("Started PostgreSQL with archive mode enabled")

	// Initialize pgbackrest stanza
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := pgbackrest.StanzaCreate(ctx, stanzaName(config.Database, config.TableGroup, config.Shard), configPath, repoPath); err != nil {
		t.Fatalf("failed to create pgbackrest stanza: %v", err)
	}
	t.Logf("Initialized pgbackrest stanza: %s", stanzaName(config.Database, config.TableGroup, config.Shard))

	// Start primary multipooler
	if err := multipooler.Start(t); err != nil {
		t.Fatalf("failed to start primary multipooler: %v", err)
	}

	// Wait for manager to be ready
	WaitForManagerReady(t, multipooler)

	// Create multigres schema and heartbeat table
	primaryPoolerClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", multipooler.GrpcPort))
	if err != nil {
		t.Fatalf("failed to connect to primary pooler: %v", err)
	}
	defer primaryPoolerClient.Close()

	_, err = primaryPoolerClient.ExecuteQuery(ctx, "CREATE SCHEMA IF NOT EXISTS multigres", 0)
	if err != nil {
		t.Fatalf("failed to create multigres schema: %v", err)
	}

	_, err = primaryPoolerClient.ExecuteQuery(ctx, `
		CREATE TABLE IF NOT EXISTS multigres.heartbeat (
			shard_id BYTEA PRIMARY KEY,
			leader_id TEXT NOT NULL,
			ts BIGINT NOT NULL
		)`, 0)
	if err != nil {
		t.Fatalf("failed to create heartbeat table: %v", err)
	}
	t.Log("Created multigres schema and heartbeat table")

	// Initialize consensus term to 1
	client, err := NewMultipoolerClient(multipooler.GrpcPort)
	if err != nil {
		t.Fatalf("failed to connect to primary multipooler: %v", err)
	}
	defer client.Close()

	if err := ResetTerm(ctx, client.Manager); err != nil {
		t.Fatalf("failed to set term for primary: %v", err)
	}
	t.Log("Primary consensus term set to 1")

	// Set pooler type to PRIMARY
	if err := SetPoolerType(ctx, client.Manager, clustermetadatapb.PoolerType_PRIMARY); err != nil {
		t.Fatalf("failed to set primary pooler type: %v", err)
	}

	t.Log("Primary initialized successfully")
}

// initializeStandby sets up a standby pgctld, PostgreSQL (with replication), consensus term, and multipooler.
// Follows the pattern from multipooler/setup_test.go:initializeStandby.
func initializeStandby(t *testing.T, baseDir string, primary *MultipoolerInstance, standby *MultipoolerInstance, config *SetupConfig) {
	t.Helper()

	pgctld := standby.Pgctld
	multipooler := standby.Multipooler

	// Start standby pgctld server
	if err := pgctld.Start(t); err != nil {
		t.Fatalf("failed to start standby pgctld %s: %v", standby.Name, err)
	}

	// Initialize standby data directory (but don't start yet)
	standbyGrpcAddr := fmt.Sprintf("localhost:%d", pgctld.GrpcPort)
	if err := endtoend.InitPostgreSQLDataDir(t, standbyGrpcAddr); err != nil {
		t.Fatalf("failed to init standby data dir %s: %v", standby.Name, err)
	}

	// Configure standby as a replica using pgBackRest backup/restore
	t.Logf("Configuring %s as replica of primary...", standby.Name)
	setupStandbyReplication(t, baseDir, primary, standby, config)

	// Start standby PostgreSQL (now configured as replica)
	if err := endtoend.StartPostgreSQL(t, standbyGrpcAddr); err != nil {
		t.Fatalf("failed to start standby PostgreSQL %s: %v", standby.Name, err)
	}

	// Create symmetric pgbackrest configuration for standby
	logPath := filepath.Join(baseDir, "logs", "pgbackrest")
	spoolPath := filepath.Join(baseDir, "pgbackrest-spool")
	lockPath := filepath.Join(baseDir, "pgbackrest-lock")

	configPath := filepath.Join(pgctld.DataDir, "pgbackrest.conf")
	backupCfg := pgbackrest.Config{
		StanzaName:  stanzaName(config.Database, config.TableGroup, config.Shard),
		PgDataPath:  filepath.Join(pgctld.DataDir, "pg_data"),
		PgPort:      pgctld.PgPort,
		PgSocketDir: filepath.Join(pgctld.DataDir, "pg_sockets"),
		PgUser:      "postgres",
		PgDatabase:  "postgres",
		AdditionalHosts: []pgbackrest.PgHost{
			{
				DataPath:  filepath.Join(primary.Pgctld.DataDir, "pg_data"),
				SocketDir: filepath.Join(primary.Pgctld.DataDir, "pg_sockets"),
				Port:      primary.Pgctld.PgPort,
				User:      "postgres",
				Database:  "postgres",
			},
		},
		LogPath:       logPath,
		SpoolPath:     spoolPath,
		LockPath:      lockPath,
		RetentionFull: 2,
	}

	if err := pgbackrest.WriteConfigFile(configPath, backupCfg); err != nil {
		t.Fatalf("failed to write standby pgbackrest config: %v", err)
	}
	t.Logf("Created pgbackrest config for %s", standby.Name)

	// Start standby multipooler
	if err := multipooler.Start(t); err != nil {
		t.Fatalf("failed to start standby multipooler %s: %v", standby.Name, err)
	}

	// Wait for manager to be ready
	WaitForManagerReady(t, multipooler)

	// Initialize consensus term to 1
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := NewMultipoolerClient(multipooler.GrpcPort)
	if err != nil {
		t.Fatalf("failed to connect to standby multipooler %s: %v", standby.Name, err)
	}
	defer client.Close()

	if err := ResetTerm(ctx, client.Manager); err != nil {
		t.Fatalf("failed to set term for standby %s: %v", standby.Name, err)
	}
	t.Logf("%s consensus term set to 1", standby.Name)

	// Verify standby is in recovery mode
	standbyPoolerClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", multipooler.GrpcPort))
	if err != nil {
		t.Fatalf("failed to create standby pooler client %s: %v", standby.Name, err)
	}
	queryResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT pg_is_in_recovery()", 1)
	standbyPoolerClient.Close()
	if err != nil {
		t.Fatalf("failed to check standby recovery status %s: %v", standby.Name, err)
	}
	if len(queryResp.Rows) == 0 || len(queryResp.Rows[0].Values) == 0 || string(queryResp.Rows[0].Values[0]) != "t" {
		t.Fatalf("%s is not in recovery mode", standby.Name)
	}

	// Note: Replication (primary_conninfo) is NOT configured here.
	// Clean state = empty primary_conninfo. SetupTest() configures replication by default.

	// Set pooler type to REPLICA
	if err := SetPoolerType(ctx, client.Manager, clustermetadatapb.PoolerType_REPLICA); err != nil {
		t.Fatalf("failed to set standby pooler type %s: %v", standby.Name, err)
	}

	t.Logf("%s initialized successfully", standby.Name)
}

// setupStandbyReplication configures a standby to replicate from the primary using pgBackRest.
// Follows the pattern from multipooler/setup_test.go:setupStandbyReplication.
func setupStandbyReplication(t *testing.T, baseDir string, primary *MultipoolerInstance, standby *MultipoolerInstance, config *SetupConfig) {
	t.Helper()

	// Remove the standby pg_data directory to prepare for pgbackrest restore
	standbyPgDataDir := filepath.Join(standby.Pgctld.DataDir, "pg_data")
	t.Logf("Removing standby pg_data directory: %s", standbyPgDataDir)
	err := os.RemoveAll(standbyPgDataDir)
	require.NoError(t, err)

	// Get primary's pgbackrest configuration
	primaryConfigPath := filepath.Join(primary.Pgctld.DataDir, "pgbackrest.conf")
	repoPath := filepath.Join(baseDir, "backup-repo", config.Database, config.TableGroup, config.Shard)

	// Create a backup on the primary using pgbackrest
	t.Logf("Creating pgBackRest backup on primary (stanza: %s)...", stanzaName(config.Database, config.TableGroup, config.Shard))

	backupCtx, backupCancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer backupCancel()

	backupCmd := exec.CommandContext(backupCtx, "pgbackrest",
		"--stanza="+stanzaName(config.Database, config.TableGroup, config.Shard),
		"--config="+primaryConfigPath,
		"--repo1-path="+repoPath,
		"--type=full",
		"--log-level-console=info",
		"backup")

	backupOutput, err := backupCmd.CombinedOutput()
	if err != nil {
		t.Logf("pgbackrest backup output: %s", string(backupOutput))
	}
	require.NoError(t, err, "pgbackrest backup should succeed")
	t.Log("pgBackRest backup completed successfully")

	// Create standby's pgbackrest configuration before restore
	logPath := filepath.Join(baseDir, "logs", "pgbackrest")
	spoolPath := filepath.Join(baseDir, "pgbackrest-spool")
	lockPath := filepath.Join(baseDir, "pgbackrest-lock")

	standbyConfigPath := filepath.Join(standby.Pgctld.DataDir, "pgbackrest.conf")
	standbyBackupCfg := pgbackrest.Config{
		StanzaName:  stanzaName(config.Database, config.TableGroup, config.Shard),
		PgDataPath:  filepath.Join(standby.Pgctld.DataDir, "pg_data"),
		PgPort:      standby.Pgctld.PgPort,
		PgSocketDir: filepath.Join(standby.Pgctld.DataDir, "pg_sockets"),
		PgUser:      "postgres",
		PgDatabase:  "postgres",
		AdditionalHosts: []pgbackrest.PgHost{
			{
				DataPath:  filepath.Join(primary.Pgctld.DataDir, "pg_data"),
				SocketDir: filepath.Join(primary.Pgctld.DataDir, "pg_sockets"),
				Port:      primary.Pgctld.PgPort,
				User:      "postgres",
				Database:  "postgres",
			},
		},
		LogPath:       logPath,
		SpoolPath:     spoolPath,
		LockPath:      lockPath,
		RetentionFull: 2,
	}

	if err := pgbackrest.WriteConfigFile(standbyConfigPath, standbyBackupCfg); err != nil {
		require.NoError(t, err, "failed to write standby pgbackrest config")
	}
	t.Logf("Created pgbackrest config for standby at %s", standbyConfigPath)

	// Restore the backup to the standby using pgbackrest
	t.Logf("Restoring pgBackRest backup to %s...", standby.Name)

	restoreCtx, restoreCancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer restoreCancel()

	restoreCmd := exec.CommandContext(restoreCtx, "pgbackrest",
		"--stanza="+stanzaName(config.Database, config.TableGroup, config.Shard),
		"--config="+standbyConfigPath,
		"--repo1-path="+repoPath,
		"--log-level-console=info",
		"restore")

	restoreOutput, err := restoreCmd.CombinedOutput()
	if err != nil {
		t.Logf("pgbackrest restore output: %s", string(restoreOutput))
	}
	require.NoError(t, err, "pgbackrest restore should succeed")
	t.Log("pgBackRest restore completed successfully")

	// Create standby.signal to put the server in recovery mode
	standbySignalPath := filepath.Join(standbyPgDataDir, "standby.signal")
	t.Logf("Creating standby.signal file: %s", standbySignalPath)
	err = os.WriteFile(standbySignalPath, []byte(""), 0o644)
	require.NoError(t, err, "Should be able to create standby.signal")

	t.Logf("%s data restored from backup and configured as replica", standby.Name)
}

// WipeNode completely resets a node to uninitialized state:
// 1. Stops PostgreSQL via pgctld
// 2. Stops multipooler
// 3. Removes pg_data directory
// 4. Deletes the node from topology
// 5. Restarts multipooler (will re-register as UNKNOWN)
// Use this to simulate data loss for bootstrap/recovery tests.
func (s *ShardSetup) WipeNode(t *testing.T, name string) {
	t.Helper()

	inst := s.GetMultipoolerInstance(name)
	if inst == nil {
		t.Fatalf("node %s not found", name)
	}

	pgctld := inst.Pgctld
	multipooler := inst.Multipooler

	// 1. Stop PostgreSQL via pgctld gRPC
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", pgctld.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Logf("Warning: failed to connect to pgctld for %s: %v", name, err)
	} else {
		pgctldClient := pgctldservice.NewPgCtldClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err = pgctldClient.Stop(ctx, &pgctldservice.StopRequest{Mode: "fast"})
		cancel()
		conn.Close()
		if err != nil {
			t.Logf("Warning: failed to stop PostgreSQL on %s (may already be stopped): %v", name, err)
		} else {
			t.Logf("Stopped PostgreSQL on %s", name)
		}
	}

	// 2. Stop multipooler
	multipooler.TerminateGracefully(t, 5*time.Second)
	t.Logf("Stopped multipooler on %s", name)

	// 3. Remove pg_data directory
	pgDataDir := filepath.Join(pgctld.DataDir, "pg_data")
	if err := os.RemoveAll(pgDataDir); err != nil {
		t.Fatalf("failed to remove pg_data for %s: %v", name, err)
	}
	t.Logf("Removed pg_data directory for %s", name)

	// 4. Delete node from topology
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      multipooler.Cell,
		Name:      multipooler.Name,
	}
	if err := s.TopoServer.UnregisterMultiPooler(ctx, id); err != nil {
		t.Logf("Warning: failed to unregister %s from topology: %v", name, err)
	} else {
		t.Logf("Unregistered %s from topology", name)
	}

	// 5. Restart multipooler (will re-register as UNKNOWN)
	if err := multipooler.Start(t); err != nil {
		t.Fatalf("failed to restart multipooler for %s: %v", name, err)
	}
	WaitForManagerReady(t, multipooler)
	t.Logf("Restarted multipooler on %s (will register as UNKNOWN)", name)
}

// WipeAllNodes resets all nodes to uninitialized state.
// Use this to set up a clean slate for bootstrap tests.
func (s *ShardSetup) WipeAllNodes(t *testing.T) {
	t.Helper()

	for name := range s.Multipoolers {
		s.WipeNode(t, name)
	}
	t.Log("Wiped all nodes - cluster is ready for bootstrap")
}

// ValidateCleanState checks that all multipoolers are in the expected clean state.
// Expected state:
//   - Primary: not in recovery, primary_conninfo="", synchronous_standby_names="", term=1, type=PRIMARY
//   - Standbys: in recovery, primary_conninfo="", wal_replay not paused, term=1, type=REPLICA
//   - MultiOrch: NOT running (multiorch starts in SetupTest and stops in cleanup)
//
// Returns an error if state is not clean.
func (s *ShardSetup) ValidateCleanState() error {
	if s == nil {
		return nil
	}

	// Require primary to exist
	if s.GetMultipoolerInstance("primary") == nil {
		return fmt.Errorf("no primary instance found")
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

		isPrimary := name == "primary"

		// Check recovery mode
		inRecovery, err := QueryStringValue(ctx, client.Pooler, "SELECT pg_is_in_recovery()")
		if err != nil {
			return fmt.Errorf("%s failed to query pg_is_in_recovery: %w", name, err)
		}

		if isPrimary {
			if inRecovery != "f" {
				return fmt.Errorf("%s pg_is_in_recovery=%s (expected f)", name, inRecovery)
			}

			// Validate GUCs are at defaults
			if err := ValidateGUCValue(ctx, client.Pooler, "primary_conninfo", "", name); err != nil {
				return err
			}
			if err := ValidateGUCValue(ctx, client.Pooler, "synchronous_standby_names", "", name); err != nil {
				return err
			}

			// Validate pooler type is PRIMARY
			if err := ValidatePoolerType(ctx, client.Manager, clustermetadatapb.PoolerType_PRIMARY, name); err != nil {
				return err
			}
		} else {
			if inRecovery != "t" {
				return fmt.Errorf("%s pg_is_in_recovery=%s (expected t)", name, inRecovery)
			}

			// Verify replication not configured (clean state = empty primary_conninfo)
			if err := ValidateGUCValue(ctx, client.Pooler, "primary_conninfo", "", name); err != nil {
				return err
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

		// Validate term is 1 for all nodes
		if err := ValidateTerm(ctx, client.Consensus, 1, name); err != nil {
			return err
		}
	}

	return nil
}

// ResetToCleanState resets all multipoolers to the expected clean state.
// This resets terms to 1, pooler types to PRIMARY/REPLICA, GUCs to defaults, resumes WAL replay,
// and stops multiorch instances.
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

		isPrimary := name == "primary"

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

		// Reset GUCs
		gucsToReset := []string{"synchronous_standby_names", "synchronous_commit", "primary_conninfo"}
		for _, guc := range gucsToReset {
			_, err := client.Pooler.ExecuteQuery(ctx, fmt.Sprintf("ALTER SYSTEM RESET %s", guc), 0)
			if err != nil {
				t.Logf("Reset: Failed to reset %s on %s: %v", guc, name, err)
			}
		}
		_, _ = client.Pooler.ExecuteQuery(ctx, "SELECT pg_reload_conf()", 0)

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

// SetupTest provides test isolation by validating clean state, configuring replication,
// and automatically restoring state changes at test cleanup.
//
// DEFAULT BEHAVIOR (no options):
//   - Validates clean state before test
//   - Saves GUC values (synchronous_standby_names, synchronous_commit, primary_conninfo)
//   - Configures replication on all standbys (sets primary_conninfo, starts WAL streaming)
//   - Registers cleanup to restore saved GUCs and reset state after test
//
// WithoutReplication():
//   - Does NOT configure replication: primary_conninfo stays empty
//   - Still saves/restores GUCs for test isolation
//   - Use for tests that set up replication from scratch
//
// WithPausedReplication():
//   - Configures replication but pauses WAL replay
//   - Use for tests that need to test pg_wal_replay_resume()
//
// WithResetGuc(gucNames...):
//   - Adds additional GUCs to save/restore
//
// Follows the pattern from multipooler/setup_test.go:setupPoolerTest.
func (s *ShardSetup) SetupTest(t *testing.T, opts ...SetupTestOption) {
	t.Helper()

	// Parse options (default: cleanup enabled)
	config := &SetupTestConfig{Cleanup: true}
	for _, opt := range opts {
		opt(config)
	}

	// Fail fast if shared processes died
	s.CheckSharedProcesses(t)

	// Validate that settings are in the expected clean state
	if err := s.ValidateCleanState(); err != nil {
		t.Fatalf("SetupTest: %v. Previous test leaked state.", err)
	}

	// Determine if we should configure replication (default: yes, unless WithoutReplication)
	shouldConfigureReplication := !config.NoReplication

	// If configuring replication and no GUCs specified yet, add the default replication GUCs
	if shouldConfigureReplication && len(config.GucsToReset) == 0 {
		config.GucsToReset = append(config.GucsToReset, "synchronous_standby_names", "synchronous_commit", "primary_conninfo")
	}

	// Save GUC values BEFORE configuring replication (so we save the clean state)
	savedGucs := make(map[string]map[string]string) // name -> guc -> value
	if len(config.GucsToReset) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for name, inst := range s.Multipoolers {
			client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if err != nil {
				t.Logf("SetupTest: failed to connect to %s for GUC save: %v", name, err)
				continue
			}
			savedGucs[name] = SaveGUCs(ctx, client.Pooler, config.GucsToReset)
			client.Close()
		}
	}

	// Configure replication if needed (after saving GUCs)
	if shouldConfigureReplication {
		primary := s.GetMultipoolerInstance("primary")
		if primary == nil {
			t.Log("SetupTest: Cannot configure replication (no primary)")
		} else {
			// Configure replication on all standbys
			var standbyNames []string
			for name := range s.Multipoolers {
				if name == "primary" {
					continue
				}
				s.configureStandbyReplication(t, name, config.PauseReplication)
				standbyNames = append(standbyNames, name)
			}

			// Configure synchronous_standby_names on primary (for multiorch)
			// Format: ANY 1 (standby, standby2) for durability policy ANY_2
			if len(standbyNames) > 0 {
				s.configurePrimarySyncStandbys(t, standbyNames)
			}
		}
	}

	// Start multiorch instances AFTER replication is configured
	// This ensures multiorch sees healthy replicas with streaming replication
	// TODO (@rafa): once we have a way to disable multiorch on a shard, we don't need
	// this big hammer of stopping / starting on each test.
	for name, mo := range s.MultiOrchInstances {
		if err := mo.Start(t); err != nil {
			t.Fatalf("SetupTest: failed to start multiorch %s: %v", name, err)
		}
		t.Logf("SetupTest: Started multiorch '%s': gRPC=%d, HTTP=%d", name, mo.GrpcPort, mo.HttpPort)
	}

	// Skip cleanup registration for isolated shards (cleanup handled by defer cleanup())
	// Note: multiorch still gets started above, but won't be cleaned up by t.Cleanup.
	// For isolated shards, the cleanup function returned by NewIsolated handles this.
	if !config.Cleanup {
		return
	}

	// Register cleanup handler
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

			isPrimary := name == "primary"

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

			// Restore saved GUC values
			if gucs, ok := savedGucs[name]; ok && len(gucs) > 0 {
				RestoreGUCs(cleanupCtx, t, client.Pooler, gucs, name)
			}

			// Always resume WAL replay (must be after GUC restoration)
			// This ensures we leave the system in a good state even if tests paused replay.
			// We always do this regardless of WithoutReplication flag because pause state persists.
			// NOTE: We don't wait for streaming because we just restored GUCs to clean state
			// (primary_conninfo=''), so there's no replication source configured.
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

// configureStandbyReplication configures a single standby to replicate from the primary.
// If pauseReplication is true, WAL replay is paused after replication is configured.
func (s *ShardSetup) configureStandbyReplication(t *testing.T, standbyName string, pauseReplication bool) {
	t.Helper()

	standby := s.GetMultipoolerInstance(standbyName)
	if standby == nil {
		t.Logf("SetupTest: standby %s not found, skipping replication setup", standbyName)
		return
	}

	primary := s.GetMultipoolerInstance("primary")
	if primary == nil {
		t.Log("SetupTest: primary not found, skipping replication setup")
		return
	}

	client, err := NewMultipoolerClient(standby.Multipooler.GrpcPort)
	if err != nil {
		t.Logf("SetupTest: failed to connect to %s: %v", standbyName, err)
		return
	}
	defer client.Close()

	// Create a context with timeout for all operations in this function
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Check if replication is already streaming
	resp, err := client.Pooler.ExecuteQuery(ctx, "SELECT status FROM pg_stat_wal_receiver", 1)
	alreadyStreaming := err == nil && len(resp.Rows) > 0 && len(resp.Rows[0].Values) > 0 &&
		string(resp.Rows[0].Values[0]) == "streaming"

	if alreadyStreaming {
		t.Logf("SetupTest: %s replication already streaming", standbyName)
	} else {
		t.Logf("SetupTest: Configuring replication on %s...", standbyName)

		// Configure replication with Force=true
		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(primary.Pgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 true,
		}
		_, err = client.Manager.SetPrimaryConnInfo(ctx, setPrimaryReq)
		if err != nil {
			t.Fatalf("SetupTest: failed to configure replication on %s: %v", standbyName, err)
		}
	}

	// Wait for replication to be streaming
	require.Eventually(t, func() bool {
		queryCtx, queryCancel := context.WithTimeout(ctx, 2*time.Second)
		defer queryCancel()
		resp, err := client.Pooler.ExecuteQuery(queryCtx, "SELECT status FROM pg_stat_wal_receiver", 1)
		if err != nil || len(resp.Rows) == 0 {
			return false
		}
		return string(resp.Rows[0].Values[0]) == "streaming"
	}, 10*time.Second, 100*time.Millisecond, "Replication should be streaming on %s", standbyName)

	// Optionally pause WAL replay
	if pauseReplication {
		_, err := client.Pooler.ExecuteQuery(ctx, "SELECT pg_wal_replay_pause()", 1)
		if err != nil {
			t.Logf("SetupTest: Failed to pause WAL replay on %s: %v", standbyName, err)
		} else {
			t.Logf("SetupTest: %s replication streaming, WAL replay PAUSED", standbyName)
			return
		}
	}

	t.Logf("SetupTest: %s replication streaming", standbyName)
}

// configurePrimarySyncStandbys sets synchronous_standby_names on the primary via gRPC.
// This is needed for multiorch to see standbys as properly configured.
func (s *ShardSetup) configurePrimarySyncStandbys(t *testing.T, standbyNames []string) {
	t.Helper()

	primary := s.GetMultipoolerInstance("primary")
	if primary == nil {
		t.Log("SetupTest: Cannot configure sync standbys (no primary)")
		return
	}

	client, err := NewMultipoolerClient(primary.Multipooler.GrpcPort)
	if err != nil {
		t.Fatalf("SetupTest: failed to connect to primary: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Build standby IDs for the gRPC request
	standbyIDs := make([]*clustermetadatapb.ID, 0, len(standbyNames))
	for _, name := range standbyNames {
		standbyIDs = append(standbyIDs, &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      s.CellName,
			Name:      name,
		})
	}

	// Configure synchronous replication via gRPC
	// Use ANY 1 method to match durability policy behavior
	configReq := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
		SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
		SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
		NumSync:           1,
		StandbyIds:        standbyIDs,
		ReloadConfig:      true,
	}

	_, err = client.Manager.ConfigureSynchronousReplication(ctx, configReq)
	if err != nil {
		t.Fatalf("SetupTest: failed to configure synchronous replication: %v", err)
	}

	t.Logf("SetupTest: Configured synchronous replication with %d standbys", len(standbyNames))
}

// configureReplication configures a standby to replicate from the primary.
// This is a helper for tests that need to set up replication.
func (s *ShardSetup) ConfigureReplication(t *testing.T, standbyName string) {
	t.Helper()

	standby := s.GetMultipoolerInstance(standbyName)
	if standby == nil {
		t.Fatalf("standby %s not found", standbyName)
	}

	primary := s.GetMultipoolerInstance("primary")
	if primary == nil {
		t.Fatalf("primary not found")
	}

	client, err := NewMultipoolerClient(standby.Multipooler.GrpcPort)
	require.NoError(t, err, "failed to connect to standby")
	defer client.Close()

	// Configure replication with Force=true
	setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
		Host:                  "localhost",
		Port:                  int32(primary.Pgctld.PgPort),
		StartReplicationAfter: true,
		StopReplicationBefore: false,
		CurrentTerm:           1,
		Force:                 true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.Manager.SetPrimaryConnInfo(ctx, setPrimaryReq)
	require.NoError(t, err, "failed to configure replication on %s", standbyName)

	// Wait for replication to be streaming
	require.Eventually(t, func() bool {
		queryCtx, queryCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer queryCancel()
		resp, err := client.Pooler.ExecuteQuery(queryCtx, "SELECT status FROM pg_stat_wal_receiver", 1)
		if err != nil || len(resp.Rows) == 0 {
			return false
		}
		return string(resp.Rows[0].Values[0]) == "streaming"
	}, 10*time.Second, 100*time.Millisecond, "replication should be streaming on %s", standbyName)

	t.Logf("Configured replication on %s", standbyName)
}

// DemotePrimary demotes the primary by putting it into standby mode.
// This is used to test failover scenarios and then reset the cluster.
func (s *ShardSetup) DemotePrimary(t *testing.T) {
	t.Helper()

	primary := s.GetMultipoolerInstance("primary")
	if primary == nil {
		t.Fatal("primary not found")
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

// GetClient returns a MultipoolerClient for the specified multipooler instance.
func (s *ShardSetup) GetClient(t *testing.T, name string) *MultipoolerClient {
	t.Helper()

	inst := s.GetMultipoolerInstance(name)
	if inst == nil {
		t.Fatalf("multipooler %s not found", name)
	}

	client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
	require.NoError(t, err, "failed to connect to %s", name)

	return client
}

// GetPrimaryClient returns a MultipoolerClient for the primary instance.
func (s *ShardSetup) GetPrimaryClient(t *testing.T) *MultipoolerClient {
	return s.GetClient(t, "primary")
}

// GetStandbyClient returns a MultipoolerClient for the standby instance.
func (s *ShardSetup) GetStandbyClient(t *testing.T) *MultipoolerClient {
	return s.GetClient(t, "standby")
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
