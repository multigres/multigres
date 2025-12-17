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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
	StanzaName       string
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

// WithStanzaName sets the pgBackRest stanza name.
func WithStanzaName(stanza string) SetupOption {
	return func(c *SetupConfig) {
		c.StanzaName = stanza
	}
}

// SetupTestConfig holds configuration for SetupTest.
type SetupTestConfig struct {
	NoReplication    bool // Don't configure replication
	PauseReplication bool // Configure replication but pause WAL replay
}

// SetupTestOption is a function that configures SetupTest behavior.
type SetupTestOption func(*SetupTestConfig)

// WithoutReplication returns an option that disables replication setup.
// Use this for tests that need to set up replication from scratch.
func WithoutReplication() SetupTestOption {
	return func(c *SetupTestConfig) {
		c.NoReplication = true
	}
}

// WithPausedReplication returns an option that starts replication but pauses WAL replay.
// Use this for tests that need to test pg_wal_replay_resume().
func WithPausedReplication() SetupTestOption {
	return func(c *SetupTestConfig) {
		c.PauseReplication = true
	}
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
		StanzaName:       "test_backup",
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
		DurabilityPolicy: "ANY_2",
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

		inst := setup.CreateMultipoolerInstance(t, name, grpcPort, pgPort, multipoolerPort, config.StanzaName)
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

	// Create multiorch instances
	if config.MultiOrchCount > 0 {
		watchTargets := []string{fmt.Sprintf("%s/%s/%s", config.Database, config.TableGroup, config.Shard)}
		for i := 0; i < config.MultiOrchCount; i++ {
			name := multiOrchName(i)
			mo := setup.CreateMultiOrchInstance(t, name, config.CellName, watchTargets)

			// Start multiorch
			if err := mo.Start(t); err != nil {
				t.Fatalf("failed to start multiorch %s: %v", name, err)
			}
			t.Logf("Started multiorch '%s': gRPC=%d, HTTP=%d", name, mo.GrpcPort, mo.HttpPort)
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

	// Create symmetric pgbackrest configuration with all standbys as additional hosts
	configPath := filepath.Join(pgctld.DataDir, "pgbackrest.conf")
	backupCfg := pgbackrest.Config{
		StanzaName:      config.StanzaName,
		PgDataPath:      filepath.Join(pgctld.DataDir, "pg_data"),
		PgPort:          pgctld.PgPort,
		PgSocketDir:     filepath.Join(pgctld.DataDir, "pg_sockets"),
		PgUser:          "postgres",
		PgDatabase:      "postgres",
		AdditionalHosts: make([]pgbackrest.PgHost, 0, len(standbys)),
		LogPath:         logPath,
		SpoolPath:       spoolPath,
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
	t.Logf("Created symmetric pgbackrest config at %s (stanza: %s)", configPath, config.StanzaName)

	// Configure archive_mode in postgresql.auto.conf BEFORE starting PostgreSQL
	pgDataDir := filepath.Join(pgctld.DataDir, "pg_data")
	autoConfPath := filepath.Join(pgDataDir, "postgresql.auto.conf")
	archiveConfig := fmt.Sprintf(`
# Archive mode for pgbackrest backups
archive_mode = on
archive_command = 'pgbackrest --stanza=%s --config=%s --repo1-path=%s archive-push %%p'
`, config.StanzaName, configPath, repoPath)
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

	if err := pgbackrest.StanzaCreate(ctx, config.StanzaName, configPath, repoPath); err != nil {
		t.Fatalf("failed to create pgbackrest stanza: %v", err)
	}
	t.Logf("Initialized pgbackrest stanza: %s", config.StanzaName)

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

	_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "CREATE SCHEMA IF NOT EXISTS multigres", 0)
	if err != nil {
		t.Fatalf("failed to create multigres schema: %v", err)
	}

	_, err = primaryPoolerClient.ExecuteQuery(context.Background(), `
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

	configPath := filepath.Join(pgctld.DataDir, "pgbackrest.conf")
	backupCfg := pgbackrest.Config{
		StanzaName:  config.StanzaName,
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
	ctx := context.Background()
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
	t.Logf("Creating pgBackRest backup on primary (stanza: %s)...", config.StanzaName)

	backupCtx, backupCancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer backupCancel()

	backupCmd := exec.CommandContext(backupCtx, "pgbackrest",
		"--stanza="+config.StanzaName,
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

	standbyConfigPath := filepath.Join(standby.Pgctld.DataDir, "pgbackrest.conf")
	standbyBackupCfg := pgbackrest.Config{
		StanzaName:  config.StanzaName,
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
		"--stanza="+config.StanzaName,
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

// ValidateCleanState checks that all multipoolers are in the expected clean state.
// Expected state:
//   - Primary: not in recovery, primary_conninfo=”, synchronous_standby_names=”, term=1, type=PRIMARY
//   - Standbys: in recovery, primary_conninfo=”, wal_replay not paused, term=1, type=REPLICA
//
// Returns an error if state is not clean.
func (s *ShardSetup) ValidateCleanState() error {
	if s == nil {
		return nil
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

			// Validate primary_conninfo and synchronous_standby_names are empty
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

			// Verify primary_conninfo empty
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
// This resets terms to 1, pooler types to PRIMARY/REPLICA, and ensures GUCs are cleared.
func (s *ShardSetup) ResetToCleanState(t *testing.T) {
	t.Helper()

	if s == nil {
		return
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
//   - Configures replication on all standbys (sets primary_conninfo, starts WAL streaming)
//   - Disables synchronous replication (safe - writes won't hang)
//   - Registers cleanup to reset to clean state after test
//
// WithoutReplication():
//   - Does NOT configure replication: primary_conninfo stays empty
//   - Use for tests that set up replication from scratch
//
// WithPausedReplication():
//   - Configures replication but pauses WAL replay
//   - Use for tests that need to test pg_wal_replay_resume()
//
// Follows the pattern from multipooler/setup_test.go:setupPoolerTest.
func (s *ShardSetup) SetupTest(t *testing.T, opts ...SetupTestOption) {
	t.Helper()

	// Parse options
	config := &SetupTestConfig{}
	for _, opt := range opts {
		opt(config)
	}

	// Fail fast if shared processes died
	s.CheckSharedProcesses(t)

	// Validate that settings are in the expected clean state
	if err := s.ValidateCleanState(); err != nil {
		t.Fatalf("SetupTest: %v. Previous test leaked state.", err)
	}

	// Configure replication unless explicitly disabled
	if !config.NoReplication {
		primary := s.GetMultipoolerInstance("primary")
		if primary == nil {
			t.Log("SetupTest: Cannot configure replication (no primary)")
		} else {
			// SAFETY: Always disable synchronous replication to prevent write hangs
			primaryClient, err := NewMultipoolerClient(primary.Multipooler.GrpcPort)
			if err == nil {
				_, err := primaryClient.Pooler.ExecuteQuery(context.Background(), "ALTER SYSTEM SET synchronous_standby_names = ''", 0)
				if err == nil {
					_, _ = primaryClient.Pooler.ExecuteQuery(context.Background(), "SELECT pg_reload_conf()", 0)
					t.Log("SetupTest: Disabled synchronous replication for safety")
				}
				primaryClient.Close()
			}

			// Configure replication on all standbys
			for name := range s.Multipoolers {
				if name == "primary" {
					continue
				}
				s.configureStandbyReplication(t, name, config.PauseReplication)
			}
		}
	}

	// Register cleanup handler
	t.Cleanup(func() {
		s.ResetToCleanState(t)

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

	// Check if replication is already streaming
	resp, err := client.Pooler.ExecuteQuery(context.Background(), "SELECT status FROM pg_stat_wal_receiver", 1)
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Manager.SetPrimaryConnInfo(ctx, setPrimaryReq)
		cancel()

		if err != nil {
			t.Fatalf("SetupTest: failed to configure replication on %s: %v", standbyName, err)
		}
	}

	// Wait for replication to be streaming
	require.Eventually(t, func() bool {
		resp, err := client.Pooler.ExecuteQuery(context.Background(), "SELECT status FROM pg_stat_wal_receiver", 1)
		if err != nil || len(resp.Rows) == 0 {
			return false
		}
		return string(resp.Rows[0].Values[0]) == "streaming"
	}, 10*time.Second, 100*time.Millisecond, "Replication should be streaming on %s", standbyName)

	// Optionally pause WAL replay
	if pauseReplication {
		_, err := client.Pooler.ExecuteQuery(context.Background(), "SELECT pg_wal_replay_pause()", 1)
		if err != nil {
			t.Logf("SetupTest: Failed to pause WAL replay on %s: %v", standbyName, err)
		} else {
			t.Logf("SetupTest: %s replication streaming, WAL replay PAUSED", standbyName)
			return
		}
	}

	t.Logf("SetupTest: %s replication streaming", standbyName)
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
		resp, err := client.Pooler.ExecuteQuery(context.Background(), "SELECT status FROM pg_stat_wal_receiver", 1)
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
		inRecovery, err := QueryStringValue(context.Background(), client.Pooler, "SELECT pg_is_in_recovery()")
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
	return makeMultipoolerID("test-cell", inst.Multipooler.ServiceID)
}
