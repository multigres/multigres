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

package multipooler

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/provisioner/local/pgbackrest"
	"github.com/multigres/multigres/go/test/endtoend"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/pathutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/pb/pgctldservice"

	// Register topo plugins
	_ "github.com/multigres/multigres/go/common/plugins/topo"
)

var (
	// Shared test infrastructure
	sharedTestSetup *MultipoolerTestSetup
	setupOnce       sync.Once
	setupError      error
)

// TestMain sets the path and cleans up after all tests
func TestMain(m *testing.M) {
	// Set the PATH so dependencies like etcd and run_in_test.sh can be found
	// Use automatic module root detection instead of hard-coded relative paths
	if err := pathutil.PrependBinToPath(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add bin to PATH: %v\n", err)
		os.Exit(1)
	}

	// Set orphan detection environment variable as baseline protection.
	// This ensures postgres processes started by in-process services will
	// have watchdogs that monitor the test process and kill postgres if
	// the test crashes. Individual tests can additionally set
	// MULTIGRES_TESTDATA_DIR for directory-deletion triggered cleanup.
	os.Setenv("MULTIGRES_TEST_PARENT_PID", fmt.Sprintf("%d", os.Getpid()))

	// Set up signal handler to ensure cleanup on interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cleanupSharedTestSetup()
		os.Exit(1)
	}()

	// Run all tests
	exitCode := m.Run()

	// Dump service logs on failure to help debug CI issues
	if exitCode != 0 {
		dumpServiceLogs()
	}

	// Clean up shared multipooler test infrastructure
	cleanupSharedTestSetup()

	// Cleanup environment variable
	os.Unsetenv("MULTIGRES_TEST_PARENT_PID")

	// Exit with the test result code
	os.Exit(exitCode)
}

// dumpServiceLogs prints service log files to help debug test failures.
// Call this before cleanup so logs are available.
func dumpServiceLogs() {
	if sharedTestSetup == nil {
		return
	}

	fmt.Println("\n" + "=" + "=== SERVICE LOGS (test failure) ===" + "=")

	instances := []*ProcessInstance{
		sharedTestSetup.PrimaryMultipooler,
		sharedTestSetup.StandbyMultipooler,
		sharedTestSetup.PrimaryPgctld,
		sharedTestSetup.StandbyPgctld,
	}

	for _, inst := range instances {
		if inst == nil || inst.LogFile == "" {
			continue
		}

		fmt.Printf("\n--- %s (%s) ---\n", inst.Name, inst.LogFile)
		content, err := os.ReadFile(inst.LogFile)
		if err != nil {
			fmt.Printf("  [error reading log: %v]\n", err)
			continue
		}
		if len(content) == 0 {
			fmt.Println("  [empty log file]")
			continue
		}
		fmt.Println(string(content))
	}

	fmt.Println("\n" + "=" + "=== END SERVICE LOGS ===" + "=")
}

// cleanupSharedTestSetup cleans up the shared test infrastructure
func cleanupSharedTestSetup() {
	if sharedTestSetup == nil {
		return
	}

	// Stop multipooler instances
	if sharedTestSetup.StandbyMultipooler != nil {
		sharedTestSetup.StandbyMultipooler.Stop()
	}
	if sharedTestSetup.PrimaryMultipooler != nil {
		sharedTestSetup.PrimaryMultipooler.Stop()
	}

	// Stop pgctld instances
	if sharedTestSetup.StandbyPgctld != nil {
		sharedTestSetup.StandbyPgctld.Stop()
	}
	if sharedTestSetup.PrimaryPgctld != nil {
		sharedTestSetup.PrimaryPgctld.Stop()
	}

	// Close topology server
	if sharedTestSetup.TopoServer != nil {
		sharedTestSetup.TopoServer.Close()
	}

	// Stop etcd
	if sharedTestSetup.EtcdCmd != nil && sharedTestSetup.EtcdCmd.Process != nil {
		_ = sharedTestSetup.EtcdCmd.Process.Kill()
		_ = sharedTestSetup.EtcdCmd.Wait()
	}

	// Clean up temp directory
	if sharedTestSetup.TempDirCleanup != nil {
		sharedTestSetup.TempDirCleanup()
	}
}

// ProcessInstance represents a process instance for testing (pgctld or multipooler)
type ProcessInstance struct {
	Name        string
	ServiceID   string // Multipooler service ID (format: cell/name)
	DataDir     string // Used by pgctld
	ConfigFile  string // Used by pgctld
	LogFile     string
	GrpcPort    int
	PgPort      int    // Used by pgctld
	PgctldAddr  string // Used by multipooler
	EtcdAddr    string // Used by multipooler for topology
	StanzaName  string // pgBackRest stanza name (used by multipooler)
	Process     *exec.Cmd
	Binary      string
	Environment []string
}

// MultipoolerTestSetup holds shared test infrastructure
type MultipoolerTestSetup struct {
	TempDir            string
	TempDirCleanup     func()
	EtcdClientAddr     string
	EtcdCmd            *exec.Cmd
	TopoServer         topoclient.Store
	PrimaryPgctld      *ProcessInstance
	StandbyPgctld      *ProcessInstance
	PrimaryMultipooler *ProcessInstance
	StandbyMultipooler *ProcessInstance
}

// Start starts the process instance (pgctld or multipooler)
func (p *ProcessInstance) Start(t *testing.T) error {
	t.Helper()

	switch p.Binary {
	case "pgctld":
		return p.startPgctld(t)
	case "multipooler":
		return p.startMultipooler(t)
	}
	return fmt.Errorf("unknown binary type: %s", p.Binary)
}

// startPgctld starts a pgctld instance (server only, PostgreSQL init/start done separately)
func (p *ProcessInstance) startPgctld(t *testing.T) error {
	t.Helper()

	t.Logf("Starting %s with binary '%s'", p.Name, p.Binary)
	t.Logf("Data dir: %s, gRPC port: %d, PG port: %d", p.DataDir, p.GrpcPort, p.PgPort)

	// Start the gRPC server
	p.Process = exec.Command(p.Binary, "server",
		"--pooler-dir", p.DataDir,
		"--grpc-port", strconv.Itoa(p.GrpcPort),
		"--pg-port", strconv.Itoa(p.PgPort),
		"--log-output", p.LogFile)

	// Set MULTIGRES_TESTDATA_DIR for directory-deletion triggered cleanup
	p.Process.Env = append(p.Environment,
		"MULTIGRES_TESTDATA_DIR="+filepath.Dir(p.DataDir),
	)

	t.Logf("Running server command: %v", p.Process.Args)
	if err := p.waitForStartup(t, 20*time.Second, 50); err != nil {
		return err
	}

	return nil
}

// startMultipooler starts a multipooler instance
func (p *ProcessInstance) startMultipooler(t *testing.T) error {
	t.Helper()

	t.Logf("Starting %s: binary '%s', gRPC port %d, ServiceID %s", p.Name, p.Binary, p.GrpcPort, p.ServiceID)

	// Build command arguments
	args := []string{
		"--grpc-port", strconv.Itoa(p.GrpcPort),
		"--database", "postgres", // Required parameter
		"--table-group", "default", // Required parameter (MVP only supports "default")
		"--shard", "0-inf", // Required parameter (MVP only supports "0-inf")
		"--pgctld-addr", p.PgctldAddr,
		"--pooler-dir", p.DataDir, // Use the same pooler dir as pgctld
		"--pg-port", strconv.Itoa(p.PgPort),
		"--service-map", "grpc-pooler,grpc-poolermanager,grpc-consensus,grpc-backup",
		"--topo-global-server-addresses", p.EtcdAddr,
		"--topo-global-root", "/multigres/global",
		"--topo-implementation", "etcd2",
		"--cell", "test-cell",
		"--service-id", p.ServiceID,
		"--log-output", p.LogFile,
	}

	// Add stanza name if configured
	if p.StanzaName != "" {
		args = append(args, "--pgbackrest-stanza", p.StanzaName)
	}

	// Start the multipooler server
	p.Process = exec.Command(p.Binary, args...)

	// Set MULTIGRES_TESTDATA_DIR for directory-deletion triggered cleanup
	p.Process.Env = append(p.Environment,
		"MULTIGRES_TESTDATA_DIR="+filepath.Dir(p.DataDir),
	)

	t.Logf("Running multipooler command: %v", p.Process.Args)
	return p.waitForStartup(t, 15*time.Second, 30)
}

// waitForStartup handles the common startup and waiting logic
func (p *ProcessInstance) waitForStartup(t *testing.T, timeout time.Duration, logInterval int) error {
	t.Helper()

	// Start the process in background (like cluster_test.go does)
	err := p.Process.Start()
	if err != nil {
		return fmt.Errorf("failed to start %s: %w", p.Name, err)
	}
	t.Logf("%s server process started with PID %d", p.Name, p.Process.Process.Pid)

	// Give the process a moment to potentially fail immediately
	time.Sleep(500 * time.Millisecond)

	// Check if process died immediately
	if p.Process.ProcessState != nil {
		t.Logf("%s process died immediately: exit code %d", p.Name, p.Process.ProcessState.ExitCode())
		p.logRecentOutput(t, "Process died immediately")
		return fmt.Errorf("%s process died immediately: exit code %d", p.Name, p.Process.ProcessState.ExitCode())
	}

	// Wait for server to be ready
	deadline := time.Now().Add(timeout)
	connectAttempts := 0
	for time.Now().Before(deadline) {
		// Check if process died during startup
		if p.Process.ProcessState != nil {
			t.Logf("%s process died during startup: exit code %d", p.Name, p.Process.ProcessState.ExitCode())
			p.logRecentOutput(t, "Process died during startup")
			return fmt.Errorf("%s process died: exit code %d", p.Name, p.Process.ProcessState.ExitCode())
		}

		connectAttempts++
		// Test gRPC connectivity
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", p.GrpcPort), 100*time.Millisecond)
		if err == nil {
			conn.Close()
			if p.Binary == "pgctld" {
				t.Logf("%s started successfully on gRPC port %d, PG port %d (after %d attempts)", p.Name, p.GrpcPort, p.PgPort, connectAttempts)
			} else {
				t.Logf("%s started successfully on gRPC port %d (after %d attempts)", p.Name, p.GrpcPort, connectAttempts)
			}
			return nil
		}
		if connectAttempts%logInterval == 0 {
			t.Logf("Still waiting for %s to start (attempt %d, error: %v)...", p.Name, connectAttempts, err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// If we timed out, try to get process status
	if p.Process.ProcessState == nil {
		t.Logf("%s process is still running but not responding on gRPC port %d", p.Name, p.GrpcPort)
	}

	t.Logf("Timeout waiting for %s after %d connection attempts", p.Name, connectAttempts)
	p.logRecentOutput(t, "Timeout waiting for server to start")
	return fmt.Errorf("timeout: %s failed to start listening on port %d after %d attempts", p.Name, p.GrpcPort, connectAttempts)
}

// logRecentOutput logs recent output from the process log file
func (p *ProcessInstance) logRecentOutput(t *testing.T, context string) {
	t.Helper()
	if p.LogFile == "" {
		return
	}

	content, err := os.ReadFile(p.LogFile)
	if err != nil {
		t.Logf("Failed to read log file %s: %v", p.LogFile, err)
		return
	}

	if len(content) == 0 {
		t.Logf("%s log file %s is empty", p.Name, p.LogFile)
		return
	}

	logContent := string(content)
	t.Logf("%s %s - Recent log output from %s:\n%s", p.Name, context, p.LogFile, logContent)
}

// Stop stops the process instance
func (p *ProcessInstance) Stop() {
	if p.Process == nil || p.Process.ProcessState != nil {
		return // Process not running
	}

	// If this is pgctld, stop PostgreSQL first via gRPC
	if p.Binary == "pgctld" {
		p.stopPostgreSQL()
	}

	// Then kill the process
	_ = p.Process.Process.Kill()
	_ = p.Process.Wait()
}

// stopPostgreSQL stops PostgreSQL via gRPC (best effort, no error handling)
func (p *ProcessInstance) stopPostgreSQL() {
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", p.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return // Can't connect, nothing we can do
	}
	defer conn.Close()

	client := pgctldservice.NewPgCtldClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Stop PostgreSQL
	_, _ = client.Stop(ctx, &pgctldservice.StopRequest{Mode: "fast"})
}

// createPgctldInstance creates a new pgctld instance configuration
func createPgctldInstance(t *testing.T, name, baseDir string, grpcPort, pgPort int) *ProcessInstance {
	t.Helper()

	dataDir := filepath.Join(baseDir, name, "data")
	logFile := filepath.Join(baseDir, name, "pgctld.log")

	// Create data directory
	err := os.MkdirAll(filepath.Dir(logFile), 0o755)
	require.NoError(t, err)

	return &ProcessInstance{
		Name:        name,
		DataDir:     dataDir,
		LogFile:     logFile,
		GrpcPort:    grpcPort,
		PgPort:      pgPort,
		Binary:      "pgctld", // Assume binary is in PATH
		Environment: append(os.Environ(), "PGCONNECT_TIMEOUT=5", "LC_ALL=en_US.UTF-8"),
	}
}

// createMultipoolerInstance creates a new multipooler instance configuration
func createMultipoolerInstance(t *testing.T, name, baseDir string, grpcPort int, pgctldAddr string, pgctldDataDir string, pgPort int, etcdAddr string, stanzaName string) *ProcessInstance {
	t.Helper()

	logFile := filepath.Join(baseDir, name, "multipooler.log")
	// Create log directory
	err := os.MkdirAll(filepath.Dir(logFile), 0o755)
	require.NoError(t, err)

	return &ProcessInstance{
		Name:        name,
		ServiceID:   name, // ServiceID is just the name, cell is passed separately via --cell
		LogFile:     logFile,
		GrpcPort:    grpcPort,
		PgPort:      pgPort,
		PgctldAddr:  pgctldAddr,
		DataDir:     pgctldDataDir, // Use the same data dir as pgctld for pooler-dir
		EtcdAddr:    etcdAddr,
		StanzaName:  stanzaName,    // pgBackRest stanza name
		Binary:      "multipooler", // Assume binary is in PATH
		Environment: append(os.Environ(), "PGCONNECT_TIMEOUT=5"),
	}
}

// initializePrimary sets up the primary pgctld, PostgreSQL, consensus term, and multipooler
func initializePrimary(t *testing.T, baseDir string, pgctld *ProcessInstance, multipooler *ProcessInstance, standbyPgctld *ProcessInstance, stanzaName string) error {
	t.Helper()

	// Start primary pgctld server
	if err := pgctld.Start(t); err != nil {
		return fmt.Errorf("failed to start primary pgctld: %w", err)
	}

	// Initialize PostgreSQL data directory (but don't start yet)
	primaryGrpcAddr := fmt.Sprintf("localhost:%d", pgctld.GrpcPort)
	if err := endtoend.InitPostgreSQLDataDir(t, primaryGrpcAddr); err != nil {
		return fmt.Errorf("failed to init PostgreSQL data dir: %w", err)
	}

	// Create pgbackrest configuration first (before starting PostgreSQL)
	// Build backup repository path with database/tablegroup/shard structure
	database := "postgres"
	tableGroup := "default"
	shard := "0-inf"
	repoPath := filepath.Join(baseDir, "backup-repo", database, tableGroup, shard)
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		return fmt.Errorf("failed to create backup repo: %w", err)
	}

	logPath := filepath.Join(baseDir, "logs", "pgbackrest")
	if err := os.MkdirAll(logPath, 0o755); err != nil {
		return fmt.Errorf("failed to create pgbackrest log dir: %w", err)
	}

	spoolPath := filepath.Join(baseDir, "pgbackrest-spool")
	if err := os.MkdirAll(spoolPath, 0o755); err != nil {
		return fmt.Errorf("failed to create pgbackrest spool dir: %w", err)
	}

	// Create symmetric pgbackrest configuration:
	// - pg1: this cluster (primary in this case)
	// - pg2: other cluster (standby in this case)
	// Each cluster treats itself as pg1 and lists others as pg2, pg3, etc.
	configPath := filepath.Join(pgctld.DataDir, "pgbackrest.conf")
	backupCfg := pgbackrest.Config{
		StanzaName:  stanzaName,
		PgDataPath:  filepath.Join(pgctld.DataDir, "pg_data"),
		PgPort:      pgctld.PgPort, // Keep for fallback even though socket-path takes precedence
		PgSocketDir: filepath.Join(pgctld.DataDir, "pg_sockets"),
		PgUser:      "postgres",
		PgDatabase:  "postgres",
		// Add standby as pg2 for symmetric configuration
		AdditionalHosts: []pgbackrest.PgHost{
			{
				DataPath:  filepath.Join(standbyPgctld.DataDir, "pg_data"),
				SocketDir: filepath.Join(standbyPgctld.DataDir, "pg_sockets"),
				Port:      standbyPgctld.PgPort,
				User:      "postgres",
				Database:  "postgres",
			},
		},
		LogPath:       logPath,
		SpoolPath:     spoolPath,
		RetentionFull: 2,
	}

	if err := pgbackrest.WriteConfigFile(configPath, backupCfg); err != nil {
		return fmt.Errorf("failed to write pgbackrest config: %w", err)
	}
	t.Logf("Created symmetric pgbackrest config at %s (pg1=self, pg2=standby, stanza: %s)", configPath, stanzaName)

	// Configure archive_mode in postgresql.auto.conf BEFORE starting PostgreSQL
	// The archive_command will fail initially until we create the stanza, but PostgreSQL
	// handles this gracefully by retrying. Once the stanza is created, archiving will work.
	pgDataDir := filepath.Join(pgctld.DataDir, "pg_data")
	autoConfPath := filepath.Join(pgDataDir, "postgresql.auto.conf")
	archiveConfig := fmt.Sprintf(`
# Archive mode for pgbackrest backups
archive_mode = on
archive_command = 'pgbackrest --stanza=%s --config=%s --repo1-path=%s archive-push %%p'
`, stanzaName, configPath, repoPath)
	f, err := os.OpenFile(autoConfPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open postgresql.auto.conf: %w", err)
	}
	if _, err := f.WriteString(archiveConfig); err != nil {
		f.Close()
		return fmt.Errorf("failed to write archive config: %w", err)
	}
	f.Close()
	t.Log("Configured archive_mode in postgresql.auto.conf")

	// Start PostgreSQL with archive mode enabled
	// Archive commands will fail until stanza is created, but that's okay
	if err := endtoend.StartPostgreSQL(t, primaryGrpcAddr); err != nil {
		return fmt.Errorf("failed to start PostgreSQL: %w", err)
	}
	t.Log("Started PostgreSQL with archive mode enabled")

	// Initialize pgbackrest stanza (PostgreSQL must be running)
	// Once stanza is created, archiving will start working
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := pgbackrest.StanzaCreate(ctx, stanzaName, configPath, repoPath); err != nil {
		return fmt.Errorf("failed to create pgbackrest stanza: %w", err)
	}
	t.Logf("Initialized pgbackrest stanza: %s", stanzaName)

	// Start primary multipooler
	if err := multipooler.Start(t); err != nil {
		return fmt.Errorf("failed to start primary multipooler: %w", err)
	}

	// Wait for manager to be ready
	waitForManagerReady(t, nil, multipooler)

	// Connect to multipooler manager
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", multipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to primary multipooler: %w", err)
	}
	defer conn.Close()

	client := multipoolermanagerpb.NewMultiPoolerManagerClient(conn)

	// Initialize consensus term to 1 via multipooler manager API
	t.Logf("Initializing consensus term to 1 for primary...")
	initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
		TermNumber:                    1,
		AcceptedTermFromCoordinatorId: nil,
		LastAcceptanceTime:            nil,
		LeaderId:                      nil,
	}

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	_, err = client.SetTerm(ctx, &multipoolermanagerdatapb.SetTermRequest{Term: initialTerm})
	cancel()
	if err != nil {
		return fmt.Errorf("failed to set term for primary: %w", err)
	}
	t.Logf("Primary consensus term set to 1")

	// Set pooler type to PRIMARY
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	changeTypeReq := &multipoolermanagerdatapb.ChangeTypeRequest{
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	_, err = client.ChangeType(ctx, changeTypeReq)
	if err != nil {
		return fmt.Errorf("failed to set primary pooler type: %w", err)
	}

	t.Logf("Primary initialized successfully")
	return nil
}

// initializeStandby sets up the standby pgctld, PostgreSQL (with replication), consensus term, and multipooler
func initializeStandby(t *testing.T, baseDir string, primaryPgctld *ProcessInstance, standbyPgctld *ProcessInstance, standbyMultipooler *ProcessInstance, stanzaName string) error {
	t.Helper()

	// Start standby pgctld server
	if err := standbyPgctld.Start(t); err != nil {
		return fmt.Errorf("failed to start standby pgctld: %w", err)
	}

	// Initialize standby data directory (but don't start yet)
	standbyGrpcAddr := fmt.Sprintf("localhost:%d", standbyPgctld.GrpcPort)
	if err := endtoend.InitPostgreSQLDataDir(t, standbyGrpcAddr); err != nil {
		return fmt.Errorf("failed to init standby data dir: %w", err)
	}

	// Configure standby as a replica using pgBackRest backup/restore
	t.Logf("Configuring standby as replica of primary...")
	setupStandbyReplication(t, primaryPgctld, standbyPgctld)

	// Start standby PostgreSQL (now configured as replica)
	if err := endtoend.StartPostgreSQL(t, standbyGrpcAddr); err != nil {
		return fmt.Errorf("failed to start standby PostgreSQL: %w", err)
	}

	// Create symmetric pgbackrest configuration for standby
	// Note: Standby shares the same backup repository and stanza as primary
	// since they're replicas. Both clusters use the same stanza name.
	//
	// Symmetric configuration:
	// - pg1: this cluster (standby in this case)
	// - pg2: other cluster (primary in this case)
	// Each cluster treats itself as pg1 and lists others as pg2, pg3, etc.
	// Build backup repository path with database/tablegroup/shard structure (same as primary)
	logPath := filepath.Join(baseDir, "logs", "pgbackrest")
	spoolPath := filepath.Join(baseDir, "pgbackrest-spool")

	configPath := filepath.Join(standbyPgctld.DataDir, "pgbackrest.conf")
	backupCfg := pgbackrest.Config{
		StanzaName:  stanzaName, // Use same stanza (they're replicas in HA setup)
		PgDataPath:  filepath.Join(standbyPgctld.DataDir, "pg_data"),
		PgPort:      standbyPgctld.PgPort, // Keep for fallback even though socket-path takes precedence
		PgSocketDir: filepath.Join(standbyPgctld.DataDir, "pg_sockets"),
		PgUser:      "postgres",
		PgDatabase:  "postgres",
		// Add primary as pg2 for symmetric configuration
		AdditionalHosts: []pgbackrest.PgHost{
			{
				DataPath:  filepath.Join(primaryPgctld.DataDir, "pg_data"),
				SocketDir: filepath.Join(primaryPgctld.DataDir, "pg_sockets"),
				Port:      primaryPgctld.PgPort,
				User:      "postgres",
				Database:  "postgres",
			},
		},
		LogPath:       logPath,
		SpoolPath:     spoolPath,
		RetentionFull: 2,
	}

	if err := pgbackrest.WriteConfigFile(configPath, backupCfg); err != nil {
		return fmt.Errorf("failed to write standby pgbackrest config: %w", err)
	}
	t.Logf("Created symmetric pgbackrest config at %s (pg1=self, pg2=primary, stanza: %s)", configPath, stanzaName)

	// Note: We don't create a new stanza for the standby because:
	// 1. It's in recovery mode, so pgbackrest won't allow stanza creation
	// 2. It shares the primary's stanza since they're replicas
	// 3. The stanza was already created when initializing the primary

	// Start standby multipooler
	if err := standbyMultipooler.Start(t); err != nil {
		return fmt.Errorf("failed to start standby multipooler: %w", err)
	}

	// Wait for manager to be ready
	waitForManagerReady(t, nil, standbyMultipooler)

	// Connect to standby multipooler manager
	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", standbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to standby multipooler: %w", err)
	}
	defer standbyConn.Close()

	standbyClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)

	// Initialize consensus term to 1 via multipooler manager API
	t.Logf("Initializing consensus term to 1 for standby...")
	initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
		TermNumber:                    1,
		AcceptedTermFromCoordinatorId: nil,
		LastAcceptanceTime:            nil,
		LeaderId:                      nil,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	_, err = standbyClient.SetTerm(ctx, &multipoolermanagerdatapb.SetTermRequest{Term: initialTerm})
	cancel()
	if err != nil {
		return fmt.Errorf("failed to set term for standby: %w", err)
	}
	t.Logf("Standby consensus term set to 1")

	// Verify standby is in recovery mode
	t.Logf("Verifying standby is in recovery mode...")
	standbyPoolerClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", standbyMultipooler.GrpcPort))
	if err != nil {
		return fmt.Errorf("failed to create standby pooler client: %w", err)
	}
	queryResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT pg_is_in_recovery()", 1)
	standbyPoolerClient.Close()
	if err != nil {
		return fmt.Errorf("failed to check standby recovery status: %w", err)
	}
	if len(queryResp.Rows) == 0 || len(queryResp.Rows[0].Values) == 0 || string(queryResp.Rows[0].Values[0]) != "true" {
		return fmt.Errorf("standby is not in recovery mode")
	}

	// Set pooler type to REPLICA
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	changeTypeReq := &multipoolermanagerdatapb.ChangeTypeRequest{
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}
	_, err = standbyClient.ChangeType(ctx, changeTypeReq)
	if err != nil {
		return fmt.Errorf("failed to set standby pooler type: %w", err)
	}

	t.Logf("Standby initialized successfully")
	return nil
}

// getSharedTestSetup creates or returns the shared test infrastructure
func getSharedTestSetup(t *testing.T) *MultipoolerTestSetup {
	t.Helper()
	setupOnce.Do(func() {
		// Set the PATH so our binaries can be found (like cluster_test.go does)
		// Use automatic module root detection instead of hard-coded relative paths
		if err := pathutil.PrependBinToPath(); err != nil {
			setupError = fmt.Errorf("failed to add bin to PATH: %w", err)
			return
		}

		// Check if PostgreSQL binaries are available
		if !utils.HasPostgreSQLBinaries() {
			setupError = fmt.Errorf("PostgreSQL binaries not found, make sure to install PostgreSQL and add it to the PATH")
			return
		}

		tempDir, tempDirCleanup := testutil.TempDir(t, "multipooler_shared_test")
		// Note: cleanup will be handled by TestMain to ensure it runs after all tests

		// Start etcd for topology
		t.Logf("Starting etcd for topology...")

		etcdDataDir := filepath.Join(tempDir, "etcd_data")
		if err := os.MkdirAll(etcdDataDir, 0o755); err != nil {
			setupError = fmt.Errorf("failed to create etcd data directory: %w", err)
			return
		}
		etcdClientAddr, etcdCmd, err := startEtcdForSharedSetup(t, etcdDataDir)
		if err != nil {
			setupError = fmt.Errorf("failed to start etcd: %w", err)
			return
		}
		// Note: cleanup will be handled by TestMain

		// Create topology server and cell
		testRoot := "/multigres"
		globalRoot := path.Join(testRoot, "global")
		cellName := "test-cell"
		cellRoot := path.Join(testRoot, cellName)

		ts, err := topoclient.OpenServer("etcd2", globalRoot, []string{etcdClientAddr}, topoclient.NewDefaultTopoConfig())
		if err != nil {
			setupError = fmt.Errorf("failed to open topology server: %w", err)
			return
		}
		// Note: cleanup will be handled by TestMain

		// Create the cell
		err = ts.CreateCell(context.Background(), cellName, &clustermetadatapb.Cell{
			ServerAddresses: []string{etcdClientAddr},
			Root:            cellRoot,
		})
		if err != nil {
			setupError = fmt.Errorf("failed to create cell: %w", err)
			return
		}

		t.Logf("Created topology cell '%s' at etcd %s", cellName, etcdClientAddr)

		// Create the database entry in topology with backup_location
		// This is needed for getBackupLocation() calls in multipooler manager
		database := "postgres"
		backupLocation := filepath.Join(tempDir, "backup-repo", database, "default", "0-inf")
		err = ts.CreateDatabase(context.Background(), database, &clustermetadatapb.Database{
			Name:             database,
			BackupLocation:   backupLocation,
			DurabilityPolicy: "ANY_2",
		})
		if err != nil {
			setupError = fmt.Errorf("failed to create database in topology: %w", err)
			return
		}
		t.Logf("Created database '%s' in topology with backup_location=%s", database, backupLocation)

		// Generate ports for shared instances using systematic allocation to avoid conflicts
		primaryGrpcPort := utils.GetFreePort(t)
		primaryPgPort := utils.GetFreePort(t)
		standbyGrpcPort := utils.GetFreePort(t)
		standbyPgPort := utils.GetFreePort(t)
		primaryMultipoolerPort := utils.GetFreePort(t)
		standbyMultipoolerPort := utils.GetFreePort(t)

		t.Logf("Shared test setup - Primary pgctld gRPC: %d, Primary PG: %d, Standby pgctld gRPC: %d, Standby PG: %d, Primary multipooler: %d, Standby multipooler: %d",
			primaryGrpcPort, primaryPgPort, standbyGrpcPort, standbyPgPort, primaryMultipoolerPort, standbyMultipoolerPort)

		// Create instances
		primaryPgctld := createPgctldInstance(t, "primary", tempDir, primaryGrpcPort, primaryPgPort)
		standbyPgctld := createPgctldInstance(t, "standby", tempDir, standbyGrpcPort, standbyPgPort)

		// Use a shared stanza name for the test setup
		// This allows both primary and standby to use the same pgBackRest stanza
		stanzaName := "test_backup"

		primaryMultipooler := createMultipoolerInstance(t, "primary-multipooler", tempDir, primaryMultipoolerPort,
			fmt.Sprintf("localhost:%d", primaryGrpcPort), primaryPgctld.DataDir, primaryPgctld.PgPort, etcdClientAddr, stanzaName)
		standbyMultipooler := createMultipoolerInstance(t, "standby-multipooler", tempDir, standbyMultipoolerPort,
			fmt.Sprintf("localhost:%d", standbyGrpcPort), standbyPgctld.DataDir, standbyPgctld.PgPort, etcdClientAddr, stanzaName)

		// Create standby data directories before initializing primary
		// This is needed for symmetric pgBackRest configuration where primary references standby paths
		if err := os.MkdirAll(filepath.Join(standbyPgctld.DataDir, "pg_data"), 0o755); err != nil {
			setupError = fmt.Errorf("failed to create standby pg_data dir: %w", err)
			return
		}
		if err := os.MkdirAll(filepath.Join(standbyPgctld.DataDir, "pg_sockets"), 0o755); err != nil {
			setupError = fmt.Errorf("failed to create standby pg_sockets dir: %w", err)
			return
		}

		// Initialize primary (pgctld, PostgreSQL, pgbackrest, consensus term, multipooler, type)
		if err := initializePrimary(t, tempDir, primaryPgctld, primaryMultipooler, standbyPgctld, stanzaName); err != nil {
			setupError = err
			return
		}

		// Initialize standby (pgctld, PostgreSQL with replication, consensus term, multipooler, type)
		if err := initializeStandby(t, tempDir, primaryPgctld, standbyPgctld, standbyMultipooler, stanzaName); err != nil {
			setupError = err
			return
		}

		sharedTestSetup = &MultipoolerTestSetup{
			TempDir:            tempDir,
			TempDirCleanup:     tempDirCleanup,
			EtcdClientAddr:     etcdClientAddr,
			EtcdCmd:            etcdCmd,
			TopoServer:         ts,
			PrimaryPgctld:      primaryPgctld,
			StandbyPgctld:      standbyPgctld,
			PrimaryMultipooler: primaryMultipooler,
			StandbyMultipooler: standbyMultipooler,
		}
		t.Logf("Shared test infrastructure started successfully")
	})

	if setupError != nil {
		t.Fatalf("Failed to setup shared test infrastructure: %v", setupError)
	}

	return sharedTestSetup
}

// startEtcdForSharedSetup starts etcd without registering t.Cleanup() handlers
// since cleanup is handled manually by TestMain via cleanupSharedTestSetup()
func startEtcdForSharedSetup(t *testing.T, dataDir string) (string, *exec.Cmd, error) {
	// Check if etcd is available in PATH
	_, err := exec.LookPath("etcd")
	if err != nil {
		return "", nil, fmt.Errorf("etcd not found in PATH: %w", err)
	}

	// Get ports for etcd (client and peer)
	clientPort := utils.GetFreePort(t)
	peerPort := utils.GetFreePort(t)

	name := "multigres_shared_test"
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

	// Wait for etcd to be ready
	time.Sleep(500 * time.Millisecond)

	return clientAddr, cmd, nil
}

// waitForManagerReady waits for the manager to be in ready state
func waitForManagerReady(t *testing.T, setup *MultipoolerTestSetup, manager *ProcessInstance) {
	t.Helper()

	// Connect to the manager
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", manager.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := multipoolermanagerpb.NewMultiPoolerManagerClient(conn)

	// Use require.Eventually to wait for manager to be ready
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		req := &multipoolermanagerdatapb.StateRequest{}
		resp, err := client.State(ctx, req)
		if err != nil {
			return false
		}
		if resp.State == "error" {
			t.Fatalf("Manager failed to initialize: %s", resp.ErrorMessage)
		}
		return resp.State == "ready"
	}, 30*time.Second, 100*time.Millisecond, "Manager should become ready within 30 seconds")

	t.Logf("Manager %s is ready", manager.Name)
}

// setupStandbyReplication configures the standby to replicate from the primary
// Assumes standby data dir is initialized but PostgreSQL is not started yet
func setupStandbyReplication(t *testing.T, primaryPgctld *ProcessInstance, standbyPgctld *ProcessInstance) {
	t.Helper()

	// Remove the standby pg_data directory to prepare for pgbackrest restore
	standbyPgDataDir := filepath.Join(standbyPgctld.DataDir, "pg_data")
	t.Logf("Removing standby pg_data directory: %s", standbyPgDataDir)
	err := os.RemoveAll(standbyPgDataDir)
	require.NoError(t, err)

	// Get primary's pgbackrest configuration
	primaryConfigPath := filepath.Join(primaryPgctld.DataDir, "pgbackrest.conf")

	// Determine the stanza name from the primary's config
	// The stanza is set during initializePrimary
	stanzaName := "test_backup" // This matches the value from initializePrimary

	// Get backup location (same structure as in initializePrimary)
	database := "postgres"
	baseDir := filepath.Dir(filepath.Dir(primaryPgctld.DataDir)) // Go up from primary/data to get base
	repoPath := filepath.Join(baseDir, "backup-repo", database, constants.DefaultTableGroup, constants.DefaultShard)

	// Create a backup on the primary using pgbackrest
	t.Logf("Creating pgBackRest backup on primary (stanza: %s, config: %s, repo: %s)...",
		stanzaName, primaryConfigPath, repoPath)

	backupCtx, backupCancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer backupCancel()

	backupCmd := exec.CommandContext(backupCtx, "pgbackrest",
		"--stanza="+stanzaName,
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
	t.Logf("pgBackRest backup completed successfully")

	// Create standby's pgbackrest configuration before restore
	// This needs to be created now (before PostgreSQL starts) so we can use it for restore
	logPath := filepath.Join(baseDir, "logs", "pgbackrest")
	spoolPath := filepath.Join(baseDir, "pgbackrest-spool")

	standbyConfigPath := filepath.Join(standbyPgctld.DataDir, "pgbackrest.conf")
	standbyBackupCfg := pgbackrest.Config{
		StanzaName:  stanzaName, // Use same stanza (they're replicas in HA setup)
		PgDataPath:  filepath.Join(standbyPgctld.DataDir, "pg_data"),
		PgPort:      standbyPgctld.PgPort,
		PgSocketDir: filepath.Join(standbyPgctld.DataDir, "pg_sockets"),
		PgUser:      "postgres",
		PgDatabase:  "postgres",
		// Add primary as pg2 for symmetric configuration
		AdditionalHosts: []pgbackrest.PgHost{
			{
				DataPath:  filepath.Join(primaryPgctld.DataDir, "pg_data"),
				SocketDir: filepath.Join(primaryPgctld.DataDir, "pg_sockets"),
				Port:      primaryPgctld.PgPort,
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
	t.Logf("Restoring pgBackRest backup to standby (config: %s)...", standbyConfigPath)

	restoreCtx, restoreCancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer restoreCancel()

	restoreCmd := exec.CommandContext(restoreCtx, "pgbackrest",
		"--stanza="+stanzaName,
		"--config="+standbyConfigPath,
		"--repo1-path="+repoPath,
		"--log-level-console=info",
		"restore")

	restoreOutput, err := restoreCmd.CombinedOutput()
	if err != nil {
		t.Logf("pgbackrest restore output: %s", string(restoreOutput))
	}
	require.NoError(t, err, "pgbackrest restore should succeed")
	t.Logf("pgBackRest restore completed successfully")

	// Create standby.signal to put the server in recovery mode
	standbySignalPath := filepath.Join(standbyPgDataDir, "standby.signal")
	t.Logf("Creating standby.signal file: %s", standbySignalPath)
	err = os.WriteFile(standbySignalPath, []byte(""), 0o644)
	require.NoError(t, err, "Should be able to create standby.signal")

	t.Logf("Standby data restored from backup and configured as replica (PostgreSQL will be started next)")
}

// makeMultipoolerID creates a multipooler ID for testing
func makeMultipoolerID(cell, name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	}
}

// Helper function to get PrimaryStatus from a manager client
func getPrimaryStatusFromClient(t *testing.T, client multipoolermanagerpb.MultiPoolerManagerClient) *multipoolermanagerdatapb.PrimaryStatus {
	t.Helper()
	statusResp, err := client.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.PrimaryStatusRequest{})
	require.NoError(t, err, "PrimaryStatus should succeed")
	require.NotNil(t, statusResp.Status, "Status should not be nil")
	return statusResp.Status
}

// Helper function to wait for synchronous replication config to converge to expected value
func waitForSyncConfigConvergenceWithClient(t *testing.T, client multipoolermanagerpb.MultiPoolerManagerClient, checkFunc func(*multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool, message string) {
	t.Helper()
	require.Eventually(t, func() bool {
		status := getPrimaryStatusFromClient(t, client)
		return checkFunc(status.SyncReplicationConfig)
	}, 5*time.Second, 200*time.Millisecond, message)
}

// Helper function to check if a standby ID is in the config
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

// cleanupOption is a function that configures cleanup behavior
type cleanupOption func(*cleanupConfig)

// cleanupConfig holds the configuration for test cleanup
type cleanupConfig struct {
	tablesToDrop     []string
	gucsToReset      []string
	noReplication    bool // Explicitly disable replication setup
	pauseReplication bool // Start replication but pause WAL replay
}

// WithResetGuc returns a cleanup option that saves and restores a GUC setting on both primary and standby
func WithResetGuc(gucNames ...string) cleanupOption {
	return func(c *cleanupConfig) {
		c.gucsToReset = append(c.gucsToReset, gucNames...)
	}
}

// WithoutReplication returns a cleanup option that explicitly disables replication setup.
// Use this for tests that need to set up replication from scratch within the test body.
// This saves and restores synchronous replication settings on primary and primary_conninfo on standby.
func WithoutReplication() cleanupOption {
	return func(c *cleanupConfig) {
		c.noReplication = true
		c.gucsToReset = append(c.gucsToReset, "synchronous_standby_names", "synchronous_commit", "primary_conninfo")
	}
}

// WithPausedReplication returns a cleanup option that starts replication but pauses WAL replay.
// Use this for tests that need to test resuming replication or need replication configured but not actively applying WAL.
func WithPausedReplication() cleanupOption {
	return func(c *cleanupConfig) {
		c.pauseReplication = true
		c.gucsToReset = append(c.gucsToReset, "synchronous_standby_names", "synchronous_commit", "primary_conninfo")
	}
}

// WithDropTables returns a cleanup option that registers tables to drop on cleanup
func WithDropTables(tables ...string) cleanupOption {
	return func(c *cleanupConfig) {
		c.tablesToDrop = append(c.tablesToDrop, tables...)
	}
}

// Helper functions for setupPoolerTest

// queryStringValue executes a query and extracts the first column of the first row as a string.
// Returns empty string and error if query fails or returns no rows.
func queryStringValue(ctx context.Context, client *endtoend.MultiPoolerTestClient, query string) (string, error) {
	resp, err := client.ExecuteQuery(ctx, query, 1)
	if err != nil {
		return "", err
	}
	if len(resp.Rows) == 0 || len(resp.Rows[0].Values) == 0 {
		return "", nil
	}
	return string(resp.Rows[0].Values[0]), nil
}

// validateGUCValue queries a GUC and returns an error if it doesn't match the expected value.
func validateGUCValue(client *endtoend.MultiPoolerTestClient, gucName, expected, instanceName string) error {
	value, err := queryStringValue(context.Background(), client, fmt.Sprintf("SHOW %s", gucName))
	if err != nil {
		return fmt.Errorf("%s failed to query %s: %w", instanceName, gucName, err)
	}
	if value != expected {
		return fmt.Errorf("%s has %s='%s' (expected '%s')", instanceName, gucName, value, expected)
	}
	return nil
}

// saveGUCs queries multiple GUC values and saves them to a map.
// Returns a map of gucName -> value. Empty values are preserved.
func saveGUCs(client *endtoend.MultiPoolerTestClient, gucNames []string) map[string]string {
	saved := make(map[string]string)
	for _, gucName := range gucNames {
		value, err := queryStringValue(context.Background(), client, fmt.Sprintf("SHOW %s", gucName))
		if err == nil {
			saved[gucName] = value
		}
	}
	return saved
}

// restoreGUCs restores GUC values from a saved map using ALTER SYSTEM.
// Empty values are treated as RESET (restore to default).
func restoreGUCs(t *testing.T, client *endtoend.MultiPoolerTestClient, savedGucs map[string]string, instanceName string) {
	t.Helper()
	for gucName, gucValue := range savedGucs {
		var query string
		if gucValue == "" {
			query = fmt.Sprintf("ALTER SYSTEM RESET %s", gucName)
		} else {
			query = fmt.Sprintf("ALTER SYSTEM SET %s = '%s'", gucName, gucValue)
		}
		_, err := client.ExecuteQuery(context.Background(), query, 1)
		if err != nil {
			t.Logf("Warning: Failed to restore %s on %s in cleanup: %v", gucName, instanceName, err)
		}
	}

	// Reload configuration to apply changes
	_, err := client.ExecuteQuery(context.Background(), "SELECT pg_reload_conf()", 1)
	if err != nil {
		t.Logf("Warning: Failed to reload config on %s in cleanup: %v", instanceName, err)
	}
}

// validateCleanState checks that primary and standby are in the expected clean state.
// Expected state:
//   - Primary: primary_conninfo=", synchronous_standby_names="
//   - Standby: pg_is_in_recovery=true, primary_conninfo=", pg_is_wal_replay_paused=false
//
// Returns an error if state is not clean.
func validateCleanState(setup *MultipoolerTestSetup) error {
	if setup == nil {
		return nil
	}

	// Validate primary state
	if setup.PrimaryMultipooler != nil {
		primaryClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		if err != nil {
			return fmt.Errorf("failed to connect to primary: %w", err)
		}
		defer primaryClient.Close()

		if err := validateGUCValue(primaryClient, "primary_conninfo", "", "Primary"); err != nil {
			return err
		}
		if err := validateGUCValue(primaryClient, "synchronous_standby_names", "", "Primary"); err != nil {
			return err
		}
	}

	// Validate standby state
	if setup.StandbyMultipooler != nil {
		standbyClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort))
		if err != nil {
			return fmt.Errorf("failed to connect to standby: %w", err)
		}
		defer standbyClient.Close()

		// Verify standby is in recovery mode
		inRecovery, err := queryStringValue(context.Background(), standbyClient, "SELECT pg_is_in_recovery()")
		if err != nil {
			return fmt.Errorf("Standby failed to query pg_is_in_recovery: %w", err)
		}
		if inRecovery != "true" {
			return fmt.Errorf("Standby pg_is_in_recovery=%s (expected true)", inRecovery)
		}

		// Verify replication not configured
		if err := validateGUCValue(standbyClient, "primary_conninfo", "", "Standby"); err != nil {
			return err
		}

		// Verify WAL replay not paused
		isPaused, err := queryStringValue(context.Background(), standbyClient, "SELECT pg_is_wal_replay_paused()")
		if err != nil {
			return fmt.Errorf("Standby failed to query pg_is_wal_replay_paused: %w", err)
		}
		if isPaused != "false" {
			return fmt.Errorf("Standby pg_is_wal_replay_paused=%s (expected false)", isPaused)
		}
	}

	return nil
}

// setupPoolerTest provides test isolation by validating clean state, optionally configuring
// replication, and automatically restoring any state changes at test cleanup.
//
// DEFAULT BEHAVIOR (no options):
//   - Configures replication: Sets primary_conninfo, standby connects to primary
//   - Starts WAL streaming: WAL receiver active, WAL replay applying changes
//   - Disables synchronous replication (safe - writes won't hang)
//
// WithoutReplication():
//   - Does NOT configure replication: primary_conninfo stays empty
//   - Standby remains disconnected from primary
//   - Use for tests that set up replication from scratch
//
// WithPausedReplication():
//   - Configures replication: Sets primary_conninfo, standby connects to primary
//   - Starts WAL streaming: WAL receiver active
//   - Pauses WAL replay: Changes stop being applied (for testing resume)
//   - Use for tests that need to test pg_wal_replay_resume()
//
// Other options: WithResetGuc(), WithDropTables()
func setupPoolerTest(t *testing.T, setup *MultipoolerTestSetup, opts ...cleanupOption) {
	t.Helper()

	// Build cleanup configuration from options
	config := &cleanupConfig{}
	for _, opt := range opts {
		opt(config)
	}

	// Validate that settings are in the expected clean state.
	// This catches state leaks from tests that don't call setupPoolerTest().
	if err := validateCleanState(setup); err != nil {
		t.Fatalf("setupPoolerTest: %v. Previous test leaked state. Make sure all subtests call setupPoolerTest().", err)
	}

	// Determine if we should configure replication (default: yes, unless WithoutReplication)
	shouldConfigureReplication := !config.noReplication

	// If configuring replication and no GUCs specified yet, add the default replication GUCs
	if shouldConfigureReplication && len(config.gucsToReset) == 0 {
		config.gucsToReset = append(config.gucsToReset, "synchronous_standby_names", "synchronous_commit", "primary_conninfo")
	}

	// Create pooler clients once and reuse throughout setup and cleanup
	var primaryPoolerClient, standbyPoolerClient *endtoend.MultiPoolerTestClient
	if setup != nil && setup.PrimaryMultipooler != nil {
		var err error
		primaryPoolerClient, err = endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		require.NoError(t, err, "Failed to connect to primary pooler")

		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "SELECT 1", 0)
		require.NoError(t, err, "Failed to query primary pooler")
	}
	if setup != nil && setup.StandbyMultipooler != nil {
		var err error
		standbyPoolerClient, err = endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort))
		require.NoError(t, err, "Failed to connect to standby pooler")

		_, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT 1", 0)
		require.NoError(t, err, "Failed to query primary pooler")
	}

	// Save GUC values BEFORE configuring replication (so we save the clean state)
	// This way cleanup will restore to clean state
	var savedPrimaryGucs, savedStandbyGucs map[string]string

	if len(config.gucsToReset) > 0 {
		if primaryPoolerClient != nil {
			savedPrimaryGucs = saveGUCs(primaryPoolerClient, config.gucsToReset)
		}
		if standbyPoolerClient != nil {
			savedStandbyGucs = saveGUCs(standbyPoolerClient, config.gucsToReset)
		}
	}

	// Configure replication if needed (after saving GUCs)
	if shouldConfigureReplication {
		if setup == nil || setup.StandbyMultipooler == nil || setup.PrimaryPgctld == nil {
			t.Log("Test setup: Cannot configure replication (setup or multipoolers are nil)")
		} else {
			// SAFETY: Always disable synchronous replication to prevent write hangs
			if primaryPoolerClient != nil {
				_, err := primaryPoolerClient.ExecuteQuery(context.Background(), "ALTER SYSTEM SET synchronous_standby_names = ''", 0)
				if err == nil {
					_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_reload_conf()", 0)
					if err == nil {
						t.Log("Test setup: Disabled synchronous replication for safety (prevents write hangs)")
					}
				}
				if err != nil {
					t.Logf("Warning: Failed to disable synchronous replication: %v", err)
				}
			}

			// Check if replication is already configured and streaming
			alreadyStreaming := false
			if standbyPoolerClient != nil {
				resp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT status FROM pg_stat_wal_receiver", 1)
				if err == nil && len(resp.Rows) > 0 && len(resp.Rows[0].Values) > 0 {
					status := string(resp.Rows[0].Values[0])
					if status == "streaming" {
						alreadyStreaming = true
						t.Log("Test setup: Replication already streaming")
					}
				}
			}

			// Configure replication if not already streaming
			if !alreadyStreaming {
				t.Log("Test setup: Configuring replication (setting primary_conninfo, starting WAL streaming)...")

				standbyConn, err := grpc.NewClient(
					fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				)
				require.NoError(t, err, "Failed to connect to standby multipooler")
				defer standbyConn.Close()

				standbyClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)

				// Set consensus term
				_, err = standbyClient.SetTerm(utils.WithShortDeadline(t), &multipoolermanagerdatapb.SetTermRequest{
					Term: &multipoolermanagerdatapb.ConsensusTerm{
						TermNumber: 1,
					},
				})
				if err != nil {
					t.Logf("Warning: Failed to set term on standby: %v", err)
				}

				// Configure replication with Force=true to ensure it works
				setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
					Host:                  "localhost",
					Port:                  int32(setup.PrimaryPgctld.PgPort),
					StartReplicationAfter: true,
					StopReplicationBefore: false,
					CurrentTerm:           1,
					Force:                 true, // Force reconfiguration to ensure it works
				}
				ctxSetPrimary, cancelSetPrimary := context.WithTimeout(context.Background(), 5*time.Second)
				_, err = standbyClient.SetPrimaryConnInfo(ctxSetPrimary, setPrimaryReq)
				cancelSetPrimary()

				if err != nil {
					t.Fatalf("Failed to configure replication: %v", err)
				}
			}

			// Wait for replication to be streaming (whether we just configured it or it was already streaming)
			if standbyPoolerClient != nil {
				require.Eventually(t, func() bool {
					resp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT status FROM pg_stat_wal_receiver", 1)
					if err != nil || len(resp.Rows) == 0 {
						return false
					}
					return string(resp.Rows[0].Values[0]) == "streaming"
				}, 10*time.Second, 100*time.Millisecond, "Replication should be streaming after setup")

				if config.pauseReplication {
					// Pause WAL replay (WAL receiver keeps streaming, but changes aren't applied)
					_, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_wal_replay_pause()", 1)
					if err != nil {
						t.Logf("Warning: Failed to pause WAL replay: %v", err)
					} else {
						t.Log("Test setup: Replication configured and streaming, WAL replay PAUSED")
					}
				} else {
					t.Log("Test setup: Replication configured and streaming, WAL replay ACTIVE")
				}
			}
		}
	}

	// Register cleanup handler
	t.Cleanup(func() {
		// Close pooler clients at the end
		defer func() {
			if primaryPoolerClient != nil {
				primaryPoolerClient.Close()
			}
			if standbyPoolerClient != nil {
				standbyPoolerClient.Close()
			}
		}()

		// Early return if setup is nil or multipoolers are nil
		if setup == nil || setup.PrimaryMultipooler == nil {
			return
		}

		// Step 1: Restore GUC values if specified (must be done first to fix replication)
		if len(savedPrimaryGucs) > 0 && primaryPoolerClient != nil {
			restoreGUCs(t, primaryPoolerClient, savedPrimaryGucs, "primary")
		}

		if len(savedStandbyGucs) > 0 && standbyPoolerClient != nil {
			restoreGUCs(t, standbyPoolerClient, savedStandbyGucs, "standby")

			// Wait for primary_conninfo to actually be cleared (if it was in the saved GUCs)
			// This prevents race conditions where the next test starts before config reload completes
			if _, hasPrimaryConnInfo := savedStandbyGucs["primary_conninfo"]; hasPrimaryConnInfo {
				require.Eventually(t, func() bool {
					value, err := queryStringValue(context.Background(), standbyPoolerClient, "SHOW primary_conninfo")
					if err != nil {
						t.Logf("Cleanup: Error checking primary_conninfo: %v", err)
						return false
					}
					return value == savedStandbyGucs["primary_conninfo"]
				}, 5*time.Second, 100*time.Millisecond, "Cleanup: Failed to restore primary_conninfo to original value")
			}
		}

		// Step 2: Always resume WAL replay (must be after GUC restoration)
		// This ensures we leave the system in a good state even if tests paused replay
		// We always do this regardless of WithoutReplication flag because pause state persists
		// NOTE: We don't wait for streaming because we just restored GUCs to clean state
		// (primary_conninfo=''), so there's no replication source configured.
		if standbyPoolerClient != nil {
			_, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_wal_replay_resume()", 1)
			if err != nil {
				t.Logf("Cleanup: Failed to resume WAL replay: %v", err)
			}
		}

		// Step 3: Drop tables if specified (must be last, requires working replication)
		if len(config.tablesToDrop) > 0 && primaryPoolerClient != nil {
			for _, table := range config.tablesToDrop {
				_, err := primaryPoolerClient.ExecuteQuery(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table), 1)
				if err != nil {
					t.Logf("Warning: Failed to drop table %s in cleanup: %v", table, err)
				}
			}
		}

		// Step 4: Validate that cleanup fully applied and state is clean
		// Use Eventually to give the system time to reach clean state
		require.Eventually(t, func() bool {
			return validateCleanState(setup) == nil
		}, 2*time.Second, 50*time.Millisecond, "Test cleanup failed: state did not return to clean state after cleanup")
	})
}
