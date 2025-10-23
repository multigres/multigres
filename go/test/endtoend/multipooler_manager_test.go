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

package endtoend

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/etcdtopo"
	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/pathutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

var (
	// Shared test infrastructure
	sharedTestSetup *MultipoolerTestSetup
	setupOnce       sync.Once
	setupError      error
)

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
	if sharedTestSetup.TempDir != "" {
		_ = os.RemoveAll(sharedTestSetup.TempDir)
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
	Process     *exec.Cmd
	Binary      string
	Environment []string
}

// MultipoolerTestSetup holds shared test infrastructure
type MultipoolerTestSetup struct {
	TempDir            string
	EtcdClientAddr     string
	EtcdCmd            *exec.Cmd
	TopoServer         topo.Store
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
	p.Process.Env = p.Environment

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

	// Start the multipooler server
	p.Process = exec.Command(p.Binary,
		"--grpc-port", strconv.Itoa(p.GrpcPort),
		"--database", "postgres", // Required parameter
		"--table-group", "test", // Required parameter
		"--pgctld-addr", p.PgctldAddr,
		"--pooler-dir", p.DataDir, // Use the same pooler dir as pgctld
		"--pg-port", strconv.Itoa(p.PgPort),
		"--service-map", "grpc-pooler,grpc-poolermanager",
		"--topo-global-server-addresses", p.EtcdAddr,
		"--topo-global-root", "/multigres/global",
		"--topo-implementation", "etcd2",
		"--cell", "test-cell",
		"--service-id", p.ServiceID,
		"--log-output", p.LogFile)
	p.Process.Env = p.Environment

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

	client := pgctldpb.NewPgCtldClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Stop PostgreSQL
	_, _ = client.Stop(ctx, &pgctldpb.StopRequest{Mode: "fast"})
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
		Environment: append(os.Environ(), "PGCONNECT_TIMEOUT=5"),
	}
}

// createMultipoolerInstance creates a new multipooler instance configuration
func createMultipoolerInstance(t *testing.T, name, baseDir string, grpcPort int, pgctldAddr string, pgctldDataDir string, pgPort int, etcdAddr string) *ProcessInstance {
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
		Binary:      "multipooler", // Assume binary is in PATH
		Environment: append(os.Environ(), "PGCONNECT_TIMEOUT=5"),
	}
}

// initializePrimary sets up the primary pgctld, PostgreSQL, consensus term, and multipooler
func initializePrimary(t *testing.T, pgctld *ProcessInstance, multipooler *ProcessInstance) error {
	t.Helper()

	// Start primary pgctld server
	if err := pgctld.Start(t); err != nil {
		return fmt.Errorf("failed to start primary pgctld: %w", err)
	}

	// Initialize and start primary PostgreSQL
	primaryGrpcAddr := fmt.Sprintf("localhost:%d", pgctld.GrpcPort)
	if err := InitAndStartPostgreSQL(t, primaryGrpcAddr); err != nil {
		return fmt.Errorf("failed to init and start primary PostgreSQL: %w", err)
	}

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
	initialTerm := &pgctldpb.ConsensusTerm{
		CurrentTerm:  1,
		VotedFor:     nil,
		LastVoteTime: nil,
		LeaderId:     nil,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	_, err = client.SetTerm(ctx, &multipoolermanagerdata.SetTermRequest{Term: initialTerm})
	cancel()
	if err != nil {
		return fmt.Errorf("failed to set term for primary: %w", err)
	}
	t.Logf("Primary consensus term set to 1")

	// Set pooler type to PRIMARY
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	changeTypeReq := &multipoolermanagerdata.ChangeTypeRequest{
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
func initializeStandby(t *testing.T, primaryPgctld *ProcessInstance, standbyPgctld *ProcessInstance, standbyMultipooler *ProcessInstance) error {
	t.Helper()

	// Start standby pgctld server
	if err := standbyPgctld.Start(t); err != nil {
		return fmt.Errorf("failed to start standby pgctld: %w", err)
	}

	// Initialize standby data directory (but don't start yet)
	standbyGrpcAddr := fmt.Sprintf("localhost:%d", standbyPgctld.GrpcPort)
	if err := InitPostgreSQLDataDir(t, standbyGrpcAddr); err != nil {
		return fmt.Errorf("failed to init standby data dir: %w", err)
	}

	// Configure standby as a replica using pg_basebackup
	t.Logf("Configuring standby as replica of primary...")
	setupStandbyReplication(t, primaryPgctld, standbyPgctld)

	// Start standby PostgreSQL (now configured as replica)
	if err := StartPostgreSQL(t, standbyGrpcAddr); err != nil {
		return fmt.Errorf("failed to start standby PostgreSQL: %w", err)
	}

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
	initialTerm := &pgctldpb.ConsensusTerm{
		CurrentTerm:  1,
		VotedFor:     nil,
		LastVoteTime: nil,
		LeaderId:     nil,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	_, err = standbyClient.SetTerm(ctx, &multipoolermanagerdata.SetTermRequest{Term: initialTerm})
	cancel()
	if err != nil {
		return fmt.Errorf("failed to set term for standby: %w", err)
	}
	t.Logf("Standby consensus term set to 1")

	// Verify standby is in recovery mode
	t.Logf("Verifying standby is in recovery mode...")
	standbyPoolerClient, err := NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", standbyMultipooler.GrpcPort))
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

	changeTypeReq := &multipoolermanagerdata.ChangeTypeRequest{
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
		// Use PrependPath to ensure our project binaries take precedence over system ones
		pathutil.PrependPath("../../../bin")

		// Check if PostgreSQL binaries are available
		if !hasPostgreSQLBinaries() {
			setupError = fmt.Errorf("PostgreSQL binaries not found, make sure to install PostgreSQL and add it to the PATH")
			return
		}

		tempDir, _ := testutil.TempDir(t, "multipooler_shared_test")
		// Note: cleanup will be handled by TestMain to ensure it runs after all tests

		// Start etcd for topology
		t.Logf("Starting etcd for topology...")
		etcdClientAddr, etcdCmd := etcdtopo.StartEtcd(t, 0)
		// Note: cleanup will be handled by TestMain

		// Create topology server and cell
		testRoot := "/multigres"
		globalRoot := path.Join(testRoot, "global")
		cellName := "test-cell"
		cellRoot := path.Join(testRoot, cellName)

		ts, err := topo.OpenServer("etcd2", globalRoot, []string{etcdClientAddr})
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

		// Generate ports for shared instances
		primaryGrpcPort := testutil.GenerateRandomPort()
		primaryPgPort := testutil.GenerateRandomPort()
		standbyGrpcPort := testutil.GenerateRandomPort()
		standbyPgPort := testutil.GenerateRandomPort()
		primaryMultipoolerPort := testutil.GenerateRandomPort()
		standbyMultipoolerPort := testutil.GenerateRandomPort()

		t.Logf("Shared test setup - Primary pgctld gRPC: %d, Primary PG: %d, Standby pgctld gRPC: %d, Standby PG: %d, Primary multipooler: %d, Standby multipooler: %d",
			primaryGrpcPort, primaryPgPort, standbyGrpcPort, standbyPgPort, primaryMultipoolerPort, standbyMultipoolerPort)

		// Create instances
		primaryPgctld := createPgctldInstance(t, "primary", tempDir, primaryGrpcPort, primaryPgPort)
		standbyPgctld := createPgctldInstance(t, "standby", tempDir, standbyGrpcPort, standbyPgPort)

		primaryMultipooler := createMultipoolerInstance(t, "primary-multipooler", tempDir, primaryMultipoolerPort,
			fmt.Sprintf("localhost:%d", primaryGrpcPort), primaryPgctld.DataDir, primaryPgctld.PgPort, etcdClientAddr)
		standbyMultipooler := createMultipoolerInstance(t, "standby-multipooler", tempDir, standbyMultipoolerPort,
			fmt.Sprintf("localhost:%d", standbyGrpcPort), standbyPgctld.DataDir, standbyPgctld.PgPort, etcdClientAddr)

		// Initialize primary (pgctld, PostgreSQL, consensus term, multipooler, type)
		if err := initializePrimary(t, primaryPgctld, primaryMultipooler); err != nil {
			setupError = err
			return
		}

		// Initialize standby (pgctld, PostgreSQL with replication, consensus term, multipooler, type)
		if err := initializeStandby(t, primaryPgctld, standbyPgctld, standbyMultipooler); err != nil {
			setupError = err
			return
		}

		sharedTestSetup = &MultipoolerTestSetup{
			TempDir:            tempDir,
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

		req := &multipoolermanagerdata.StatusRequest{}
		resp, err := client.Status(ctx, req)
		if err != nil {
			return false
		}
		if resp.State == "error" {
			t.Fatalf("Manager failed to initialize: %s", resp.ErrorMessage)
		}
		return resp.State == "ready"
	}, 5*time.Second, 100*time.Millisecond, "Manager should become ready within 30 seconds")

	t.Logf("Manager %s is ready", manager.Name)
}

// setupStandbyReplication configures the standby to replicate from the primary
// Assumes standby data dir is initialized but PostgreSQL is not started yet
func setupStandbyReplication(t *testing.T, primaryPgctld *ProcessInstance, standbyPgctld *ProcessInstance) {
	t.Helper()

	// Backup standby's original configuration before pg_basebackup overwrites it
	standbyPgDataDir := filepath.Join(standbyPgctld.DataDir, "pg_data")
	configBackupDir := filepath.Join(standbyPgctld.DataDir, "config_backup")

	t.Logf("Backing up standby configuration to: %s", configBackupDir)
	err := os.MkdirAll(configBackupDir, 0o755)
	require.NoError(t, err)

	// Remove the standby pg_data directory to prepare for pg_basebackup
	t.Logf("Removing standby pg_data directory: %s", standbyPgDataDir)
	err = os.RemoveAll(standbyPgDataDir)
	require.NoError(t, err)

	// Create base backup from primary using pg_basebackup
	// Note: pg_basebackup needs to write to the pg_data subdirectory, not the pooler-dir
	// We do NOT use -R flag because we want to test SetPrimaryConnInfo RPC method later
	t.Logf("Creating base backup from primary (port %d) to standby pg_data dir...", primaryPgctld.PgPort)
	basebackupCmd := exec.Command("pg_basebackup",
		"-h", "localhost",
		"-p", strconv.Itoa(primaryPgctld.PgPort),
		"-U", "postgres",
		"-D", standbyPgDataDir,
		"-X", "stream",
		"-c", "fast")

	basebackupCmd.Env = append(os.Environ(), "PGPASSWORD=postgres")
	output, err := basebackupCmd.CombinedOutput()
	if err != nil {
		t.Logf("pg_basebackup output: %s", string(output))
	}
	require.NoError(t, err, "pg_basebackup should succeed")

	t.Logf("Base backup completed successfully")

	// Create standby.signal to put the server in recovery mode
	standbySignalPath := filepath.Join(standbyPgDataDir, "standby.signal")
	t.Logf("Creating standby.signal file: %s", standbySignalPath)
	err = os.WriteFile(standbySignalPath, []byte(""), 0o644)
	require.NoError(t, err, "Should be able to create standby.signal")

	t.Logf("Standby data copied and configured as replica (PostgreSQL will be started next)")
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
func getPrimaryStatusFromClient(t *testing.T, client multipoolermanagerpb.MultiPoolerManagerClient) *multipoolermanagerdata.PrimaryStatus {
	t.Helper()
	statusResp, err := client.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
	require.NoError(t, err, "PrimaryStatus should succeed")
	require.NotNil(t, statusResp.Status, "Status should not be nil")
	return statusResp.Status
}

// Helper function to wait for synchronous replication config to converge to expected value
func waitForSyncConfigConvergenceWithClient(t *testing.T, client multipoolermanagerpb.MultiPoolerManagerClient, checkFunc func(*multipoolermanagerdata.SynchronousReplicationConfiguration) bool, message string) {
	t.Helper()
	require.Eventually(t, func() bool {
		status := getPrimaryStatusFromClient(t, client)
		return checkFunc(status.SyncReplicationConfig)
	}, 5*time.Second, 200*time.Millisecond, message)
}

// Helper function to check if a standby ID is in the config
func containsStandbyIDInConfig(config *multipoolermanagerdata.SynchronousReplicationConfiguration, cell, name string) bool {
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

// TestMultipoolerPrimaryPosition tests the replication API functionality
func TestMultipoolerPrimaryPosition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	t.Run("PrimaryPosition_Primary", func(t *testing.T) {
		// Connect to primary multipooler
		conn, err := grpc.NewClient(
			fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		defer conn.Close()

		client := multipoolermanagerpb.NewMultiPoolerManagerClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		req := &multipoolermanagerdata.PrimaryPositionRequest{}
		resp, err := client.PrimaryPosition(ctx, req)
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Message() == "unknown service multipoolermanager.MultiPoolerManager" {
				t.Logf("Got 'unknown service' error - checking multipooler logs:")
				setup.PrimaryMultipooler.logRecentOutput(t, "Debug - unknown service error")
			}
		}

		// Assert that it succeeds and returns a valid LSN
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.NotEmpty(t, resp.LsnPosition, "LSN should not be empty")

		// PostgreSQL LSN format is typically like "0/1234ABCD"
		assert.Contains(t, resp.LsnPosition, "/", "LSN should be in PostgreSQL format (e.g., 0/1234ABCD)")
	})

	t.Run("PrimaryPosition_Standby", func(t *testing.T) {
		// Connect to standby multipooler
		conn, err := grpc.NewClient(
			fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		defer conn.Close()

		client := multipoolermanagerpb.NewMultiPoolerManagerClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		req := &multipoolermanagerdata.PrimaryPositionRequest{}
		_, err = client.PrimaryPosition(ctx, req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "operation not allowed")
	})
}

// TestReplicationAPIs tests the replication-related API functionality (SetPrimaryConnInfo, WaitForLSN, etc.)
func TestReplicationAPIs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Create shared clients for all subtests
	primaryPoolerClient, err := NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { primaryPoolerClient.Close() })

	standbyPoolerClient, err := NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { standbyPoolerClient.Close() })

	primaryConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryConn.Close() })
	primaryManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(primaryConn)

	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)

	t.Run("ConfigureReplicationAndValidate", func(t *testing.T) {
		t.Log("Creating table and inserting data in primary...")
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "CREATE TABLE IF NOT EXISTS test_replication (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err, "Should be able to create table in primary")

		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "INSERT INTO test_replication (data) VALUES ('test data')", 0)
		require.NoError(t, err, "Should be able to insert data in primary")

		// Get LSN from primary using PrimaryPosition RPC
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		primaryPosResp, err := primaryManagerClient.PrimaryPosition(ctx, &multipoolermanagerdata.PrimaryPositionRequest{})
		require.NoError(t, err)
		primaryLSN := primaryPosResp.LsnPosition
		t.Logf("Primary LSN after insert: %s", primaryLSN)

		// Validate data is NOT in standby yet (no replication configured)
		t.Log("Validating data is NOT in standby (replication not configured)...")

		// Use WaitForLSN to verify standby cannot reach primary's LSN without replication
		// This should timeout since replication is not configured
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		waitReq := &multipoolermanagerdata.WaitForLSNRequest{
			TargetLsn: primaryLSN,
		}
		_, err = standbyManagerClient.WaitForLSN(ctx, waitReq)
		require.Error(t, err, "WaitForLSN should timeout without replication configured")
		// Check that the error is a timeout (gRPC code DEADLINE_EXCEEDED or CANCELED)
		st, ok := status.FromError(err)
		require.True(t, ok, "Error should be a gRPC status error")
		assert.Contains(t, []string{"DeadlineExceeded", "Canceled"}, st.Code().String(),
			"Error should be a timeout error code, got: %s", st.Code().String())
		t.Log("Confirmed: standby cannot reach primary LSN without replication configured")

		// Verify table doesn't exist in standby
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_replication'", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		tableCount := string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "0", tableCount, "Table should not exist in standby yet")

		// Configure replication using SetPrimaryConnInfo RPC
		t.Log("Configuring replication via SetPrimaryConnInfo RPC...")

		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Call SetPrimaryConnInfo with StartReplicationAfter=true
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")
		t.Log("Replication configured successfully")

		// Wait for standby to catch up to primary's LSN using WaitForLSN API
		t.Logf("Waiting for standby to catch up to primary LSN: %s", primaryLSN)

		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		waitReq = &multipoolermanagerdata.WaitForLSNRequest{
			TargetLsn: primaryLSN,
		}
		_, err = standbyManagerClient.WaitForLSN(ctx, waitReq)
		require.NoError(t, err, "Standby should catch up to primary LSN after replication is configured")

		// Verify the table now exists in standby
		require.Eventually(t, func() bool {
			resp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_replication'", 1)
			if err != nil || len(resp.Rows) == 0 {
				return false
			}
			tableCount := string(resp.Rows[0].Values[0])
			return tableCount == "1"
		}, 15*time.Second, 500*time.Millisecond, "Table should exist in standby after replication")

		// Verify the data replicated
		dataResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM test_replication", 1)
		require.NoError(t, err)
		require.Len(t, dataResp.Rows, 1)
		rowCount := string(dataResp.Rows[0].Values[0])
		assert.Equal(t, "1", rowCount, "Should have 1 row in standby")

		t.Log("Replication is working! Data successfully replicated from primary to standby")

		// Cleanup: Drop the test table from primary
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "DROP TABLE IF EXISTS test_replication", 0)
		require.NoError(t, err)
	})

	t.Run("TermMismatchRejected", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Try to set primary conn info with stale term (current term is 1, we'll try with 0)
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           0, // Stale term (lower than current term 1)
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.Error(t, err, "SetPrimaryConnInfo should fail with stale term")
		assert.Contains(t, err.Error(), "consensus term too old", "Error should mention term is too old")

		// Try again with force=true, should succeed
		setPrimaryReq.Force = true
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed with force=true")
	})

	t.Run("StopReplicationBeforeFlag", func(t *testing.T) {
		// This test verifies that StopReplicationBefore flag stops replication before setting primary_conninfo

		// First ensure replication is running by checking pg_stat_wal_receiver
		t.Log("Verifying replication is running...")
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT status FROM pg_stat_wal_receiver", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		initialStatus := string(queryResp.Rows[0].Values[0])
		t.Logf("Initial WAL receiver status: %s", initialStatus)
		assert.Equal(t, "streaming", initialStatus, "Replication should be streaming")

		// Check if WAL replay is not paused
		queryResp, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused := string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "false", isPaused, "WAL replay should not be paused initially")

		// Call SetPrimaryConnInfo with StopReplicationBefore=true and StartReplicationAfter=false
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		t.Log("Calling SetPrimaryConnInfo with StopReplicationBefore=true, StartReplicationAfter=false...")
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: false, // Don't start after
			StopReplicationBefore: true,  // Stop before
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Verify that WAL replay is now paused
		t.Log("Verifying replication is paused after StopReplicationBefore...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused = string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "true", isPaused, "WAL replay should be paused after StopReplicationBefore=true")

		t.Log("Replication successfully stopped with StopReplicationBefore flag")

		// Resume replication for cleanup using StartReplication RPC
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		startReq := &multipoolermanagerdata.StartReplicationRequest{}
		_, err = standbyManagerClient.StartReplication(ctx, startReq)
		require.NoError(t, err)
	})

	t.Run("StartReplicationAfterFlag", func(t *testing.T) {
		// This test verifies that replication only starts if StartReplicationAfter=true

		// Stop replication using StopReplication RPC
		t.Log("Stopping replication using StopReplication RPC...")
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		stopReq := &multipoolermanagerdata.StopReplicationRequest{}
		_, err = standbyManagerClient.StopReplication(ctx, stopReq)
		require.NoError(t, err)
		cancel()

		// Verify replication is paused
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused := string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "true", isPaused, "WAL replay should be paused")

		// Call SetPrimaryConnInfo with StartReplicationAfter=false
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		t.Log("Calling SetPrimaryConnInfo with StartReplicationAfter=false...")
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: false, // Don't start after
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Verify replication is still paused (not started)
		t.Log("Verifying replication remains paused when StartReplicationAfter=false...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused = string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "true", isPaused, "WAL replay should still be paused when StartReplicationAfter=false")

		// Now call again with StartReplicationAfter=true
		t.Log("Calling SetPrimaryConnInfo with StartReplicationAfter=true...")
		setPrimaryReq.StartReplicationAfter = true
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Verify replication is now running
		t.Log("Verifying replication started when StartReplicationAfter=true...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused = string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "false", isPaused, "WAL replay should be running after StartReplicationAfter=true")

		t.Log("Replication successfully started with StartReplicationAfter flag")
	})

	t.Run("WaitForLSN_Standby_Success", func(t *testing.T) {
		// Insert data on primary to generate a new LSN
		t.Log("Creating table and inserting data on primary...")
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "CREATE TABLE IF NOT EXISTS test_wait_lsn (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err, "Should be able to create table in primary")

		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "INSERT INTO test_wait_lsn (data) VALUES ('test data for wait lsn')", 0)
		require.NoError(t, err, "Should be able to insert data in primary")

		// Get LSN from primary
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		primaryPosResp, err := primaryManagerClient.PrimaryPosition(ctx, &multipoolermanagerdata.PrimaryPositionRequest{})
		require.NoError(t, err)
		targetLSN := primaryPosResp.LsnPosition
		t.Logf("Target LSN from primary: %s", targetLSN)

		// Wait for standby to reach the target LSN
		t.Log("Waiting for standby to reach target LSN...")
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		waitReq := &multipoolermanagerdata.WaitForLSNRequest{
			TargetLsn: targetLSN,
		}
		_, err = standbyManagerClient.WaitForLSN(ctx, waitReq)
		require.NoError(t, err, "WaitForLSN should succeed on standby")

		t.Log("Standby successfully reached target LSN")

		// Verify the data replicated
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM test_wait_lsn", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		rowCount := string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "1", rowCount, "Should have 1 row in standby")

		// Cleanup
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "DROP TABLE IF EXISTS test_wait_lsn", 0)
		require.NoError(t, err)
	})

	t.Run("WaitForLSN_Primary_Fails", func(t *testing.T) {
		// WaitForLSN should fail on PRIMARY pooler type
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		waitReq := &multipoolermanagerdata.WaitForLSNRequest{
			TargetLsn: "0/1000000",
		}
		_, err = primaryManagerClient.WaitForLSN(ctx, waitReq)
		require.Error(t, err, "WaitForLSN should fail on primary")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
	})

	t.Run("WaitForLSN_Timeout", func(t *testing.T) {
		// Test timeout behavior by waiting for a very high LSN that won't be reached
		t.Log("Testing timeout with unreachable LSN...")

		// Use a very high LSN that won't be reached in the timeout period
		unreachableLSN := "FF/FFFFFFFF"

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		waitReq := &multipoolermanagerdata.WaitForLSNRequest{
			TargetLsn: unreachableLSN,
		}
		_, err = standbyManagerClient.WaitForLSN(ctx, waitReq)
		st, ok := status.FromError(err)
		require.True(t, ok, "Error should be a gRPC status error")
		assert.Contains(t, []string{"DeadlineExceeded", "Canceled"}, st.Code().String(),
			"Error should be a timeout error code, got: %s", st.Code().String())
		t.Log("WaitForLSN correctly timed out")
	})

	t.Run("StartReplication_Success", func(t *testing.T) {
		// This test verifies that StartReplication successfully resumes WAL replay on standby

		// First stop replication using StopReplication RPC
		t.Log("Stopping replication using StopReplication RPC...")
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		stopReq := &multipoolermanagerdata.StopReplicationRequest{}
		_, err = standbyManagerClient.StopReplication(ctx, stopReq)
		require.NoError(t, err)
		cancel()

		// Verify replication is paused
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused := string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "true", isPaused, "WAL replay should be paused")
		t.Log("Confirmed: WAL replay is paused")

		// Call StartReplication RPC
		t.Log("Calling StartReplication RPC...")
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		startReq := &multipoolermanagerdata.StartReplicationRequest{}
		_, err = standbyManagerClient.StartReplication(ctx, startReq)
		require.NoError(t, err, "StartReplication should succeed on standby")

		// Verify replication is now running
		t.Log("Verifying replication is running after StartReplication...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused = string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "false", isPaused, "WAL replay should be running after StartReplication")

		t.Log("StartReplication successfully resumed WAL replay")
	})

	t.Run("StartReplication_Primary_Fails", func(t *testing.T) {
		// StartReplication should fail on PRIMARY pooler type
		t.Log("Testing StartReplication on PRIMARY pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		startReq := &multipoolermanagerdata.StartReplicationRequest{}
		_, err = primaryManagerClient.StartReplication(ctx, startReq)
		require.Error(t, err, "StartReplication should fail on primary")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
		t.Log("Confirmed: StartReplication correctly rejected on PRIMARY pooler")
	})

	t.Run("StopReplication_Success", func(t *testing.T) {
		// This test verifies that StopReplication successfully pauses WAL replay on standby

		// First ensure replication is running
		t.Log("Ensuring replication is running...")
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused := string(queryResp.Rows[0].Values[0])
		if isPaused == "true" {
			// Resume it first using StartReplication RPC
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			startReq := &multipoolermanagerdata.StartReplicationRequest{}
			_, err = standbyManagerClient.StartReplication(ctx, startReq)
			require.NoError(t, err)
			cancel()
		}
		t.Log("Confirmed: WAL replay is running")

		// Call StopReplication RPC
		t.Log("Calling StopReplication RPC...")
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		stopReq := &multipoolermanagerdata.StopReplicationRequest{}
		_, err = standbyManagerClient.StopReplication(ctx, stopReq)
		require.NoError(t, err, "StopReplication should succeed on standby")

		// Verify replication is now paused
		t.Log("Verifying replication is paused after StopReplication...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused = string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "true", isPaused, "WAL replay should be paused after StopReplication")

		t.Log("StopReplication successfully paused WAL replay")

		// Resume replication for cleanup using StartReplication RPC
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		startReq := &multipoolermanagerdata.StartReplicationRequest{}
		_, err = standbyManagerClient.StartReplication(ctx, startReq)
		require.NoError(t, err)
	})

	t.Run("StopReplication_Primary_Fails", func(t *testing.T) {
		// StopReplication should fail on PRIMARY pooler type
		t.Log("Testing StopReplication on PRIMARY pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		stopReq := &multipoolermanagerdata.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(ctx, stopReq)
		require.Error(t, err, "StopReplication should fail on primary")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
		t.Log("Confirmed: StopReplication correctly rejected on PRIMARY pooler")
	})

	t.Run("ResetReplication_Success", func(t *testing.T) {
		// This test verifies that ResetReplication successfully disconnects the standby from the primary
		// and that data inserted after reset does not replicate until replication is re-enabled

		// First ensure replication is configured
		t.Log("Ensuring replication is configured...")
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")
		cancel()

		// Verify replication is working by checking pg_stat_wal_receiver
		t.Log("Verifying replication is working...")
		require.Eventually(t, func() bool {
			queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM pg_stat_wal_receiver WHERE status = 'streaming'", 1)
			if err != nil || len(queryResp.Rows) == 0 {
				return false
			}
			count := string(queryResp.Rows[0].Values[0])
			return count == "1"
		}, 10*time.Second, 500*time.Millisecond, "Replication should be streaming")
		t.Log("Confirmed: Replication is streaming")

		// Call ResetReplication RPC
		t.Log("Calling ResetReplication RPC...")
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		resetReq := &multipoolermanagerdata.ResetReplicationRequest{}
		_, err = standbyManagerClient.ResetReplication(ctx, resetReq)
		require.NoError(t, err, "ResetReplication should succeed on standby")

		// Verify that primary_conninfo is cleared by checking pg_stat_wal_receiver
		// After resetting, the WAL receiver should eventually disconnect
		t.Log("Verifying replication is disconnected after ResetReplication...")
		require.Eventually(t, func() bool {
			queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM pg_stat_wal_receiver", 1)
			if err != nil || len(queryResp.Rows) == 0 {
				return false
			}
			count := string(queryResp.Rows[0].Values[0])
			return count == "0"
		}, 10*time.Second, 500*time.Millisecond, "WAL receiver should disconnect after ResetReplication")

		t.Log("ResetReplication successfully disconnected standby from primary")

		// Sanity check: Insert a row on primary, verify it does NOT replicate to standby
		t.Log("Sanity check: Inserting data on primary after ResetReplication...")
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "CREATE TABLE IF NOT EXISTS test_reset_replication (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err, "Should be able to create table on primary")

		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "INSERT INTO test_reset_replication (data) VALUES ('should not replicate')", 0)
		require.NoError(t, err, "Should be able to insert data on primary")

		// Get LSN from primary after the insert
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		primaryPosResp, err := primaryManagerClient.PrimaryPosition(ctx, &multipoolermanagerdata.PrimaryPositionRequest{})
		require.NoError(t, err)
		primaryLSNAfterInsert := primaryPosResp.LsnPosition
		t.Logf("Primary LSN after insert: %s", primaryLSNAfterInsert)
		cancel()

		// Verify standby CANNOT reach the primary LSN (replication is disconnected)
		t.Log("Verifying standby cannot reach primary LSN (replication disconnected)...")
		waitReq := &multipoolermanagerdata.WaitForLSNRequest{
			TargetLsn: primaryLSNAfterInsert,
		}
		_, err = standbyManagerClient.WaitForLSN(utils.WithShortDeadline(t), waitReq)
		require.Error(t, err, "WaitForLSN should timeout since replication is disconnected")
		t.Log("Confirmed: Standby cannot reach primary LSN (data did NOT replicate)")

		// Re-enable replication using SetPrimaryConnInfo
		t.Log("Re-enabling replication...")
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		setPrimaryReq = &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")
		cancel()

		// Wait for standby to catch up to primary's LSN
		t.Logf("Waiting for standby to catch up to primary LSN: %s", primaryLSNAfterInsert)
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		waitReq = &multipoolermanagerdata.WaitForLSNRequest{
			TargetLsn: primaryLSNAfterInsert,
		}
		_, err = standbyManagerClient.WaitForLSN(ctx, waitReq)
		require.NoError(t, err, "Standby should catch up after re-enabling replication")

		// Verify the table now exists on standby
		t.Log("Verifying data replicated after re-enabling replication...")
		dataResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM test_reset_replication", 1)
		require.NoError(t, err)
		require.Len(t, dataResp.Rows, 1)
		rowCount := string(dataResp.Rows[0].Values[0])
		assert.Equal(t, "1", rowCount, "Should have 1 row on standby after re-enabling replication")

		t.Log("Confirmed: Data successfully replicated after re-enabling replication")

		// Cleanup: Drop the test table from primary
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "DROP TABLE IF EXISTS test_reset_replication", 0)
		require.NoError(t, err)
	})

	t.Run("ResetReplication_Primary_Fails", func(t *testing.T) {
		// ResetReplication should fail on PRIMARY pooler type
		t.Log("Testing ResetReplication on PRIMARY pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		resetReq := &multipoolermanagerdata.ResetReplicationRequest{}
		_, err = primaryManagerClient.ResetReplication(ctx, resetReq)
		require.Error(t, err, "ResetReplication should fail on primary")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
		t.Log("Confirmed: ResetReplication correctly rejected on PRIMARY pooler")
	})
}

// TestReplicationStatus tests the ReplicationStatus API
func TestReplicationStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Connect to manager clients
	primaryManagerConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryManagerConn.Close() })

	primaryManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(primaryManagerConn)

	standbyManagerConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyManagerConn.Close() })
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyManagerConn)

	// Ensure managers are ready
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	t.Run("ReplicationStatus_Primary_Fails", func(t *testing.T) {
		// ReplicationStatus should fail on PRIMARY pooler type
		t.Log("Testing ReplicationStatus on PRIMARY (should fail)...")

		_, err := primaryManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.Error(t, err, "ReplicationStatus should fail on PRIMARY")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
		t.Log("Confirmed: ReplicationStatus correctly rejected on PRIMARY pooler")
	})

	t.Run("ReplicationStatus_Standby_NoReplication", func(t *testing.T) {
		// Test ReplicationStatus on standby when replication is not configured
		t.Log("Testing ReplicationStatus on standby with no replication configured...")

		// First, ensure replication is stopped
		_, err := standbyManagerClient.ResetReplication(utils.WithShortDeadline(t), &multipoolermanagerdata.ResetReplicationRequest{})
		require.NoError(t, err, "ResetReplication should succeed")

		// Wait for config to take effect (pg_reload_conf is async)
		t.Log("Waiting for primary_conninfo to be cleared...")
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				t.Logf("ReplicationStatus error: %v", err)
				return false
			}
			// Config cleared when PrimaryConnInfo is nil or Host is empty
			return statusResp.Status.PrimaryConnInfo == nil ||
				statusResp.Status.PrimaryConnInfo.Host == ""
		}, 5*time.Second, 200*time.Millisecond, "primary_conninfo should be cleared after ResetReplication")

		// Get final status
		statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed on standby")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify fields
		assert.NotEmpty(t, statusResp.Status.Lsn, "LSN should not be empty")
		assert.False(t, statusResp.Status.IsWalReplayPaused, "WAL replay should not be paused by default")

		// PrimaryConnInfo should be nil or empty when no replication is configured
		if statusResp.Status.PrimaryConnInfo != nil {
			assert.Empty(t, statusResp.Status.PrimaryConnInfo.Host, "Host should be empty when no replication configured")
		}
	})

	t.Run("ReplicationStatus_Standby_WithReplication", func(t *testing.T) {
		// Configure replication
		t.Log("Configuring replication on standby...")
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err := standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Wait for config to take effect (pg_reload_conf is async)
		t.Log("Waiting for primary_conninfo to be set...")
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				t.Logf("ReplicationStatus error: %v", err)
				return false
			}
			// Config set when PrimaryConnInfo is not nil and Host is populated
			return statusResp.Status.PrimaryConnInfo != nil &&
				statusResp.Status.PrimaryConnInfo.Host != ""
		}, 5*time.Second, 200*time.Millisecond, "primary_conninfo should be set after SetPrimaryConnInfo")

		// Get final status
		statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify LSN
		assert.NotEmpty(t, statusResp.Status.Lsn, "LSN should not be empty")
		assert.Contains(t, statusResp.Status.Lsn, "/", "LSN should be in PostgreSQL format (e.g., 0/1234ABCD)")

		// Verify replication is not paused
		assert.False(t, statusResp.Status.IsWalReplayPaused, "WAL replay should not be paused")

		// Verify PrimaryConnInfo is populated
		require.NotNil(t, statusResp.Status.PrimaryConnInfo, "PrimaryConnInfo should not be nil")
		assert.Equal(t, "localhost", statusResp.Status.PrimaryConnInfo.Host, "Host should match")
		assert.Equal(t, int32(setup.PrimaryPgctld.PgPort), statusResp.Status.PrimaryConnInfo.Port, "Port should match")
		assert.NotEmpty(t, statusResp.Status.PrimaryConnInfo.Raw, "Raw connection string should not be empty")
	})

	t.Run("ReplicationStatus_Standby_PausedReplication", func(t *testing.T) {
		// Configure replication but stop it
		t.Log("Configuring replication and then stopping it...")
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: false,
			StopReplicationBefore: true,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err := standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Wait for config to take effect and WAL replay to be paused
		t.Log("Waiting for WAL replay to be paused...")
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				t.Logf("ReplicationStatus error: %v", err)
				return false
			}
			return statusResp.Status.IsWalReplayPaused
		}, 5*time.Second, 200*time.Millisecond, "WAL replay should be paused after SetPrimaryConnInfo with StopReplicationBefore")

		// Get final status
		statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify WAL replay is paused
		assert.True(t, statusResp.Status.IsWalReplayPaused, "WAL replay should be paused")
		assert.NotEmpty(t, statusResp.Status.WalReplayPauseState, "Pause state should not be empty")
		// Clean up: resume replication
		startReq := &multipoolermanagerdata.StartReplicationRequest{}
		_, err = standbyManagerClient.StartReplication(utils.WithShortDeadline(t), startReq)
		require.NoError(t, err, "StartReplication should succeed")
	})
}

// TestConfigureSynchronousReplication tests the ConfigureSynchronousReplication API
func TestConfigureSynchronousReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Create shared clients for all subtests
	primaryConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryConn.Close() })
	primaryManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(primaryConn)

	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)

	primaryPoolerClient, err := NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { primaryPoolerClient.Close() })

	t.Run("ConfigureSynchronousReplication_Primary_Success", func(t *testing.T) {
		// This test verifies that ConfigureSynchronousReplication successfully configures
		// synchronous replication on the primary
		t.Log("Testing ConfigureSynchronousReplication on PRIMARY...")

		// The application_name used by standby is: {cell}_{name}
		// For test purposes, we'll use a simple standby name
		standbyAppName := "test-standby"

		// Configure synchronous replication with FIRST method
		req := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds:        []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "test-standby")},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "ConfigureSynchronousReplication should succeed on primary")

		t.Log("ConfigureSynchronousReplication completed successfully")

		// Wait for configuration to converge and verify using PrimaryStatus API
		t.Log("Waiting for configuration to converge...")
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.SynchronousCommit == multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON &&
				config.SynchronousMethod == multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST &&
				config.NumSync == 1 &&
				containsStandbyIDInConfig(config, "test-cell", standbyAppName)
		}, "Configuration should converge to expected values")

		// Verify the configuration using PrimaryStatus
		t.Log("Verifying configuration using PrimaryStatus...")
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON, status.SyncReplicationConfig.SynchronousCommit, "synchronous_commit should be ON")
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST, status.SyncReplicationConfig.SynchronousMethod, "synchronous_method should be FIRST")
		assert.Equal(t, int32(1), status.SyncReplicationConfig.NumSync, "num_sync should be 1")
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", standbyAppName), "standby should be in configuration")

		t.Log("Synchronous replication configured and verified successfully")

		// Clean up: Reset synchronous replication configuration to PostgreSQL defaults
		t.Log("Cleaning up: Resetting synchronous replication configuration...")
		resetReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), resetReq)
		require.NoError(t, err, "Reset configuration should succeed")
	})

	t.Run("ConfigureSynchronousReplication_Primary_AnyMethod", func(t *testing.T) {
		// This test verifies that ConfigureSynchronousReplication works with ANY method
		t.Log("Testing ConfigureSynchronousReplication with ANY method on PRIMARY...")

		// Use multiple standby IDs to test the ANY method with multiple standbys
		standbyIDs := []*clustermetadatapb.ID{
			makeMultipoolerID("test-cell", "test-standby-1"),
			makeMultipoolerID("test-cell", "test-standby-2"),
		}

		// Configure synchronous replication with ANY method
		req := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			NumSync:           1,
			StandbyIds:        standbyIDs,
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "ConfigureSynchronousReplication should succeed on primary")

		t.Log("ConfigureSynchronousReplication with ANY method completed successfully")

		// Wait for configuration to converge and verify using PrimaryStatus API
		t.Log("Waiting for configuration to converge...")
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.SynchronousCommit == multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY &&
				config.SynchronousMethod == multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY &&
				config.NumSync == 1 &&
				len(config.StandbyIds) == 2
		}, "Configuration should converge to expected values")

		// Verify the configuration using PrimaryStatus
		t.Log("Verifying configuration using PrimaryStatus...")
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY, status.SyncReplicationConfig.SynchronousCommit, "synchronous_commit should be REMOTE_APPLY")
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY, status.SyncReplicationConfig.SynchronousMethod, "synchronous_method should be ANY")
		assert.Equal(t, int32(1), status.SyncReplicationConfig.NumSync, "num_sync should be 1")
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2, "should have 2 standbys configured")

		t.Log("Synchronous replication with ANY method configured and verified successfully")

		// Clean up
		t.Log("Cleaning up: Resetting synchronous replication configuration...")
		resetReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), resetReq)
		require.NoError(t, err, "Reset configuration should succeed")
	})

	t.Run("ConfigureSynchronousReplication_AllCommitLevels", func(t *testing.T) {
		// This test verifies that all SynchronousCommitLevel values work correctly
		t.Log("Testing ConfigureSynchronousReplication with all commit levels...")

		testCases := []struct {
			level multipoolermanagerdata.SynchronousCommitLevel
		}{
			{
				level: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_OFF,
			},
			{
				level: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL,
			},
			{
				level: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE,
			},
			{
				level: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			},
			{
				level: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.level.String(), func(t *testing.T) {
				// Configure with this commit level
				req := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
					SynchronousCommit: tc.level,
					SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
					NumSync:           1,
					StandbyIds:        []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "test-standby")},
					ReloadConfig:      true,
				}
				_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), req)
				require.NoError(t, err, "ConfigureSynchronousReplication should succeed for %s", tc.level.String())

				t.Logf("ConfigureSynchronousReplication with %s completed successfully", tc.level.String())

				// Wait for configuration to converge and verify using PrimaryStatus API
				t.Logf("Waiting for configuration to converge to %s...", tc.level.String())
				waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
					return config != nil && config.SynchronousCommit == tc.level
				}, "Configuration should converge to expected commit level")

				// Verify the configuration using PrimaryStatus
				status := getPrimaryStatusFromClient(t, primaryManagerClient)
				assert.Equal(t, tc.level, status.SyncReplicationConfig.SynchronousCommit, "synchronous_commit should be %s", tc.level.String())

				t.Logf("Successfully verified synchronous_commit level: %s", tc.level.String())
			})
		}

		// Clean up: Reset to PostgreSQL defaults
		t.Log("Cleaning up: Resetting synchronous replication configuration...")
		resetReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), resetReq)
		require.NoError(t, err, "Reset configuration should succeed")
	})

	t.Run("ConfigureSynchronousReplication_AllSynchronousMethods", func(t *testing.T) {
		// This test verifies that FIRST and ANY methods work correctly with different num_sync values
		t.Log("Testing ConfigureSynchronousReplication with all synchronous methods...")

		testCases := []struct {
			name       string
			method     multipoolermanagerdata.SynchronousMethod
			numSync    int32
			standbyIDs []*clustermetadatapb.ID
		}{
			{
				name:    "FIRST_1_SingleStandby",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				numSync: 1,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
				},
			},
			{
				name:    "FIRST_1_MultipleStandbys",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				numSync: 1,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
				},
			},
			{
				name:    "FIRST_2_MultipleStandbys",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				numSync: 2,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
				},
			},
			{
				name:    "FIRST_3_MultipleStandbys",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				numSync: 3,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
					makeMultipoolerID("test-cell", "standby-4"),
				},
			},
			{
				name:    "ANY_1_SingleStandby",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				numSync: 1,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
				},
			},
			{
				name:    "ANY_1_MultipleStandbys",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				numSync: 1,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
				},
			},
			{
				name:    "ANY_2_MultipleStandbys",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				numSync: 2,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
				},
			},
			{
				name:    "ANY_3_MultipleStandbys",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				numSync: 3,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
					makeMultipoolerID("test-cell", "standby-4"),
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Configure with this synchronous method
				req := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
					SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
					SynchronousMethod: tc.method,
					NumSync:           tc.numSync,
					StandbyIds:        tc.standbyIDs,
					ReloadConfig:      true,
				}
				_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), req)
				require.NoError(t, err, "ConfigureSynchronousReplication should succeed for %s", tc.name)

				t.Logf("ConfigureSynchronousReplication with %s completed successfully", tc.name)

				// Wait for configuration to converge and verify using PrimaryStatus API
				t.Logf("Waiting for configuration to converge for %s...", tc.name)
				waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
					return config != nil &&
						config.SynchronousMethod == tc.method &&
						config.NumSync == tc.numSync &&
						len(config.StandbyIds) == len(tc.standbyIDs)
				}, "Configuration should converge to expected values")

				// Verify the configuration using PrimaryStatus
				status := getPrimaryStatusFromClient(t, primaryManagerClient)
				assert.Equal(t, tc.method, status.SyncReplicationConfig.SynchronousMethod, "synchronous_method should match")
				assert.Equal(t, tc.numSync, status.SyncReplicationConfig.NumSync, "num_sync should match")
				assert.Len(t, status.SyncReplicationConfig.StandbyIds, len(tc.standbyIDs), "should have correct number of standbys")

				t.Logf("Successfully verified synchronous method configuration: %s", tc.name)
			})
		}

		// Clean up: Reset to PostgreSQL defaults
		t.Log("Cleaning up: Resetting synchronous replication configuration...")
		resetReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), resetReq)
		require.NoError(t, err, "Reset configuration should succeed")
	})

	t.Run("ConfigureSynchronousReplication_EndToEnd_WithRealStandby", func(t *testing.T) {
		// This test validates the complete synchronous replication flow:
		// 1. Configure primary with remote_apply and the actual standby name
		// 2. Ensure standby is connected and replicating
		// 3. Verify writes succeed (synchronous replication satisfied)
		// 4. Disconnect standby using ResetReplication
		// 5. Verify writes timeout (synchronous replication cannot be satisfied)
		t.Log("Testing end-to-end synchronous replication with real standby...")

		// The standby's application_name is constructed as: {cell}_{name}
		// Use the ServiceID from the setup which is the multipooler name
		standbyID := makeMultipoolerID("test-cell", setup.StandbyMultipooler.ServiceID)
		standbyAppName := fmt.Sprintf("test-cell_%s", setup.StandbyMultipooler.ServiceID)
		t.Logf("Using standby application_name from setup: %s", standbyAppName)

		// Configure synchronous replication on primary with remote_apply and actual standby
		t.Log("Configuring synchronous replication on primary with remote_apply...")
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds:        []*clustermetadatapb.ID{standbyID},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err, "ConfigureSynchronousReplication should succeed on primary")

		// Ensure standby is connected and replicating
		t.Log("Ensuring standby is connected to primary and replicating...")
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Wait for config to take effect and replication to establish (pg_reload_conf is async)
		t.Log("Waiting for replication to be configured...")
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.Status.PrimaryConnInfo != nil &&
				statusResp.Status.PrimaryConnInfo.Host != "" &&
				!statusResp.Status.IsWalReplayPaused
		}, 5*time.Second, 200*time.Millisecond, "Replication should be configured and active")

		// Verify standby is connected and replicating using ReplicationStatus API
		t.Log("Verifying standby is connected and replicating...")
		statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed")
		require.NotNil(t, statusResp.Status, "Status should not be nil")
		require.NotNil(t, statusResp.Status.PrimaryConnInfo, "PrimaryConnInfo should not be nil")
		t.Logf("Standby replication status: LSN=%s, is_paused=%v, pause_state=%s, primary_conn_info=%s",
			statusResp.Status.Lsn, statusResp.Status.IsWalReplayPaused, statusResp.Status.WalReplayPauseState, statusResp.Status.PrimaryConnInfo.Raw)

		// Verify standby is not paused
		require.False(t, statusResp.Status.IsWalReplayPaused, "Standby should be actively replicating (not paused)")

		// Verify primary_conninfo contains the expected application_name
		require.Equal(t, standbyAppName, statusResp.Status.PrimaryConnInfo.ApplicationName,
			"PrimaryConnInfo.ApplicationName should match expected standby application name")

		// Test write with synchronous replication enabled
		t.Log("Testing write with synchronous replication enabled...")

		// Reconnect to pick up the new synchronous_standby_names configuration
		primaryPoolerClient.Close()
		primaryPoolerClient, err = NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		require.NoError(t, err)
		t.Cleanup(func() { primaryPoolerClient.Close() })

		// Create a test table and insert data - this should succeed because standby is available
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "CREATE TABLE IF NOT EXISTS test_sync_repl (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err, "Table creation should succeed with standby available")

		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "INSERT INTO test_sync_repl (data) VALUES ('test-with-standby')", 0)
		require.NoError(t, err, "Insert should succeed with standby connected and replicating")
		t.Log("Write succeeded with synchronous replication enabled")

		// Verify standby caught up to primary after the successful write
		t.Log("Verifying standby caught up to primary after successful write...")
		primaryPosResp, err := primaryManagerClient.PrimaryPosition(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryPositionRequest{})
		require.NoError(t, err, "Should be able to get primary position")
		primaryLSN := primaryPosResp.LsnPosition
		t.Logf("Primary LSN after write: %s", primaryLSN)

		waitReq := &multipoolermanagerdata.WaitForLSNRequest{
			TargetLsn: primaryLSN,
		}
		_, err = standbyManagerClient.WaitForLSN(utils.WithShortDeadline(t), waitReq)
		require.NoError(t, err, "Standby should have caught up to primary after successful write")
		t.Log("Standby successfully caught up to primary")

		// Disconnect standby using ResetReplication
		t.Log("Disconnecting standby using ResetReplication...")
		_, err = standbyManagerClient.ResetReplication(utils.WithShortDeadline(t), &multipoolermanagerdata.ResetReplicationRequest{})
		require.NoError(t, err, "ResetReplication should succeed")

		// Wait for standby to fully disconnect
		t.Log("Waiting for standby to disconnect...")
		require.Eventually(t, func() bool {
			standbyPoolerClient, err := NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort))
			if err != nil {
				return false
			}
			defer standbyPoolerClient.Close()
			queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM pg_stat_wal_receiver", 1)
			if err != nil || len(queryResp.Rows) == 0 {
				return false
			}
			count := string(queryResp.Rows[0].Values[0])
			return count == "0"
		}, 10*time.Second, 500*time.Millisecond, "Standby should disconnect after ResetReplication")
		t.Log("Standby disconnected successfully")

		// Get standby LSN before attempting the write
		t.Log("Getting standby LSN before failed write attempt...")
		standbyStatusBefore, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.NoError(t, err, "Should be able to get standby status")
		standbyLSNBefore := standbyStatusBefore.Status.Lsn
		t.Logf("Standby LSN before write attempt: %s", standbyLSNBefore)

		// Test write timeout without standby
		t.Log("Testing write timeout without standby available...")
		// Create a new connection for this test
		primaryPoolerClient.Close()
		primaryPoolerClient, err = NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		require.NoError(t, err)
		t.Cleanup(func() { primaryPoolerClient.Close() })

		// This insert should timeout because synchronous_commit=remote_apply requires standby confirmation
		// Use a 3-second context timeout so the test doesn't wait too long
		timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer timeoutCancel()
		_, err = primaryPoolerClient.ExecuteQuery(timeoutCtx, "INSERT INTO test_sync_repl (data) VALUES ('test-without-standby')", 0)
		require.Error(t, err, "Insert should timeout without standby available")
		assert.Contains(t, err.Error(), "DeadlineExceeded", "Error should indicate a deadline exceeded")
		t.Log("Write correctly timed out without standby available")

		// Verify standby LSN did not advance
		t.Log("Verifying standby LSN did not advance after failed write...")
		standbyStatusAfter, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.NoError(t, err, "Should be able to get standby status")
		standbyLSNAfter := standbyStatusAfter.Status.Lsn
		t.Logf("Standby LSN after failed write: %s", standbyLSNAfter)
		assert.Equal(t, standbyLSNBefore, standbyLSNAfter, "Standby LSN should not have advanced since replication is disconnected and write failed")

		// Cleanup: Reconnect standby and reset synchronous replication
		t.Log("Cleanup: Reconnecting standby and resetting synchronous replication...")
		setPrimaryReq = &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed during cleanup")

		// Reset synchronous replication to defaults
		resetReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), resetReq)
		require.NoError(t, err, "Reset configuration should succeed")

		// Drop test table
		primaryPoolerClient.Close()
		primaryPoolerClient, err = NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		require.NoError(t, err)
		t.Cleanup(func() { primaryPoolerClient.Close() })
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "DROP TABLE IF EXISTS test_sync_repl", 0)
		require.NoError(t, err)

		t.Log("End-to-end synchronous replication test completed successfully")
	})

	t.Run("ConfigureSynchronousReplication_ClearConfig", func(t *testing.T) {
		// This test verifies that ConfigureSynchronousReplication can clear the configuration
		// by providing an empty standby list
		t.Log("Testing ConfigureSynchronousReplication can clear configuration...")

		// First, configure synchronous replication with some standbys
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby1"),
				makeMultipoolerID("test-cell", "standby2"),
			},
			ReloadConfig: true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err, "ConfigureSynchronousReplication should succeed")

		// Wait for initial configuration to converge and verify using PrimaryStatus API
		t.Log("Waiting for initial configuration to converge...")
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.SynchronousMethod == multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST &&
				config.NumSync == 1 &&
				len(config.StandbyIds) == 2
		}, "Initial configuration should converge")

		// Verify configuration is set using PrimaryStatus
		t.Log("Verifying initial configuration using PrimaryStatus...")
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST, status.SyncReplicationConfig.SynchronousMethod)
		assert.Equal(t, int32(1), status.SyncReplicationConfig.NumSync)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2, "should have 2 standbys")
		t.Log("Initial configuration verified")

		// Now clear the configuration by providing empty standby list
		t.Log("Clearing synchronous_standby_names configuration...")
		clearReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), clearReq)
		require.NoError(t, err, "ConfigureSynchronousReplication should succeed with empty config")

		// Wait for cleared configuration to converge and verify using PrimaryStatus API
		t.Log("Waiting for configuration to be cleared...")
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.NumSync == 0 &&
				len(config.StandbyIds) == 0
		}, "Configuration should be cleared")

		// Verify configuration is cleared using PrimaryStatus
		t.Log("Verifying configuration is cleared using PrimaryStatus...")
		status = getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, int32(0), status.SyncReplicationConfig.NumSync, "num_sync should be 0")
		assert.Empty(t, status.SyncReplicationConfig.StandbyIds, "standby list should be empty")
		t.Log("Successfully verified synchronous_standby_names is cleared")
	})

	t.Run("ConfigureSynchronousReplication_Standby_Fails", func(t *testing.T) {
		// ConfigureSynchronousReplication should fail on REPLICA pooler type
		t.Log("Testing ConfigureSynchronousReplication on REPLICA pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		req := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby1"),
			},
			ReloadConfig: true,
		}
		_, err := standbyManagerClient.ConfigureSynchronousReplication(ctx, req)
		require.Error(t, err, "ConfigureSynchronousReplication should fail on standby")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on REPLICA")
		t.Log("Confirmed: ConfigureSynchronousReplication correctly rejected on REPLICA pooler")
	})
}

func TestUpdateSynchronousStandbyList(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Create shared clients for all subtests
	primaryConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryConn.Close() })
	primaryManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(primaryConn)

	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)

	primaryPoolerClient, err := NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { primaryPoolerClient.Close() })

	t.Run("UpdateSynchronousStandbyList_Add_Success", func(t *testing.T) {
		t.Log("Testing UpdateSynchronousStandbyList ADD operation...")

		// First, configure initial synchronous replication with one standby
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds:        []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby1")},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err, "Initial configuration should succeed")

		// Wait for config to converge
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 1 && containsStandbyIDInConfig(config, "test-cell", "standby1")
		}, "Initial config should converge")

		// Verify initial configuration
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST, status.SyncReplicationConfig.SynchronousMethod)
		assert.Equal(t, int32(1), status.SyncReplicationConfig.NumSync)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 1)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		t.Log("Initial configuration verified")

		// Test term validation: Set term to 2
		_, err = primaryManagerClient.SetTerm(utils.WithShortDeadline(t), &multipoolermanagerdata.SetTermRequest{
			Term: &pgctldpb.ConsensusTerm{CurrentTerm: 2},
		})
		require.NoError(t, err, "SetTerm to 2 should succeed")
		t.Log("Set term to 2")

		// Try to ADD with stale term 1 (should fail)
		updateReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
			StandbyIds:    []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby2")},
			ReloadConfig:  true,
			ConsensusTerm: 1,
			Force:         false,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.Error(t, err, "ADD with stale term should fail")
		assert.Contains(t, err.Error(), "consensus term too old", "Error should mention stale term")
		t.Log("Confirmed: ADD correctly rejected with stale term 1")

		// Now ADD a second standby with correct term 2 (should succeed)
		updateReq.ConsensusTerm = 2
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.NoError(t, err, "ADD operation with correct term should succeed")
		t.Log("Confirmed: ADD succeeded with correct term 2")

		// Wait for config to converge with both standbys
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				containsStandbyIDInConfig(config, "test-cell", "standby2")
		}, "Config should converge with both standbys")

		// Verify both standbys are now in the list
		status = getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST, status.SyncReplicationConfig.SynchronousMethod)
		assert.Equal(t, int32(1), status.SyncReplicationConfig.NumSync)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))

		t.Log("ADD operation verified successfully")

		// Cleanup
		resetReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), resetReq)
		require.NoError(t, err)
	})

	t.Run("UpdateSynchronousStandbyList_Add_Idempotent", func(t *testing.T) {
		t.Log("Testing UpdateSynchronousStandbyList ADD operation is idempotent...")

		// Configure with two standbys
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby1"),
				makeMultipoolerID("test-cell", "standby2"),
			},
			ReloadConfig: true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for config to converge
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				containsStandbyIDInConfig(config, "test-cell", "standby2")
		}, "Initial config should converge")

		initialStatus := getPrimaryStatusFromClient(t, primaryManagerClient)
		t.Log("Initial configuration verified")

		// Try to ADD a standby that already exists
		updateReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
			StandbyIds:    []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby1")},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.NoError(t, err, "ADD should be idempotent")

		// Wait for config to settle - should remain unchanged (idempotent)
		// We wait for the same configuration to ensure pg_reload_conf() completes
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				containsStandbyIDInConfig(config, "test-cell", "standby2")
		}, "Config should remain unchanged (idempotent)")

		// Configuration should be unchanged (idempotent)
		afterStatus := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, len(initialStatus.SyncReplicationConfig.StandbyIds), len(afterStatus.SyncReplicationConfig.StandbyIds), "Standby count should be unchanged")
		assert.True(t, containsStandbyIDInConfig(afterStatus.SyncReplicationConfig, "test-cell", "standby1"))
		assert.True(t, containsStandbyIDInConfig(afterStatus.SyncReplicationConfig, "test-cell", "standby2"))

		t.Log("ADD idempotency verified successfully")

		// Cleanup
		resetReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), resetReq)
		require.NoError(t, err)
	})

	t.Run("UpdateSynchronousStandbyList_Remove_Success", func(t *testing.T) {
		t.Log("Testing UpdateSynchronousStandbyList REMOVE operation...")

		// Configure with three standbys
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			NumSync:           2,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby1"),
				makeMultipoolerID("test-cell", "standby2"),
				makeMultipoolerID("test-cell", "standby3"),
			},
			ReloadConfig: true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for config to converge
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 3 && containsStandbyIDInConfig(config, "test-cell", "standby2")
		}, "Initial config with 3 standbys should converge")

		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY, status.SyncReplicationConfig.SynchronousMethod)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))
		t.Log("Initial configuration verified")

		// REMOVE standby2
		updateReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE,
			StandbyIds:    []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby2")},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.NoError(t, err, "REMOVE operation should succeed")

		// Wait for config to converge without standby2
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				!containsStandbyIDInConfig(config, "test-cell", "standby2") &&
				containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				containsStandbyIDInConfig(config, "test-cell", "standby3")
		}, "Config should converge without standby2")

		// Verify standby2 was removed
		status = getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY, status.SyncReplicationConfig.SynchronousMethod)
		assert.Equal(t, int32(2), status.SyncReplicationConfig.NumSync)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby3"))

		t.Log("REMOVE operation verified successfully")

		// Cleanup
		resetReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), resetReq)
		require.NoError(t, err)
	})

	t.Run("UpdateSynchronousStandbyList_Replace_Success", func(t *testing.T) {
		t.Log("Testing UpdateSynchronousStandbyList REPLACE operation...")

		// Configure initial set
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby1"),
				makeMultipoolerID("test-cell", "standby2"),
			},
			ReloadConfig: true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for config to converge
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				containsStandbyIDInConfig(config, "test-cell", "standby2")
		}, "Initial config should converge")

		t.Log("Initial configuration verified")

		// REPLACE with completely different set
		updateReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation: multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REPLACE,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby3"),
				makeMultipoolerID("test-cell", "standby4"),
			},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.NoError(t, err, "REPLACE operation should succeed")

		// Wait for config to converge with new standbys
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				containsStandbyIDInConfig(config, "test-cell", "standby3") &&
				containsStandbyIDInConfig(config, "test-cell", "standby4") &&
				!containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				!containsStandbyIDInConfig(config, "test-cell", "standby2")
		}, "Config should converge with replaced standbys")

		// Verify list was completely replaced
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST, status.SyncReplicationConfig.SynchronousMethod)
		assert.Equal(t, int32(1), status.SyncReplicationConfig.NumSync)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2)
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby3"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby4"))

		t.Log("REPLACE operation verified successfully")

		// Cleanup
		resetReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), resetReq)
		require.NoError(t, err)
	})

	t.Run("UpdateSynchronousStandbyList_NoSyncReplication_Fails", func(t *testing.T) {
		t.Log("Testing UpdateSynchronousStandbyList fails when sync replication not configured...")

		// Ensure synchronous replication is not configured
		resetReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), resetReq)
		require.NoError(t, err)

		// Wait for config to be cleared
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 0
		}, "Config should be cleared")

		// Try to update when no sync replication is configured
		updateReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
			StandbyIds:    []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby1")},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.Error(t, err, "Should fail when sync replication not configured")
		assert.Contains(t, err.Error(), "not configured", "Error should mention sync replication not configured")

		t.Log("Verified: UpdateSynchronousStandbyList correctly fails when sync replication not configured")
	})

	t.Run("UpdateSynchronousStandbyList_Standby_Fails", func(t *testing.T) {
		t.Log("Testing UpdateSynchronousStandbyList on REPLICA pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		updateReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
			StandbyIds:    []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby1")},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err := standbyManagerClient.UpdateSynchronousStandbyList(ctx, updateReq)
		require.Error(t, err, "UpdateSynchronousStandbyList should fail on standby")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on REPLICA")

		t.Log("Confirmed: UpdateSynchronousStandbyList correctly rejected on REPLICA pooler")
	})
}

func TestPrimaryStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Create shared clients for all subtests
	primaryConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryConn.Close() })
	primaryManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(primaryConn)

	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)

	t.Run("PrimaryStatus_NoSyncReplication", func(t *testing.T) {
		t.Log("Testing PrimaryStatus without synchronous replication configured...")

		// Clear any existing sync replication configuration
		clearReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), clearReq)
		require.NoError(t, err)

		// Get primary status
		statusResp, err := primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
		require.NoError(t, err, "PrimaryStatus should succeed")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify LSN is present and valid format
		assert.NotEmpty(t, statusResp.Status.Lsn, "LSN should be present")
		assert.Regexp(t, `^[0-9A-F]+/[0-9A-F]+$`, statusResp.Status.Lsn, "LSN should be in PostgreSQL format (X/XXXXXXXX)")
		t.Logf("Primary LSN: %s", statusResp.Status.Lsn)

		// Verify ready status
		assert.True(t, statusResp.Status.Ready, "Primary should be ready")

		// Verify sync replication config is present but with empty standby list
		require.NotNil(t, statusResp.Status.SyncReplicationConfig, "Sync replication config should be present")
		assert.Equal(t, multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			statusResp.Status.SyncReplicationConfig.SynchronousCommit, "Should have synchronous_commit level")
		assert.Empty(t, statusResp.Status.SyncReplicationConfig.StandbyIds, "StandbyIds should be empty when not configured")
		assert.Equal(t, int32(0), statusResp.Status.SyncReplicationConfig.NumSync, "NumSync should be 0")

		t.Log("PrimaryStatus without sync replication verified successfully")
	})

	t.Run("PrimaryStatus_WithSyncReplication", func(t *testing.T) {
		t.Log("Testing PrimaryStatus with synchronous replication configured...")

		// Configure synchronous replication
		standbyIDs := []*clustermetadatapb.ID{
			makeMultipoolerID("test-cell", "standby1"),
			makeMultipoolerID("test-cell", "standby2"),
		}
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			NumSync:           2,
			StandbyIds:        standbyIDs,
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for configuration to converge - pg_reload_conf() is asynchronous
		t.Log("Waiting for configuration to converge...")
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.SynchronousCommit == multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY &&
				config.SynchronousMethod == multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY &&
				config.NumSync == 2 &&
				len(config.StandbyIds) == 2
		}, "Configuration should converge to expected values")

		// Get primary status and verify
		statusResp, err := primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
		require.NoError(t, err, "PrimaryStatus should succeed")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify sync replication config is present and correct
		require.NotNil(t, statusResp.Status.SyncReplicationConfig, "Sync replication config should be present")
		assert.Equal(t, multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			statusResp.Status.SyncReplicationConfig.SynchronousCommit, "Synchronous commit level should match")
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			statusResp.Status.SyncReplicationConfig.SynchronousMethod, "Synchronous method should match")
		assert.Equal(t, int32(2), statusResp.Status.SyncReplicationConfig.NumSync, "NumSync should match")
		assert.Len(t, statusResp.Status.SyncReplicationConfig.StandbyIds, 2, "Should have 2 standby IDs")

		// Verify standby IDs
		standbyIDMap := make(map[string]bool)
		for _, id := range statusResp.Status.SyncReplicationConfig.StandbyIds {
			key := fmt.Sprintf("%s_%s", id.Cell, id.Name)
			standbyIDMap[key] = true
		}
		assert.True(t, standbyIDMap["test-cell_standby1"], "standby1 should be in the list")
		assert.True(t, standbyIDMap["test-cell_standby2"], "standby2 should be in the list")

		t.Log("PrimaryStatus with sync replication verified successfully")

		// Cleanup
		clearReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), clearReq)
		require.NoError(t, err)
	})

	t.Run("PrimaryStatus_WithConnectedFollower", func(t *testing.T) {
		t.Log("Testing PrimaryStatus with connected follower...")

		// Ensure standby is connected and replicating
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err := standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err)

		// Wait for replication to be established
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.Status.PrimaryConnInfo != nil &&
				statusResp.Status.PrimaryConnInfo.Host != "" &&
				!statusResp.Status.IsWalReplayPaused
		}, 5*time.Second, 200*time.Millisecond, "Replication should be established")

		// Get primary status
		statusResp, err := primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
		require.NoError(t, err, "PrimaryStatus should succeed")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify followers list contains the standby
		require.NotEmpty(t, statusResp.Status.Followers, "Should have at least one follower")

		// Find our standby in the followers list
		expectedAppName := fmt.Sprintf("test-cell_%s", setup.StandbyMultipooler.ServiceID)
		foundStandby := false
		for _, follower := range statusResp.Status.Followers {
			if follower.Cell == "test-cell" && follower.Name == setup.StandbyMultipooler.ServiceID {
				foundStandby = true
				break
			}
		}
		assert.True(t, foundStandby, "Standby should be in followers list with application_name: %s", expectedAppName)
		t.Logf("Found %d connected follower(s)", len(statusResp.Status.Followers))

		t.Log("PrimaryStatus with connected follower verified successfully")
	})

	t.Run("PrimaryStatus_Standby_Fails", func(t *testing.T) {
		t.Log("Testing PrimaryStatus on REPLICA pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		_, err := standbyManagerClient.PrimaryStatus(ctx, &multipoolermanagerdata.PrimaryStatusRequest{})
		require.Error(t, err, "PrimaryStatus should fail on standby")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on REPLICA")

		t.Log("Confirmed: PrimaryStatus correctly rejected on REPLICA pooler")
	})
}
