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
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/test/endtoend"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/pathutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/pb/pgctldservice"

	// Register topo plugins
	_ "github.com/multigres/multigres/go/plugins/topo"
)

var (
	// Shared test infrastructure
	sharedTestSetup *MultipoolerTestSetup
	setupOnce       sync.Once
	setupError      error
)

// TestMain sets the path and cleans up after all tests
func TestMain(m *testing.M) {
	// Set the PATH so etcd and run_in_test.sh can be found
	pathutil.PrependPath("../../../../bin")
	pathutil.PrependPath("../")  // Add test/endtoend directory for run_in_test.sh

	// Run all tests
	exitCode := m.Run()

	// Clean up shared multipooler test infrastructure
	cleanupSharedTestSetup()

	// Exit with the test result code
	os.Exit(exitCode)
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
		"--log-output", p.LogFile,
		"--test-orphan-detection")
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
		"--service-map", "grpc-pooler,grpc-poolermanager,grpc-consensus",
		"--topo-global-server-addresses", p.EtcdAddr,
		"--topo-global-root", "/multigres/global",
		"--topo-implementation", "etcd2",
		"--cell", "test-cell",
		"--service-id", p.ServiceID,
		"--log-output", p.LogFile,
		"--test-orphan-detection")
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
	if err := endtoend.InitAndStartPostgreSQL(t, primaryGrpcAddr); err != nil {
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
	initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
		CurrentTerm:  1,
		VotedFor:     nil,
		LastVoteTime: nil,
		LeaderId:     nil,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
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
func initializeStandby(t *testing.T, primaryPgctld *ProcessInstance, standbyPgctld *ProcessInstance, standbyMultipooler *ProcessInstance) error {
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

	// Configure standby as a replica using pg_basebackup
	t.Logf("Configuring standby as replica of primary...")
	setupStandbyReplication(t, primaryPgctld, standbyPgctld)

	// Start standby PostgreSQL (now configured as replica)
	if err := endtoend.StartPostgreSQL(t, standbyGrpcAddr); err != nil {
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
	initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
		CurrentTerm:  1,
		VotedFor:     nil,
		LastVoteTime: nil,
		LeaderId:     nil,
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
		// Use PrependPath to ensure our project binaries take precedence over system ones
		pathutil.PrependPath("../../../../bin")

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
		etcdClientAddr, etcdCmd, err := startEtcdForSharedSetup(etcdDataDir)
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

		// Generate ports for shared instances using systematic allocation to avoid conflicts
		primaryGrpcPort := utils.GetNextPort()
		primaryPgPort := utils.GetNextPort()
		standbyGrpcPort := utils.GetNextPort()
		standbyPgPort := utils.GetNextPort()
		primaryMultipoolerPort := utils.GetNextPort()
		standbyMultipoolerPort := utils.GetNextPort()

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
func startEtcdForSharedSetup(dataDir string) (string, *exec.Cmd, error) {
	// Check if etcd is available in PATH
	_, err := exec.LookPath("etcd")
	if err != nil {
		return "", nil, fmt.Errorf("etcd not found in PATH: %w", err)
	}

	// Get port for etcd using the same mechanism as other tests to avoid conflicts
	port := utils.GetNextEtcd2Port()

	name := "multigres_shared_test"
	clientAddr := fmt.Sprintf("http://localhost:%v", port)
	peerAddr := fmt.Sprintf("http://localhost:%v", port+1)
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

		req := &multipoolermanagerdatapb.StatusRequest{}
		resp, err := client.Status(ctx, req)
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

// setupReplicationTestCleanup registers a cleanup function that resets replication configuration
// on both primary and standby using raw SQL to ensure cleanup is independent of implementation bugs
func setupReplicationTestCleanup(t *testing.T, setup *MultipoolerTestSetup) {
	t.Cleanup(func() {
		// Early return if setup is nil or multipoolers are nil
		if setup == nil || setup.PrimaryMultipooler == nil || setup.StandbyMultipooler == nil {
			t.Log("Cleanup: Skipping replication config reset (setup or multipoolers are nil)")
			return
		}

		t.Log("Cleanup: Resetting replication configuration via SQL...")

		// Reset primary: clear synchronous replication settings
		primaryClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		if err == nil {
			defer primaryClient.Close()

			// Reset synchronous_standby_names to empty
			_, err = primaryClient.ExecuteQuery(context.Background(), "ALTER SYSTEM SET synchronous_standby_names = ''", 1)
			if err != nil {
				t.Logf("Warning: Failed to reset synchronous_standby_names on primary in cleanup: %v", err)
			}

			// Reset synchronous_commit to default (on)
			_, err = primaryClient.ExecuteQuery(context.Background(), "ALTER SYSTEM SET synchronous_commit = 'on'", 1)
			if err != nil {
				t.Logf("Warning: Failed to reset synchronous_commit on primary in cleanup: %v", err)
			}

			// Reload configuration to apply changes
			_, err = primaryClient.ExecuteQuery(context.Background(), "SELECT pg_reload_conf()", 1)
			if err != nil {
				t.Logf("Warning: Failed to reload config on primary in cleanup: %v", err)
			}
		} else {
			t.Logf("Warning: Failed to connect to primary in cleanup: %v", err)
		}

		// Reset standby: clear primary_conninfo
		standbyClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort))
		if err == nil {
			defer standbyClient.Close()

			// Reset primary_conninfo to empty
			_, err = standbyClient.ExecuteQuery(context.Background(), "ALTER SYSTEM SET primary_conninfo = ''", 1)
			if err != nil {
				t.Logf("Warning: Failed to reset primary_conninfo on standby in cleanup: %v", err)
			}

			// Reload configuration to apply changes
			_, err = standbyClient.ExecuteQuery(context.Background(), "SELECT pg_reload_conf()", 1)
			if err != nil {
				t.Logf("Warning: Failed to reload config on standby in cleanup: %v", err)
			}
		} else {
			t.Logf("Warning: Failed to connect to standby in cleanup: %v", err)
		}
	})
}
