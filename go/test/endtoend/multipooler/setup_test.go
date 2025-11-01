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
	"strings"
	"sync"
	"syscall"
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

// cleanupOrphanedProcesses finds and kills any orphaned test processes from previous runs.
// This handles the case where a test was killed ungracefully (SIGKILL, crash, etc.)
// and left processes running.
func cleanupOrphanedProcesses() {
	// Look for processes with our temp directory pattern in their command line
	// This catches etcd, pgctld, multipooler, and postgres processes from previous test runs
	patterns := []string{
		"multipooler_shared_test", // Our temp dir pattern
		"multigres_shared_test",   // etcd name pattern
	}

	for _, pattern := range patterns {
		cmd := exec.Command("pgrep", "-f", pattern)
		output, err := cmd.Output()
		if err != nil {
			// pgrep returns exit code 1 if no processes found, which is fine
			continue
		}

		// Parse PIDs from output
		pidStrs := string(output)
		if pidStrs == "" {
			continue
		}

		// Split into lines and kill each PID
		lines := strings.Split(strings.TrimSpace(pidStrs), "\n")
		for _, pidStr := range lines {
			pidStr = strings.TrimSpace(pidStr)
			if pidStr == "" {
				continue
			}

			pid, err := strconv.Atoi(pidStr)
			if err != nil {
				continue
			}

			// Don't kill ourselves!
			if pid == os.Getpid() {
				continue
			}

			// Try to kill the orphaned process
			// Use negative PID to kill process group if it's a group leader
			_ = syscall.Kill(-pid, syscall.SIGTERM)
			// Also try positive PID in case it's not a group leader
			_ = syscall.Kill(pid, syscall.SIGTERM)
		}

		// Give processes a moment to exit
		time.Sleep(1 * time.Second)

		// Force kill any that didn't respond to SIGTERM
		for _, pidStr := range lines {
			pidStr = strings.TrimSpace(pidStr)
			if pidStr == "" {
				continue
			}

			pid, err := strconv.Atoi(pidStr)
			if err != nil {
				continue
			}

			if pid == os.Getpid() {
				continue
			}

			_ = syscall.Kill(-pid, syscall.SIGKILL)
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
}

// TestMain sets the path and cleans up after all tests
func TestMain(m *testing.M) {
	// Set the PATH so etcd can be found
	pathutil.PrependPath("../../../../bin")

	// Clean up any orphaned processes from previous crashed test runs
	cleanupOrphanedProcesses()

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

	// Stop etcd (kill entire process group)
	if sharedTestSetup.EtcdCmd != nil && sharedTestSetup.EtcdCmd.Process != nil {
		killProcessGroup(sharedTestSetup.EtcdCmd.Process.Pid, 3*time.Second, sharedTestSetup.EtcdCmd)
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
		"--log-output", p.LogFile)
	p.Process.Env = p.Environment
	// Create new process group so we can kill all descendants (postgres + children)
	p.Process.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

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
		"--log-output", p.LogFile)
	p.Process.Env = p.Environment
	// Create new process group so we can kill all descendants
	p.Process.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

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

// killProcessGroup kills a process group (the process and all its descendants).
// Sends SIGTERM first, then SIGKILL if the process doesn't exit within the timeout.
// The cmd parameter is used to wait for the process to exit.
func killProcessGroup(pid int, timeout time.Duration, cmd *exec.Cmd) {
	// Send SIGTERM to the entire process group (negative PID kills the group)
	_ = syscall.Kill(-pid, syscall.SIGTERM)

	// Wait for process to exit with timeout
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-done:
		// Process exited cleanly
		return
	case <-time.After(timeout):
		// Process didn't exit in time, force kill the entire group
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		_ = cmd.Wait()
	}
}

// Stop stops the process instance and all its descendants
func (p *ProcessInstance) Stop() {
	if p.Process == nil || p.Process.ProcessState != nil {
		return // Process not running
	}

	// If this is pgctld, try graceful PostgreSQL shutdown first via gRPC
	if p.Binary == "pgctld" {
		p.stopPostgreSQL()
	}

	// Kill the process group (process + all descendants)
	killProcessGroup(p.Process.Process.Pid, 5*time.Second, p.Process)
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

	cmd := exec.Command("etcd",
		"-name", name,
		"-advertise-client-urls", clientAddr,
		"-initial-advertise-peer-urls", peerAddr,
		"-listen-client-urls", clientAddr,
		"-listen-peer-urls", peerAddr,
		"-initial-cluster", initialCluster,
		"-data-dir", dataDir)
	// Create new process group so we can kill all descendants
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

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

// validateGUCValue queries a GUC and fails the test if it doesn't match the expected value.
func validateGUCValue(t *testing.T, client *endtoend.MultiPoolerTestClient, gucName, expected, instanceName string) {
	t.Helper()
	value, err := queryStringValue(context.Background(), client, fmt.Sprintf("SHOW %s", gucName))
	if err == nil && value != expected {
		t.Fatalf("setupPoolerTest: %s has %s='%s' (expected '%s'). "+
			"Previous test leaked state. Make sure all subtests call setupPoolerTest().",
			instanceName, gucName, value, expected)
	}
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
//   - Primary: primary_conninfo=”, synchronous_standby_names=”
//   - Standby: pg_is_in_recovery=true, primary_conninfo=”, pg_is_wal_replay_paused=false
//
// This catches state leaks from tests that don't call setupPoolerTest().
func validateCleanState(t *testing.T, setup *MultipoolerTestSetup) {
	t.Helper()
	if setup == nil {
		return
	}

	// Validate primary state
	if setup.PrimaryMultipooler != nil {
		primaryClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		if err == nil {
			defer primaryClient.Close()
			validateGUCValue(t, primaryClient, "primary_conninfo", "", "Primary")
			validateGUCValue(t, primaryClient, "synchronous_standby_names", "", "Primary")
		}
	}

	// Validate standby state
	if setup.StandbyMultipooler != nil {
		standbyClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort))
		if err == nil {
			defer standbyClient.Close()

			// Verify standby is in recovery mode
			inRecovery, err := queryStringValue(context.Background(), standbyClient, "SELECT pg_is_in_recovery()")
			if err == nil && inRecovery != "true" {
				t.Fatalf("setupPoolerTest: Standby pg_is_in_recovery=%s (expected true). "+
					"Previous test leaked state. Make sure all subtests call setupPoolerTest().", inRecovery)
			}

			// Verify replication not configured and WAL replay not paused
			validateGUCValue(t, standbyClient, "primary_conninfo", "", "Standby")

			isPaused, err := queryStringValue(context.Background(), standbyClient, "SELECT pg_is_wal_replay_paused()")
			if err == nil && isPaused != "false" {
				t.Fatalf("setupPoolerTest: Standby pg_is_wal_replay_paused=%s (expected false). "+
					"Previous test leaked state. Make sure all subtests call setupPoolerTest().", isPaused)
			}
		}
	}
}

// setupPoolerTest registers cleanup functions based on the provided options.
//
// DEFAULT BEHAVIOR (no options):
//   - Configures replication: Sets primary_conninfo, standby connects to primary
//   - Starts WAL streaming: WAL receiver active, WAL replay applying changes
//   - Disables synchronous replication (safe - writes won't hang)
//   - Saves/restores: synchronous_standby_names, synchronous_commit, primary_conninfo
//   - At cleanup: restores to the saved state, resumes WAL replay if paused
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
	validateCleanState(t, setup)

	// Determine if we should configure replication (default: yes, unless WithoutReplication)
	shouldConfigureReplication := !config.noReplication

	// If configuring replication and no GUCs specified yet, add the default replication GUCs
	if shouldConfigureReplication && len(config.gucsToReset) == 0 {
		config.gucsToReset = append(config.gucsToReset, "synchronous_standby_names", "synchronous_commit", "primary_conninfo")
	}

	// Save GUC values BEFORE configuring replication (so we save the clean state)
	// This way cleanup will restore to clean state
	var savedPrimaryGucs, savedStandbyGucs map[string]string

	if len(config.gucsToReset) > 0 && setup != nil && setup.PrimaryMultipooler != nil {
		// Save GUC values from primary
		if primaryClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort)); err == nil {
			savedPrimaryGucs = saveGUCs(primaryClient, config.gucsToReset)
			primaryClient.Close()
		}

		// Save GUC values from standby
		if setup.StandbyMultipooler != nil {
			if standbyClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort)); err == nil {
				savedStandbyGucs = saveGUCs(standbyClient, config.gucsToReset)
				standbyClient.Close()
			}
		}
	}

	// Configure replication if needed (after saving GUCs)
	if shouldConfigureReplication {
		if setup == nil || setup.StandbyMultipooler == nil || setup.PrimaryPgctld == nil {
			t.Log("Test setup: Cannot configure replication (setup or multipoolers are nil)")
		} else {
			t.Log("Test setup: Configuring replication (setting primary_conninfo, starting WAL streaming)...")

			// SAFETY: Disable synchronous replication to prevent write hangs
			primaryPoolerClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
			if err == nil {
				_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "ALTER SYSTEM SET synchronous_standby_names = ''", 0)
				if err == nil {
					_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_reload_conf()", 0)
					if err == nil {
						t.Log("Test setup: Disabled synchronous replication for safety (prevents write hangs)")
					}
				}
				if err != nil {
					t.Logf("Warning: Failed to disable synchronous replication: %v", err)
				}
				primaryPoolerClient.Close()
			}

			standbyConn, err := grpc.NewClient(
				fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err == nil {
				standbyClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)

				// Set consensus term
				_, err = standbyClient.SetTerm(utils.WithShortDeadline(t), &multipoolermanagerdatapb.SetTermRequest{
					Term: &multipoolermanagerdatapb.ConsensusTerm{
						CurrentTerm: 1,
					},
				})
				if err != nil {
					t.Logf("Warning: Failed to set term on standby: %v", err)
				}

				// Configure replication
				setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
					Host:                  "localhost",
					Port:                  int32(setup.PrimaryPgctld.PgPort),
					StartReplicationAfter: true,
					StopReplicationBefore: false,
					CurrentTerm:           1,
					Force:                 false,
				}
				ctxSetPrimary, cancelSetPrimary := context.WithTimeout(context.Background(), 5*time.Second)
				_, err = standbyClient.SetPrimaryConnInfo(ctxSetPrimary, setPrimaryReq)
				cancelSetPrimary()
				standbyConn.Close()

				if err != nil {
					t.Logf("Warning: Failed to configure replication: %v", err)
				} else {
					// Wait for replication to actually start streaming before the test begins
					standbyPoolerClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort))
					if err == nil {
						defer standbyPoolerClient.Close()

						require.Eventually(t, func() bool {
							resp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT status FROM pg_stat_wal_receiver", 1)
							if err != nil || len(resp.Rows) == 0 {
								return false
							}
							return string(resp.Rows[0].Values[0]) == "streaming"
						}, 5*time.Second, 100*time.Millisecond, "Replication should be streaming after setup")

						if config.pauseReplication {
							// Pause WAL replay (WAL receiver keeps streaming, but changes aren't applied)
							_, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_wal_replay_pause()", 1)
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
			} else {
				t.Logf("Warning: Failed to connect to standby to configure replication: %v", err)
			}
		}
	}

	// Register cleanup handler
	t.Cleanup(func() {
		// Early return if setup is nil or multipoolers are nil
		if setup == nil || setup.PrimaryMultipooler == nil {
			return
		}

		// Step 1: Restore GUC values if specified (must be done first to fix replication)
		if len(savedPrimaryGucs) > 0 {
			if primaryClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort)); err == nil {
				defer primaryClient.Close()
				restoreGUCs(t, primaryClient, savedPrimaryGucs, "primary")
			} else {
				t.Logf("Warning: Failed to connect to primary for GUC restoration: %v", err)
			}
		}

		if len(savedStandbyGucs) > 0 && setup.StandbyMultipooler != nil {
			if standbyClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort)); err == nil {
				defer standbyClient.Close()
				restoreGUCs(t, standbyClient, savedStandbyGucs, "standby")
			} else {
				t.Logf("Warning: Failed to connect to standby for GUC restoration: %v", err)
			}
		}

		// Step 2: Always resume WAL replay (must be after GUC restoration)
		// This ensures we leave the system in a good state even if tests paused replay
		// We always do this regardless of WithoutReplication flag because pause state persists
		// NOTE: We don't wait for streaming because we just restored GUCs to clean state
		// (primary_conninfo=''), so there's no replication source configured.
		if setup.StandbyMultipooler != nil {
			standbyClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort))
			if err == nil {
				defer standbyClient.Close()

				_, err = standbyClient.ExecuteQuery(context.Background(), "SELECT pg_wal_replay_resume()", 1)
				if err != nil {
					t.Logf("Cleanup: Failed to resume WAL replay: %v", err)
				}
			} else {
				t.Logf("Warning: Failed to connect to standby to resume WAL replay: %v", err)
			}
		}

		// Step 3: Drop tables if specified (must be last, requires working replication)
		if len(config.tablesToDrop) > 0 {
			primaryClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
			if err == nil {
				defer primaryClient.Close()

				for _, table := range config.tablesToDrop {
					_, err = primaryClient.ExecuteQuery(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table), 1)
					if err != nil {
						t.Logf("Warning: Failed to drop table %s in cleanup: %v", table, err)
					}
				}
			} else {
				t.Logf("Warning: Failed to connect to primary for table cleanup: %v", err)
			}
		}
	})
}
