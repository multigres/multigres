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
	"github.com/multigres/multigres/go/tools/pathutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

var (
	// Shared test infrastructure
	sharedTestSetup *MultipoolerTestSetup
	setupOnce       sync.Once
	setupError      error
)

// ProcessInstance represents a process instance for testing (pgctld or multipooler)
type ProcessInstance struct {
	Name        string
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
	Cleanup            func()
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

// startPgctld starts a pgctld instance
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

	grpcAddr := fmt.Sprintf("localhost:%d", p.GrpcPort)
	if err := InitAndStartPostgreSQL(t, grpcAddr); err != nil {
		return fmt.Errorf("failed to initialize and start PostgreSQL: %w", err)
	}

	return nil
}

// startMultipooler starts a multipooler instance
func (p *ProcessInstance) startMultipooler(t *testing.T) error {
	t.Helper()

	t.Logf("Starting %s: binary '%s', gRPC port %d", p.Name, p.Binary, p.GrpcPort)

	// Start the multipooler server
	p.Process = exec.Command(p.Binary,
		"--grpc-port", strconv.Itoa(p.GrpcPort),
		"--database", "postgres", // Required parameter
		"--table-group", "test", // Required parameter
		"--pgctld-addr", p.PgctldAddr,
		"--pooler-dir", p.DataDir, // Use the same pooler dir as pgctld
		"--pg-port", strconv.Itoa(p.PgPort),
		"--service-map", "grpc-poolerquery,grpc-poolermanager",
		"--topo-global-server-addresses", p.EtcdAddr,
		"--topo-global-root", "/multigres/global",
		"--topo-implementation", "etcd2",
		"--cell", "test-cell",
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
	if p.Process != nil && p.Process.ProcessState == nil {
		_ = p.Process.Process.Kill()
		_ = p.Process.Wait()
	}
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

		tempDir, cleanup := testutil.TempDir(t, "multipooler_shared_test")

		// Start etcd for topology
		t.Logf("Starting etcd for topology...")
		etcdClientAddr, etcdCmd := etcdtopo.StartEtcd(t, 0)

		// Create topology server and cell
		testRoot := "/multigres"
		globalRoot := path.Join(testRoot, "global")
		cellName := "test-cell"
		cellRoot := path.Join(testRoot, cellName)

		ts, err := topo.OpenServer("etcd2", globalRoot, []string{etcdClientAddr})
		if err != nil {
			_ = etcdCmd.Process.Kill()
			cleanup()
			setupError = fmt.Errorf("failed to open topology server: %w", err)
			return
		}

		// Create the cell
		err = ts.CreateCell(context.Background(), cellName, &clustermetadatapb.Cell{
			ServerAddresses: []string{etcdClientAddr},
			Root:            cellRoot,
		})
		if err != nil {
			ts.Close()
			_ = etcdCmd.Process.Kill()
			cleanup()
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

		// Start pgctld instances
		if err := primaryPgctld.Start(t); err != nil {
			cleanup()
			setupError = fmt.Errorf("failed to start primary pgctld: %w", err)
			return
		}

		if err := standbyPgctld.Start(t); err != nil {
			primaryPgctld.Stop()
			cleanup()
			setupError = fmt.Errorf("failed to start standby pgctld: %w", err)
			return
		}

		// Start multipooler instances
		if err := primaryMultipooler.Start(t); err != nil {
			standbyPgctld.Stop()
			primaryPgctld.Stop()
			cleanup()
			setupError = fmt.Errorf("failed to start primary multipooler: %w", err)
			return
		}

		if err := standbyMultipooler.Start(t); err != nil {
			primaryMultipooler.Stop()
			standbyPgctld.Stop()
			primaryPgctld.Stop()
			cleanup()
			setupError = fmt.Errorf("failed to start standby multipooler: %w", err)
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
			Cleanup: func() {
				standbyMultipooler.Stop()
				primaryMultipooler.Stop()
				standbyPgctld.Stop()
				primaryPgctld.Stop()
				ts.Close()
				_ = etcdCmd.Process.Kill()
				cleanup()
			},
		}

		t.Logf("Shared test infrastructure started successfully")
	})

	if setupError != nil {
		t.Fatalf("Failed to setup shared test infrastructure: %v", setupError)
	}

	return sharedTestSetup
}

// cleanupSharedResources cleans up shared test resources (called by existing TestMain)
func cleanupSharedResources() {
	if sharedTestSetup != nil {
		sharedTestSetup.Cleanup()
	}
}

// TestMultipoolerReplicationApi tests the replication API functionality
func TestMultipoolerReplicationApi(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	t.Run("PrimaryPosition_Primary", func(t *testing.T) {
		// Connect to primary multipooler
		conn, err := grpc.NewClient(
			fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		defer conn.Close()

		client := multipoolermanagerpb.NewMultiPoolerManagerClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

		t.Logf("PrimaryPosition returned LSN: %s", resp.LsnPosition)
		// TODO: Once we have methods to change the server to a standby, we can
		// test that PrimaryPosition returns an error when the server is a standby.
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		req := &multipoolermanagerdata.PrimaryPositionRequest{}
		_, err = client.PrimaryPosition(ctx, req)

		// Assert that it succeeds and returns a valid LSN
		// TODO: This should error because this is not yet a real standby.
		require.NoError(t, err)

	})
}
