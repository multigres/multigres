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

// Package shardsetup provides shared test infrastructure for end-to-end tests.
// It sets up the infrastructure for testing a single shard: multipoolers (pgctld + multipooler pairs)
// and optionally multiorch instances.
package shardsetup

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// ProcessInstance represents a process instance for testing (pgctld, multipooler, or multiorch).
// This struct is extracted from multipooler/setup_test.go and extended for multiorch support.
type ProcessInstance struct {
	Name        string
	DataDir     string // Used by pgctld, multipooler
	ConfigFile  string // Used by pgctld
	LogFile     string
	GrpcPort    int
	PgPort      int    // Used by pgctld
	PgctldAddr  string // Used by multipooler
	EtcdAddr    string // Used by multipooler for topology
	GlobalRoot  string // Topology global root path (used by multipooler, multiorch, multigateway)
	Process     *exec.Cmd
	Binary      string
	Environment []string

	// Multiorch-specific fields
	HttpPort                            int      // HTTP port (used by multiorch for /ready endpoint)
	Cell                                string   // Cell name (used by multipooler and multiorch)
	WatchTargets                        []string // Database/tablegroup/shard targets to watch (multiorch)
	ServiceID                           string   // Service ID (used by multipooler and multiorch)
	PrimaryFailoverGracePeriodBase      string   // Grace period base before primary failover (e.g., "0s", "10s")
	PrimaryFailoverGracePeriodMaxJitter string   // Max jitter for grace period (e.g., "0s", "5s")

	// PgBackRest-specific fields (used by multipooler)
	PgBackRestCertPaths *local.PgBackRestCertPaths // pgBackRest TLS certificate paths
	PgBackRestPort      int                        // pgBackRest server port
}

// Start starts the process instance (pgctld, multipooler, multiorch, or multigateway).
// Follows the proven pattern from multipooler/setup_test.go.
func (p *ProcessInstance) Start(ctx context.Context, t *testing.T) error {
	t.Helper()

	switch p.Binary {
	case "pgctld":
		return p.startPgctld(ctx, t)
	case "multipooler":
		return p.startMultipooler(ctx, t)
	case "multiorch":
		return p.startMultiOrch(ctx, t)
	case "multigateway":
		return p.startMultigateway(ctx, t)
	}
	return fmt.Errorf("unknown binary type: %s", p.Binary)
}

// startPgctld starts a pgctld instance (server only, PostgreSQL init/start done separately).
// Copied from multipooler/setup_test.go.
func (p *ProcessInstance) startPgctld(ctx context.Context, t *testing.T) error {
	t.Helper()

	t.Logf("Starting %s with binary '%s'", p.Name, p.Binary)
	t.Logf("Data dir: %s, gRPC port: %d, PG port: %d", p.DataDir, p.GrpcPort, p.PgPort)

	// Start the gRPC server
	p.Process = exec.Command(p.Binary, "server",
		"--pooler-dir", p.DataDir,
		"--grpc-port", strconv.Itoa(p.GrpcPort),
		"--pg-port", strconv.Itoa(p.PgPort),
		"--timeout", "60",
		"--log-output", p.LogFile)

	// Set MULTIGRES_TESTDATA_DIR for directory-deletion triggered cleanup
	p.Process.Env = append(p.Environment,
		"MULTIGRES_TESTDATA_DIR="+filepath.Dir(p.DataDir),
	)

	t.Logf("Running server command: %v", p.Process.Args)
	if err := p.waitForStartup(ctx, t, 20*time.Second, 50); err != nil {
		return err
	}

	return nil
}

// startMultipooler starts a multipooler instance.
// Copied from multipooler/setup_test.go.
func (p *ProcessInstance) startMultipooler(ctx context.Context, t *testing.T) error {
	t.Helper()

	t.Logf("Starting %s: binary '%s', gRPC port %d, cell %s", p.Name, p.Binary, p.GrpcPort, p.Cell)

	// Build command arguments
	// Socket file path for Unix socket connection (uses trust auth per pg_hba.conf)
	socketFile := filepath.Join(p.DataDir, "pg_sockets", fmt.Sprintf(".s.PGSQL.%d", p.PgPort))
	args := []string{
		"--grpc-port", strconv.Itoa(p.GrpcPort),
		"--database", "postgres", // Required parameter
		"--table-group", "default", // Required parameter (MVP only supports "default")
		"--shard", "0-inf", // Required parameter (MVP only supports "0-inf")
		"--pgctld-addr", p.PgctldAddr,
		"--pooler-dir", p.DataDir, // Use the same pooler dir as pgctld
		"--pg-port", strconv.Itoa(p.PgPort),
		"--socket-file", socketFile, // Unix socket for trust authentication
		"--service-map", "grpc-pooler,grpc-poolermanager,grpc-consensus,grpc-backup",
		"--topo-global-server-addresses", p.EtcdAddr,
		"--topo-global-root", "/multigres/global",
		"--cell", p.Cell,
		"--service-id", p.Name,
		"--hostname", "localhost",
		"--log-output", p.LogFile,
		"--log-level", "debug",
	}

	// Add pgBackRest certificate paths and port if configured
	if p.PgBackRestCertPaths != nil {
		args = append(args,
			"--pgbackrest-cert-file", p.PgBackRestCertPaths.ServerCertFile,
			"--pgbackrest-key-file", p.PgBackRestCertPaths.ServerKeyFile,
			"--pgbackrest-ca-file", p.PgBackRestCertPaths.CACertFile,
		)
	}
	if p.PgBackRestPort > 0 {
		args = append(args, "--pgbackrest-port", strconv.Itoa(p.PgBackRestPort))
	}

	// Start the multipooler server
	p.Process = exec.Command(p.Binary, args...)

	// Set MULTIGRES_TESTDATA_DIR for directory-deletion triggered cleanup
	p.Process.Env = append(p.Environment,
		"MULTIGRES_TESTDATA_DIR="+filepath.Dir(p.DataDir),
	)

	t.Logf("Running multipooler command: %v", p.Process.Args)
	return p.waitForStartup(ctx, t, 15*time.Second, 30)
}

// startMultiOrch starts a multiorch instance.
// Follows the pattern from multiorch/multiorch_helpers.go:startMultiOrch.
func (p *ProcessInstance) startMultiOrch(ctx context.Context, t *testing.T) error {
	t.Helper()

	t.Logf("Starting %s: binary '%s', gRPC port %d, HTTP port %d, service-id %s", p.Name, p.Binary, p.GrpcPort, p.HttpPort, p.ServiceID)

	args := []string{
		"--cell", p.Cell,
		"--service-id", p.ServiceID,
		"--watch-targets", strings.Join(p.WatchTargets, ","),
		"--topo-global-server-addresses", p.EtcdAddr,
		"--topo-global-root", "/multigres/global",
		"--grpc-port", strconv.Itoa(p.GrpcPort),
		"--http-port", strconv.Itoa(p.HttpPort),
		"--hostname", "localhost",
		"--bookkeeping-interval", "2s",
		"--cluster-metadata-refresh-interval", "500ms",
		"--pooler-health-check-interval", "500ms",
		"--recovery-cycle-interval", "500ms",
		"--log-level", "debug",
	}

	// Add grace period flags if configured (defaults to 0 for fast tests)
	if p.PrimaryFailoverGracePeriodBase != "" {
		args = append(args, "--primary-failover-grace-period-base", p.PrimaryFailoverGracePeriodBase)
	}
	if p.PrimaryFailoverGracePeriodMaxJitter != "" {
		args = append(args, "--primary-failover-grace-period-max-jitter", p.PrimaryFailoverGracePeriodMaxJitter)
	}

	p.Process = exec.Command(p.Binary, args...)
	if p.DataDir != "" {
		p.Process.Dir = p.DataDir
	}

	// Set up logging like multiorch_helpers.go does
	if p.LogFile != "" {
		logF, err := os.Create(p.LogFile)
		if err != nil {
			return fmt.Errorf("failed to create log file: %w", err)
		}
		p.Process.Stdout = logF
		p.Process.Stderr = logF
	}

	// Start the process with trace context propagation
	if err := telemetry.StartCmd(ctx, p.Process); err != nil {
		return fmt.Errorf("failed to start multiorch: %w", err)
	}
	t.Logf("Started multiorch (pid: %d, grpc: %d, http: %d, log: %s)",
		p.Process.Process.Pid, p.GrpcPort, p.HttpPort, p.LogFile)

	// Wait for multiorch to be ready (using TCP port check like multiorch_helpers.go)
	if err := WaitForPortReady(t, "multiorch", p.GrpcPort, 15*time.Second); err != nil {
		return err
	}
	return nil
}

// startMultigateway starts a multigateway instance.
func (p *ProcessInstance) startMultigateway(ctx context.Context, t *testing.T) error {
	t.Helper()

	t.Logf("Starting %s: binary '%s', PG port %d, gRPC port %d, HTTP port %d", p.Name, p.Binary, p.PgPort, p.GrpcPort, p.HttpPort)

	args := []string{
		"--cell", p.Cell,
		"--service-id", p.ServiceID,
		"--pg-port", strconv.Itoa(p.PgPort),
		"--pg-bind-address", "127.0.0.1",
		"--topo-global-server-addresses", p.EtcdAddr,
		"--topo-global-root", p.GlobalRoot,
		"--grpc-port", strconv.Itoa(p.GrpcPort),
		"--http-port", strconv.Itoa(p.HttpPort),
		"--hostname", "localhost",
		"--log-level", "debug",
	}

	p.Process = exec.Command(p.Binary, args...)

	// Set MULTIGRES_TESTDATA_DIR for directory-deletion triggered cleanup
	p.Process.Env = append(p.Environment,
		"MULTIGRES_TESTDATA_DIR="+filepath.Dir(p.LogFile),
	)

	// Set up logging
	if p.LogFile != "" {
		logF, err := os.Create(p.LogFile)
		if err != nil {
			return fmt.Errorf("failed to create log file: %w", err)
		}
		p.Process.Stdout = logF
		p.Process.Stderr = logF
	}

	// Start the process with trace context propagation
	if err := telemetry.StartCmd(ctx, p.Process); err != nil {
		return fmt.Errorf("failed to start multigateway: %w", err)
	}
	t.Logf("Started multigateway (pid: %d, pg: %d, grpc: %d, http: %d, log: %s)",
		p.Process.Process.Pid, p.PgPort, p.GrpcPort, p.HttpPort, p.LogFile)

	// Wait for multigateway to be ready (Status RPC check)
	if err := WaitForPortReady(t, "multigateway", p.GrpcPort, 3*time.Second); err != nil {
		return err
	}
	t.Logf("Multigateway is ready")

	return nil
}

// waitForStartup handles the common startup and waiting logic.
// Copied from multipooler/setup_test.go.
func (p *ProcessInstance) waitForStartup(ctx context.Context, t *testing.T, timeout time.Duration, logInterval int) error {
	t.Helper()

	// Start the process in background with trace context propagation
	err := telemetry.StartCmd(ctx, p.Process)
	if err != nil {
		return fmt.Errorf("failed to start %s: %w", p.Name, err)
	}
	t.Logf("%s server process started with PID %d", p.Name, p.Process.Process.Pid)

	// Give the process a moment to potentially fail immediately
	time.Sleep(500 * time.Millisecond)

	// Check if process died immediately
	if p.Process.ProcessState != nil {
		t.Logf("%s process died immediately: exit code %d", p.Name, p.Process.ProcessState.ExitCode())
		p.LogRecentOutput(t, "Process died immediately")
		return fmt.Errorf("%s process died immediately: exit code %d", p.Name, p.Process.ProcessState.ExitCode())
	}

	// Wait for server to be ready
	deadline := time.Now().Add(timeout)
	connectAttempts := 0
	for time.Now().Before(deadline) {
		// Check if process died during startup
		if p.Process.ProcessState != nil {
			t.Logf("%s process died during startup: exit code %d", p.Name, p.Process.ProcessState.ExitCode())
			p.LogRecentOutput(t, "Process died during startup")
			return fmt.Errorf("%s process died: exit code %d", p.Name, p.Process.ProcessState.ExitCode())
		}

		connectAttempts++
		// Test gRPC connectivity
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", p.GrpcPort), 100*time.Millisecond)
		if err == nil {
			conn.Close()
			t.Logf("%s started successfully on gRPC port %d (after %d attempts)", p.Name, p.GrpcPort, connectAttempts)
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
	p.LogRecentOutput(t, "Timeout waiting for server to start")
	return fmt.Errorf("timeout: %s failed to start listening on port %d after %d attempts", p.Name, p.GrpcPort, connectAttempts)
}

// LogRecentOutput logs recent output from the process log file.
// Copied from multipooler/setup_test.go.
func (p *ProcessInstance) LogRecentOutput(t *testing.T, context string) {
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

// Stop stops the process instance.
// Copied from multipooler/setup_test.go.
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

// IsRunning checks if the process is still running.
// Returns false if the process has exited or was never started.
// Copied from multipooler/setup_test.go.
func (p *ProcessInstance) IsRunning() bool {
	if p == nil || p.Process == nil || p.Process.Process == nil {
		return false
	}
	// ProcessState is set after Wait() returns, meaning process has exited
	if p.Process.ProcessState != nil {
		return false
	}
	// Signal 0 checks if process exists without actually sending a signal
	err := p.Process.Process.Signal(syscall.Signal(0))
	return err == nil
}

// StopPostgres stops PostgreSQL via pgctld gRPC (best effort, no error handling).
// Use this to stop postgres before removing data directories for auto-restore tests.
func (p *ProcessInstance) StopPostgres(t *testing.T) {
	t.Helper()
	p.stopPostgreSQL()
}

// stopPostgreSQL stops PostgreSQL via gRPC (best effort, no error handling).
// Copied from multipooler/setup_test.go.
func (p *ProcessInstance) stopPostgreSQL() {
	conn, err := grpc.NewClient(
		fmt.Sprintf("passthrough:///localhost:%d", p.GrpcPort),
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

// TerminateGracefully gracefully terminates a process by first sending SIGTERM,
// waiting for graceful shutdown, and only using SIGKILL if necessary.
// Follows the pattern from multiorch/multiorch_helpers.go:terminateProcess.
func (p *ProcessInstance) TerminateGracefully(t *testing.T, timeout time.Duration) {
	t.Helper()
	if p.Process == nil || p.Process.Process == nil {
		return
	}

	// Try graceful shutdown with SIGTERM first
	if err := p.Process.Process.Signal(os.Interrupt); err != nil {
		t.Logf("Failed to send SIGTERM to %s: %v, forcing kill", p.Name, err)
		_ = p.Process.Process.Kill()
		_ = p.Process.Wait()
		return
	}

	// Wait for graceful shutdown with timeout
	done := make(chan error, 1)
	go func() {
		done <- p.Process.Wait()
	}()

	select {
	case <-time.After(timeout):
		t.Logf("%s did not terminate gracefully within %v, forcing kill", p.Name, timeout)
		_ = p.Process.Process.Kill()
		<-done // Wait for process to actually die
	case err := <-done:
		if err != nil {
			t.Logf("%s terminated with error: %v", p.Name, err)
		} else {
			t.Logf("%s terminated gracefully", p.Name)
		}
	}
}

// CleanupFunc returns a cleanup function that gracefully terminates the process.
// Use this to get a cleanup function for an existing ProcessInstance.
func (p *ProcessInstance) CleanupFunc(t *testing.T) func() {
	return func() { p.TerminateGracefully(t, 5*time.Second) }
}

// WaitForPortReady waits for a process to be ready by checking its gRPC port.
// Follows the pattern from multiorch/multiorch_helpers.go:waitForProcessReady.
func WaitForPortReady(t *testing.T, name string, grpcPort int, timeout time.Duration) error {
	t.Helper()

	deadline := time.Now().Add(timeout)
	connectAttempts := 0
	for time.Now().Before(deadline) {
		connectAttempts++
		// Test gRPC connectivity
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", grpcPort), 100*time.Millisecond)
		if err == nil {
			conn.Close()
			t.Logf("%s ready on gRPC port %d (after %d attempts)", name, grpcPort, connectAttempts)
			return nil
		}
		if connectAttempts%10 == 0 {
			t.Logf("Still waiting for %s to start (attempt %d)...", name, connectAttempts)
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("timeout: %s failed to start listening on port %d after %d attempts", name, grpcPort, connectAttempts)
}
