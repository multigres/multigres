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

// Package clustersetup holds the end-to-end test harness pieces shared by the
// Multigres topology (shardsetup) and the Minigres topology (minigressetup):
// process management, etcd and topology records, clients, the Cluster interface
// that query-serving tests use, and the MULTIGRES_E2E_TOPOLOGY switch.
package clustersetup

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/tools/pathutil"
	"github.com/multigres/multigres/go/tools/telemetry"
)

const (
	// TestPostgresPassword is the password used for the postgres user in tests.
	TestPostgresPassword = "test_password_123"
)

// GetTestUserDSN returns a DSN for connecting to multigateway as a regular
// test client. Uses DefaultTestUser and TestPostgresPassword.
func GetTestUserDSN(host string, port int, args ...string) string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=postgres %s",
		host, port, DefaultTestUser, TestPostgresPassword, strings.Join(args, " "))
}

// GetPostgresDSN returns a DSN for connecting directly to PostgreSQL as the
// cluster owner (DefaultPostgresUser). Used for pgctld-level test connections
// that bypass multigateway (e.g. backup restore verification).
func GetPostgresDSN(host string, port int, args ...string) string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=postgres %s",
		host, port, constants.DefaultPostgresUser, TestPostgresPassword, strings.Join(args, " "))
}

// RunTestMain runs the test suite with proper environment setup.
// It handles:
//   - Setting up PATH for binaries
//   - Setting environment variables for orphan detection
//   - Initializing telemetry (no-op if OTEL not configured)
//   - Handling signals for graceful shutdown
//
// Cleanup should be handled by the caller via SharedSetupManager.
// Returns the exit code that should be passed to os.Exit().
//
// Example usage in main_test.go:
//
//	func TestMain(m *testing.M) {
//		exitCode := clustersetup.RunTestMain(m)
//		if exitCode != 0 {
//			setupManager.DumpLogs()
//		}
//		setupManager.Cleanup()
//		os.Exit(exitCode)
//	}
func RunTestMain(m *testing.M) int {
	// Set the PATH so dependencies like etcd and run_in_test.sh can be found
	if err := pathutil.PrependBinToPath(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add bin to PATH: %v\n", err)
		return 1
	}

	// Set orphan detection environment variable as baseline protection
	os.Setenv("MULTIGRES_TEST_PARENT_PID", strconv.Itoa(os.Getpid()))

	// Initialize telemetry (no-op if OTEL environment variables aren't set)
	tel := telemetry.NewTelemetry()
	ctx := context.Background()
	if err := tel.InitTelemetry(ctx, "tests"); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Failed to initialize telemetry: %v\n", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := tel.ShutdownTelemetry(shutdownCtx); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Failed to shutdown telemetry: %v\n", err)
		}
	}()

	// Set up signal handler for cleanup on interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Run tests in goroutine so we can select on completion or signal
	exitCodeChan := make(chan int, 1)
	go func() {
		exitCodeChan <- m.Run()
	}()

	// Wait for tests to complete OR signal
	var exitCode int
	select {
	case exitCode = <-exitCodeChan:
		// Tests finished normally
	case <-sigChan:
		// Interrupted - treat as failure
		exitCode = 1
	}

	// Cleanup environment variable
	os.Unsetenv("MULTIGRES_TEST_PARENT_PID")

	return exitCode
}

// SharedSetupManager manages a cluster shared by the tests of a package. The
// cluster is created on first use and cleaned up from TestMain. C is the
// cluster type: a Multigres shard or a Minigres instance.
type SharedSetupManager[C Cluster] struct {
	setup       C
	setupFunc   func(t *testing.T) C
	setupDone   bool
	setupErr    error
	testsFailed bool
}

// NewSharedSetupManager creates a SharedSetupManager that builds its cluster
// with setupFunc on first use.
func NewSharedSetupManager[C Cluster](setupFunc func(t *testing.T) C) *SharedSetupManager[C] {
	return &SharedSetupManager[C]{
		setupFunc: setupFunc,
	}
}

// Get returns the shared cluster, creating it on first use.
func (m *SharedSetupManager[C]) Get(t *testing.T) C {
	t.Helper()

	if m.setupErr != nil {
		t.Fatalf("Failed to setup shared test infrastructure: %v", m.setupErr)
	}

	if !m.setupDone {
		m.setup = m.setupFunc(t)
		m.setupDone = true
	}

	return m.setup
}

// Cleanup cleans up the shared cluster. Call it from TestMain after the tests.
// The temporary directory is deleted only if the tests passed (DumpLogs was not
// called).
func (m *SharedSetupManager[C]) Cleanup() {
	if m.setupDone {
		m.setup.Cleanup(m.testsFailed)
	}
}

// DumpLogs marks the tests as failed and prints the log location. Call it from
// TestMain on failure, before Cleanup. Set TEST_PRINT_LOGS to also print the
// log contents.
func (m *SharedSetupManager[C]) DumpLogs() {
	m.testsFailed = true
	if m.setupDone {
		m.setup.DumpServiceLogs()
	}
}
