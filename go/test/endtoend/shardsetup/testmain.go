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

// Package shardsetup provides shared test infrastructure for end-to-end tests of a single shard.
//
// # Overview
//
// This package provides a ShardSetup struct that manages the infrastructure for testing
// a PostgreSQL shard: multipoolers (pgctld + multipooler pairs) and optionally multiorch instances.
//
// # Usage
//
// To use this package, follow these steps:
//
// 1. Create a main_test.go file in your test package with TestMain:
//
//	package yourpackage
//
//	import (
//		"os"
//		"testing"
//
//		"github.com/multigres/multigres/go/test/endtoend/shardsetup"
//	)
//
//	var sharedSetup *shardsetup.ShardSetup
//
//	func TestMain(m *testing.M) {
//		exitCode := shardsetup.RunTestMain(m, func(t *testing.T) *shardsetup.ShardSetup {
//			sharedSetup = shardsetup.New(t,
//				shardsetup.WithMultipoolerCount(2), // primary + standby
//				shardsetup.WithMultiOrchCount(0),   // no multiorch for basic tests
//			)
//			return sharedSetup
//		})
//		os.Exit(exitCode)
//	}
//
// 2. In your tests, use the shared setup:
//
//	func TestSomething(t *testing.T) {
//		sharedSetup.SetupTest(t) // Validates clean state and registers cleanup
//
//		// Your test code here
//		client := sharedSetup.GetPrimaryClient(t)
//		defer client.Close()
//		// ...
//	}
//
// # Naming Convention
//
// Multipooler instances are named by index:
//   - Index 0: "primary"
//   - Index 1: "standby"
//   - Index 2+: "standby2", "standby3", etc.
//
// Access instances by name:
//
//	setup.GetMultipoolerInstance("primary")
//	setup.GetMultipoolerInstance("standby")
//	setup.PrimaryMultipooler() // shorthand for GetMultipooler("primary")
//	setup.StandbyMultipooler() // shorthand for GetMultipooler("standby")
//
// # Test Isolation
//
// The SetupTest method provides test isolation:
//   - Validates all nodes are in expected clean state before test
//   - Registers cleanup handler to reset state after test
//   - Clean state means: terms=1, types=PRIMARY/REPLICA, GUCs reset, WAL replay active
//
// # Replication
//
// By default, standbys are created in recovery mode but NOT actively replicating.
// To configure replication during a test:
//
//	setup.ConfigureReplication(t, "standby")
//
// This allows tests to set up replication from scratch if needed.
package shardsetup

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/multigres/multigres/go/tools/pathutil"
)

const (
	// TestPostgresPassword is the password used for the postgres user in tests.
	// This is set via PGPASSWORD env var before pgctld initializes PostgreSQL.
	TestPostgresPassword = "test_password_123"
)

// SetupFunc is a function that creates a ShardSetup for testing.
// It receives a testing.T that can be used for logging during setup.
type SetupFunc func(t *testing.T) *ShardSetup

// RunTestMain runs the test suite with proper environment setup.
// It handles:
//   - Setting up PATH for binaries
//   - Setting environment variables for orphan detection
//   - Handling signals for graceful shutdown
//
// Cleanup should be handled by the caller via SharedSetupManager.
// Returns the exit code that should be passed to os.Exit().
//
// Example usage in main_test.go:
//
//	func TestMain(m *testing.M) {
//		exitCode := shardsetup.RunTestMain(m)
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
	os.Setenv("MULTIGRES_TEST_PARENT_PID", fmt.Sprintf("%d", os.Getpid()))

	// Set PGPASSWORD to a known value so tests can authenticate
	os.Setenv("PGPASSWORD", TestPostgresPassword)

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

// SharedSetupManager manages a shared ShardSetup across tests.
// Use this when you want to share setup between tests using sync.Once pattern.
type SharedSetupManager struct {
	setup     *ShardSetup
	setupFunc SetupFunc
	setupDone bool
	setupErr  error
}

// NewSharedSetupManager creates a new SharedSetupManager.
func NewSharedSetupManager(setupFunc SetupFunc) *SharedSetupManager {
	return &SharedSetupManager{
		setupFunc: setupFunc,
	}
}

// Get returns the shared setup, creating it if necessary.
// This is safe to call from multiple tests - the setup is created once.
func (m *SharedSetupManager) Get(t *testing.T) *ShardSetup {
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

// Cleanup cleans up the shared setup.
// Call this from TestMain after tests complete.
func (m *SharedSetupManager) Cleanup() {
	if m.setup != nil {
		m.setup.Cleanup()
	}
}

// DumpLogs dumps service logs if tests failed.
// Call this from TestMain before cleanup.
func (m *SharedSetupManager) DumpLogs() {
	if m.setup != nil {
		m.setup.DumpServiceLogs()
	}
}
