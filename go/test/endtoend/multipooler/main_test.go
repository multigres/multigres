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
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/multigres/multigres/go/tools/pathutil"
)

const (
	// testPostgresPassword is the password used for the postgres user in tests.
	// This is set via PGPASSWORD env var before pgctld initializes PostgreSQL.
	testPostgresPassword = "test_password_123"
)

// TestMain sets the path and cleans up after all tests
func TestMain(m *testing.M) {
	// Set the PATH so dependencies like etcd and run_in_test.sh can be found
	// Use automatic module root detection instead of hard-coded relative paths
	if err := pathutil.PrependBinToPath(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add bin to PATH: %v\n", err)
		os.Exit(1) //nolint:forbidigo // TestMain() is allowed to call os.Exit
	}

	// Set orphan detection environment variable as baseline protection.
	// This ensures postgres processes started by in-process services will
	// have watchdogs that monitor the test process and kill postgres if
	// the test crashes. Individual tests can additionally set
	// MULTIGRES_TESTDATA_DIR for directory-deletion triggered cleanup.
	os.Setenv("MULTIGRES_TEST_PARENT_PID", fmt.Sprintf("%d", os.Getpid()))

	// Set PGPASSWORD to a known value so tests can authenticate.
	// pgctld uses this when initializing PostgreSQL.
	os.Setenv("PGPASSWORD", testPostgresPassword)

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

	// Single cleanup path for both normal completion and interrupt
	if exitCode != 0 {
		dumpServiceLogs()
	}

	// Clean up shared multipooler test infrastructure
	cleanupSharedTestSetup()

	// Cleanup environment variable
	os.Unsetenv("MULTIGRES_TEST_PARENT_PID")

	// Exit with the test result code
	os.Exit(exitCode) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}
