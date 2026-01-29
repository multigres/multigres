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
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/tools/pathutil"
)

// TestMain sets the path and cleans up after all tests
func TestMain(m *testing.M) {
	// Set the PATH so etcd and orphan detection scripts can be found
	// Use automatic module root detection instead of hard-coded relative paths
	if err := pathutil.PrependBinToPath(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add directories to PATH: %v\n", err)
		os.Exit(1) //nolint:forbidigo // TestMain() is allowed to call os.Exit
	}

	// Set orphan detection environment variable so postgres processes
	// started by in-process services will have watchdogs that monitor
	// the test process and kill postgres if the test crashes
	os.Setenv("MULTIGRES_TEST_PARENT_PID", strconv.Itoa(os.Getpid()))

	// Run all tests
	exitCode := shardsetup.RunTestMain(m)

	// Cleanup environment variable
	os.Unsetenv("MULTIGRES_TEST_PARENT_PID")

	// Exit with the test result code
	os.Exit(exitCode) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}
