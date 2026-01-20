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

package queryserving

import (
	"os"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// setupManager manages the shared test setup for tests in this package.
var setupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	// Create a 2-node cluster for testing (primary + standby)
	// We only use the primary for transaction tests, but shardsetup requires 2 nodes for bootstrap
	return shardsetup.New(t, shardsetup.WithMultipoolerCount(2))
})

// TestMain sets the path and cleans up after all tests.
func TestMain(m *testing.M) {
	exitCode := shardsetup.RunTestMain(m)
	if exitCode != 0 {
		setupManager.DumpLogs()
	}
	setupManager.Cleanup()
	os.Exit(exitCode) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}

// getSharedSetup returns the shared setup for tests.
func getSharedSetup(t *testing.T) *shardsetup.ShardSetup {
	t.Helper()
	return setupManager.Get(t)
}
