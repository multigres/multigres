// Copyright 2026 Supabase, Inc.
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

package benchmarking

import (
	"os"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// setupManager manages the shared test setup for benchmark tests.
//
// Debug logging is disabled here because profiling showed a non-trivial fraction
// of multigateway/multipooler CPU time was spent inside the slog handler under
// the pgbench workload. Suppressing debug+info keeps measurement noise out of
// the hot path while still surfacing warnings/errors if something misbehaves.
var setupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	return shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby
		shardsetup.WithMultigateway(),      // enable multigateway
		shardsetup.WithLogLevel("warn"),
	)
})

// TestMain sets up and tears down the shared cluster.
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
