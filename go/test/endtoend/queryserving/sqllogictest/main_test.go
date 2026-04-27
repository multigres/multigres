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

// Package sqllogictest runs sqllogictest-rs against two targets (a standalone
// PostgreSQL and a Multigres multigateway) and fails on divergence. See the
// package README for rationale, runner choice, and how to expand the corpus.
package sqllogictest

import (
	"os"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// setupManager lazily spins up a 2-multipooler cluster with a multigateway
// the first time a test asks for it. The cluster is shared across any
// sub-tests in this package. A separate standalone PostgreSQL is started in
// the test function itself (not the manager) because it depends on the
// PostgresBuilder output directory.
var setupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	return shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby
		shardsetup.WithMultigateway(),
	)
})

func TestMain(m *testing.M) {
	exitCode := shardsetup.RunTestMain(m)
	if exitCode != 0 {
		setupManager.DumpLogs()
	}
	setupManager.Cleanup()
	os.Exit(exitCode) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}

func getSharedSetup(t *testing.T) *shardsetup.ShardSetup {
	t.Helper()
	return setupManager.Get(t)
}
