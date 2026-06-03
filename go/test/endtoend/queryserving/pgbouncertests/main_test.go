// Copyright 2026 Supabase, Inc.
// Portions derived from PgBouncer (ISC License),
// Copyright (c) 2007-2009 Marko Kreen, Skype Technologies OÜ.
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

// Package pgbouncertests ports the proxy-generic, stateful pooling/resilience
// scenarios from PgBouncer's test suite (https://github.com/pgbouncer/pgbouncer,
// ISC License — see the LICENSE file in this directory) to Go end-to-end tests
// against the multigateway → multipooler → postgres path.
//
// See README.md for the triage that selects which PgBouncer scenarios are
// ported here (and which are out of scope / already covered elsewhere).
//
// The suite is gated behind RUN_EXTENDED_QUERY_SERVING_TESTS (the "Run Extended
// Query Serving Tests" PR label); every test self-skips otherwise, so the shared
// cluster is never built during a plain `go test ./...`.
package pgbouncertests

import (
	"os"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// setupManager manages the shared 2-node (primary + standby) cluster with
// multigateway enabled, reused across the tests in this package.
var setupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	return shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby (bootstrap needs 2)
		shardsetup.WithMultigateway(),
	)
})

// TestMain runs the shared test-main harness and dumps logs on failure.
func TestMain(m *testing.M) {
	exitCode := shardsetup.RunTestMain(m)
	if exitCode != 0 {
		setupManager.DumpLogs()
	}
	setupManager.Cleanup()
	os.Exit(exitCode) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}

// getSharedSetup returns the shared cluster for tests in this package.
func getSharedSetup(t *testing.T) *shardsetup.ShardSetup {
	t.Helper()
	return setupManager.Get(t)
}
