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

package pgregresstest

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// regressionOverridesConfPath returns the absolute path to the
// testdata/pg<major>/regression_overrides.conf snippet that forces lc_* GUCs
// to 'C' on each pgctld. Resolved via runtime.Caller so the path is correct
// regardless of the test's working directory at run time.
func regressionOverridesConfPath() string {
	_, file, _, _ := runtime.Caller(0)
	pkgDir := filepath.Dir(file)
	return filepath.Join(pkgDir, "testdata", "pg17", "regression_overrides.conf")
}

// setupManager manages the shared test setup for tests in this package.
var setupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	// Create a 2-node cluster with multigateway for PostgreSQL regression tests
	return shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby
		shardsetup.WithMultigateway(),      // enable multigateway for PostgreSQL connections
		// Stamp multigres_vpid:<id> on every PG backend so the isolation
		// harness shim (public.multigres_test_session_is_blocked) can map
		// virtual PIDs back to real backend PIDs through multigateway.
		shardsetup.WithVpidStamping(),
		// Force C locale on initdb so locale-sensitive expected outputs
		// (char/varchar collation, to_char 'L' currency symbol, etc.) reproduce
		// upstream PostgreSQL behavior. Keep UTF8 encoding so the unicode /
		// collate.utf8 / json_encoding tests still see a real multibyte
		// codec — pg_regress upstream runs initdb with just --no-locale
		// (locale=C) and inherits the default encoding (UTF8 on modern
		// installs); --encoding=SQL_ASCII would break those tests.
		shardsetup.WithPgInitdbArgs("--no-locale --encoding=UTF8"),
		// The pgctld postgresql.conf template hard-codes en_US.UTF-8 for the
		// locale GUCs, which overrides initdb's --no-locale. Append a snippet
		// that resets lc_* to 'C' so currency/number/time/message formatting
		// matches upstream expected fixtures.
		shardsetup.WithPgInitdbExtraConfFiles(regressionOverridesConfPath()),
	)
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
