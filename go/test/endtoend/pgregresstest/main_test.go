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
	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
)

// testdataConfPath returns the absolute path to a postgresql.conf snippet under
// this package's testdata/pg<major>/<parts...>. Resolved via runtime.Caller so
// the path is correct regardless of the test's working directory at run time.
func testdataConfPath(parts ...string) string {
	_, file, _, _ := runtime.Caller(0)
	pkgDir := filepath.Dir(file)
	return filepath.Join(append([]string{pkgDir, "testdata", "pg17"}, parts...)...)
}

// regressionOverridesConfPath returns the snippet that forces lc_* GUCs to 'C'
// on each pgctld so locale-sensitive expected outputs reproduce upstream.
func regressionOverridesConfPath() string {
	return testdataConfPath("regression_overrides.conf")
}

// externalServerConfPaths returns the postgresql.conf snippets the external
// extensions selected for this run need applied to the cluster before postgres
// starts (ExternalExtension.ServerConfigFile, e.g. pg_cron's
// shared_preload_libraries). It is gated on the external suite actually running:
// the snippet would load a library (pg_cron) that is only installed when the
// external suite runs, so applying it otherwise would make postgres fail to
// start. PGEXTERNAL_TESTS narrowing is honored via ExternalModules.
func externalServerConfPaths() []string {
	extendedGate := os.Getenv(suiteutil.EnvRunExtendedQueryServingTests) == "1"
	runExternal := extendedGate || os.Getenv("RUN_PGEXTERNAL") == "1"
	if !runExternal {
		return nil
	}
	var paths []string
	for _, ext := range ExternalModules() {
		if ext.ServerConfigFile == "" {
			continue
		}
		paths = append(paths, testdataConfPath("external", ext.ServerConfigFile))
	}
	return paths
}

// setupManager manages the shared test setup for tests in this package.
var setupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	opts := []shardsetup.SetupOption{
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
	}
	// Some external extensions need server-level config the pooled query path
	// can't set (pg_cron's background worker needs shared_preload_libraries).
	// Apply their snippets to the shared cluster when the external suite runs.
	if confs := externalServerConfPaths(); len(confs) > 0 {
		opts = append(opts, shardsetup.WithPgInitdbExtraConfFiles(confs...))
	}
	return shardsetup.New(t, opts...)
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
