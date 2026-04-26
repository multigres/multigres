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

package suiteutil

import (
	"os"
	"testing"
)

// Env var names that gate the heavyweight compatibility / benchmark suites.
// Each also corresponds to a GitHub PR label of the same human-readable name
// (see .github/workflows/*.yml).
const (
	// EnvRunExtendedQueryServingTests enables pgregresstest (regression +
	// isolation) and sqllogictest. Matches the "Run Extended Query Serving
	// Tests" PR label.
	EnvRunExtendedQueryServingTests = "RUN_EXTENDED_QUERY_SERVING_TESTS"

	// EnvRunBenchmarks enables the pgbench benchmark suite. Matches the
	// "Run Benchmarks" PR label.
	EnvRunBenchmarks = "RUN_BENCHMARKS"
)

// SkipUnlessEnabled skips the test unless the named environment variable is
// set to "1". Use at the top of heavy opt-in suites so local `go test ./...`
// runs don't accidentally trigger a 60-minute build+run.
func SkipUnlessEnabled(t *testing.T, envVar string) {
	t.Helper()
	if os.Getenv(envVar) != "1" {
		t.Skipf("skipping: set %s=1 to run", envVar)
	}
}
