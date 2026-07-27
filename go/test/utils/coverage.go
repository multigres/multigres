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

package utils

import (
	"os"
	"testing"
	"time"
)

// coverageTimeoutFactor is how much test-side wait budgets are stretched when
// the suite runs under coverage instrumentation. The "Code Coverage" CI
// workflow runs the end-to-end suite ~2-3x slower than an uninstrumented run,
// so a factor of 3 keeps timing-sensitive bootstrap/failover waits from tripping
// bounds that a normal run clears comfortably, with headroom to spare on slow
// GitHub Actions runners.
const coverageTimeoutFactor = 3

// RunningUnderCoverage reports whether the current test process is running with
// coverage instrumentation, accounting for both mechanisms the "Code Coverage"
// workflow uses:
//
//   - Direct coverage ("Direct test coverage" job): `go test -cover
//     -coverpkg=./...` instruments the test process itself, so testing.CoverMode()
//     is non-empty. This is the job that flakes, and it is invisible to a
//     GOCOVERDIR check because no GOCOVERDIR is set.
//   - Subprocess coverage ("Subprocess coverage" job): `make build-coverage`
//     plus a GOCOVERDIR environment variable instruments the spawned service
//     binaries; testing.CoverMode() is empty there.
//
// Both make the end-to-end suite run materially slower, so timing-sensitive
// waits need proportionally more headroom in either case.
func RunningUnderCoverage() bool {
	return testing.CoverMode() != "" || os.Getenv("GOCOVERDIR") != ""
}

// ScaleTimeout returns d unchanged during a normal run and d multiplied by a
// fixed factor when running under coverage instrumentation (see
// RunningUnderCoverage). Use it for test-side wait budgets and context
// deadlines — Eventually/poll timeouts and RPC ceilings — so a slow coverage
// run does not fail a bound that a normal run passes.
//
// Widening a ceiling never slows the happy path: it only defers the moment a
// genuinely stuck operation is declared failed. Negative-path tests that assert
// a deadline is hit still hit it, just later, because the operation under test
// never completes regardless of the deadline.
func ScaleTimeout(d time.Duration) time.Duration {
	if RunningUnderCoverage() {
		return d * coverageTimeoutFactor
	}
	return d
}
