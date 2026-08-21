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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestScaleTimeout_UnderCoverageEnv verifies the GOCOVERDIR (subprocess
// coverage) detection path deterministically, independent of whether this test
// binary itself is coverage-instrumented: with GOCOVERDIR set, RunningUnderCoverage
// is true and ScaleTimeout multiplies by the coverage factor.
func TestScaleTimeout_UnderCoverageEnv(t *testing.T) {
	t.Setenv("GOCOVERDIR", t.TempDir())

	assert.True(t, RunningUnderCoverage(), "GOCOVERDIR set should count as running under coverage")
	assert.Equal(t, 10*time.Second*coverageTimeoutFactor, ScaleTimeout(10*time.Second),
		"timeout should be multiplied by the coverage factor when instrumented")
	assert.Equal(t, time.Duration(0), ScaleTimeout(0), "zero timeout stays zero even under coverage")
}

// TestScaleTimeout_Identity checks that scaling is a no-op when the process is
// not instrumented. It is skipped when the test binary is itself run under
// coverage (`go test -cover` or GOCOVERDIR), where the no-op branch cannot be
// exercised.
func TestScaleTimeout_Identity(t *testing.T) {
	if RunningUnderCoverage() {
		t.Skip("test binary is coverage-instrumented; identity branch is unreachable here")
	}
	assert.Equal(t, 7*time.Second, ScaleTimeout(7*time.Second), "no scaling without coverage")
}
