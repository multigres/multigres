// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multipooler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/telemetry"
)

// TestPostgresUnrecoverableDefaults pins the shipped defaults for the
// unrecoverable-postgres quarantine classifier: it is ENABLED by default with a
// 5m budget and a 3-attempt floor. A deployment without a replacement actor opts
// out with --postgres-unrecoverable-timeout=0 (see the disabled-path coverage in
// the manager package's TestMonitor_StartFatalLoop_NotQuarantinedWhenDisabled).
func TestPostgresUnrecoverableDefaults(t *testing.T) {
	mp := NewMultipooler(telemetry.NewTelemetry())

	// A positive default timeout is what turns the classifier on.
	assert.Equal(t, 5*time.Minute, mp.postgresUnrecoverableTimeout.Default(),
		"the unrecoverable-postgres quarantine should be enabled by default (5m)")
	assert.Positive(t, mp.postgresUnrecoverableTimeout.Default(),
		"a positive default timeout means the classifier is on by default")

	// The companion attempts floor defaults to 3 and stays within the accepted bounds.
	assert.Equal(t, 3, mp.postgresUnrecoverableMinAttempts.Default())
	require.NoError(t, validateUnrecoverableMinAttempts(mp.postgresUnrecoverableMinAttempts.Default()),
		"the default min-attempts floor must be a valid value")
}

func TestValidateUnrecoverableMinAttempts(t *testing.T) {
	tests := []struct {
		n  int
		ok bool
	}{
		{0, false},  // unset / below floor
		{1, false},  // a floor of 1 defeats the purpose
		{2, true},   // lower bound (inclusive)
		{3, true},   // default
		{9, true},   // upper bound (inclusive, since 10 is exclusive)
		{10, false}, // upper bound is exclusive
		{50, false},
		{-1, false},
	}
	for _, tc := range tests {
		err := validateUnrecoverableMinAttempts(tc.n)
		if tc.ok {
			require.NoError(t, err, "n=%d should be valid", tc.n)
		} else {
			assert.Error(t, err, "n=%d should be rejected", tc.n)
		}
	}

	// The default must fall inside the valid range.
	require.NoError(t, validateUnrecoverableMinAttempts(defaultPostgresUnrecoverableMinAttempts))
}
