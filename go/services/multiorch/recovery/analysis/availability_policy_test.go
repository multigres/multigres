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

package analysis

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multiorch/config"
)

func TestNewAvailabilityPolicy_ReadsTheLeaderPostgresResponseThresholdFromConfig(t *testing.T) {
	cfg := config.NewTestConfig(config.WithLeaderPostgresResponseThreshold(7 * time.Second))
	assert.Equal(t, 7*time.Second, NewAvailabilityPolicy(cfg).LeaderPostgresResponseThreshold)
}

func TestNewAvailabilityPolicy_NilConfigLeavesTheConfigValuesUnset(t *testing.T) {
	policy := NewAvailabilityPolicy(nil)
	assert.Zero(t, policy.LeaderPostgresResponseThreshold)
	assert.NotZero(t, policy.ObservationFreshness, "the built-in thresholds still apply")
}

func TestDefaultAvailabilityPolicy_MatchesTheConfigDefaults(t *testing.T) {
	assert.Equal(t, 30*time.Second, DefaultAvailabilityPolicy().LeaderPostgresResponseThreshold)
}

func TestDefaultAvailabilityPolicy_IgnoresTheEnvironment(t *testing.T) {
	t.Setenv("MT_LEADER_POSTGRES_RESPONSE_THRESHOLD", "1s")
	require.Equal(t, time.Second, NewAvailabilityPolicy(config.NewTestConfig()).LeaderPostgresResponseThreshold,
		"the environment reaches a configured policy")
	assert.Equal(t, 30*time.Second, DefaultAvailabilityPolicy().LeaderPostgresResponseThreshold)
}
