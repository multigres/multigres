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

package clustersetup

import (
	"os"
	"testing"
)

// TopologyEnvVar selects which topology the end-to-end tests run against. The
// value "minigres" selects a single Minigres process; anything else, or unset,
// keeps the Multigres topology.
const TopologyEnvVar = "MULTIGRES_E2E_TOPOLOGY"

// IsMinigres reports whether the tests run against the Minigres topology.
func IsMinigres() bool {
	return os.Getenv(TopologyEnvVar) == "minigres"
}

// RequireMultigresTopology skips the test under the Minigres topology. Call it
// at the start of tests, or in the helpers that build their clusters, when they
// need features only Multigres has (Multiorch, replicas, several gateways).
// reason names the feature, so the skip explains itself.
func RequireMultigresTopology(t *testing.T, reason string) {
	t.Helper()
	if IsMinigres() {
		t.Skipf("requires the Multigres topology: %s", reason)
	}
}
