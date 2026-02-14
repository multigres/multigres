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
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// checkPrimaryUnreachable evaluates common preconditions shared by all
// primary-is-dead analyzers. Returns true when the primary is unreachable
// — each analyzer then applies its own visibility and connectivity checks.
func checkPrimaryUnreachable(analysis *store.ReplicationAnalysis) bool {
	// Only analyze replicas (primaries can't report themselves as dead)
	if analysis.IsPrimary {
		return false
	}

	// Skip if replica is not initialized (ShardNeedsBootstrap handles that)
	if !analysis.IsInitialized {
		return false
	}

	// Skip if no primary exists in topology
	if analysis.PrimaryPoolerID == nil {
		return false
	}

	// Primary is reachable — no failure to detect
	if analysis.PrimaryReachable {
		return false
	}

	return true
}
