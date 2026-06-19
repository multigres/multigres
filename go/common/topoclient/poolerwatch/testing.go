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

package poolerwatch

import (
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// SeedForTest drives the cache through an upsert event without needing a
// topology source. The *testing.T argument is required so production code
// cannot reach into the cache's ingress path.
func SeedForTest[T any](t *testing.T, cache *PoolerCache[T], pooler *clustermetadatapb.MultiPooler) {
	t.Helper()
	// Bypass Config.Filter — seeding is an explicit test intent that should
	// not be gated by the cache's filter (which mirrors production targeting).
	cache.upsert(pooler)
}

// DeleteForTest evicts an entry from the cache outright, regardless of the
// configured VanishedGrace. Equivalent to applyDelete with grace=0: OnGone
// fires synchronously with GoneVanished and the entry is gone from reads.
//
// Tests that need to model a topology delete + grace window should advance
// the test clock and rely on sweep() instead. DeleteForTest is the "remove
// this from my fixture now" affordance.
func DeleteForTest[T any](t *testing.T, cache *PoolerCache[T], id topoclient.ComponentID) {
	t.Helper()
	cache.deleteImmediate(id)
}
