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

package store

import (
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// Pooler is multiorch's per-pooler cache rider. It bundles the proto health
// state with the per-pooler HealthStream, so the cache is the single source
// of truth for "everything we track about this pooler" — no parallel
// registries to keep in sync.
//
// The embedded *PoolerHealthState is promoted, so existing readers can
// access fields (MultiPooler, IsLastCheckValid, Status, …) directly on
// *Pooler.
type Pooler struct {
	*multiorchdatapb.PoolerHealthState

	// HealthStream is populated when a stream goroutine is running for this
	// pooler. Nil for entries created outside the streaming path (tests
	// that seed the cache via SeedCache).
	HealthStream *HealthStream
}

// IsInitialized reports whether the pooler has been initialized. A pooler is
// considered initialized based on the IsInitialized field from the Status
// RPC (data-directory state, not LSN). The node must also be reachable for
// us to trust the value.
func (p *Pooler) IsInitialized() bool {
	if !p.IsLastCheckValid {
		return false
	}
	if p.MultiPooler == nil {
		return false
	}
	return p.GetStatus().GetIsInitialized()
}
