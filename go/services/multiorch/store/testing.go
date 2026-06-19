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
	"log/slog"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/poolerwatch"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// NewTestCache builds a standalone PoolerCache for tests. It has no
// topology source; tests seed entries via SeedCache. The OnLive and
// OnUpdate hooks mirror orch's production behavior so test paths yield
// the same PoolerHealthState rider shape.
func NewTestCache(t *testing.T) *PoolerCache {
	t.Helper()
	cache := poolerwatch.New(t.Context(), poolerwatch.Config[*Pooler]{
		ShutdownGrace: time.Hour,
		VanishedGrace: time.Hour,
		Logger:        slog.Default(),
	})
	cache.Start(poolerwatch.Hooks[*Pooler]{
		OnLive: func(p *clustermetadatapb.MultiPooler, _ *Pooler) *Pooler {
			return &Pooler{PoolerHealthState: &multiorchdatapb.PoolerHealthState{MultiPooler: p}}
		},
		OnUpdate: func(_, curr *clustermetadatapb.MultiPooler, rider *Pooler) {
			rider.MultiPooler = curr
		},
	})
	return cache
}

// SeedCache inserts a fully-formed PoolerHealthState via the legitimate
// cache path (SeedForTest upsert + DoUpdate) and returns the entry's ID.
//
// The *testing.T argument is required so production code cannot call this
// (production code has no testing.T to pass). The cache itself has no Set
// method; production code reaches state through OnLive hooks.
func SeedCache(t *testing.T, cache *PoolerCache, state *Pooler) topoclient.ComponentID {
	t.Helper()
	if state == nil || state.MultiPooler == nil {
		return ""
	}
	poolerwatch.SeedForTest(t, cache, state.MultiPooler)
	id := topoclient.ComponentIDString(state.MultiPooler.Id)
	cache.DoUpdate(id, func(*Pooler) *Pooler {
		return state
	})
	return id
}
