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

	"github.com/multigres/multigres/go/common/topoclient/poolerwatch"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// NewTestCache builds a standalone PoolerCache for tests. It has no
// topology source, so tests seed entries directly via Set or ApplyUpsert.
// The OnLive and OnUpdate hooks mirror orch's production behavior so
// production-flavored test paths (driving via ApplyUpsert) yield the same
// PoolerHealthState rider shape.
func NewTestCache(t *testing.T) *PoolerCache {
	t.Helper()
	return poolerwatch.New(t.Context(), poolerwatch.Config[*multiorchdatapb.PoolerHealthState]{
		Hooks: poolerwatch.Hooks[*multiorchdatapb.PoolerHealthState]{
			OnLive: func(p *clustermetadatapb.MultiPooler, _ *multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState {
				return &multiorchdatapb.PoolerHealthState{MultiPooler: p}
			},
			OnUpdate: func(_, curr *clustermetadatapb.MultiPooler, rider *multiorchdatapb.PoolerHealthState) {
				rider.MultiPooler = curr
			},
		},
		ShutdownGrace: time.Hour,
		VanishedGrace: time.Hour,
		Logger:        slog.Default(),
	})
}
