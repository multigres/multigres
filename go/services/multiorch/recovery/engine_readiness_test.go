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

package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

func TestAllPoolersUnreachable(t *testing.T) {
	ctx := context.Background()
	threshold := 15 * time.Second

	t.Run("no poolers returns false", func(t *testing.T) {
		engine := newTestEngine(ctx, t)
		assert.False(t, engine.AllPoolersUnreachable(threshold))
	})

	t.Run("all poolers healthy returns false", func(t *testing.T) {
		engine := newTestEngine(ctx, t)
		engine.poolerStore.Set("pooler1", &multiorchdatapb.PoolerHealthState{
			IsLastCheckValid: true,
			LastSeen:         timestamppb.Now(),
		})
		engine.poolerStore.Set("pooler2", &multiorchdatapb.PoolerHealthState{
			IsLastCheckValid: true,
			LastSeen:         timestamppb.Now(),
		})
		assert.False(t, engine.AllPoolersUnreachable(threshold))
	})

	t.Run("some poolers unreachable returns false", func(t *testing.T) {
		engine := newTestEngine(ctx, t)
		engine.poolerStore.Set("pooler1", &multiorchdatapb.PoolerHealthState{
			IsLastCheckValid: false,
			LastSeen:         timestamppb.New(time.Now().Add(-1 * time.Hour)),
		})
		engine.poolerStore.Set("pooler2", &multiorchdatapb.PoolerHealthState{
			IsLastCheckValid: true,
			LastSeen:         timestamppb.Now(),
		})
		assert.False(t, engine.AllPoolersUnreachable(threshold))
	})

	t.Run("all poolers unreachable within threshold returns false", func(t *testing.T) {
		engine := newTestEngine(ctx, t)
		recentTime := timestamppb.New(time.Now().Add(-5 * time.Second))
		engine.poolerStore.Set("pooler1", &multiorchdatapb.PoolerHealthState{
			IsLastCheckValid: false,
			LastSeen:         recentTime,
		})
		engine.poolerStore.Set("pooler2", &multiorchdatapb.PoolerHealthState{
			IsLastCheckValid: false,
			LastSeen:         recentTime,
		})
		assert.False(t, engine.AllPoolersUnreachable(threshold))
	})

	t.Run("all poolers unreachable beyond threshold returns true", func(t *testing.T) {
		engine := newTestEngine(ctx, t)
		oldTime := timestamppb.New(time.Now().Add(-1 * time.Minute))
		engine.poolerStore.Set("pooler1", &multiorchdatapb.PoolerHealthState{
			IsLastCheckValid: false,
			LastSeen:         oldTime,
		})
		engine.poolerStore.Set("pooler2", &multiorchdatapb.PoolerHealthState{
			IsLastCheckValid: false,
			LastSeen:         oldTime,
		})
		assert.True(t, engine.AllPoolersUnreachable(threshold))
	})

	t.Run("all poolers never seen returns true", func(t *testing.T) {
		engine := newTestEngine(ctx, t)
		engine.poolerStore.Set("pooler1", &multiorchdatapb.PoolerHealthState{
			IsLastCheckValid: false,
		})
		engine.poolerStore.Set("pooler2", &multiorchdatapb.PoolerHealthState{
			IsLastCheckValid: false,
		})
		assert.True(t, engine.AllPoolersUnreachable(threshold))
	})
}
