// Copyright 2025 Supabase, Inc.
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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestPooler_HealthWithin(t *testing.T) {
	now := time.Now()
	const maxAge = 15 * time.Second

	tests := []struct {
		name       string
		pooler     *multiorchdatapb.PoolerHealthState
		expectOK   bool
		expectInit bool // only checked when expectOK
	}{
		{
			name: "never observed",
			pooler: &multiorchdatapb.PoolerHealthState{
				Multipooler: &clustermetadatapb.Multipooler{},
				Status:      &multipoolermanagerdatapb.Status{IsInitialized: true},
			},
			expectOK: false,
		},
		{
			name: "observation older than maxAge is untrustworthy",
			pooler: &multiorchdatapb.PoolerHealthState{
				Multipooler: &clustermetadatapb.Multipooler{},
				LastSeen:    timestamppb.New(now.Add(-time.Minute)),
				Status:      &multipoolermanagerdatapb.Status{IsInitialized: true},
			},
			expectOK: false,
		},
		{
			name: "fresh observation counts even with the connectivity flag false",
			pooler: &multiorchdatapb.PoolerHealthState{
				StreamConnected: false, // e.g. a momentary stream blip
				Multipooler:     &clustermetadatapb.Multipooler{},
				LastSeen:        timestamppb.New(now),
				Status:          &multipoolermanagerdatapb.Status{IsInitialized: true},
			},
			expectOK:   true,
			expectInit: true,
		},
		{
			name: "fresh and initialized",
			pooler: &multiorchdatapb.PoolerHealthState{
				Multipooler: &clustermetadatapb.Multipooler{},
				LastSeen:    timestamppb.New(now),
				Status:      &multipoolermanagerdatapb.Status{IsInitialized: true},
			},
			expectOK:   true,
			expectInit: true,
		},
		{
			name: "fresh but not initialized",
			pooler: &multiorchdatapb.PoolerHealthState{
				Multipooler: &clustermetadatapb.Multipooler{},
				LastSeen:    timestamppb.New(now),
				Status:      &multipoolermanagerdatapb.Status{IsInitialized: false},
			},
			expectOK:   true,
			expectInit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hs, ok := NewPooler(tt.pooler, nil).HealthWithin(now, maxAge)
			require.Equal(t, tt.expectOK, ok)
			if tt.expectOK {
				require.Equal(t, tt.expectInit, hs.GetStatus().GetIsInitialized())
			}
		})
	}
}
