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

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestIsVoluntarilyDraining(t *testing.T) {
	tests := []struct {
		name  string
		state *multiorchdatapb.PoolerHealthState
		want  bool
	}{
		{
			name: "draining replica reports voluntary",
			state: &multiorchdatapb.PoolerHealthState{
				Status: &multipoolermanagerdatapb.Status{
					PoolerType: clustermetadatapb.PoolerType_REPLICA,
					QueryServingStatus: &clustermetadatapb.ServingStatus{
						Signal: clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING,
					},
				},
			},
			want: true,
		},
		{
			name: "draining primary reports voluntary",
			state: &multiorchdatapb.PoolerHealthState{
				Status: &multipoolermanagerdatapb.Status{
					PoolerType: clustermetadatapb.PoolerType_PRIMARY,
					QueryServingStatus: &clustermetadatapb.ServingStatus{
						Signal: clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING,
					},
				},
			},
			want: true,
		},
		{
			name: "involuntary PoolerType.DRAINED is not voluntary even with DRAINING signal",
			state: &multiorchdatapb.PoolerHealthState{
				Status: &multipoolermanagerdatapb.Status{
					PoolerType: clustermetadatapb.PoolerType_DRAINED,
					QueryServingStatus: &clustermetadatapb.ServingStatus{
						Signal: clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING,
					},
				},
			},
			want: false,
		},
		{
			name: "ACTIVE signal is not voluntary drain",
			state: &multiorchdatapb.PoolerHealthState{
				Status: &multipoolermanagerdatapb.Status{
					PoolerType: clustermetadatapb.PoolerType_REPLICA,
					QueryServingStatus: &clustermetadatapb.ServingStatus{
						Signal: clustermetadatapb.ServingSignal_SERVING_SIGNAL_ACTIVE,
					},
				},
			},
			want: false,
		},
		{
			name: "missing QueryServingStatus is not voluntary drain",
			state: &multiorchdatapb.PoolerHealthState{
				Status: &multipoolermanagerdatapb.Status{
					PoolerType: clustermetadatapb.PoolerType_REPLICA,
				},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsVoluntarilyDraining(tc.state))
		})
	}
}
