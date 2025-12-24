// Copyright 2025 Supabase, Inc.
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

package poolergateway

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

func TestPoolerHealth_IsServing(t *testing.T) {
	tests := []struct {
		name     string
		health   *PoolerHealth
		expected bool
	}{
		{
			name:     "nil health returns false",
			health:   nil,
			expected: false,
		},
		{
			name: "SERVING returns true",
			health: &PoolerHealth{
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
			expected: true,
		},
		{
			name: "SERVING_RDONLY returns true",
			health: &PoolerHealth{
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING_RDONLY,
			},
			expected: true,
		},
		{
			name: "NOT_SERVING returns false",
			health: &PoolerHealth{
				ServingStatus: clustermetadatapb.PoolerServingStatus_NOT_SERVING,
			},
			expected: false,
		},
		{
			name: "DRAINED returns false",
			health: &PoolerHealth{
				ServingStatus: clustermetadatapb.PoolerServingStatus_NOT_SERVING,
			},
			expected: false,
		},
		{
			name: "BACKUP returns false",
			health: &PoolerHealth{
				ServingStatus: clustermetadatapb.PoolerServingStatus_BACKUP,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.health.IsServing()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPoolerHealth_SimpleCopy(t *testing.T) {
	t.Run("nil health returns nil", func(t *testing.T) {
		var h *PoolerHealth
		copy := h.SimpleCopy()
		assert.Nil(t, copy)
	})

	t.Run("copies all fields", func(t *testing.T) {
		target := &query.Target{
			TableGroup: "tg1",
			Shard:      "0",
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}
		poolerID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "pooler1",
		}
		primaryObs := &multipoolerservice.PrimaryObservation{
			PrimaryId:  poolerID,
			VotingTerm: 42,
		}
		lastErr := errors.New("test error")
		lastResp := time.Now()

		original := &PoolerHealth{
			Target:             target,
			PoolerID:           poolerID,
			ServingStatus:      clustermetadatapb.PoolerServingStatus_SERVING,
			PrimaryObservation: primaryObs,
			LastError:          lastErr,
			LastResponse:       lastResp,
		}

		copy := original.SimpleCopy()

		// Verify all fields are copied
		require.NotNil(t, copy)
		assert.Equal(t, original.Target, copy.Target)
		assert.Equal(t, original.PoolerID, copy.PoolerID)
		assert.Equal(t, original.ServingStatus, copy.ServingStatus)
		assert.Equal(t, original.PrimaryObservation, copy.PrimaryObservation)
		assert.Equal(t, original.LastError, copy.LastError)
		assert.Equal(t, original.LastResponse, copy.LastResponse)

		// Verify it's a different struct instance
		assert.NotSame(t, original, copy)

		// Verify pointer fields point to same underlying objects (shallow copy)
		assert.Same(t, original.Target, copy.Target)
		assert.Same(t, original.PoolerID, copy.PoolerID)
		assert.Same(t, original.PrimaryObservation, copy.PrimaryObservation)
	})

	t.Run("modifying copy does not affect original", func(t *testing.T) {
		original := &PoolerHealth{
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			LastError:     nil,
		}

		copy := original.SimpleCopy()
		copy.ServingStatus = clustermetadatapb.PoolerServingStatus_NOT_SERVING
		copy.LastError = errors.New("new error")

		// Original should be unchanged
		assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, original.ServingStatus)
		assert.Nil(t, original.LastError)
	})
}

func TestErrPoolerUninitialized(t *testing.T) {
	// Verify the error exists and has expected message
	assert.NotNil(t, errPoolerUninitialized)
	assert.Contains(t, errPoolerUninitialized.Error(), "not initialized")
}
