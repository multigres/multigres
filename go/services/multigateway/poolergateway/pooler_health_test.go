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

package poolergateway

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/prototest"
)

func TestPoolerHealthIsServing(t *testing.T) {
	tests := []struct {
		name     string
		health   *poolerHealth
		expected bool
	}{
		{
			name:     "nil health returns false",
			health:   nil,
			expected: false,
		},
		{
			name: "SERVING returns true",
			health: &poolerHealth{
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
			expected: true,
		},
		{
			name: "NOT_SERVING returns false",
			health: &poolerHealth{
				ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.health.isServing()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPoolerHealthSimpleCopy(t *testing.T) {
	t.Run("nil health returns nil", func(t *testing.T) {
		var h *poolerHealth
		copy := h.simpleCopy()
		assert.Nil(t, copy)
	})

	t.Run("copies all fields", func(t *testing.T) {
		poolerID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "pooler1",
		}
		primaryObs := &clustermetadatapb.LeaderObservation{LeaderId: poolerID, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 42}}
		lastErr := errors.New("test error")
		lastResp := time.Now()

		original := &poolerHealth{
			PoolerID:          poolerID,
			ServingStatus:     clustermetadatapb.PoolerServingStatus_SERVING,
			LeaderObservation: primaryObs,
			LastError:         lastErr,
			LastResponse:      lastResp,
		}

		copy := original.simpleCopy()

		// Verify all fields are copied
		require.NotNil(t, copy)
		prototest.AssertEqual(t, original.PoolerID, copy.PoolerID)
		assert.Equal(t, original.ServingStatus, copy.ServingStatus)
		prototest.AssertEqual(t, original.LeaderObservation, copy.LeaderObservation)
		assert.Equal(t, original.LastError, copy.LastError)
		assert.Equal(t, original.LastResponse, copy.LastResponse)

		// Verify it's a different struct instance
		assert.NotSame(t, original, copy)

		// Verify pointer fields point to same underlying objects (shallow copy)
		assert.Same(t, original.PoolerID, copy.PoolerID)
		assert.Same(t, original.LeaderObservation, copy.LeaderObservation)
	})

	t.Run("modifying copy does not affect original", func(t *testing.T) {
		original := &poolerHealth{
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			LastError:     nil,
		}

		copy := original.simpleCopy()
		copy.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
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
