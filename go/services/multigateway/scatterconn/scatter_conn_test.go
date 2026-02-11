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

package scatterconn

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/services/multigateway/handler"
)

func TestMergedSettings(t *testing.T) {
	tests := []struct {
		name     string
		startup  map[string]string
		session  map[string]string
		expected map[string]string
	}{
		{
			name:     "both nil",
			startup:  nil,
			session:  nil,
			expected: nil,
		},
		{
			name:     "only startup params",
			startup:  map[string]string{"DateStyle": "German", "TimeZone": "UTC"},
			session:  nil,
			expected: map[string]string{"DateStyle": "German", "TimeZone": "UTC"},
		},
		{
			name:     "only session settings",
			startup:  nil,
			session:  map[string]string{"work_mem": "64MB"},
			expected: map[string]string{"work_mem": "64MB"},
		},
		{
			name:     "session overrides startup for same key",
			startup:  map[string]string{"DateStyle": "German", "TimeZone": "UTC"},
			session:  map[string]string{"DateStyle": "ISO"},
			expected: map[string]string{"DateStyle": "ISO", "TimeZone": "UTC"},
		},
		{
			name:     "disjoint params merge",
			startup:  map[string]string{"DateStyle": "German"},
			session:  map[string]string{"work_mem": "64MB"},
			expected: map[string]string{"DateStyle": "German", "work_mem": "64MB"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := handler.NewMultiGatewayConnectionState()
			if tt.startup != nil {
				state.StartupParams = tt.startup
			}
			for k, v := range tt.session {
				state.SetSessionVariable(k, v)
			}

			result := mergedSettings(state)
			assert.Equal(t, tt.expected, result)
		})
	}
}
