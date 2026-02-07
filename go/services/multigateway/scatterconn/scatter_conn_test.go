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

package scatterconn

import (
	"testing"

	"github.com/multigres/multigres/go/services/multigateway/handler"
)

func TestMergedSettings(t *testing.T) {
	tests := []struct {
		name            string
		startupParams   map[string]string
		sessionSettings map[string]string
		want            map[string]string
	}{
		{
			name:            "both nil",
			startupParams:   nil,
			sessionSettings: nil,
			want:            nil,
		},
		{
			name:            "only startup params",
			startupParams:   map[string]string{"DateStyle": "ISO, MDY"},
			sessionSettings: nil,
			want:            map[string]string{"DateStyle": "ISO, MDY"},
		},
		{
			name:            "only session settings",
			startupParams:   nil,
			sessionSettings: map[string]string{"work_mem": "64MB"},
			want:            map[string]string{"work_mem": "64MB"},
		},
		{
			name:            "session overrides startup",
			startupParams:   map[string]string{"DateStyle": "ISO, MDY", "TimeZone": "UTC"},
			sessionSettings: map[string]string{"DateStyle": "SQL, DMY"},
			want:            map[string]string{"DateStyle": "SQL, DMY", "TimeZone": "UTC"},
		},
		{
			name:            "disjoint params merge",
			startupParams:   map[string]string{"DateStyle": "ISO, MDY"},
			sessionSettings: map[string]string{"work_mem": "64MB"},
			want:            map[string]string{"DateStyle": "ISO, MDY", "work_mem": "64MB"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := handler.NewMultiGatewayConnectionState()
			state.StartupParams = tc.startupParams
			for k, v := range tc.sessionSettings {
				state.SetSessionVariable(k, v)
			}

			got := mergedSettings(state)

			if tc.want == nil {
				if got != nil {
					t.Fatalf("expected nil, got %v", got)
				}
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("expected %d keys, got %d: %v", len(tc.want), len(got), got)
			}
			for k, wantV := range tc.want {
				if gotV, ok := got[k]; !ok || gotV != wantV {
					t.Errorf("key %q: want %q, got %q (ok=%v)", k, wantV, gotV, ok)
				}
			}
		})
	}
}
