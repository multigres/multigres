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

package topoclient_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestComponentIDString(t *testing.T) {
	tests := []struct {
		name string
		id   *clustermetadatapb.ID
		want topoclient.ComponentID
	}{
		{
			name: "pooler",
			id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "abc"},
			want: "multipooler-zone1-abc",
		},
		{
			name: "orch",
			id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "zone2", Name: "xyz"},
			want: "multiorch-zone2-xyz",
		},
		{
			name: "gateway",
			id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIGATEWAY, Cell: "zone3", Name: "gw"},
			want: "multigateway-zone3-gw",
		},
		{
			// nil must not panic: the Get accessors yield a clearly-malformed
			// "unknown--" sentinel rather than crashing the caller.
			name: "nil id",
			id:   nil,
			want: "unknown--",
		},
		{
			// An unset component falls back to "unknown" via the generated name map.
			name: "unset component",
			id:   &clustermetadatapb.ID{Cell: "zone1", Name: "abc"},
			want: "unknown-zone1-abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, topoclient.ComponentIDString(tt.id))
		})
	}
}

// TestComponentIDStringNilDoesNotPanic documents the contract relied on by
// callers that may hold a not-yet-validated ID (e.g. logging or job tags).
func TestComponentIDStringNilDoesNotPanic(t *testing.T) {
	require.NotPanics(t, func() {
		_ = topoclient.ComponentIDString(nil)
	})
}
