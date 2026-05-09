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

package topoclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractPoolerIDFromPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{"relative path", "poolers/multipooler-cell1-pooler1/Pooler", "multipooler-cell1-pooler1"},
		{"simple id", "poolers/some-complex-id/Pooler", "some-complex-id"},
		{"short id", "poolers/id/Pooler", "id"},
		{"absolute etcd path", "/multigres/zone1/poolers/multipooler-zone1-p1/Pooler", "multipooler-zone1-p1"},
		{"missing id segment", "poolers/Pooler", ""},
		{"wrong directory", "other/path/Pooler", ""},
		{"wrong filename", "poolers/id/other", ""},
		{"empty", "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, extractPoolerIDFromPath(tc.path))
		})
	}
}
