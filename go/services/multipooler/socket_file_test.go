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

package multipooler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolveSocketFilePath(t *testing.T) {
	tests := []struct {
		name          string
		configured    string
		explicitlySet bool
		poolerDir     string
		pgPort        int
		want          string
	}{
		{
			name:      "unset flag with pooler-dir derives the socket path",
			poolerDir: "/data/pooler-1", pgPort: 5433,
			want: "/data/pooler-1/pg_sockets/.s.PGSQL.5433",
		},
		{
			name:       "explicit path wins over derivation",
			configured: "/custom/.s.PGSQL.5432", poolerDir: "/data/pooler-1", pgPort: 5432,
			want: "/custom/.s.PGSQL.5432",
		},
		{
			name:          "explicitly empty forces TCP",
			explicitlySet: true, poolerDir: "/data/pooler-1", pgPort: 5432,
			want: "",
		},
		{
			name:   "no pooler-dir keeps TCP",
			pgPort: 5432,
			want:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, resolveSocketFilePath(tt.configured, tt.explicitlySet, tt.poolerDir, tt.pgPort))
		})
	}
}
