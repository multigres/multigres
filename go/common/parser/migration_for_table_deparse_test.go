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

package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateMigrationForTableDeparse guards CreateMigrationStmt.SqlString on the
// FOR object-list path. It previously called PublicationObjSpec.SqlString, which
// is unimplemented, so deparsing any `CREATE MIGRATION ... FOR TABLE ...`
// panicked; the renderer now reuses the shared publication object-list helper.
func TestCreateMigrationForTableDeparse(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{
			in:   "CREATE MIGRATION m CONNECTION src FOR TABLE t1",
			want: "CREATE MIGRATION m CONNECTION src FOR TABLE t1",
		},
		{
			in:   "CREATE MIGRATION m CONNECTION src FOR TABLE t1, TABLE public.t2",
			want: "CREATE MIGRATION m CONNECTION src FOR TABLE t1, public.t2",
		},
		{
			in:   "CREATE MIGRATION m CONNECTION src FOR TABLES IN SCHEMA s1",
			want: "CREATE MIGRATION m CONNECTION src FOR TABLES IN SCHEMA s1",
		},
		{
			in:   "CREATE MIGRATION m CONNECTION src FOR ALL TABLES",
			want: "CREATE MIGRATION m CONNECTION src FOR ALL TABLES",
		},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			stmts, err := ParseSQL(tt.in)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			// Must not panic, and must round-trip to the canonical spelling.
			assert.Equal(t, tt.want, stmts[0].SqlString())
		})
	}
}
