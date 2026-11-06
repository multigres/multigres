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

package shardref

import (
	"context"
	"regexp"
	"testing"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	checkLegacyPattern = regexp.QuoteMeta(CheckLegacyTypeSQL)
	checkTypePattern   = regexp.QuoteMeta(CheckTypeSQL)
	createTypePattern  = regexp.QuoteMeta(CreateTypeSQL)
	renamePattern      = regexp.QuoteMeta("ALTER TYPE multigres.shard_key RENAME TO shard_ref")
)

// existingType answers CheckTypeSQL for an existing type.
func existingType(typtype, relkind string) *mockResult {
	return &mockResult{columns: []string{"typtype", "relkind"}, rows: [][]any{{typtype, relkind}}}
}

type mockResult struct {
	columns []string
	rows    [][]any
}

func TestEnsureCompositeType(t *testing.T) {
	tests := []struct {
		name          string
		existing      *mockResult // nil: no type of that name
		wantCreate    bool
		errorContains string
	}{
		{name: "absent is created", existing: nil, wantCreate: true},
		{name: "standalone composite is accepted", existing: existingType("c", "c")},
		{name: "table row type is rejected", existing: existingType("c", "r"), errorContains: "conflicts with an existing table"},
		{name: "partitioned table row type is rejected", existing: existingType("c", "p"), errorContains: "conflicts with an existing partitioned table"},
		{name: "view row type is rejected", existing: existingType("c", "v"), errorContains: "conflicts with an existing view"},
		{name: "materialized view row type is rejected", existing: existingType("c", "m"), errorContains: "conflicts with an existing materialized view"},
		{name: "foreign table row type is rejected", existing: existingType("c", "f"), errorContains: "conflicts with an existing foreign table"},
		{name: "enum is rejected", existing: existingType("e", ""), errorContains: "conflicts with an existing enum type"},
		{name: "domain is rejected", existing: existingType("d", ""), errorContains: "conflicts with an existing domain"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := mock.NewQueryService()
			if tt.existing == nil {
				m.AddQueryPatternOnce(checkTypePattern, mock.MakeQueryResult(nil, nil))
			} else {
				m.AddQueryPatternOnce(checkTypePattern, mock.MakeQueryResult(tt.existing.columns, tt.existing.rows))
			}
			created := false
			m.AddQueryPatternWithCallback(createTypePattern, mock.MakeQueryResult(nil, nil), func(string) { created = true })

			err := EnsureCompositeType(context.Background(), m, TypeName, CreateTypeSQL)
			if tt.errorContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
				assert.Contains(t, err.Error(), "multigres.shard_ref")
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantCreate, created)
			assert.NoError(t, m.ExpectationsWereMet())
		})
	}
}

func TestEnsure_LegacyRename(t *testing.T) {
	tests := []struct {
		name          string
		legacyExists  bool // a standalone composite multigres.shard_key
		currentExists bool // any type named multigres.shard_ref
		want          LegacyRename
		wantRename    bool
	}{
		{name: "no legacy type", want: LegacyAbsent},
		{name: "legacy type only is renamed", legacyExists: true, want: LegacyRenamed, wantRename: true},
		{name: "both exist is left alone", legacyExists: true, currentExists: true, want: LegacyConflict},
		{name: "shard_ref only", currentExists: true, want: LegacyAbsent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := mock.NewQueryService()
			m.AddQueryPatternOnce(checkLegacyPattern,
				mock.MakeQueryResult([]string{"legacy", "current"}, [][]any{{tt.legacyExists, tt.currentExists}}))
			renamed := false
			m.AddQueryPatternWithCallback(renamePattern, mock.MakeQueryResult(nil, nil), func(string) { renamed = true })
			// After the rename (or when shard_ref already exists) the type check
			// finds the composite; otherwise it is absent and gets created.
			if tt.wantRename || tt.currentExists {
				m.AddQueryPatternOnce(checkTypePattern, mock.MakeQueryResult([]string{"typtype", "relkind"}, [][]any{{"c", "c"}}))
			} else {
				m.AddQueryPatternOnce(checkTypePattern, mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce(createTypePattern, mock.MakeQueryResult(nil, nil))
			}

			got, err := Ensure(context.Background(), m)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantRename, renamed, "ALTER TYPE ... RENAME must run only when the legacy composite exists and shard_ref does not")
			assert.NoError(t, m.ExpectationsWereMet())
		})
	}
}
