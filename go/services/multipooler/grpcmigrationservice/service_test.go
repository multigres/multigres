// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcmigrationservice

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
)

func tableObject(name string) *migratorpb.SelectionObject {
	return &migratorpb.SelectionObject{Object: &migratorpb.SelectionObject_Table{Table: &migratorpb.TableSpec{QualifiedName: name}}}
}

func schemaObject(name string) *migratorpb.SelectionObject {
	return &migratorpb.SelectionObject{Object: &migratorpb.SelectionObject_Schema{Schema: name}}
}

func TestFoldTableSelection(t *testing.T) {
	t.Run("unions all_tables, schema objects, and table objects", func(t *testing.T) {
		got, err := foldTableSelection(&migratorpb.CreateMigrationRequest{
			AllTables: true,
			Objects:   []*migratorpb.SelectionObject{schemaObject("sales"), tableObject("public.orders")},
		})
		require.NoError(t, err)
		require.Equal(t, []string{"*", "sales.*", "public.orders"}, got)
	})

	t.Run("no selection yields an empty pattern list", func(t *testing.T) {
		got, err := foldTableSelection(&migratorpb.CreateMigrationRequest{})
		require.NoError(t, err)
		require.Empty(t, got)
	})

	// The per-table clauses are accepted on the wire but not yet backed; each must
	// surface a typed feature_not_supported (0A000) rather than being dropped.
	for _, tc := range []struct {
		name string
		spec *migratorpb.TableSpec
	}{
		{"columns", &migratorpb.TableSpec{QualifiedName: "public.orders", Columns: []string{"id"}}},
		{"where", &migratorpb.TableSpec{QualifiedName: "public.orders", Where: "id > 0"}},
		{"include_descendants", &migratorpb.TableSpec{QualifiedName: "public.orders", IncludeDescendants: true}},
	} {
		t.Run("rejects "+tc.name+" as not-yet-supported", func(t *testing.T) {
			_, err := foldTableSelection(&migratorpb.CreateMigrationRequest{
				Objects: []*migratorpb.SelectionObject{{Object: &migratorpb.SelectionObject_Table{Table: tc.spec}}},
			})
			require.Error(t, err)
			require.True(t, mterrors.IsErrorCode(err, mterrors.PgSSFeatureNotSupported),
				"want feature_not_supported (0A000), got %v", err)
		})
	}
}

func TestMigrationRef(t *testing.T) {
	require.Equal(t, "m123", migrationRef("m123", ""))
	require.Equal(t, "m123", migrationRef("m123", "nightly"), "id takes precedence over name")
	require.Equal(t, "nightly", migrationRef("", "nightly"))
	require.Equal(t, "", migrationRef("", ""))
}
