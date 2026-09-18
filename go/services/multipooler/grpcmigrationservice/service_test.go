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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
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

func TestPhaseToProto(t *testing.T) {
	cases := map[migration.Phase]migratorpb.MigrationPhase{
		migration.PhaseCreated:           migratorpb.MigrationPhase_MIGRATION_PHASE_CREATED,
		migration.PhaseValidating:        migratorpb.MigrationPhase_MIGRATION_PHASE_VALIDATING,
		migration.PhaseSchemaCopy:        migratorpb.MigrationPhase_MIGRATION_PHASE_SCHEMA_COPY,
		migration.PhaseCreatePublication: migratorpb.MigrationPhase_MIGRATION_PHASE_CREATE_PUBLICATION,
		migration.PhaseCopying:           migratorpb.MigrationPhase_MIGRATION_PHASE_COPYING,
		migration.PhaseImporting:         migratorpb.MigrationPhase_MIGRATION_PHASE_IMPORTING,
		migration.PhaseExporting:         migratorpb.MigrationPhase_MIGRATION_PHASE_EXPORTING,
		migration.PhaseSwitchingToImport: migratorpb.MigrationPhase_MIGRATION_PHASE_SWITCHING_TO_IMPORT,
		migration.PhaseSwitchingToExport: migratorpb.MigrationPhase_MIGRATION_PHASE_SWITCHING_TO_EXPORT,
		migration.PhaseCompleting:        migratorpb.MigrationPhase_MIGRATION_PHASE_COMPLETING,
		migration.PhaseFailed:            migratorpb.MigrationPhase_MIGRATION_PHASE_FAILED,
	}
	for in, want := range cases {
		require.Equal(t, want, phaseToProto(in), "phase %s", in)
	}
	require.Equal(t, migratorpb.MigrationPhase_MIGRATION_PHASE_UNSPECIFIED, phaseToProto(migration.Phase("bogus")))
}

func TestDirToProto(t *testing.T) {
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT, dirToProto(migration.DirectionImport))
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT, dirToProto(migration.DirectionExport))
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_UNSPECIFIED, dirToProto(migration.Direction("bogus")))
}

func TestProjToProto(t *testing.T) {
	since := time.Now()
	p := &migration.Projection{
		ID: "m1", Name: "nightly", Source: "h:5432/db", TargetDatabase: "db", TargetShard: "0",
		Tables: []string{"public.orders"}, Phase: migration.PhaseExporting,
		ActiveDirection: migration.DirectionExport, CaughtUp: true, TotalRelations: 2, ReadyRelations: 2,
		PublicationName: "mt_pub_m1", SubscriptionName: "mt_sub_m1", StreamingSince: &since,
	}
	got := projToProto(p)
	require.Equal(t, "m1", got.GetId())
	require.Equal(t, "nightly", got.GetName())
	require.Equal(t, "h:5432/db", got.GetSource())
	require.Equal(t, []string{"public.orders"}, got.GetTables())
	require.Equal(t, migratorpb.MigrationPhase_MIGRATION_PHASE_EXPORTING, got.GetPhase())
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT, got.GetActiveDirection())
	require.True(t, got.GetCaughtUp())
	require.NotNil(t, got.GetStreamingSince())

	// StreamingSince is optional: nil in, nil out.
	require.Nil(t, projToProto(&migration.Projection{ID: "m2"}).GetStreamingSince())
}
