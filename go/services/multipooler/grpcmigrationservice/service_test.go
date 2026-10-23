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

	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
)

func TestFoldTableSelection(t *testing.T) {
	t.Run("nil objects yields no patterns", func(t *testing.T) {
		require.Empty(t, foldTableSelection(nil))
	})

	t.Run("all", func(t *testing.T) {
		got := foldTableSelection(&migratorpb.SelectionObject{Object: &migratorpb.SelectionObject_All{All: true}})
		require.Equal(t, []string{"*"}, got)
	})

	t.Run("all explicitly false yields no patterns", func(t *testing.T) {
		got := foldTableSelection(&migratorpb.SelectionObject{Object: &migratorpb.SelectionObject_All{All: false}})
		require.Empty(t, got)
	})

	t.Run("schema list", func(t *testing.T) {
		got := foldTableSelection(&migratorpb.SelectionObject{Object: &migratorpb.SelectionObject_Schema{
			Schema: &migratorpb.SchemaSpec{Schemata: []string{"sales", "reporting"}},
		}})
		require.Equal(t, []string{"sales.*", "reporting.*"}, got)
	})

	t.Run("table list", func(t *testing.T) {
		got := foldTableSelection(&migratorpb.SelectionObject{Object: &migratorpb.SelectionObject_Table{
			Table: &migratorpb.TableSpec{QualifiedNames: []string{"public.orders", "public.customers"}},
		}})
		require.Equal(t, []string{"public.orders", "public.customers"}, got)
	})
}

func TestToRef(t *testing.T) {
	require.Equal(t, migration.Ref{ID: 123}, toRef(123, ""))
	require.Equal(t, migration.Ref{ID: 123, Name: "nightly"}, toRef(123, "nightly"), "id and name both carried; the store resolves id first")
	require.Equal(t, migration.Ref{Name: "nightly"}, toRef(0, "nightly"))
	require.Equal(t, migration.Ref{}, toRef(0, ""))
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

func TestMigToProto(t *testing.T) {
	p := &migration.Projection{
		ID: 1, Name: "nightly", Source: "h:5432/db", ConnectionName: "src",
		TargetDatabase: "db", TargetShard: "0", Tables: []string{"public.orders"},
	}
	got := migToProto(p)
	require.Equal(t, int64(1), got.GetId())
	require.Equal(t, "nightly", got.GetName())
	require.Equal(t, "src", got.GetConnectionName())
	require.Equal(t, "db", got.GetTarget().GetDatabase())
	require.Equal(t, "0", got.GetTarget().GetShard())
	require.Equal(t, []string{"public.orders"}, got.GetObjects().GetTable().GetQualifiedNames())

	require.Nil(t, migToProto(&migration.Projection{ID: 2}).GetObjects(), "no resolved tables yields no SelectionObject")
}

func TestStatusToProto(t *testing.T) {
	since := time.Now()
	p := &migration.Projection{
		ID: 1, Phase: migration.PhaseExporting,
		ActiveDirection: migration.DirectionExport, CaughtUp: true, TotalRelations: 2, ReadyRelations: 2,
		PublicationName: "mt_pub_1", SubscriptionName: "mt_sub_1", StreamingSince: &since,
	}
	got := statusToProto(p)
	require.Equal(t, int64(1), got.GetId())
	require.Equal(t, migratorpb.MigrationPhase_MIGRATION_PHASE_EXPORTING, got.GetPhase())
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT, got.GetActiveDirection())
	require.True(t, got.GetCaughtUp())
	require.NotNil(t, got.GetStreamingSince())

	// StreamingSince is optional: nil in, nil out.
	require.Nil(t, statusToProto(&migration.Projection{ID: 2}).GetStreamingSince())
}
