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

package manager

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

func TestConstructorRejectsUnknownLeadershipModeBeforeSideEffects(t *testing.T) {
	// A nil config would panic if construction got as far as creating clients.
	mgr, err := NewMultipoolerManager(slog.Default(), &clustermetadatapb.Multipooler{LeadershipMode: 99}, nil)
	require.Nil(t, mgr)
	require.ErrorContains(t, err, "leadership mode is not supported")
}

func newUnmanagedTestManager(t *testing.T) *MultipoolerManager {
	t.Helper()
	pm, err := NewMultipoolerManager(slog.Default(), &clustermetadatapb.Multipooler{
		Id:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "external"},
		ShardRef:       &clustermetadatapb.ShardRef{Database: "db", TableGroup: "migrateTG", Shard: "0-inf"},
		LeadershipMode: clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED,
	}, &Config{PgctldAddr: "localhost:1", ConsensusEnabled: true, ExternalBackend: &ExternalBackend{Host: "db.example.com", Port: 5432, Database: "db"}})
	require.NoError(t, err)
	t.Cleanup(func() {
		pm.cancel()
		pm.shutdownCancel()
	})
	return pm
}

func TestUnmanagedConstructorOmitsManagementComponents(t *testing.T) {
	pm := newUnmanagedTestManager(t)
	require.True(t, pm.IsUnmanaged())
	require.Nil(t, pm.pgctldClient)
	require.Nil(t, pm.consensusMgr)
	require.Nil(t, pm.backup)
	require.NotNil(t, pm.QueryServiceControl())
	require.Zero(t, pm.BackupStatusSnapshot().CompleteCount)

	pm.StartBackupHealth()
	require.False(t, pm.backupHealthEnabled)
}

func TestUnmanagedGracefulShutdownIsIdempotent(t *testing.T) {
	pm := newUnmanagedTestManager(t)
	pm.GracefulShutdown(t.Context())
	pm.GracefulShutdown(t.Context())
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, pm.record.ServingStatus())
	require.Error(t, pm.shutdownCtx.Err())
}

func TestUnmanagedRoutingStateNeverCarriesConsensusRule(t *testing.T) {
	pm := newUnmanagedTestManager(t)
	// A nil consensus status would derive REPLICA for a managed pooler.
	got := pm.stateManager.deriveRoutingState(pgmode.Primary, nil)
	require.Equal(t, servingstate.RoutingRolePrimary, got.Role)
	require.Nil(t, got.Rule)
	got = pm.stateManager.deriveRoutingState(pgmode.InRecovery, nil)
	require.Equal(t, servingstate.RoutingRoleUnknown, got.Role)
}

func TestUnmanagedPoolerCannotServeTheDefaultTablegroup(t *testing.T) {
	_, err := NewMultipoolerManager(slog.Default(), &clustermetadatapb.Multipooler{
		Id:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "external"},
		ShardRef:       &clustermetadatapb.ShardRef{Database: "db", TableGroup: "default", Shard: "0-inf"},
		LeadershipMode: clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED,
	}, &Config{})
	require.Error(t, err)
	require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	require.ErrorContains(t, err, "default tablegroup")
}

// TestUnmanagedPoolerIsNeverHeldForAMigration: an unmanaged pooler's database is external, so it
// never reads migration state from it and is never held, whatever the hold was before.
func TestUnmanagedPoolerIsNeverHeldForAMigration(t *testing.T) {
	pm := newUnmanagedTestManager(t)
	pm.migrationImportHold.Store(int32(migrationHoldHeld))

	pm.refreshMigrationHold(t.Context())

	require.Equal(t, migrationHoldNotHeld, migrationHoldState(pm.migrationImportHold.Load()))
	require.False(t, pm.migrationServingHold())
}

// TestTheExternalBackendIsRequiredExactlyForAnUnmanagedPooler: the mode says which kind of
// pooler this is, and the backend follows from it.
func TestTheExternalBackendIsRequiredExactlyForAnUnmanagedPooler(t *testing.T) {
	good := &ExternalBackend{Host: "db.example.com", Port: 5432, Database: "db"}
	construct := func(mode clustermetadatapb.LeadershipMode, backend *ExternalBackend) error {
		pm, err := NewMultipoolerManager(slog.Default(), &clustermetadatapb.Multipooler{
			Id:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "p"},
			ShardRef:       &clustermetadatapb.ShardRef{Database: "db", TableGroup: "migrateTG", Shard: "0-inf"},
			LeadershipMode: mode,
		}, &Config{ExternalBackend: backend})
		if err == nil {
			pm.cancel()
			pm.shutdownCancel()
		}
		return err
	}
	const unmanaged = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED
	require.NoError(t, construct(unmanaged, good))
	require.ErrorContains(t, construct(unmanaged, nil), "requires an external backend")
	for _, backend := range []*ExternalBackend{
		{Port: 5432, Database: "db"},
		{Host: "db.example.com", Database: "db"},
		{Host: "db.example.com", Port: 5432},
	} {
		require.ErrorContains(t, construct(unmanaged, backend), "requires a host, a port and a database")
	}
	for _, mode := range []clustermetadatapb.LeadershipMode{
		clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_CONSENSUS,
		clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC,
	} {
		require.ErrorContains(t, construct(mode, good), "managed pooler cannot have an external backend", mode.String())
	}
}

// TestAnUnmanagedPoolerRefusesTheDefaultTablegroupWithTheSharedError: startup and the manager
// refuse it with the same error.
func TestAnUnmanagedPoolerRefusesTheDefaultTablegroupWithTheSharedError(t *testing.T) {
	want := ValidateUnmanagedTableGroup(constants.DefaultTableGroup)
	require.ErrorContains(t, want, "default tablegroup")
	require.NoError(t, ValidateUnmanagedTableGroup("migrateTG"))

	_, err := NewMultipoolerManager(slog.Default(), &clustermetadatapb.Multipooler{
		Id:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "p"},
		ShardRef:       &clustermetadatapb.ShardRef{Database: "db", TableGroup: constants.DefaultTableGroup, Shard: "0-inf"},
		LeadershipMode: clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED,
	}, &Config{ExternalBackend: &ExternalBackend{Host: "db.example.com", Port: 5432, Database: "db"}})
	require.EqualError(t, err, want.Error())
}
