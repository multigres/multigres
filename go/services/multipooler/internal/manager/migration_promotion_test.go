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

package manager

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus/consensustest"
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
)

// selectMigrationsPattern matches the store's load of the whole migration
// table, and nothing else the manager or the coordinator issues.
const selectMigrationsPattern = `LEFT JOIN multigres\.migration_tables`

// migrationRow is one row of the store's migration load: an IMPORT migration in
// COPYING, as a standby holds it when the primary it follows fails over.
func migrationRow(id string) *sqltypes.Row {
	return &sqltypes.Row{Values: []sqltypes.Value{
		sqltypes.Value(id),                     // migration_id
		sqltypes.Value("COPYING"),              // migration_phase
		sqltypes.Value(""),                     // migration_name
		sqltypes.Value("42"),                   // connection_id
		sqltypes.Value("testdb"),               // (migration_target).database
		sqltypes.Value("0"),                    // (migration_target).shard
		sqltypes.Value("default"),              // (migration_target).table_group
		sqltypes.Value("0"),                    // sequence_margin
		sqltypes.Value("true"),                 // copy_data
		sqltypes.Value("false"),                // skip_schema_copy
		sqltypes.Value("IMPORT"),               // direction
		sqltypes.Value("[]"),                   // quiesce_roles
		sqltypes.Value("true"),                 // public_had_connect
		sqltypes.Value("false"),                // quiesce_applied
		sqltypes.Value("{}"),                   // quiesce_role_conn_limits
		sqltypes.Value("{}"),                   // quiesce_roles_had_connect
		sqltypes.Value(""),                     // last_error
		sqltypes.Value(""),                     // reverse_link_error
		sqltypes.Value("2026-01-01T00:00:00Z"), // created_at
		nil,                                    // streaming_since
		sqltypes.Value(""),                     // owner_role
		sqltypes.Value(`["public.orders"]`),    // tables
	}}
}

// TestPromoteForgetsMigrationsReadBeforeTheTenure reproduces a node whose
// coordinator read the migration table while its postgres was briefly out of
// recovery before shard bootstrap, when there were no migrations, and which is
// later promoted over a primary that had one running. The promoted node must
// read the table afresh: answering from the earlier read reports the running
// migration as not found and leaves it unreconciled for the life of the process.
func TestPromoteForgetsMigrationsReadBeforeTheTenure(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-pooler"}
	coordinatorA := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "coordinator-a"}
	otherPooler := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "other-pooler"}
	recruitedTerm := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       7,
		AcceptedCoordinatorId:  coordinatorA,
		CoordinatorInitiatedAt: recruitTS,
		OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 0, LeaderSubterm: 1},
	}
	req := &consensusdatapb.PromoteRequest{
		Proposal: &consensusdatapb.CoordinatorProposal{
			TermRevocation: recruitedTerm,
			ProposalLeader: &clustermetadatapb.PoolerAddress{Id: selfID, Host: "pg-primary.internal", PostgresPort: 5432},
			ProposedTransition: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{
				RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 7},
				CohortMembers: []*clustermetadatapb.ID{selfID, otherPooler},
				CoordinatorId: coordinatorA,
				CreationTime:  ruleCreatedTS,
			}},
		},
	}
	ref := migration.Ref{ID: 1794851141667694486}

	m := mock.NewQueryService()
	// Before bootstrap: postgres is out of recovery and holds no migrations.
	m.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
	m.AddQueryPatternOnce(selectMigrationsPattern, &sqltypes.Result{})
	addTenureMigrationEnsure(m)
	expectLeaderPromoteMocks(m)
	addTenureMigrationEnsure(m)
	// After promotion: the migration has arrived by physical replication.
	m.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
	m.AddQueryPatternOnce(selectMigrationsPattern, &sqltypes.Result{Rows: []*sqltypes.Row{migrationRow("1794851141667694486")}})

	pm, tmpDir := setupManagerWithMockDB(t, m, &fakeRuleStore{pos: makeRulePosition(0)})
	consensustest.SeedTerm(t, tmpDir, recruitedTerm)
	_, err := pm.consensusMgr.Promises().Load()
	require.NoError(t, err)

	coord, err := pm.MigrationCoordinatorIfPrimary(t.Context())
	require.NoError(t, err)
	_, err = coord.GetMigration(t.Context(), ref, "")
	require.ErrorIs(t, err, migration.ErrNotFound, "before bootstrap there is no migration")

	_, err = pm.Promote(t.Context(), req)
	require.NoError(t, err)

	coord, err = pm.MigrationCoordinatorIfPrimary(t.Context())
	require.NoError(t, err)
	got, err := coord.GetMigration(t.Context(), ref, "")
	require.NoError(t, err, "the promoted primary must see the migration it inherited")
	assert.Equal(t, migration.PhaseCopying, got.Phase)
	assert.NoError(t, m.ExpectationsWereMet())
}
