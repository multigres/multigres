// Copyright 2025 Supabase, Inc.
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
	"context"
	"errors"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/pubsub"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
	"github.com/multigres/multigres/go/services/multipooler/internal/shardref"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockPoolerController implements poolerserver.PoolerController for testing.
type mockPoolerController struct {
	queryService *mock.QueryService
}

func (m *mockPoolerController) Open(context.Context) error { return nil }
func (m *mockPoolerController) Close() error               { return nil }
func (m *mockPoolerController) IsHealthy() error           { return nil }
func (m *mockPoolerController) IsServing() bool            { return true }
func (m *mockPoolerController) OnStateChange(context.Context, servingstate.State) error {
	return nil
}

func (m *mockPoolerController) StartRequest(*query.Target, poolerserver.RequestKind) error {
	return nil
}

func (m *mockPoolerController) AwaitStateChange(context.Context, servingstate.RoutingRole, clustermetadatapb.PoolerServingStatus) {
}
func (m *mockPoolerController) Executor() (queryservice.QueryService, error) { return nil, nil }
func (m *mockPoolerController) InternalQueryService() executor.InternalQueryService {
	return m.queryService
}
func (m *mockPoolerController) RegisterGRPCServices()                {}
func (m *mockPoolerController) SetPubSubListener(_ *pubsub.Listener) {}
func (m *mockPoolerController) PubSubListener() *pubsub.Listener     { return nil }

var _ poolerserver.PoolerController = (*mockPoolerController)(nil)

// newTestManagerWithMock creates a test MultipoolerManager with a mock query service
func newTestManagerWithMock(t *testing.T, tableGroup, shard string) (*MultipoolerManager, *mock.QueryService) {
	logger := slog.New(slog.DiscardHandler)
	mockQueryService := mock.NewQueryService()

	// Create a memorytopo store for tests that need topoClient (e.g., GetRemoteOperationTimeout)
	ctx := context.Background()
	topoStore := memorytopo.NewServer(ctx, "test-cell")

	multipooler := &clustermetadatapb.Multipooler{
		ShardRef: &clustermetadatapb.ShardRef{
			TableGroup: tableGroup,
			Shard:      shard,
		},
	}

	svcID := &clustermetadatapb.ID{Cell: "test-cell", Name: "test-pooler"}
	svcPoolerID, err := consensus.NewReplicaID(svcID)
	if err != nil {
		panic(err)
	}

	pm := &MultipoolerManager{
		logger:          logger,
		qsc:             &mockPoolerController{queryService: mockQueryService},
		topoClient:      topoStore,
		config:          &Config{},
		record:          newRecordFromProto(multipooler),
		serviceID:       svcID,
		servicePoolerID: svcPoolerID,
		consensusMgr: consensus.NewManagerForTesting(
			t, svcID,
			consensus.NewConsensusPromises("", svcID),
			consensus.NewRuleStore(logger, mockQueryService, noopSyncStandbyManager{}),
			nil,
		),
	}

	return pm, mockQueryService
}

// addShardRefEnsure registers the queries shardref.Ensure runs on a shard with
// neither the legacy multigres.shard_key composite nor multigres.shard_ref.
// They are not consumed, since createSidecarSchema ensures shard_ref twice
// (createMigrationTable and ensureSidecarSchemas); the mock reports the type
// as absent both times.
func addShardRefEnsure(m *mock.QueryService, record func(string)) {
	m.AddQueryPatternWithCallback(regexp.QuoteMeta(shardref.CheckLegacyTypeSQL),
		mock.MakeQueryResult([]string{"legacy", "current"}, [][]any{{false, false}}), record)
	m.AddQueryPatternWithCallback(regexp.QuoteMeta(shardref.CheckTypeSQL), mock.MakeQueryResult(nil, nil), record)
	m.AddQueryPatternWithCallback(regexp.QuoteMeta(shardref.CreateTypeSQL), mock.MakeQueryResult(nil, nil), record)
}

// addMigrationTableCreate registers the migration coordinator's state DDL that
// createSidecarSchema runs on every shard (createMigrationTable).
func addMigrationTableCreate(m *mock.QueryService) {
	m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.migration ", mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.migration_tables", mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.migration_journal", mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce("CREATE INDEX IF NOT EXISTS migration_journal_migration_id_idx", mock.MakeQueryResult(nil, nil))
}

func TestCreateSidecarSchema(t *testing.T) {
	tests := []struct {
		name          string
		tableGroup    string
		setupMock     func(m *mock.QueryService)
		expectError   bool
		errorContains string
	}{
		{
			name:       "successful schema creation for default tablegroup",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE UNLOGGED TABLE IF NOT EXISTS multigres.backend_vpid", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("INSERT INTO multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.rule_history", mock.MakeQueryResult(nil, nil))
				addFreshCatalogEnsure(m)
				addShardRefEnsure(m, nil)
				addMigrationTableCreate(m)
			},
			expectError: false,
		},
		{
			name:       "pgbackrest_repos table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos", errors.New("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create pgbackrest_repos table",
		},
		{
			name:       "schema creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("CREATE SCHEMA multigres", errors.New("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to create multigres schema",
		},
		{
			name:       "heartbeat table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE TABLE multigres.heartbeat", errors.New("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create heartbeat table",
		},
		{
			name:       "current_rule table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE TABLE multigres.current_rule", errors.New("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create current_rule table",
		},
		{
			name:       "current_rule initialization fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("INSERT INTO multigres.current_rule", errors.New("insert failed"))
			},
			expectError:   true,
			errorContains: "failed to initialize current_rule",
		},
		{
			name:       "rule_history table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("INSERT INTO multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE TABLE multigres.rule_history", errors.New("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create rule_history table",
		},
		{
			name:       "catalog schema ensure fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("INSERT INTO multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.rule_history", mock.MakeQueryResult(nil, nil))
				addShardRefEnsure(m, nil)
				addMigrationTableCreate(m)
				m.AddQueryPatternOnce("CREATE UNLOGGED TABLE IF NOT EXISTS multigres.backend_vpid", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("pg_advisory_xact_lock", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce(catalogStatePresentPattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{false}}))
				m.AddQueryPatternOnceWithError("^DROP TABLE IF EXISTS multigres.shard", errors.New("drop failed"))
			},
			expectError:   true,
			errorContains: "failed to apply catalog format 1 migration",
		},
		{
			name:       "non-catalog shard skips catalog schema",
			tableGroup: "commerce",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE UNLOGGED TABLE IF NOT EXISTS multigres.backend_vpid", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("INSERT INTO multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE multigres.rule_history", mock.MakeQueryResult(nil, nil))
				addShardRefEnsure(m, nil)
				addMigrationTableCreate(m)
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(t, tt.tableGroup, constants.DefaultShard)

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			err := pm.createSidecarSchema(ctx, testBootstrapPolicy())

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

// TestCreateSidecarSchema_CatalogTablesOnlyOnCatalogShard verifies that every
// shard gets the shard-local sidecar tables, but only the catalog shard
// (default/0-inf) runs the catalog schema ensure, and that it runs as part of
// ensureSidecarSchemas, after every other bootstrap step.
func TestCreateSidecarSchema_CatalogTablesOnlyOnCatalogShard(t *testing.T) {
	shardLocal := []string{
		"CREATE SCHEMA multigres",
		"CREATE TABLE multigres.heartbeat",
		"CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos",
		"CREATE TABLE multigres.current_rule",
		"INSERT INTO multigres.current_rule",
		"CREATE TABLE multigres.rule_history",
		"CREATE UNLOGGED TABLE IF NOT EXISTS multigres.backend_vpid",
	}
	catalogRef := regexp.MustCompile(`multigres\.(catalog_state|tablegroup|tablegroup_table|shard|shard_index|shard_key|shard_key_column|operation)\b|pg_advisory_xact_lock`)

	tests := []struct {
		name        string
		tableGroup  string
		shard       string
		wantCatalog bool
	}{
		{name: "catalog shard", tableGroup: constants.DefaultTableGroup, shard: constants.DefaultShard, wantCatalog: true},
		{name: "non-default tablegroup", tableGroup: "commerce", shard: "0-80", wantCatalog: false},
		{name: "non-default tablegroup full range", tableGroup: "commerce", shard: constants.DefaultShard, wantCatalog: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(t, tt.tableGroup, tt.shard)

			var queries []string
			record := func(q string) { queries = append(queries, q) }
			addShardRefEnsure(mockQueryService, record)
			mockQueryService.AddQueryPatternWithCallback(catalogStatePresentPattern,
				mock.MakeQueryResult([]string{"exists"}, [][]any{{false}}), record)
			mockQueryService.AddQueryPatternWithCallback(".*", mock.MakeQueryResult(nil, nil), record)

			require.NoError(t, pm.createSidecarSchema(context.Background(), testBootstrapPolicy()))

			indexOf := func(want string) int {
				return slices.IndexFunc(queries, func(q string) bool { return strings.Contains(q, want) })
			}
			for _, want := range shardLocal {
				assert.GreaterOrEqual(t, indexOf(want), 0, "expected shard-local statement %q", want)
			}
			// backend_vpid (ensureSidecarSchemas) runs after every other
			// bootstrap step.
			vpid := indexOf("multigres.backend_vpid")
			for _, want := range shardLocal[:len(shardLocal)-1] {
				assert.Less(t, indexOf(want), vpid, "ensureSidecarSchemas must run after %q", want)
			}

			firstCatalog := slices.IndexFunc(queries, catalogRef.MatchString)
			if !tt.wantCatalog {
				assert.Equal(t, -1, firstCatalog, "non-catalog shard must not touch catalog tables: %v", queries)
				return
			}
			// The catalog ensure runs inside ensureSidecarSchemas, after
			// backend_vpid, and applies the format 1 migration, every catalog
			// table and the seed rows.
			assert.Greater(t, firstCatalog, vpid, "catalog ensure must run as part of ensureSidecarSchemas")
			for _, stmt := range slices.Concat(catalogMigrations[0].statements, catalogTables, catalogSeed) {
				assert.Contains(t, queries, stmt)
			}
			assert.Equal(t, "COMMIT", queries[len(queries)-1])
		})
	}
}

func TestCreateBackendVpidTableDDL(t *testing.T) {
	pm, mockQueryService := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)

	seen := false
	mockQueryService.AddQueryPatternWithCallback("CREATE UNLOGGED TABLE IF NOT EXISTS multigres.backend_vpid", mock.MakeQueryResult(nil, nil), func(q string) {
		seen = true
		assert.Contains(t, q, "ALTER TABLE multigres.backend_vpid SET")
		assert.Contains(t, q, "autovacuum_vacuum_scale_factor = 0")
		assert.Contains(t, q, "autovacuum_vacuum_threshold = 100")
		assert.Contains(t, q, "autovacuum_analyze_scale_factor = 0")
		assert.Contains(t, q, "autovacuum_analyze_threshold = 100")
		assert.Contains(t, q, "REVOKE ALL PRIVILEGES ON TABLE multigres.backend_vpid FROM PUBLIC")
		assert.Contains(t, q, "GRANT SELECT ON TABLE multigres.backend_vpid TO PUBLIC")
		assert.NotContains(t, q, "GRANT SELECT, INSERT")
		assert.NotContains(t, q, "UPDATE, DELETE ON multigres.backend_vpid TO PUBLIC")
	})

	err := pm.createBackendVpidTable(context.Background())
	assert.NoError(t, err)
	assert.True(t, seen, "backend_vpid DDL should run")
}

// TestCreateBackendVpidTable_IdempotentAcrossRepeatedCalls guards the
// assumption openLocked's "ensure backend_vpid exists on every open/resume"
// check relies on: createBackendVpidTable uses CREATE TABLE IF NOT EXISTS, so
// calling it repeatedly (once per Pause/resume cycle: pg_rewind,
// restart-as-standby, stale-primary demote, in addition to process restart)
// is harmless and never errors just because the table is already there.
func TestCreateBackendVpidTable_IdempotentAcrossRepeatedCalls(t *testing.T) {
	pm, mockQueryService := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)

	calls := 0
	mockQueryService.AddQueryPatternWithCallback("CREATE UNLOGGED TABLE IF NOT EXISTS multigres.backend_vpid",
		mock.MakeQueryResult(nil, nil), func(string) { calls++ })

	for range 3 {
		assert.NoError(t, pm.createBackendVpidTable(context.Background()))
	}
	assert.Equal(t, 3, calls, "repeated calls across Open/resume cycles must all succeed, not just the first")
}
