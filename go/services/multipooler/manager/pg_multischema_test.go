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
	"io"
	"log/slog"
	"testing"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/executor"
	"github.com/multigres/multigres/go/services/multipooler/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/pubsub"

	"github.com/stretchr/testify/assert"
)

// mockPoolerController implements poolerserver.PoolerController for testing.
type mockPoolerController struct {
	queryService *mock.QueryService
}

func (m *mockPoolerController) Open(context.Context) error { return nil }
func (m *mockPoolerController) Close() error               { return nil }
func (m *mockPoolerController) IsHealthy() error           { return nil }
func (m *mockPoolerController) IsServing() bool            { return true }
func (m *mockPoolerController) OnStateChange(context.Context, clustermetadatapb.PoolerType, clustermetadatapb.PoolerServingStatus) error {
	return nil
}
func (m *mockPoolerController) StartRequest(*query.Target, bool) error { return nil }
func (m *mockPoolerController) AwaitStateChange(context.Context, clustermetadatapb.PoolerType, clustermetadatapb.PoolerServingStatus) {
}
func (m *mockPoolerController) Executor() (queryservice.QueryService, error) { return nil, nil }
func (m *mockPoolerController) InternalQueryService() executor.InternalQueryService {
	return m.queryService
}
func (m *mockPoolerController) RegisterGRPCServices()                {}
func (m *mockPoolerController) SetPubSubListener(_ *pubsub.Listener) {}
func (m *mockPoolerController) PubSubListener() *pubsub.Listener     { return nil }

var _ poolerserver.PoolerController = (*mockPoolerController)(nil)

// newTestManagerWithMock creates a test MultiPoolerManager with a mock query service
func newTestManagerWithMock(tableGroup, shard string) (*MultiPoolerManager, *mock.QueryService) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockQueryService := mock.NewQueryService()

	// Create a memorytopo store for tests that need topoClient (e.g., GetRemoteOperationTimeout)
	ctx := context.Background()
	topoStore := memorytopo.NewServer(ctx, "test-cell")

	multiPooler := &clustermetadatapb.MultiPooler{
		TableGroup: tableGroup,
		Shard:      shard,
	}

	svcID := &clustermetadatapb.ID{Cell: "test-cell", Name: "test-pooler"}
	svcPoolerID, err := newPoolerID(svcID)
	if err != nil {
		panic(err)
	}

	pm := &MultiPoolerManager{
		logger:          logger,
		qsc:             &mockPoolerController{queryService: mockQueryService},
		topoClient:      topoStore,
		config:          &Config{},
		multipooler:     multiPooler,
		serviceID:       svcID,
		servicePoolerID: svcPoolerID,
	}
	pm.rules = newRuleStore(logger, mockQueryService)

	return pm, mockQueryService
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
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("INSERT INTO multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.rule_history", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.tablegroup_table", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.shard", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE OR REPLACE FUNCTION multigres.notify_schema_change()", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("DROP EVENT TRIGGER IF EXISTS multigres_schema_change", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE EVENT TRIGGER multigres_schema_change", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:       "schema change notify function creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.leadership_history", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE INDEX IF NOT EXISTS idx_leadership_history_term_event", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.tablegroup_table", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.shard", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE OR REPLACE FUNCTION multigres.notify_schema_change()", errors.New("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to create schema change notify function",
		},
		{
			name:       "drop event trigger fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.leadership_history", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE INDEX IF NOT EXISTS idx_leadership_history_term_event", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.tablegroup_table", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.shard", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE OR REPLACE FUNCTION multigres.notify_schema_change()", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("DROP EVENT TRIGGER IF EXISTS multigres_schema_change", errors.New("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to drop existing schema change event trigger",
		},
		{
			name:       "create event trigger fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.leadership_history", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE INDEX IF NOT EXISTS idx_leadership_history_term_event", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.tablegroup_table", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.shard", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE OR REPLACE FUNCTION multigres.notify_schema_change()", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("DROP EVENT TRIGGER IF EXISTS multigres_schema_change", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE EVENT TRIGGER multigres_schema_change", errors.New("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to create schema change event trigger",
		},
		{
			name:       "schema creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("CREATE SCHEMA IF NOT EXISTS multigres", errors.New("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to create multigres schema",
		},
		{
			name:       "heartbeat table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE TABLE IF NOT EXISTS multigres.heartbeat", errors.New("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create heartbeat table",
		},
		{
			name:       "current_rule table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE TABLE IF NOT EXISTS multigres.current_rule", errors.New("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create current_rule table",
		},
		{
			name:       "current_rule initialization fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("INSERT INTO multigres.current_rule", errors.New("insert failed"))
			},
			expectError:   true,
			errorContains: "failed to initialize current_rule",
		},
		{
			name:       "rule_history table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("INSERT INTO multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE TABLE IF NOT EXISTS multigres.rule_history", errors.New("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create rule_history table",
		},
		{
			name:       "tablegroup table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("INSERT INTO multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.rule_history", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE TABLE IF NOT EXISTS multigres.tablegroup", errors.New("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create tablegroup table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(tt.tableGroup, constants.DefaultShard)

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			err := pm.createSidecarSchema(ctx)

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

func TestInitializeMultischemaData(t *testing.T) {
	tests := []struct {
		name          string
		tableGroup    string
		shard         string
		setupMock     func(m *mock.QueryService)
		expectError   bool
		errorContains string
	}{
		{
			name:       "successful data initialization",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT oid FROM multigres.tablegroup", mock.MakeQueryResult([]string{"oid"}, [][]any{{int64(1)}}))
				m.AddQueryPatternOnce("INSERT INTO multigres.shard", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:          "rejects non-default tablegroup",
			tableGroup:    "custom",
			shard:         constants.DefaultShard,
			setupMock:     func(m *mock.QueryService) {},
			expectError:   true,
			errorContains: "only default tablegroup is supported",
		},
		{
			name:          "rejects non-default shard",
			tableGroup:    constants.DefaultTableGroup,
			shard:         "shard-1",
			setupMock:     func(m *mock.QueryService) {},
			expectError:   true,
			errorContains: "only shard " + constants.DefaultShard + " is supported",
		},
		{
			name:       "tablegroup insert fails",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("INSERT INTO multigres.tablegroup", errors.New("insert failed"))
			},
			expectError:   true,
			errorContains: "failed to insert tablegroup",
		},
		{
			name:       "shard insert fails",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT oid FROM multigres.tablegroup", mock.MakeQueryResult([]string{"oid"}, [][]any{{int64(1)}}))
				m.AddQueryPatternOnceWithError("INSERT INTO multigres.shard", errors.New("insert failed"))
			},
			expectError:   true,
			errorContains: "failed to insert shard",
		},
		{
			name:       "idempotent insert (conflict)",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
			setupMock: func(m *mock.QueryService) {
				// ON CONFLICT DO NOTHING still succeeds
				m.AddQueryPatternOnce("INSERT INTO multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT oid FROM multigres.tablegroup", mock.MakeQueryResult([]string{"oid"}, [][]any{{int64(1)}}))
				m.AddQueryPatternOnce("INSERT INTO multigres.shard", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(tt.tableGroup, tt.shard)

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			err := pm.initializeMultischemaData(ctx)

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
