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
	"github.com/multigres/multigres/go/multipooler/executor"
	"github.com/multigres/multigres/go/multipooler/executor/mock"
	"github.com/multigres/multigres/go/multipooler/poolerserver"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

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
func (m *mockPoolerController) SetServingType(context.Context, clustermetadatapb.PoolerServingStatus) error {
	return nil
}
func (m *mockPoolerController) Executor() (queryservice.QueryService, error) { return nil, nil }
func (m *mockPoolerController) InternalQueryService() executor.InternalQueryService {
	return m.queryService
}
func (m *mockPoolerController) RegisterGRPCServices() {}

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

	pm := &MultiPoolerManager{
		logger:      logger,
		qsc:         &mockPoolerController{queryService: mockQueryService},
		topoClient:  topoStore,
		config:      &Config{},
		multipooler: multiPooler,
		serviceID:   &clustermetadatapb.ID{Cell: "test-cell", Name: "test-pooler"},
	}

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
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.durability_policy", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE INDEX IF NOT EXISTS idx_durability_policy_active", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.leadership_history", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE INDEX IF NOT EXISTS idx_leadership_history_term_event", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.tablegroup_table", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.shard", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
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
			name:       "durability_policy table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE TABLE IF NOT EXISTS multigres.durability_policy", errors.New("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create durability_policy table",
		},
		{
			name:       "index creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.durability_policy", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE INDEX IF NOT EXISTS idx_durability_policy_active", errors.New("index creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create durability_policy index",
		},
		{
			name:       "leadership_history table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.durability_policy", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE INDEX IF NOT EXISTS idx_durability_policy_active", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE TABLE IF NOT EXISTS multigres.leadership_history", errors.New("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create leadership_history table",
		},
		{
			name:       "leadership_history index creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.durability_policy", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE INDEX IF NOT EXISTS idx_durability_policy_active", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.leadership_history", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE INDEX IF NOT EXISTS idx_leadership_history_term_event", errors.New("index creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create leadership_history index",
		},
		{
			name:       "tablegroup table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.durability_policy", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE INDEX IF NOT EXISTS idx_durability_policy_active", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.leadership_history", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE INDEX IF NOT EXISTS idx_leadership_history_term", mock.MakeQueryResult(nil, nil))
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

func TestInsertDurabilityPolicy(t *testing.T) {
	tests := []struct {
		name          string
		policyName    string
		quorumRule    []byte
		setupMock     func(m *mock.QueryService)
		expectError   bool
		errorContains string
	}{
		{
			name:       "successful insert",
			policyName: "default-policy",
			quorumRule: []byte(`{"required_count": 1, "quorum_type": "ANY"}`),
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.durability_policy", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:       "insert with conflict (idempotent)",
			policyName: "existing-policy",
			quorumRule: []byte(`{"required_count": 2, "quorum_type": "FIRST"}`),
			setupMock: func(m *mock.QueryService) {
				// ON CONFLICT DO NOTHING still succeeds
				m.AddQueryPatternOnce("INSERT INTO multigres.durability_policy", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:       "insert fails with db error",
			policyName: "test-policy",
			quorumRule: []byte(`{"required_count": 1}`),
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("INSERT INTO multigres.durability_policy", errors.New("connection refused"))
			},
			expectError:   true,
			errorContains: "failed to insert durability policy",
		},
		{
			name:       "insert with complex quorum rule",
			policyName: "complex-policy",
			quorumRule: []byte(`{"required_count": 3, "quorum_type": "ANY", "cells": ["zone1", "zone2", "zone3"]}`),
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.durability_policy", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			err := pm.insertDurabilityPolicy(ctx, tt.policyName, tt.quorumRule)

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

func TestInsertLeadershipHistory(t *testing.T) {
	tests := []struct {
		name            string
		termNumber      int64
		leaderID        string
		coordinatorID   string
		walPosition     string
		operation       string
		reason          string
		cohortMembers   []string
		acceptedMembers []string
		force           bool
		setupMock       func(m *mock.QueryService)
		expectError     bool
		errorContains   string
	}{
		{
			name:            "successful insert",
			termNumber:      1,
			leaderID:        "leader-1",
			coordinatorID:   "coordinator-1",
			walPosition:     "0/1234567",
			operation:       "promotion",
			reason:          "Leadership changed due to manual promotion",
			cohortMembers:   []string{"member-1", "member-2", "member-3"},
			acceptedMembers: []string{"member-1", "member-2"},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.leadership_history", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:            "insert fails with database error",
			termNumber:      2,
			leaderID:        "leader-2",
			coordinatorID:   "coordinator-2",
			walPosition:     "0/2345678",
			operation:       "failover",
			reason:          "Leadership changed due to failover",
			cohortMembers:   []string{"member-1", "member-2"},
			acceptedMembers: []string{"member-1"},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("INSERT INTO multigres.leadership_history", errors.New("connection refused"))
			},
			expectError:   true,
			errorContains: "failed to insert history record",
		},
		{
			name:            "force mode skips insert entirely",
			termNumber:      2,
			leaderID:        "leader-2",
			coordinatorID:   "coordinator-2",
			walPosition:     "0/2345678",
			operation:       "configure",
			reason:          "Emergency replication GUC change",
			cohortMembers:   []string{"member-1", "member-2"},
			acceptedMembers: []string{"member-1"},
			force:           true,
			setupMock: func(m *mock.QueryService) {
				// No mock needed - force mode skips the insert entirely
			},
			expectError: false,
		},
		{
			name:            "insert with empty cohort and accepted members arrays",
			termNumber:      3,
			leaderID:        "leader-3",
			coordinatorID:   "coordinator-3",
			walPosition:     "0/3456789",
			operation:       "bootstrap",
			reason:          "Initial cluster bootstrap",
			cohortMembers:   []string{},
			acceptedMembers: []string{},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.leadership_history", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

			tt.setupMock(mockQueryService)

			err := pm.insertHistoryRecord(t.Context(), tt.termNumber, "promotion", tt.leaderID, tt.coordinatorID,
				tt.walPosition, tt.operation, tt.reason, tt.cohortMembers, tt.acceptedMembers, tt.force)

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

func TestInsertReplicationConfigHistory(t *testing.T) {
	tests := []struct {
		name          string
		termNumber    int64
		operation     string
		reason        string
		standbyIDs    []*clustermetadatapb.ID
		setupMock     func(m *mock.QueryService)
		expectError   bool
		errorContains string
	}{
		{
			name:       "successful insert with configure operation",
			termNumber: 1,
			operation:  "configure",
			reason:     "ConfigureSynchronousReplication called",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "us-west", Name: "replica-1"},
				{Cell: "us-west", Name: "replica-2"},
			},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.leadership_history", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:       "successful insert with add operation",
			termNumber: 2,
			operation:  "add",
			reason:     "UpdateSynchronousStandbyList: add",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "us-west", Name: "replica-3"},
			},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.leadership_history", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:       "insert fails with database error",
			termNumber: 3,
			operation:  "remove",
			reason:     "UpdateSynchronousStandbyList: remove",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "us-west", Name: "replica-1"},
			},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("INSERT INTO multigres.leadership_history", errors.New("timeout waiting for sync replication"))
			},
			expectError:   true,
			errorContains: "failed to insert history record",
		},
		{
			name:       "insert with empty standby list",
			termNumber: 4,
			operation:  "configure",
			reason:     "ConfigureSynchronousReplication called",
			standbyIDs: []*clustermetadatapb.ID{},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.leadership_history", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

			tt.setupMock(mockQueryService)

			ctx := context.Background()

			// Convert standby IDs to application names
			standbyNames := make([]string, len(tt.standbyIDs))
			for i, id := range tt.standbyIDs {
				standbyNames[i] = generateApplicationName(id)
			}

			leaderID := generateApplicationName(pm.serviceID)
			err := pm.insertHistoryRecord(ctx, tt.termNumber, "replication_config", leaderID, "", "", tt.operation, tt.reason, standbyNames, nil, false /* force */)

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
