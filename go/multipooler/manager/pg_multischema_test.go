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
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/multipooler/executor"
	"github.com/multigres/multigres/go/multipooler/executor/mock"
	"github.com/multigres/multigres/go/multipooler/poolerserver"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"github.com/stretchr/testify/assert"
)

// mockPoolerController implements poolerserver.PoolerController for testing.
type mockPoolerController struct {
	querier *mock.Querier
}

func (m *mockPoolerController) Open(context.Context) error { return nil }
func (m *mockPoolerController) Close() error               { return nil }
func (m *mockPoolerController) IsHealthy() error           { return nil }
func (m *mockPoolerController) IsServing() bool            { return true }
func (m *mockPoolerController) SetServingType(context.Context, clustermetadatapb.PoolerServingStatus) error {
	return nil
}
func (m *mockPoolerController) Executor() (queryservice.QueryService, error)        { return nil, nil }
func (m *mockPoolerController) InternalQueryService() executor.InternalQueryService { return m.querier }
func (m *mockPoolerController) RegisterGRPCServices()                               {}

var _ poolerserver.PoolerController = (*mockPoolerController)(nil)

// newTestManagerWithMock creates a test MultiPoolerManager with a mock querier
func newTestManagerWithMock(tableGroup, shard string) (*MultiPoolerManager, *mock.Querier) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockQuerier := mock.NewQuerier()

	pm := &MultiPoolerManager{
		logger: logger,
		qsc:    &mockPoolerController{querier: mockQuerier},
		config: &Config{
			TableGroup: tableGroup,
			Shard:      shard,
		},
	}

	return pm, mockQuerier
}

func TestCreateSidecarSchema(t *testing.T) {
	tests := []struct {
		name          string
		tableGroup    string
		setupMock     func(m *mock.Querier)
		expectError   bool
		errorContains string
	}{
		{
			name:       "successful schema creation for default tablegroup",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.Querier) {
				m.AddQueryPattern("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("CREATE TABLE IF NOT EXISTS multigres.durability_policy", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("CREATE INDEX IF NOT EXISTS idx_durability_policy_active", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("CREATE TABLE IF NOT EXISTS multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("CREATE TABLE IF NOT EXISTS multigres.tablegroup_table", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("CREATE TABLE IF NOT EXISTS multigres.shard", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:       "schema creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.Querier) {
				m.AddQueryPatternWithError("CREATE SCHEMA IF NOT EXISTS multigres", fmt.Errorf("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to create multigres schema",
		},
		{
			name:       "heartbeat table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.Querier) {
				m.AddQueryPattern("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternWithError("CREATE TABLE IF NOT EXISTS multigres.heartbeat", fmt.Errorf("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create heartbeat table",
		},
		{
			name:       "durability_policy table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.Querier) {
				m.AddQueryPattern("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternWithError("CREATE TABLE IF NOT EXISTS multigres.durability_policy", fmt.Errorf("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create durability_policy table",
		},
		{
			name:       "index creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.Querier) {
				m.AddQueryPattern("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("CREATE TABLE IF NOT EXISTS multigres.durability_policy", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternWithError("CREATE INDEX IF NOT EXISTS idx_durability_policy_active", fmt.Errorf("index creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create durability_policy index",
		},
		{
			name:       "tablegroup table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.Querier) {
				m.AddQueryPattern("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("CREATE TABLE IF NOT EXISTS multigres.durability_policy", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("CREATE INDEX IF NOT EXISTS idx_durability_policy_active", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternWithError("CREATE TABLE IF NOT EXISTS multigres.tablegroup", fmt.Errorf("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create tablegroup table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQuerier := newTestManagerWithMock(tt.tableGroup, constants.DefaultShard)

			tt.setupMock(mockQuerier)

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
		})
	}
}

func TestInsertDurabilityPolicy(t *testing.T) {
	tests := []struct {
		name          string
		policyName    string
		quorumRule    []byte
		setupMock     func(m *mock.Querier)
		expectError   bool
		errorContains string
	}{
		{
			name:       "successful insert",
			policyName: "default-policy",
			quorumRule: []byte(`{"required_count": 1, "quorum_type": "ANY"}`),
			setupMock: func(m *mock.Querier) {
				m.AddQueryPattern("INSERT INTO multigres.durability_policy", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:       "insert with conflict (idempotent)",
			policyName: "existing-policy",
			quorumRule: []byte(`{"required_count": 2, "quorum_type": "FIRST"}`),
			setupMock: func(m *mock.Querier) {
				// ON CONFLICT DO NOTHING still succeeds
				m.AddQueryPattern("INSERT INTO multigres.durability_policy", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:       "insert fails with db error",
			policyName: "test-policy",
			quorumRule: []byte(`{"required_count": 1}`),
			setupMock: func(m *mock.Querier) {
				m.AddQueryPatternWithError("INSERT INTO multigres.durability_policy", fmt.Errorf("connection refused"))
			},
			expectError:   true,
			errorContains: "failed to insert durability policy",
		},
		{
			name:       "insert with complex quorum rule",
			policyName: "complex-policy",
			quorumRule: []byte(`{"required_count": 3, "quorum_type": "ANY", "cells": ["zone1", "zone2", "zone3"]}`),
			setupMock: func(m *mock.Querier) {
				m.AddQueryPattern("INSERT INTO multigres.durability_policy", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQuerier := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

			tt.setupMock(mockQuerier)

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
		})
	}
}

func TestInitializeMultischemaData(t *testing.T) {
	tests := []struct {
		name          string
		tableGroup    string
		shard         string
		setupMock     func(m *mock.Querier)
		expectError   bool
		errorContains string
	}{
		{
			name:       "successful data initialization",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
			setupMock: func(m *mock.Querier) {
				m.AddQueryPattern("INSERT INTO multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("SELECT oid FROM multigres.tablegroup", mock.MakeQueryResult([]string{"oid"}, [][]any{{int64(1)}}))
				m.AddQueryPattern("INSERT INTO multigres.shard", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:          "rejects non-default tablegroup",
			tableGroup:    "custom",
			shard:         constants.DefaultShard,
			setupMock:     func(m *mock.Querier) {},
			expectError:   true,
			errorContains: "only default tablegroup is supported",
		},
		{
			name:          "rejects non-default shard",
			tableGroup:    constants.DefaultTableGroup,
			shard:         "shard-1",
			setupMock:     func(m *mock.Querier) {},
			expectError:   true,
			errorContains: "only shard " + constants.DefaultShard + " is supported",
		},
		{
			name:       "tablegroup insert fails",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
			setupMock: func(m *mock.Querier) {
				m.AddQueryPatternWithError("INSERT INTO multigres.tablegroup", fmt.Errorf("insert failed"))
			},
			expectError:   true,
			errorContains: "failed to insert tablegroup",
		},
		{
			name:       "shard insert fails",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
			setupMock: func(m *mock.Querier) {
				m.AddQueryPattern("INSERT INTO multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("SELECT oid FROM multigres.tablegroup", mock.MakeQueryResult([]string{"oid"}, [][]any{{int64(1)}}))
				m.AddQueryPatternWithError("INSERT INTO multigres.shard", fmt.Errorf("insert failed"))
			},
			expectError:   true,
			errorContains: "failed to insert shard",
		},
		{
			name:       "idempotent insert (conflict)",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
			setupMock: func(m *mock.Querier) {
				// ON CONFLICT DO NOTHING still succeeds
				m.AddQueryPattern("INSERT INTO multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("SELECT oid FROM multigres.tablegroup", mock.MakeQueryResult([]string{"oid"}, [][]any{{int64(1)}}))
				m.AddQueryPattern("INSERT INTO multigres.shard", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQuerier := newTestManagerWithMock(tt.tableGroup, tt.shard)

			tt.setupMock(mockQuerier)

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
		})
	}
}
