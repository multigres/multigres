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
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// newTestManagerWithMultipooler creates a test MultiPoolerManager with a mock DB and multipooler metadata
func newTestManagerWithMultipooler(t *testing.T, tableGroup, shard string) (*MultiPoolerManager, sqlmock.Sqlmock) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	pm := &MultiPoolerManager{
		logger: logger,
		db:     mockDB,
	}

	// Set up cached multipooler with tablegroup and shard
	multipooler := &clustermetadatapb.MultiPooler{
		TableGroup: tableGroup,
		Shard:      shard,
	}
	pm.cachedMultipooler.multipooler = topo.NewMultiPoolerInfo(multipooler, nil)

	return pm, mock
}

func TestCreateSidecarSchema(t *testing.T) {
	tests := []struct {
		name          string
		tableGroup    string
		setupMock     func(mock sqlmock.Sqlmock)
		expectError   bool
		errorContains string
	}{
		{
			name:       "successful schema creation for default tablegroup",
			tableGroup: "default",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("CREATE SCHEMA IF NOT EXISTS multigres")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS multigres.heartbeat")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS multigres.durability_policy")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE INDEX IF NOT EXISTS idx_durability_policy_active")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS multigres.tablegroup")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS multigres.table")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS multigres.shard")).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			expectError: false,
		},
		{
			name:          "rejects non-default tablegroup",
			tableGroup:    "custom",
			setupMock:     func(mock sqlmock.Sqlmock) {},
			expectError:   true,
			errorContains: "only default tablegroup is supported",
		},
		{
			name:       "schema creation fails",
			tableGroup: "default",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("CREATE SCHEMA IF NOT EXISTS multigres")).
					WillReturnError(fmt.Errorf("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to create multigres schema",
		},
		{
			name:       "heartbeat table creation fails",
			tableGroup: "default",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("CREATE SCHEMA IF NOT EXISTS multigres")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS multigres.heartbeat")).
					WillReturnError(fmt.Errorf("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create heartbeat table",
		},
		{
			name:       "durability_policy table creation fails",
			tableGroup: "default",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("CREATE SCHEMA IF NOT EXISTS multigres")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS multigres.heartbeat")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS multigres.durability_policy")).
					WillReturnError(fmt.Errorf("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create durability_policy table",
		},
		{
			name:       "index creation fails",
			tableGroup: "default",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("CREATE SCHEMA IF NOT EXISTS multigres")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS multigres.heartbeat")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS multigres.durability_policy")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE INDEX IF NOT EXISTS idx_durability_policy_active")).
					WillReturnError(fmt.Errorf("index creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create durability_policy index",
		},
		{
			name:       "tablegroup table creation fails",
			tableGroup: "default",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("CREATE SCHEMA IF NOT EXISTS multigres")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS multigres.heartbeat")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS multigres.durability_policy")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE INDEX IF NOT EXISTS idx_durability_policy_active")).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS multigres.tablegroup")).
					WillReturnError(fmt.Errorf("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create tablegroup table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mock := newTestManagerWithMultipooler(t, tt.tableGroup, "0-inf")
			defer pm.db.Close()

			tt.setupMock(mock)

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

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestInsertDurabilityPolicy(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		name          string
		policyName    string
		quorumRule    []byte
		setupMock     func(mock sqlmock.Sqlmock)
		expectError   bool
		errorContains string
	}{
		{
			name:       "successful insert",
			policyName: "default-policy",
			quorumRule: []byte(`{"required_count": 1, "quorum_type": "ANY"}`),
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO multigres.durability_policy")).
					WithArgs("default-policy", []byte(`{"required_count": 1, "quorum_type": "ANY"}`)).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectError: false,
		},
		{
			name:       "insert with conflict (idempotent)",
			policyName: "existing-policy",
			quorumRule: []byte(`{"required_count": 2, "quorum_type": "FIRST"}`),
			setupMock: func(mock sqlmock.Sqlmock) {
				// ON CONFLICT DO NOTHING returns 0 rows affected
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO multigres.durability_policy")).
					WithArgs("existing-policy", []byte(`{"required_count": 2, "quorum_type": "FIRST"}`)).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			expectError: false,
		},
		{
			name:       "insert fails with db error",
			policyName: "test-policy",
			quorumRule: []byte(`{"required_count": 1}`),
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO multigres.durability_policy")).
					WithArgs("test-policy", []byte(`{"required_count": 1}`)).
					WillReturnError(fmt.Errorf("connection refused"))
			},
			expectError:   true,
			errorContains: "failed to insert durability policy",
		},
		{
			name:       "insert with complex quorum rule",
			policyName: "complex-policy",
			quorumRule: []byte(`{"required_count": 3, "quorum_type": "ANY", "cells": ["zone1", "zone2", "zone3"]}`),
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO multigres.durability_policy")).
					WithArgs("complex-policy", []byte(`{"required_count": 3, "quorum_type": "ANY", "cells": ["zone1", "zone2", "zone3"]}`)).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer mockDB.Close()

			tt.setupMock(mock)

			pm := &MultiPoolerManager{
				logger: logger,
				db:     mockDB,
			}

			ctx := context.Background()
			err = pm.insertDurabilityPolicy(ctx, tt.policyName, tt.quorumRule)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestInitializeMultischemaData(t *testing.T) {
	tests := []struct {
		name          string
		tableGroup    string
		shard         string
		setupMock     func(mock sqlmock.Sqlmock)
		expectError   bool
		errorContains string
	}{
		{
			name:       "successful data initialization",
			tableGroup: "default",
			shard:      "0-inf",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO multigres.tablegroup")).
					WithArgs("default").
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectQuery(regexp.QuoteMeta("SELECT oid FROM multigres.tablegroup WHERE name = $1")).
					WithArgs("default").
					WillReturnRows(sqlmock.NewRows([]string{"oid"}).AddRow(1))
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO multigres.shard")).
					WithArgs(int64(1), "0-inf").
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectError: false,
		},
		{
			name:          "rejects non-default tablegroup",
			tableGroup:    "custom",
			shard:         "0-inf",
			setupMock:     func(mock sqlmock.Sqlmock) {},
			expectError:   true,
			errorContains: "only default tablegroup is supported",
		},
		{
			name:          "rejects non-default shard",
			tableGroup:    "default",
			shard:         "shard-1",
			setupMock:     func(mock sqlmock.Sqlmock) {},
			expectError:   true,
			errorContains: "only shard 0-inf is supported",
		},
		{
			name:       "tablegroup insert fails",
			tableGroup: "default",
			shard:      "0-inf",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO multigres.tablegroup")).
					WithArgs("default").
					WillReturnError(fmt.Errorf("insert failed"))
			},
			expectError:   true,
			errorContains: "failed to insert tablegroup",
		},
		{
			name:       "shard insert fails",
			tableGroup: "default",
			shard:      "0-inf",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO multigres.tablegroup")).
					WithArgs("default").
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectQuery(regexp.QuoteMeta("SELECT oid FROM multigres.tablegroup WHERE name = $1")).
					WithArgs("default").
					WillReturnRows(sqlmock.NewRows([]string{"oid"}).AddRow(1))
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO multigres.shard")).
					WithArgs(int64(1), "0-inf").
					WillReturnError(fmt.Errorf("insert failed"))
			},
			expectError:   true,
			errorContains: "failed to insert shard",
		},
		{
			name:       "idempotent insert (conflict)",
			tableGroup: "default",
			shard:      "0-inf",
			setupMock: func(mock sqlmock.Sqlmock) {
				// ON CONFLICT DO NOTHING returns 0 rows affected
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO multigres.tablegroup")).
					WithArgs("default").
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery(regexp.QuoteMeta("SELECT oid FROM multigres.tablegroup WHERE name = $1")).
					WithArgs("default").
					WillReturnRows(sqlmock.NewRows([]string{"oid"}).AddRow(1))
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO multigres.shard")).
					WithArgs(int64(1), "0-inf").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mock := newTestManagerWithMultipooler(t, tt.tableGroup, tt.shard)
			defer pm.db.Close()

			tt.setupMock(mock)

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

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
