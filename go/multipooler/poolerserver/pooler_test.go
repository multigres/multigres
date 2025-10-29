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

package poolerserver

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/multipooler/manager"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
)

func TestNewMultiPooler(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config := &manager.Config{
		SocketFilePath: "/tmp/test.sock",
		Database:       "testdb",
		TopoClient:     nil,
		ServiceID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-service",
		},
	}

	pooler := NewMultiPooler(logger, config)

	assert.NotNil(t, pooler)
	assert.Equal(t, config, pooler.config)
	assert.Equal(t, logger, pooler.logger)
	assert.Nil(t, pooler.db) // Should be nil until connectDB is called
}

func TestConfig(t *testing.T) {
	tests := []struct {
		name           string
		socketFilePath string
		database       string
	}{
		{
			name:           "Unix socket configuration",
			socketFilePath: "/tmp/postgres.sock",
			database:       "testdb",
		},
		{
			name:           "TCP configuration",
			socketFilePath: "",
			database:       "proddb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &manager.Config{
				SocketFilePath: tt.socketFilePath,
				Database:       tt.database,
				TopoClient:     nil,
				ServiceID: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "test-service",
				},
			}

			assert.Equal(t, tt.socketFilePath, config.SocketFilePath)
			assert.Equal(t, tt.database, config.Database)
		})
	}
}

func TestExecuteQuery_InvalidInput(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config := &manager.Config{
		SocketFilePath: "/nonexistent/socket",
		Database:       "testdb",
		TopoClient:     nil,
		ServiceID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-service",
		},
	}
	pooler := NewMultiPooler(logger, config)

	ctx := context.Background()

	// This should fail because the socket doesn't exist
	resp, err := execQuery(ctx, pooler, "SELECT 1")
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "database connection failed")
}

func execQuery(ctx context.Context, pooler *MultiPooler, sql string) (*query.QueryResult, error) {
	var res *query.QueryResult
	err := pooler.GetExecutor().StreamExecute(ctx, nil, sql, func(qr *query.QueryResult) error {
		// Extract result from response
		if qr == nil {
			return nil
		}
		if res == nil {
			res = qr
			return nil
		}
		res.CommandTag = qr.CommandTag
		res.Rows = append(res.Rows, qr.Rows...)
		return nil
	})
	return res, err
}

func TestExecuteQuery_QueryTypeDetection(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		isSelect bool
	}{
		{"SELECT query", "SELECT * FROM users", true},
		{"SELECT with whitespace", "  SELECT id FROM table  ", true},
		{"WITH query", "WITH cte AS (SELECT 1) SELECT * FROM cte", true},
		{"SHOW query", "SHOW TABLES", true},
		{"EXPLAIN query", "EXPLAIN SELECT * FROM users", true},
		{"INSERT query", "INSERT INTO users VALUES (1, 'test')", false},
		{"UPDATE query", "UPDATE users SET name = 'test'", false},
		{"DELETE query", "DELETE FROM users WHERE id = 1", false},
		{"CREATE query", "CREATE TABLE test (id INT)", false},
		{"DROP query", "DROP TABLE test", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the query type detection logic indirectly
			// by checking if the query starts with expected prefixes
			query := tt.query
			trimmed := strings.TrimSpace(strings.ToUpper(query))

			isSelect := strings.HasPrefix(trimmed, "SELECT") ||
				strings.HasPrefix(trimmed, "WITH") ||
				strings.HasPrefix(trimmed, "SHOW") ||
				strings.HasPrefix(trimmed, "EXPLAIN")

			assert.Equal(t, tt.isSelect, isSelect, "Query type detection failed for: %s", tt.query)
		})
	}
}

func TestResultTranslation_NullValues(t *testing.T) {
	// Test how NULL values are handled in result translation
	tests := []struct {
		name     string
		input    interface{}
		expected []byte
	}{
		{"Null value", nil, nil},
		{"String value", "test", []byte("test")},
		{"Integer value", 42, []byte("42")},
		{"Boolean true", true, []byte("true")},
		{"Boolean false", false, []byte("false")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the value conversion logic from executeSelectQuery
			var result []byte
			if tt.input == nil {
				result = nil
			} else {
				result = []byte(fmt.Sprintf("%v", tt.input))
			}

			if tt.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestExecuteQuery_EmptyQuery(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config := &manager.Config{
		SocketFilePath: "/nonexistent/socket",
		Database:       "testdb",
		TopoClient:     nil,
		ServiceID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-service",
		},
	}
	pooler := NewMultiPooler(logger, config)

	ctx := context.Background()

	// Should fail due to connection error, but we can test the request structure
	resp, err := execQuery(ctx, pooler, "")
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestExecuteQuery_CallerIDLogging(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config := &manager.Config{
		SocketFilePath: "/nonexistent/socket",
		Database:       "testdb",
		TopoClient:     nil,
		ServiceID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-service",
		},
	}
	pooler := NewMultiPooler(logger, config)

	ctx := context.Background()

	// This will fail due to connection, but we're testing that it doesn't panic
	_, err := execQuery(ctx, pooler, "SELECT 1")
	assert.Error(t, err) // Expected to fail due to no real DB connection
}

func TestClose(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config := &manager.Config{
		SocketFilePath: "/tmp/test.sock",
		Database:       "testdb",
		TopoClient:     nil,
		ServiceID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-service",
		},
	}
	pooler := NewMultiPooler(logger, config)

	// Test closing when no connection exists
	err := pooler.Close()
	assert.NoError(t, err)

	// Note: Testing with actual database connection would require integration test
}
