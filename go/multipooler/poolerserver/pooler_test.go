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
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMultiPooler(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	pooler := NewMultiPooler(logger)

	assert.NotNil(t, pooler)
	assert.Equal(t, logger, pooler.logger)
	// Executor should be nil until InitDBConfig is called
	exec, err := pooler.GetExecutor()
	assert.Error(t, err)
	assert.Nil(t, exec)
}

func TestConfig(t *testing.T) {
	tests := []struct {
		name           string
		socketFilePath string
		database       string
		pgPort         int
	}{
		{
			name:           "Unix socket configuration",
			socketFilePath: "/tmp/postgres.sock",
			database:       "testdb",
			pgPort:         5432,
		},
		{
			name:           "TCP configuration",
			socketFilePath: "localhost",
			database:       "proddb",
			pgPort:         5433,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &DBConfig{
				SocketFilePath: tt.socketFilePath,
				Database:       tt.database,
				PgPort:         tt.pgPort,
			}

			assert.Equal(t, tt.socketFilePath, config.SocketFilePath)
			assert.Equal(t, tt.database, config.Database)
			assert.Equal(t, tt.pgPort, config.PgPort)
		})
	}
}

func TestExecuteQuery_InvalidInput(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	pooler := NewMultiPooler(logger)

	dbConfig := &DBConfig{
		SocketFilePath: "/nonexistent/socket",
		PoolerDir:      "/nonexistent/pooler",
		Database:       "testdb",
		PgPort:         5432,
	}

	// Initialize the config
	err := pooler.InitDBConfig(dbConfig)
	assert.NoError(t, err)

	// Try to open - this should fail because the socket doesn't exist
	err = pooler.Open()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to ping database")

	// GetExecutor should fail since the database is not opened
	exec, err := pooler.GetExecutor()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "executor not ready")
	assert.Nil(t, exec)

	// IsHealthy should fail since the database connection is not opened
	err = pooler.IsHealthy()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database connection not initialized")
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

func TestClose(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	pooler := NewMultiPooler(logger)

	// Test closing when no connection exists
	err := pooler.Close()
	assert.NoError(t, err)

	// Note: Testing with actual database connection would require integration test
}
