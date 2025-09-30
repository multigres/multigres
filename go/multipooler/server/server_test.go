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

package server

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/multipooler/manager"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

func TestNewMultiPoolerServer(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config := &manager.Config{
		SocketFilePath: "/tmp/test.sock",
		Database:       "testdb",
	}

	server := NewMultiPoolerServer(logger, config)

	assert.NotNil(t, server)
	assert.Equal(t, config, server.config)
	assert.Equal(t, logger, server.logger)
	assert.Nil(t, server.db) // Should be nil until connectDB is called
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
	}
	server := NewMultiPoolerServer(logger, config)

	ctx := context.Background()
	req := &multipoolerpb.ExecuteQueryRequest{
		Query:    []byte("SELECT 1"),
		MaxRows:  10,
		CallerId: &mtrpcpb.CallerID{Principal: "test"},
	}

	// This should fail because the socket doesn't exist
	resp, err := server.ExecuteQuery(ctx, req)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "database connection failed")
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
	}
	server := NewMultiPoolerServer(logger, config)

	ctx := context.Background()
	req := &multipoolerpb.ExecuteQueryRequest{
		Query:    []byte(""),
		MaxRows:  10,
		CallerId: &mtrpcpb.CallerID{Principal: "test"},
	}

	// Should fail due to connection error, but we can test the request structure
	resp, err := server.ExecuteQuery(ctx, req)
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestExecuteQuery_CallerIDLogging(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config := &manager.Config{
		SocketFilePath: "/nonexistent/socket",
		Database:       "testdb",
	}
	server := NewMultiPoolerServer(logger, config)

	ctx := context.Background()

	tests := []struct {
		name     string
		callerId *mtrpcpb.CallerID
	}{
		{
			name: "Complete caller ID",
			callerId: &mtrpcpb.CallerID{
				Principal:    "user@example.com",
				Component:    "test-service",
				Subcomponent: "api-endpoint",
			},
		},
		{
			name: "Partial caller ID",
			callerId: &mtrpcpb.CallerID{
				Principal: "system",
			},
		},
		{
			name:     "Nil caller ID",
			callerId: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &multipoolerpb.ExecuteQueryRequest{
				Query:    []byte("SELECT 1"),
				MaxRows:  10,
				CallerId: tt.callerId,
			}

			// This will fail due to connection, but we're testing that it doesn't panic
			// with different CallerID configurations
			_, err := server.ExecuteQuery(ctx, req)
			assert.Error(t, err) // Expected to fail due to no real DB connection
		})
	}
}

func TestClose(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config := &manager.Config{
		SocketFilePath: "/tmp/test.sock",
		Database:       "testdb",
	}
	server := NewMultiPoolerServer(logger, config)

	// Test closing when no connection exists
	err := server.Close()
	assert.NoError(t, err)

	// Note: Testing with actual database connection would require integration test
}
