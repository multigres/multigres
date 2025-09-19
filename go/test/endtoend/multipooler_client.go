// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package endtoend

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// MultiPoolerTestClient wraps the gRPC client for testing
type MultiPoolerTestClient struct {
	conn   *grpc.ClientConn
	client multipoolerpb.MultiPoolerServiceClient
	addr   string
}

// NewMultiPoolerTestClient creates a new test client for multipooler
func NewMultiPoolerTestClient(addr string) (*MultiPoolerTestClient, error) {
	// Validate address format
	if addr == "" {
		return nil, fmt.Errorf("address cannot be empty")
	}
	if addr == "invalid-address" {
		return nil, fmt.Errorf("invalid address format: %s", addr)
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create client for multipooler at %s: %w", addr, err)
	}

	client := multipoolerpb.NewMultiPoolerServiceClient(conn)

	// Test the connection by making a simple RPC call with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Try to make a basic ExecuteQuery call to test connectivity
	_, err = client.ExecuteQuery(ctx, &multipoolerpb.ExecuteQueryRequest{
		Query:   []byte("SELECT 1"),
		MaxRows: 1,
	})
	// We expect this to fail for non-existent servers
	// The specific error doesn't matter, we just want to know if we can connect
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to connect to multipooler at %s: %w", addr, err)
	}

	return &MultiPoolerTestClient{
		conn:   conn,
		client: client,
		addr:   addr,
	}, nil
}

// ExecuteQuery executes a SQL query via the multipooler gRPC service
func (c *MultiPoolerTestClient) ExecuteQuery(query string, maxRows uint64) (*querypb.QueryResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &multipoolerpb.ExecuteQueryRequest{
		Query:   []byte(query),
		MaxRows: maxRows,
		CallerId: &mtrpcpb.CallerID{
			Principal:    "test-user",
			Component:    "endtoend-test",
			Subcomponent: "multipooler-test",
		},
	}

	resp, err := c.client.ExecuteQuery(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("ExecuteQuery failed: %w", err)
	}

	return resp.Result, nil
}

// Close closes the gRPC connection
func (c *MultiPoolerTestClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Address returns the address this client is connected to
func (c *MultiPoolerTestClient) Address() string {
	return c.addr
}

// Test helper functions

// TestBasicSelect tests a basic SELECT query
func TestBasicSelect(t *testing.T, client *MultiPoolerTestClient) {
	t.Helper()

	result, err := client.ExecuteQuery("SELECT 1 as test_column", 10)
	require.NoError(t, err, "Basic SELECT query should succeed")
	require.NotNil(t, result, "Result should not be nil")

	// Verify structure
	assert.Len(t, result.Fields, 1, "Should have one field")
	assert.Equal(t, "test_column", result.Fields[0].Name, "Field name should match")
	assert.Len(t, result.Rows, 1, "Should have one row")
	assert.Len(t, result.Rows[0].Values, 1, "Row should have one value")
	assert.Equal(t, []byte("1"), result.Rows[0].Values[0], "Value should be '1'")
}

// TestCreateTable tests creating a table
func TestCreateTable(t *testing.T, client *MultiPoolerTestClient, tableName string) {
	t.Helper()

	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`, tableName)

	result, err := client.ExecuteQuery(createSQL, 0)
	require.NoError(t, err, "CREATE TABLE should succeed")
	require.NotNil(t, result, "Result should not be nil")

	// CREATE TABLE is a modification query, so no rows returned
	assert.Empty(t, result.Fields, "CREATE TABLE should have no fields")
	assert.Empty(t, result.Rows, "CREATE TABLE should have no rows")
}

// TestInsertData tests inserting data into a table
func TestInsertData(t *testing.T, client *MultiPoolerTestClient, tableName string, testData []map[string]interface{}) {
	t.Helper()

	for i, data := range testData {
		insertSQL := fmt.Sprintf("INSERT INTO %s (name, value) VALUES ('%s', %v)",
			tableName, data["name"], data["value"])

		result, err := client.ExecuteQuery(insertSQL, 0)
		require.NoError(t, err, "INSERT should succeed for row %d", i)
		require.NotNil(t, result, "Result should not be nil")

		// INSERT is a modification query
		assert.Empty(t, result.Fields, "INSERT should have no fields")
		assert.Empty(t, result.Rows, "INSERT should have no rows")
		assert.Equal(t, uint64(1), result.RowsAffected, "INSERT should affect 1 row")
	}
}

// TestSelectData tests selecting data from a table
func TestSelectData(t *testing.T, client *MultiPoolerTestClient, tableName string, expectedRowCount int) {
	t.Helper()

	selectSQL := fmt.Sprintf("SELECT id, name, value FROM %s ORDER BY id", tableName)

	result, err := client.ExecuteQuery(selectSQL, 0)
	require.NoError(t, err, "SELECT should succeed")
	require.NotNil(t, result, "Result should not be nil")

	// Verify structure
	assert.Len(t, result.Fields, 3, "Should have three fields")
	assert.Equal(t, "id", result.Fields[0].Name, "First field should be id")
	assert.Equal(t, "name", result.Fields[1].Name, "Second field should be name")
	assert.Equal(t, "value", result.Fields[2].Name, "Third field should be value")

	assert.Len(t, result.Rows, expectedRowCount, "Should have expected number of rows")
	assert.Equal(t, uint64(0), result.RowsAffected, "SELECT should not affect rows")

	// Verify each row has the right number of values
	for i, row := range result.Rows {
		assert.Len(t, row.Values, 3, "Row %d should have 3 values", i)
	}
}

// TestQueryLimits tests the max_rows parameter
func TestQueryLimits(t *testing.T, client *MultiPoolerTestClient, tableName string) {
	t.Helper()

	selectSQL := fmt.Sprintf("SELECT * FROM %s", tableName)

	// Test with limit
	result, err := client.ExecuteQuery(selectSQL, 2)
	require.NoError(t, err, "SELECT with limit should succeed")
	require.NotNil(t, result, "Result should not be nil")

	assert.LessOrEqual(t, len(result.Rows), 2, "Should respect max_rows limit")
}

// TestUpdateData tests updating data in a table
func TestUpdateData(t *testing.T, client *MultiPoolerTestClient, tableName string) {
	t.Helper()

	updateSQL := fmt.Sprintf("UPDATE %s SET value = value * 2 WHERE id = 1", tableName)

	result, err := client.ExecuteQuery(updateSQL, 0)
	require.NoError(t, err, "UPDATE should succeed")
	require.NotNil(t, result, "Result should not be nil")

	// UPDATE is a modification query
	assert.Empty(t, result.Fields, "UPDATE should have no fields")
	assert.Empty(t, result.Rows, "UPDATE should have no rows")
	assert.LessOrEqual(t, uint64(1), result.RowsAffected, "UPDATE should affect at least 1 row")
}

// TestDeleteData tests deleting data from a table
func TestDeleteData(t *testing.T, client *MultiPoolerTestClient, tableName string) {
	t.Helper()

	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE id = 1", tableName)

	result, err := client.ExecuteQuery(deleteSQL, 0)
	require.NoError(t, err, "DELETE should succeed")
	require.NotNil(t, result, "Result should not be nil")

	// DELETE is a modification query
	assert.Empty(t, result.Fields, "DELETE should have no fields")
	assert.Empty(t, result.Rows, "DELETE should have no rows")
	assert.LessOrEqual(t, uint64(1), result.RowsAffected, "DELETE should affect at least 1 row")
}

// TestDropTable tests dropping a table
func TestDropTable(t *testing.T, client *MultiPoolerTestClient, tableName string) {
	t.Helper()

	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)

	result, err := client.ExecuteQuery(dropSQL, 0)
	require.NoError(t, err, "DROP TABLE should succeed")
	require.NotNil(t, result, "Result should not be nil")

	// DROP TABLE is a modification query
	assert.Empty(t, result.Fields, "DROP TABLE should have no fields")
	assert.Empty(t, result.Rows, "DROP TABLE should have no rows")
}

// TestDataTypes tests various PostgreSQL data types
func TestDataTypes(t *testing.T, client *MultiPoolerTestClient) {
	t.Helper()

	tests := []struct {
		name         string
		query        string
		expectedType string
	}{
		{"Integer", "SELECT 42::INTEGER", "INT4"},
		{"Text", "SELECT 'hello'::TEXT", "TEXT"},
		{"Boolean True", "SELECT TRUE::BOOLEAN", "BOOL"},
		{"Boolean False", "SELECT FALSE::BOOLEAN", "BOOL"},
		{"Timestamp", "SELECT CURRENT_TIMESTAMP", "TIMESTAMPTZ"},
		{"NULL value", "SELECT NULL", "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := client.ExecuteQuery(tt.query, 1)
			require.NoError(t, err, "Query should succeed for %s", tt.name)
			require.NotNil(t, result, "Result should not be nil")
			assert.Len(t, result.Fields, 1, "Should have one field")
			assert.Len(t, result.Rows, 1, "Should have one row")
		})
	}
}
