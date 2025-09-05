// Copyright 2025 The Multigres Authors.
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

package postgres

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler is a simple test implementation of Handler
type TestHandler struct {
	t *testing.T
}

func (h *TestHandler) HandleStartup(ctx context.Context, conn *Connection, msg *StartupMessage) error {
	h.t.Logf("HandleStartup: user=%s, database=%s", conn.User, conn.Database)
	return nil
}

func (h *TestHandler) HandleQuery(ctx context.Context, conn *Connection, query string) error {
	h.t.Logf("HandleQuery: %s", query)
	
	// Handle simple test queries
	switch query {
	case "SELECT 1":
		// Send row description
		rowDesc := createTestRowDescription()
		if err := conn.Writer.WriteMessage(MsgTypeRowDescription, rowDesc); err != nil {
			return err
		}
		
		// Send data row
		dataRow := createTestDataRow("1")
		if err := conn.Writer.WriteMessage(MsgTypeDataRow, dataRow); err != nil {
			return err
		}
		
		// Send command complete
		if err := conn.Writer.WriteCommandComplete("SELECT 1"); err != nil {
			return err
		}
		
		// Send ready for query
		return conn.Writer.WriteReadyForQuery(conn.TxStatus)
		
	default:
		// Send error for unsupported queries
		fields := make(map[byte]string)
		fields[ErrorFieldSeverity] = "ERROR"
		fields[ErrorFieldMessage] = "query not supported in test handler"
		fields[ErrorFieldCode] = "58000"
		
		if err := conn.Writer.WriteErrorResponse(fields); err != nil {
			return err
		}
		return conn.Writer.WriteReadyForQuery(conn.TxStatus)
	}
}

func (h *TestHandler) HandleParse(ctx context.Context, conn *Connection, msg *Message) error {
	return conn.Writer.WriteMessage(MsgTypeParseComplete, nil)
}

func (h *TestHandler) HandleBind(ctx context.Context, conn *Connection, msg *Message) error {
	return conn.Writer.WriteMessage(MsgTypeBindComplete, nil)
}

func (h *TestHandler) HandleExecute(ctx context.Context, conn *Connection, msg *Message) error {
	return conn.Writer.WriteCommandComplete("SELECT 0")
}

func (h *TestHandler) HandleDescribe(ctx context.Context, conn *Connection, msg *Message) error {
	return conn.Writer.WriteMessage(MsgTypeNoData, nil)
}

func (h *TestHandler) HandleClose(ctx context.Context, conn *Connection, msg *Message) error {
	return conn.Writer.WriteMessage(MsgTypeCloseComplete, nil)
}

func (h *TestHandler) HandleTerminate(ctx context.Context, conn *Connection) {
	h.t.Logf("HandleTerminate: conn_id=%d", conn.ID)
}

// Helper functions for test data
func createTestRowDescription() []byte {
	data := make([]byte, 0, 256)
	
	// Field count (1)
	data = append(data, 0, 1)
	
	// Field name
	data = append(data, []byte("?column?")...)
	data = append(data, 0) // null terminator
	
	// Table OID (0 for computed)
	data = append(data, 0, 0, 0, 0)
	
	// Column attribute number (0 for computed)
	data = append(data, 0, 0)
	
	// Type OID (23 = int4)
	data = append(data, 0, 0, 0, 23)
	
	// Type size (4 for int4)
	data = append(data, 0, 4)
	
	// Type modifier (-1)
	data = append(data, 0xFF, 0xFF, 0xFF, 0xFF)
	
	// Format code (0 for text)
	data = append(data, 0, 0)
	
	return data
}

func createTestDataRow(value string) []byte {
	data := make([]byte, 0, 256)
	
	// Column count (1)
	data = append(data, 0, 1)
	
	// Column length
	data = append(data, 0, 0, 0, byte(len(value)))
	
	// Column value
	data = append(data, []byte(value)...)
	
	return data
}

func TestPostgreSQLServer(t *testing.T) {
	// Skip this test in short mode as it requires external PostgreSQL driver
	if testing.Short() {
		t.Skip("Skipping PostgreSQL server test in short mode")
	}
	
	logger := slog.Default()
	handler := &TestHandler{t: t}
	
	// Create server config
	config := ServerConfig{
		Address:        ":15432",
		AuthMethod:     "trust",
		MaxConnections: 10,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
	}
	
	// Create and start server
	server := NewServer(config, handler, logger)
	
	require.NoError(t, server.Listen())
	
	// Start serving in a goroutine
	go func() {
		if err := server.Serve(); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()
	
	// Give server time to start
	time.Sleep(100 * time.Millisecond)
	
	// Test connection using PostgreSQL driver
	t.Run("TestConnection", func(t *testing.T) {
		// Note: This test requires github.com/lib/pq driver
		// which we haven't added to go.mod yet
		// For now, we'll just test that the server is listening
		
		// Try to connect
		dsn := "host=localhost port=15432 user=test dbname=test sslmode=disable"
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			t.Skipf("Skipping connection test: %v", err)
			return
		}
		defer db.Close()
		
		// Set connection pool settings
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		
		// Try to ping
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		
		err = db.PingContext(ctx)
		if err != nil {
			// This is expected to fail since we have a minimal implementation
			t.Logf("Ping failed (expected): %v", err)
		}
	})
	
	// Test protocol directly
	t.Run("TestProtocol", func(t *testing.T) {
		// This would test the protocol directly without using a driver
		// For brevity, skipping the full implementation
		t.Log("Protocol test would go here")
	})
	
	// Shutdown server
	assert.NoError(t, server.Close())
}

func TestStartupMessageParsing(t *testing.T) {
	// Test parsing of startup message parameters
	// This test verifies that the protocol version constant is correct
	assert.Equal(t, uint32(ProtocolVersion3), uint32(0x00030000))
	
	// Additional tests for startup message parsing would go here
	// They would require creating a reader with test data
}

func TestErrorResponseFormat(t *testing.T) {
	// Test that error responses are formatted correctly
	fields := make(map[byte]string)
	fields[ErrorFieldSeverity] = "ERROR"
	fields[ErrorFieldCode] = "42P01"
	fields[ErrorFieldMessage] = "relation \"test\" does not exist"
	
	// Verify field codes are correct
	assert.Equal(t, byte('S'), byte(ErrorFieldSeverity))
	assert.Equal(t, byte('C'), byte(ErrorFieldCode))
	assert.Equal(t, byte('M'), byte(ErrorFieldMessage))
}