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
	"sync"

	"github.com/multigres/multigres/go/pb/query"
)

// testPreparedStatement represents a prepared statement for testing.
type testPreparedStatement struct {
	Name       string
	Query      string
	ParamTypes []uint32
}

// testPortal represents a portal for testing.
type testPortal struct {
	Name      string
	Statement *testPreparedStatement
}

// testConnectionState holds test connection state.
type testConnectionState struct {
	mu                 sync.Mutex
	preparedStatements map[string]*testPreparedStatement
	portals            map[string]*testPortal
}

// newTestConnectionState creates a new test connection state.
func newTestConnectionState() *testConnectionState {
	return &testConnectionState{
		preparedStatements: make(map[string]*testPreparedStatement),
		portals:            make(map[string]*testPortal),
	}
}

// testHandlerWithState is a test handler that manages connection state.
type testHandlerWithState struct{}

// getConnectionState retrieves or initializes the test connection state.
func (h *testHandlerWithState) getConnectionState(conn *Conn) *testConnectionState {
	state := conn.GetConnectionState()
	if state == nil {
		newState := newTestConnectionState()
		conn.SetConnectionState(newState)
		return newState
	}
	return state.(*testConnectionState)
}

func (h *testHandlerWithState) HandleQuery(ctx context.Context, conn *Conn, queryStr string, callback func(ctx context.Context, result *query.QueryResult) error) error {
	return nil
}

func (h *testHandlerWithState) HandleParse(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
	if queryStr == "" {
		return fmt.Errorf("query string cannot be empty")
	}

	stmt := &testPreparedStatement{
		Name:       name,
		Query:      queryStr,
		ParamTypes: paramTypes,
	}

	state := h.getConnectionState(conn)
	state.mu.Lock()
	state.preparedStatements[name] = stmt
	state.mu.Unlock()

	return nil
}

func (h *testHandlerWithState) HandleBind(ctx context.Context, conn *Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
	state := h.getConnectionState(conn)

	state.mu.Lock()
	stmt := state.preparedStatements[stmtName]
	state.mu.Unlock()

	if stmt == nil {
		return fmt.Errorf("prepared statement \"%s\" does not exist", stmtName)
	}

	portal := &testPortal{
		Name:      portalName,
		Statement: stmt,
	}

	state.mu.Lock()
	state.portals[portalName] = portal
	state.mu.Unlock()

	return nil
}

func (h *testHandlerWithState) HandleExecute(ctx context.Context, conn *Conn, portalName string, maxRows int32, callback func(ctx context.Context, result *query.QueryResult) error) error {
	state := h.getConnectionState(conn)

	state.mu.Lock()
	portal := state.portals[portalName]
	state.mu.Unlock()

	if portal == nil {
		return fmt.Errorf("portal \"%s\" does not exist", portalName)
	}

	// Return a simple test result via callback
	return callback(ctx, &query.QueryResult{
		Fields: []*query.Field{
			{Name: "column1", Type: "int4"},
		},
		Rows: []*query.Row{
			{Values: [][]byte{[]byte("1")}},
		},
		CommandTag:   "SELECT 1",
		RowsAffected: 1,
	})
}

func (h *testHandlerWithState) HandleDescribe(ctx context.Context, conn *Conn, typ byte, name string) (*query.StatementDescription, error) {
	return nil, fmt.Errorf("not implemented")
}

func (h *testHandlerWithState) HandleClose(ctx context.Context, conn *Conn, typ byte, name string) error {
	return fmt.Errorf("not implemented")
}

func (h *testHandlerWithState) HandleSync(ctx context.Context, conn *Conn) error {
	return nil
}
