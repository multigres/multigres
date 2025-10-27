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

package executor

import (
	"log/slog"

	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/server"
	"github.com/multigres/multigres/go/servenv"
)

// Executor is the query execution engine for multigateway.
// It handles query routing to appropriate multipooler instances and manages
// result streaming back to clients.
type Executor struct {
	senv   *servenv.ServEnv
	logger *slog.Logger
}

// NewExecutor creates a new executor instance.
func NewExecutor(senv *servenv.ServEnv) *Executor {
	return &Executor{
		senv:   senv,
		logger: senv.GetLogger().With("component", "multigateway_executor"),
	}
}

// StreamExecute executes a query and streams results back via the callback function.
// This method will eventually route queries to multipooler via gRPC.
//
// The callback function is invoked for each chunk of results. For large result sets,
// the callback may be invoked multiple times with partial results.
func (e *Executor) StreamExecute(
	conn *server.Conn,
	sql string,
	callback func(*query.QueryResult) error,
) error {
	e.logger.Debug("executing query",
		"query", sql,
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	// TODO(GuptaManan100): Implement actual query routing to multipooler
	// For now, return a stub result to demonstrate the pattern
	result := &query.QueryResult{
		Fields: []*query.Field{
			{
				Name:         "message",
				Type:         "text",
				DataTypeOid:  25, // text type OID
				DataTypeSize: -1, // variable length
				Format:       0,  // text format
			},
		},
		Rows: []*query.Row{
			{
				Values: [][]byte{
					[]byte("Hello from multigateway!"),
				},
			},
		},
		CommandTag: "SELECT 1",
	}

	return callback(result)
}
