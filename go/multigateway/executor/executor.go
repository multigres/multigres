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

	"github.com/multigres/multigres/go/multigateway/planner"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/server"
	"github.com/multigres/multigres/go/servenv"
)

const (
	// TODO(GuptaManan100): Remove this and use discovery to find the table group and use that.
	DefaultTableGroup = "default"
)

// Executor is the query execution engine for multigateway.
// It handles query planning, routing to appropriate multipooler instances,
// and result streaming back to clients.
type Executor struct {
	senv    *servenv.ServEnv
	planner *planner.Planner
	logger  *slog.Logger
}

// NewExecutor creates a new executor instance.
func NewExecutor(senv *servenv.ServEnv) *Executor {
	logger := senv.GetLogger().With("component", "multigateway_executor")

	return &Executor{
		senv:    senv,
		planner: planner.NewPlanner(DefaultTableGroup, logger),
		logger:  logger,
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

	// Step 1: Plan the query
	plan, err := e.planner.Plan(sql, conn)
	if err != nil {
		e.logger.Error("query planning failed",
			"query", sql,
			"error", err)
		return err
	}

	e.logger.Debug("query plan created",
		"plan", plan.String(),
		"tablegroup", plan.GetTableGroup())

	// Step 2: Execute the plan
	err = plan.StreamExecute(conn, callback)
	if err != nil {
		e.logger.Error("query execution failed",
			"query", sql,
			"plan", plan.String(),
			"error", err)
		return err
	}

	e.logger.Debug("query execution completed",
		"query", sql,
		"tablegroup", plan.GetTableGroup())

	return nil
}
