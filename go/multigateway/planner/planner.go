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

// Package planner handles query planning for multigateway.
// It analyzes SQL queries and creates execution plans with appropriate primitives.
package planner

import (
	"log/slog"

	"github.com/multigres/multigres/go/multigateway/engine"
	"github.com/multigres/multigres/go/pgprotocol/server"
)

// Planner is responsible for creating query execution plans.
type Planner struct {
	// defaultTableGroup is the tablegroup to use when routing queries.
	// For Phase 1, all queries are routed to this tablegroup.
	defaultTableGroup string

	logger *slog.Logger
}

// NewPlanner creates a new query planner.
func NewPlanner(defaultTableGroup string, logger *slog.Logger) *Planner {
	return &Planner{
		defaultTableGroup: defaultTableGroup,
		logger:            logger,
	}
}

// Plan creates an execution plan for the given SQL query.
//
// Phase 1 Implementation:
// - Does NOT parse the query (just passes SQL through)
// - Routes ALL queries to the default tablegroup
// - Creates a simple Route primitive
//
// Future phases will:
// - Parse SQL to understand query structure
// - Analyze which tablegroups are needed
// - Create more complex primitives (joins, aggregates, etc.)
func (p *Planner) Plan(sql string, conn *server.Conn) (*engine.Plan, error) {
	p.logger.Debug("planning query",
		"query", sql,
		"user", conn.User(),
		"database", conn.Database(),
		"default_tablegroup", p.defaultTableGroup)

	// Phase 1: Create a simple route to default tablegroup
	// TODO(Phase 2): Parse SQL and analyze query structure
	// TODO(Phase 2): Determine actual target tablegroup(s) and shards
	// TODO(Phase 3): Create complex primitives for joins, aggregates, etc.

	route := engine.NewRoute(
		p.defaultTableGroup,
		"", // Empty shard for unsharded/any shard
		sql,
	)

	plan := engine.NewPlan(sql, route)

	p.logger.Debug("plan created",
		"plan", plan.String(),
		"tablegroup", plan.GetTableGroup())

	return plan, nil
}

// SetDefaultTableGroup updates the default tablegroup for routing.
// This allows dynamic configuration changes.
func (p *Planner) SetDefaultTableGroup(tableGroup string) {
	p.defaultTableGroup = tableGroup
	p.logger.Info("default tablegroup updated", "tablegroup", tableGroup)
}

// GetDefaultTableGroup returns the current default tablegroup.
func (p *Planner) GetDefaultTableGroup() string {
	return p.defaultTableGroup
}
