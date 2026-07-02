// Copyright 2026 Supabase, Inc.
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

package planner

import (
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

const pgListeningChannelsUnsupported = "pg_listening_channels() is gateway-managed; only SELECT pg_listening_channels() and SELECT * FROM pg_listening_channels() are supported"

func unsupportedPgListeningChannels() error {
	return mterrors.NewFeatureNotSupported(pgListeningChannelsUnsupported)
}

func (p *Planner) planPgListeningChannels(sql string, stmt *ast.SelectStmt) (*engine.Plan, error) {
	if !isSupportedPgListeningChannelsSelect(stmt) {
		return nil, unsupportedPgListeningChannels()
	}
	plan := engine.NewPlan(sql, engine.NewPgListeningChannels(sql))
	plan.Type = engine.PlanTypePgListeningChannels
	return plan, nil
}

func isSupportedPgListeningChannelsSelect(stmt *ast.SelectStmt) bool {
	if stmt == nil {
		return false
	}
	switch stmt.SqlString() {
	case "SELECT pg_listening_channels()",
		"SELECT pg_catalog.pg_listening_channels()",
		"SELECT * FROM pg_listening_channels()":
		return true
	default:
		return false
	}
}
