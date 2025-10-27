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

// Package engine contains the query execution primitives for multigateway.
// Primitives are the building blocks of query plans and handle routing,
// execution coordination, and result aggregation.
package engine

import (
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/server"
)

// Primitive is the building block of the query execution plan.
// Each primitive represents an operation in the query execution tree
// (e.g., route to tablegroup, join, aggregate, etc.).
type Primitive interface {
	// StreamExecute executes the primitive and streams results via callback.
	// This is the main execution method for all primitives.
	StreamExecute(
		conn *server.Conn,
		callback func(*query.QueryResult) error,
	) error

	// GetTableGroup returns the target tablegroup for this primitive.
	// Returns empty string if primitive doesn't target a specific tablegroup.
	GetTableGroup() string

	// GetQuery returns the SQL query to be executed.
	GetQuery() string

	// String returns a description of the primitive for logging/debugging.
	String() string
}
