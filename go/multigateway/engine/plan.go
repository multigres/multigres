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

package engine

import (
	"fmt"

	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/server"
)

// Plan represents a query execution plan.
// It contains the root primitive and metadata about the query.
type Plan struct {
	// Original is the original SQL query string.
	Original string

	// Primitive is the root execution primitive.
	// In Phase 1, this will always be a Route primitive.
	Primitive Primitive
}

// NewPlan creates a new query plan.
func NewPlan(original string, primitive Primitive) *Plan {
	return &Plan{
		Original:  original,
		Primitive: primitive,
	}
}

// StreamExecute executes the plan by calling the root primitive's StreamExecute.
func (p *Plan) StreamExecute(
	conn *server.Conn,
	callback func(*query.QueryResult) error,
) error {
	return p.Primitive.StreamExecute(conn, callback)
}

// GetTableGroup returns the target tablegroup from the primitive.
func (p *Plan) GetTableGroup() string {
	return p.Primitive.GetTableGroup()
}

// String returns a string representation of the plan for debugging.
func (p *Plan) String() string {
	return fmt.Sprintf("Plan{original=%q, primitive=%s}", p.Original, p.Primitive.String())
}
