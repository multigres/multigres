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
	"context"
	"fmt"
	"strings"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/multigateway/handler"
)

// Sequence executes multiple primitives in order.
// If any primitive fails, execution stops and returns the error.
// This enables composable execution plans (similar to PostgreSQL's Append node).
type Sequence struct {
	Primitives []Primitive
}

// NewSequence creates a new Sequence primitive.
func NewSequence(primitives []Primitive) *Sequence {
	return &Sequence{Primitives: primitives}
}

// StreamExecute executes each primitive in order, stopping on first error.
func (s *Sequence) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// Execute each primitive in order
	for i, p := range s.Primitives {
		if err := p.StreamExecute(ctx, exec, conn, state, callback); err != nil {
			return fmt.Errorf("primitive %d (%s) failed: %w", i, p.String(), err)
		}
	}
	return nil
}

// GetTableGroup returns the tablegroup from the first primitive that has one.
func (s *Sequence) GetTableGroup() string {
	// Return tablegroup from first primitive that has one
	for _, p := range s.Primitives {
		if tg := p.GetTableGroup(); tg != "" {
			return tg
		}
	}
	return ""
}

// GetQuery returns the query from the first primitive that has one.
func (s *Sequence) GetQuery() string {
	// Return query from first primitive that has one
	for _, p := range s.Primitives {
		if q := p.GetQuery(); q != "" {
			return q
		}
	}
	return ""
}

// String returns a string representation of the sequence for debugging.
func (s *Sequence) String() string {
	parts := make([]string, len(s.Primitives))
	for i, p := range s.Primitives {
		parts[i] = p.String()
	}
	return fmt.Sprintf("Sequence[%s]", strings.Join(parts, ", "))
}

// Ensure Sequence implements Primitive interface.
var _ Primitive = (*Sequence)(nil)
