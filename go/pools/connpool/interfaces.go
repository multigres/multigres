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

// Package connpool provides a Vitess-inspired connection pooler with
// ConnectionState-aware connection management.
package connpool

import "context"

// Connection represents a pooled database connection.
// Implementations must be safe for concurrent use by a single client.
type Connection interface {
	// State returns the current state of the connection.
	// Returns nil if the connection has no state modifiers applied.
	State() *ConnectionState

	// IsClosed returns true if the connection has been closed.
	IsClosed() bool

	// Close closes the connection and releases associated resources.
	Close() error

	// ApplyState applies the given state to the connection by executing
	// the necessary SQL commands (e.g., SET commands for settings).
	// Returns an error if the state cannot be applied.
	ApplyState(ctx context.Context, state *ConnectionState) error

	// ResetState resets the connection to a clean state with no modifiers.
	// This typically involves running RESET commands or equivalent SQL.
	ResetState(ctx context.Context) error
}
