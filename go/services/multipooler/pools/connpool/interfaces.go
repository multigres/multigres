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

// Settings-aware connection management.
package connpool

import (
	"context"

	"github.com/multigres/multigres/go/services/multipooler/connstate"
)

// Connection represents a pooled database connection.
// Implementations must be safe for concurrent use by a single client.
type Connection interface {
	// Settings returns the current settings applied to this connection.
	// Returns nil if the connection has no settings applied (clean connection).
	// This is used by the pool for routing connections to the appropriate bucket.
	Settings() *connstate.Settings

	// IsClosed returns true if the connection has been closed.
	IsClosed() bool

	// Close closes the connection and releases associated resources.
	Close() error

	// ApplySettings transitions the connection to the desired settings state.
	// It diffs current tracked settings against desired: executes individual
	// RESET commands for removed variables, then SET SESSION commands for all
	// desired variables. Updates tracked state to desired.
	ApplySettings(ctx context.Context, desired *connstate.Settings) error

	// ResetAllSettings resets the connection to a clean state with no settings.
	// This executes RESET ALL to clear all session variables at once.
	ResetAllSettings(ctx context.Context) error
}
