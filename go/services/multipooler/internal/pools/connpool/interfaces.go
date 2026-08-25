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

	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"
)

// SessionDivergence lists the GUC names on which a backend's real session
// state disagrees with its tracked settings label. It carries names only,
// never values: GUC values can hold sensitive data (e.g. request claims) and
// this type flows into logs and metrics.
type SessionDivergence struct {
	// Untracked are GUCs with source='session' on the backend that the
	// tracked settings label does not contain. This is the dangerous class:
	// session state that escaped tracking (e.g. a set_config hidden in a
	// routine body) and would leak to the next borrower.
	Untracked []string

	// Phantom are tracked label entries with no session-source GUC on the
	// backend: tracking claims a SET that the backend never saw (or saw
	// reverted), so the label promises state the connection doesn't have.
	Phantom []string

	// Mismatched are names present in both whose values still differ after
	// asking PostgreSQL to normalize the tracked value (unit and case
	// spellings like '64MB' vs '65536' are not divergence).
	Mismatched []string
}

// IsDiverged reports whether any divergence was found.
func (d SessionDivergence) IsDiverged() bool {
	return len(d.Untracked)+len(d.Phantom)+len(d.Mismatched) > 0
}

// SessionStateVerifier is implemented by pooled connections that can compare
// their tracked settings label against the backend's real session state. The
// pool's session-state scrubber probes idle connections through this
// interface and replaces any that report divergence.
type SessionStateVerifier interface {
	VerifySessionState(ctx context.Context) (SessionDivergence, error)
}

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
