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

// Divergence lists the names on which a backend's real state disagrees with
// the state the pool tracks for it. It carries names only, never values:
// tracked values can hold sensitive data (e.g. request claims) and this type
// flows into logs and metrics.
type Divergence struct {
	// Untracked is real backend state that tracking does not know about.
	// This is the dangerous class: state that escaped tracking (e.g. a
	// set_config hidden in a routine body) and would leak to the next
	// borrower.
	Untracked []string

	// Phantom is tracked state with no real counterpart on the backend:
	// tracking claims something the backend never saw (or saw reverted).
	Phantom []string

	// Mismatched are names present on both sides whose values differ.
	Mismatched []string
}

// IsDiverged reports whether any divergence was found.
func (d Divergence) IsDiverged() bool {
	return len(d.Untracked)+len(d.Phantom)+len(d.Mismatched) > 0
}

// ConnChecker verifies one concern of a pooled connection's real backend
// state against what the pool tracks for it — session GUCs, prepared
// statements, residual locks, and so on. The pool's scrubber runs every
// registered checker against idle connections and replaces any connection
// that reports divergence; checkers only detect, the scrubber acts.
//
// Register checkers with Pool.RegisterChecker before Pool.Open.
type ConnChecker[C Connection] interface {
	// Name is the bounded label used for logs and metrics (e.g.
	// "session_state").
	Name() string

	// Check compares the connection's real backend state against its
	// tracked state. It must be side-effect-free and runs only on idle
	// connections. An error means no verdict could be produced (the
	// connection is not punished for it).
	Check(ctx context.Context, conn C) (Divergence, error)
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
