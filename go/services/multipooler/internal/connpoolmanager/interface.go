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

package connpoolmanager

import (
	"context"
	"time"

	"github.com/multigres/multigres/go/services/multipooler/internal/pools/admin"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
)

// PoolManager defines the interface for connection pool management.
// Each user gets their own RegularPool and ReservedPool that connect
// directly as that user (using trust/peer auth), eliminating the need
// for SET ROLE.
//
// This interface is useful for testing components that depend on the manager,
// allowing them to use mock implementations.
//
// Example usage in tests:
//
//	type mockManager struct {
//	    connpoolmanager.PoolManager // embed for default nil implementations
//	    // override specific methods as needed
//	}
type PoolManager interface {
	// Open initializes all connection pools with the given connection configuration.
	// Connection settings (socket file, host, port, database) come from connConfig,
	// while credentials are managed internally via viper flags.
	Open(ctx context.Context, connConfig *ConnectionConfig)

	// Close shuts down all connection pools. This is a terminal close.
	Close()

	// CloseForReopen closes all pools as the first half of a reopen (a Close
	// immediately followed by an Open, e.g. to refresh stale file descriptors
	// after a PostgreSQL restart). It must be paired with a subsequent Open.
	// Unlike Close, it marks the close as transient so connection requests
	// racing the reopen wait for the Open and retry, rather than failing.
	CloseForReopen()

	// PgUser returns the configured PostgreSQL user for system queries.
	PgUser() string

	// PgPassword returns the resolved PostgreSQL password and an "ok" flag
	// indicating whether a password source was successfully resolved at
	// startup. !ok means ResolvePgPassword has not run successfully and
	// should be treated as an invariant violation by callers (production
	// startup guarantees Resolve runs first).
	PgPassword() (string, bool)

	// --- Admin Pool Operations ---

	// GetAdminConn acquires an admin connection from the pool.
	GetAdminConn(ctx context.Context) (admin.PooledConn, error)

	// --- Regular Pool Operations ---

	// GetRegularConn acquires a regular connection for the specified user,
	// optionally carrying SCRAM passthrough keys from the caller's session.
	// Keys may be nil for admin/internal callers that dial via the
	// local-trust line in pg_hba.conf. When non-nil, keys are consumed only
	// when this call triggers first-time user-pool creation; subsequent
	// calls reuse the existing pool regardless of the keys they pass.
	GetRegularConn(ctx context.Context, user string, clientKey, serverKey []byte) (regular.PooledConn, error)

	// GetRegularConnWithSettings is GetRegularConn that additionally applies
	// per-session settings. Settings are provided as a map and internally
	// converted via the shared SettingsCache.
	GetRegularConnWithSettings(ctx context.Context, settings map[string]string, user string, clientKey, serverKey []byte) (regular.PooledConn, error)

	// --- Reserved Pool Operations ---

	// NewReservedConn creates a new reserved connection for the specified
	// user, optionally carrying SCRAM passthrough keys. Settings are
	// provided as a map and internally converted via the shared
	// SettingsCache. Optional ReservedConnOption values configure
	// validate-with-retry behavior. Key-consumption semantics match
	// GetRegularConn.
	NewReservedConn(ctx context.Context, settings map[string]string, user string, clientKey, serverKey []byte, opts ...reserved.ReservedConnOption) (*reserved.Conn, error)

	// GetReservedConn retrieves an existing reserved connection by ID for the specified user.
	GetReservedConn(connID int64, user string) (*reserved.Conn, bool)

	// PrepareReservedConn ensures an existing reserved connection is safe for
	// user SQL by reconciling it to the gateway's session settings. If the
	// connstate cache is marked untrusted, reconciliation is forced instead of
	// relying on pointer equality.
	PrepareReservedConn(ctx context.Context, conn *reserved.Conn, settings map[string]string) error

	// ApplySettingsToConn ensures the connection's settings match the given
	// session settings. If they differ, executes SET commands on the connection
	// and updates its tracked state. This is needed because reserved connections
	// bypass the pool's normal ApplySettings mechanism.
	ApplySettingsToConn(ctx context.Context, conn *regular.Conn, settings map[string]string) error

	// --- Drain ---

	// WaitForDrain blocks until all lent connections have been returned or ctx is cancelled.
	// Used during graceful shutdown to wait for in-flight queries to complete.
	WaitForDrain(ctx context.Context) error

	// WaitForReservedDrain blocks until all RESERVED connections (transactions,
	// temp tables, portals, COPY) have been released or ctx is cancelled. It
	// ignores transient single-query borrows, so the first stage of a graceful
	// drain can wait for transactions while still serving single queries.
	WaitForReservedDrain(ctx context.Context) error

	// CloseReservedConnections kills all active reserved connections across all user pools.
	// Used after drain grace period expires to prevent reserved connections from being
	// used in a non-serving state.
	CloseReservedConnections(ctx context.Context) int

	// --- Stats ---

	// Stats returns statistics for all pools.
	Stats() ManagerStats

	// CredentialQueryRecorder returns a narrow recorder for auth-path
	// observations made by the gRPC service. May return nil when the
	// manager is unopened or metric init failed; the underlying *Metrics
	// receiver is nil-safe, so callers can treat nil as the noop sink.
	CredentialQueryRecorder() CredentialQueryRecorder
}

// CredentialQueryRecorder records credential-query latency and error-type
// labels. Implemented by *Metrics; declared as an interface so callers
// (e.g. grpcpoolerservice) do not couple to the full pool-manager metrics
// surface.
type CredentialQueryRecorder interface {
	RecordCredentialQuery(ctx context.Context, d time.Duration, errorType string)
}

// Compile-time check that Manager implements PoolManager.
var _ PoolManager = (*Manager)(nil)
