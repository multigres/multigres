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

	"github.com/multigres/multigres/go/services/multipooler/pools/admin"
	"github.com/multigres/multigres/go/services/multipooler/pools/regular"
	"github.com/multigres/multigres/go/services/multipooler/pools/reserved"
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

	// Close shuts down all connection pools.
	Close()

	// PgUser returns the configured PostgreSQL user for system queries.
	PgUser() string

	// --- Admin Pool Operations ---

	// GetAdminConn acquires an admin connection from the pool.
	GetAdminConn(ctx context.Context) (admin.PooledConn, error)

	// --- Regular Pool Operations ---

	// GetRegularConn acquires a regular connection for the specified user.
	GetRegularConn(ctx context.Context, user string) (regular.PooledConn, error)

	// GetRegularConnWithSettings acquires a regular connection with specific settings for the user.
	// Settings are provided as a map and internally converted via the shared SettingsCache.
	GetRegularConnWithSettings(ctx context.Context, settings map[string]string, user string) (regular.PooledConn, error)

	// --- Reserved Pool Operations ---

	// NewReservedConn creates a new reserved connection for the specified user.
	// Settings are provided as a map and internally converted via the shared SettingsCache.
	// Optional ReservedConnOption values configure validate-with-retry behavior.
	NewReservedConn(ctx context.Context, settings map[string]string, user string, opts ...reserved.ReservedConnOption) (*reserved.Conn, error)

	// GetReservedConn retrieves an existing reserved connection by ID for the specified user.
	GetReservedConn(connID int64, user string) (*reserved.Conn, bool)

	// ApplySettingsToConn ensures the connection's settings match the given
	// session settings. If they differ, executes SET commands on the connection
	// and updates its tracked state. This is needed because reserved connections
	// bypass the pool's normal ApplySettings mechanism.
	ApplySettingsToConn(ctx context.Context, conn *regular.Conn, settings map[string]string) error

	// --- Drain ---

	// WaitForDrain blocks until all lent connections have been returned or ctx is cancelled.
	// Used during graceful shutdown to wait for in-flight queries to complete.
	WaitForDrain(ctx context.Context) error

	// CloseReservedConnections kills all active reserved connections across all user pools.
	// Used after drain grace period expires to prevent reserved connections from being
	// used in a non-serving state.
	CloseReservedConnections(ctx context.Context) int

	// --- Stats ---

	// Stats returns statistics for all pools.
	Stats() ManagerStats
}

// Compile-time check that Manager implements PoolManager.
var _ PoolManager = (*Manager)(nil)
