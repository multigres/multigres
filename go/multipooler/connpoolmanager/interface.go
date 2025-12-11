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

	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/multipooler/pools/admin"
	"github.com/multigres/multigres/go/multipooler/pools/regular"
	"github.com/multigres/multigres/go/multipooler/pools/reserved"
	"github.com/multigres/multigres/go/pgprotocol/client"
)

// PoolManager defines the interface for connection pool management.
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
	// Open initializes all connection pools with the given client configuration.
	Open(ctx context.Context, clientConfig *client.Config)

	// Close shuts down all connection pools.
	Close()

	// --- Admin Pool Operations ---

	// GetAdminConn acquires an admin connection from the pool.
	GetAdminConn(ctx context.Context) (admin.PooledConn, error)

	// --- Regular Pool Operations ---

	// GetRegularConn acquires a regular connection with the specified user role.
	GetRegularConn(ctx context.Context, user string) (regular.PooledConn, error)

	// GetRegularConnWithSettings acquires a regular connection with settings and user role.
	GetRegularConnWithSettings(ctx context.Context, settings *connstate.Settings, user string) (regular.PooledConn, error)

	// --- Reserved Pool Operations ---

	// NewReservedConn creates a new reserved connection.
	NewReservedConn(ctx context.Context, settings *connstate.Settings, user string) (*reserved.Conn, error)

	// GetReservedConn retrieves an existing reserved connection by ID.
	GetReservedConn(connID int64) (*reserved.Conn, bool)

	// --- Stats ---

	// Stats returns statistics for all pools.
	Stats() ManagerStats
}

// Compile-time check that Manager implements PoolManager.
var _ PoolManager = (*Manager)(nil)
