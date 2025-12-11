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

// Package connpoolmanager provides a unified manager for all connection pool types.
package connpoolmanager

import (
	"context"
	"log/slog"

	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/multipooler/pools/admin"
	"github.com/multigres/multigres/go/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/multipooler/pools/regular"
	"github.com/multigres/multigres/go/multipooler/pools/reserved"
	"github.com/multigres/multigres/go/pgprotocol/client"
)

// Manager orchestrates all connection pool types (admin, regular, reserved).
// It provides a unified interface for connection acquisition and lifecycle management.
//
// Usage:
//
//	mgr := connpoolmanager.NewManager(config)
//	mgr.Open(ctx)
//	defer mgr.Close()
//
//	// Get connections as needed
//	adminConn, _ := mgr.GetAdminConn(ctx)
//	regularConn, _ := mgr.GetRegularConn(ctx, settings, user)
//	reservedConn, _ := mgr.NewReservedConn(ctx, settings, user)
type Manager struct {
	config *Config
	logger *slog.Logger

	adminPool    *admin.Pool
	regularPool  *regular.Pool
	reservedPool *reserved.Pool
}

// Config holds configuration for all connection pools managed by the Manager.
type Config struct {
	// ClientConfig is the PostgreSQL connection configuration.
	// Used by all pools to connect to the database.
	ClientConfig *client.Config

	// AdminPoolConfig configures the admin connection pool.
	AdminPoolConfig *connpool.Config

	// RegularPoolConfig configures the regular connection pool.
	RegularPoolConfig *connpool.Config

	// ReservedPoolConfig configures the reserved connection pool.
	ReservedPoolConfig *reserved.PoolConfig

	// Logger for pool operations.
	Logger *slog.Logger
}

// NewManager creates a new connection pool manager.
// Call Open() to initialize the pools before use.
func NewManager(config *Config) *Manager {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		config: config,
		logger: logger,
	}
}

// Open initializes all connection pools and starts background workers.
// Must be called before using the manager.
func (m *Manager) Open(ctx context.Context) {
	// Create admin pool first (regular pool depends on it for kill operations).
	m.adminPool = admin.NewPool(&admin.PoolConfig{
		ClientConfig:   m.config.ClientConfig,
		ConnPoolConfig: m.config.AdminPoolConfig,
	})
	m.adminPool.Open(ctx)

	// Create regular pool with reference to admin pool.
	m.regularPool = regular.NewPool(&regular.PoolConfig{
		ClientConfig:   m.config.ClientConfig,
		ConnPoolConfig: m.config.RegularPoolConfig,
		AdminPool:      m.adminPool,
	})
	m.regularPool.Open(ctx)

	// Create reserved pool wrapping the regular pool.
	m.reservedPool = reserved.NewPool(ctx, m.config.ReservedPoolConfig, m.regularPool)

	m.logger.InfoContext(ctx, "connection pool manager opened")
}

// Close shuts down all connection pools.
// Closes pools in reverse order of creation: reserved -> regular -> admin.
func (m *Manager) Close() {
	if m.reservedPool != nil {
		m.reservedPool.Close()
	}
	if m.regularPool != nil {
		m.regularPool.Close()
	}
	if m.adminPool != nil {
		m.adminPool.Close()
	}

	m.logger.Info("connection pool manager closed")
}

// --- Admin Pool Operations ---

// GetAdminConn acquires an admin connection from the pool.
// Admin connections are used for control plane operations like killing queries.
// The caller must call Recycle() on the returned connection to return it to the pool.
func (m *Manager) GetAdminConn(ctx context.Context) (admin.PooledConn, error) {
	return m.adminPool.Get(ctx)
}

// --- Regular Pool Operations ---

// GetRegularConn acquires a regular connection from the pool with the specified user role.
// The caller must call Recycle() on the returned connection to return it to the pool.
func (m *Manager) GetRegularConn(ctx context.Context, user string) (regular.PooledConn, error) {
	return m.regularPool.Get(ctx, user)
}

// GetRegularConnWithSettings acquires a regular connection with specific settings and user role.
// The caller must call Recycle() on the returned connection to return it to the pool.
func (m *Manager) GetRegularConnWithSettings(ctx context.Context, settings *connstate.Settings, user string) (regular.PooledConn, error) {
	return m.regularPool.GetWithSettings(ctx, settings, user)
}

// --- Reserved Pool Operations ---

// NewReservedConn creates a new reserved connection for transactions or portal operations.
// The connection is assigned a unique ID for client-side tracking.
// The caller must call Release() when done with the connection.
func (m *Manager) NewReservedConn(ctx context.Context, settings *connstate.Settings, user string) (*reserved.Conn, error) {
	return m.reservedPool.NewConn(ctx, settings, user)
}

// GetReservedConn retrieves an existing reserved connection by ID.
// Returns nil, false if the connection is not found or has timed out.
func (m *Manager) GetReservedConn(connID int64) (*reserved.Conn, bool) {
	return m.reservedPool.Get(connID)
}

// --- Stats ---

// Stats returns statistics for all pools.
func (m *Manager) Stats() ManagerStats {
	return ManagerStats{
		Admin:    m.adminPool.Stats(),
		Regular:  m.regularPool.Stats(),
		Reserved: m.reservedPool.Stats(),
	}
}

// ManagerStats holds statistics for all managed pools.
type ManagerStats struct {
	Admin    connpool.PoolStats
	Regular  connpool.PoolStats
	Reserved reserved.PoolStats
}
