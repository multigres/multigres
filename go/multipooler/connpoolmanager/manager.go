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
// The regular and reserved pools are completely separate - transactions go to the
// reserved pool while simple queries go to the regular pool. This ensures transaction
// traffic doesn't compete with read traffic for connections.
//
// Usage:
//
//	cfg := connpoolmanager.NewConfig(reg)
//	cfg.RegisterFlags(cmd.Flags())
//	// ... parse flags ...
//	mgr := cfg.NewManager()
//	mgr.Open(ctx, logger, connConfig)
//	defer mgr.Close()
//
//	// Get connections as needed
//	adminConn, _ := mgr.GetAdminConn(ctx)
//	regularConn, _ := mgr.GetRegularConn(ctx, user)
//	reservedConn, _ := mgr.NewReservedConn(ctx, settings, user)
type Manager struct {
	config *Config
	logger *slog.Logger // Set by Open()

	adminPool    *admin.Pool
	regularPool  *regular.Pool
	reservedPool *reserved.Pool // Manages its own underlying regular pool
}

// Open initializes all connection pools and starts background workers.
// Must be called after RegisterFlags() and flag parsing.
//
// Parameters:
//   - ctx: Context for pool operations
//   - logger: Logger for pool operations (uses slog.Default() if nil)
//   - connConfig: Connection settings (socket file, host, port, database)
func (m *Manager) Open(ctx context.Context, logger *slog.Logger, connConfig *ConnectionConfig) {
	if logger == nil {
		logger = slog.Default()
	}
	m.logger = logger

	// Build client configs using credentials from viper config and connection settings from connConfig.
	adminClientConfig := m.buildClientConfig(connConfig, m.config.AdminUser(), m.config.AdminPassword())
	appClientConfig := m.buildClientConfig(connConfig, m.config.AppUser(), m.config.AppPassword())

	// Build pool configs from viper values.
	adminPoolConfig := &connpool.Config{
		Capacity: m.config.AdminCapacity(),
		Logger:   m.logger,
	}
	regularPoolConfig := &connpool.Config{
		Capacity:     m.config.RegularCapacity(),
		MaxIdleCount: m.config.RegularMaxIdle(),
		IdleTimeout:  m.config.RegularIdleTimeout(),
		MaxLifetime:  m.config.RegularMaxLifetime(),
		Logger:       m.logger,
	}
	// Reserved pool manages its own underlying regular pool (separate from the main regular pool)
	// to keep transaction traffic isolated from simple query traffic.
	reservedRegularPoolConfig := &connpool.Config{
		Capacity:     m.config.ReservedCapacity(),
		MaxIdleCount: m.config.ReservedMaxIdle(),
		IdleTimeout:  m.config.ReservedIdleTimeout(),
		MaxLifetime:  m.config.ReservedMaxLifetime(),
		Logger:       m.logger,
	}

	// Create admin pool first (regular pools depend on it for kill operations).
	m.adminPool = admin.NewPool(&admin.PoolConfig{
		ClientConfig:   adminClientConfig,
		ConnPoolConfig: adminPoolConfig,
	})
	m.adminPool.Open(ctx)

	// Create regular pool for simple queries using app credentials.
	m.regularPool = regular.NewPool(&regular.PoolConfig{
		ClientConfig:   appClientConfig,
		ConnPoolConfig: regularPoolConfig,
		AdminPool:      m.adminPool,
	})
	m.regularPool.Open(ctx)

	// Create reserved pool (it manages its own underlying regular pool internally) using app credentials.
	m.reservedPool = reserved.NewPool(ctx, &reserved.PoolConfig{
		IdleTimeout: m.config.ReservedIdleTimeout(),
		Logger:      m.logger,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig:   appClientConfig,
			ConnPoolConfig: reservedRegularPoolConfig,
			AdminPool:      m.adminPool,
		},
	})

	m.logger.InfoContext(ctx, "connection pool manager opened",
		"admin_user", m.config.AdminUser(),
		"app_user", m.config.AppUser(),
		"admin_capacity", adminPoolConfig.Capacity,
		"regular_capacity", regularPoolConfig.Capacity,
		"regular_max_idle", regularPoolConfig.MaxIdleCount,
		"regular_idle_timeout", regularPoolConfig.IdleTimeout,
		"regular_max_lifetime", regularPoolConfig.MaxLifetime,
		"reserved_capacity", reservedRegularPoolConfig.Capacity,
		"reserved_max_idle", reservedRegularPoolConfig.MaxIdleCount,
		"reserved_idle_timeout", m.config.ReservedIdleTimeout(),
		"reserved_max_lifetime", reservedRegularPoolConfig.MaxLifetime,
	)
}

// buildClientConfig creates a client.Config with the specified user and password,
// using connection settings from the provided ConnectionConfig.
func (m *Manager) buildClientConfig(connConfig *ConnectionConfig, user, password string) *client.Config {
	return &client.Config{
		SocketFile: connConfig.SocketFile,
		Host:       connConfig.Host,
		Port:       connConfig.Port,
		Database:   connConfig.Database,
		User:       user,
		Password:   password,
	}
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
	Admin    connpool.PoolStats // Admin pool for control operations
	Regular  connpool.PoolStats // Regular pool for simple queries
	Reserved reserved.PoolStats // Reserved pool stats (includes underlying regular pool stats)
}
