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
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/multipooler/pools/admin"
	"github.com/multigres/multigres/go/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/multipooler/pools/regular"
	"github.com/multigres/multigres/go/multipooler/pools/reserved"
	"github.com/multigres/multigres/go/pgprotocol/client"
)

// Manager orchestrates per-user connection pools with a shared admin pool.
// Each user gets their own RegularPool and ReservedPool that connect directly
// as that user via trust/peer authentication.
//
// Usage:
//
//	cfg := connpoolmanager.NewConfig(reg)
//	cfg.RegisterFlags(cmd.Flags())
//	// ... parse flags ...
//	mgr := cfg.NewManager(logger)
//	mgr.Open(ctx, connConfig)
//	defer mgr.Close()
//
//	// Get connections as needed
//	adminConn, _ := mgr.GetAdminConn(ctx)
//	regularConn, _ := mgr.GetRegularConn(ctx, user)
//	reservedConn, _ := mgr.NewReservedConn(ctx, settings, user)
type Manager struct {
	config     *Config
	logger     *slog.Logger      // Set by Open()
	connConfig *ConnectionConfig // Stored for lazy pool creation

	adminPool     *admin.Pool              // Shared admin pool for kill operations
	settingsCache *connstate.SettingsCache // Shared settings cache for all users
	metrics       *Metrics                 // OpenTelemetry metrics

	mu        sync.Mutex
	userPools map[string]*UserPool // Per-user connection pools
	closed    bool
}

// Open initializes the manager and creates the shared admin pool.
// User pools are created lazily on first connection request.
//
// Parameters:
//   - ctx: Context for pool operations
//   - connConfig: Connection settings (socket file, host, port, database)
func (m *Manager) Open(ctx context.Context, connConfig *ConnectionConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.connConfig = connConfig
	m.userPools = make(map[string]*UserPool)
	m.settingsCache = connstate.NewSettingsCache(m.config.SettingsCacheSize())
	m.closed = false

	// Build admin client config
	adminClientConfig := m.buildClientConfig(m.config.AdminUser(), m.config.AdminPassword())

	// Build admin pool config
	adminPoolConfig := &connpool.Config{
		Name:     "admin",
		Capacity: m.config.AdminCapacity(),
		Logger:   m.logger,
	}

	// Create shared admin pool (used by all user pools for kill operations)
	m.adminPool = admin.NewPool(ctx, &admin.PoolConfig{
		ClientConfig:   adminClientConfig,
		ConnPoolConfig: adminPoolConfig,
	})
	m.adminPool.Open()

	m.logger.InfoContext(ctx, "connection pool manager opened",
		"admin_user", m.config.AdminUser(),
		"admin_capacity", adminPoolConfig.Capacity,
		"user_regular_capacity", m.config.UserRegularCapacity(),
		"user_reserved_capacity", m.config.UserReservedCapacity(),
		"max_users", m.config.MaxUsers(),
		"settings_cache_size", m.config.SettingsCacheSize(),
	)
}

// buildClientConfig creates a client.Config with the specified user and password.
func (m *Manager) buildClientConfig(user, password string) *client.Config {
	return &client.Config{
		SocketFile: m.connConfig.SocketFile,
		Host:       m.connConfig.Host,
		Port:       m.connConfig.Port,
		Database:   m.connConfig.Database,
		User:       user,
		Password:   password,
	}
}

// getOrCreateUserPool returns the pool for the given user, creating it if needed.
func (m *Manager) getOrCreateUserPool(ctx context.Context, user string) (*UserPool, error) {
	if user == "" {
		return nil, errors.New("user cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if closed
	if m.closed {
		return nil, errors.New("manager is closed")
	}

	// Check if pool already exists
	if pool, ok := m.userPools[user]; ok {
		return pool, nil
	}

	// Check max users limit
	// TODO: Consider garbage collecting user pools after a period of inactivity
	// to reduce the odds of reaching the user pool maximum.
	if maxUsers := m.config.MaxUsers(); maxUsers > 0 && int64(len(m.userPools)) >= maxUsers {
		return nil, fmt.Errorf("maximum number of user pools (%d) reached", maxUsers)
	}

	// Create new user pool with per-user pool names for metric cardinality.
	// Note: Including username in pool names enables per-user monitoring but increases
	// metric cardinality. If this becomes an issue with many users, we can make it configurable.
	pool := NewUserPool(ctx, &UserPoolConfig{
		ClientConfig: m.buildClientConfig(user, ""), // Trust auth - no password
		AdminPool:    m.adminPool,
		RegularPoolConfig: &connpool.Config{
			Name:            "regular:" + user,
			Capacity:        m.config.UserRegularCapacity(),
			MaxIdleCount:    m.config.UserRegularMaxIdle(),
			IdleTimeout:     m.config.UserRegularIdleTimeout(),
			MaxLifetime:     m.config.UserRegularMaxLifetime(),
			ConnectionCount: m.metrics.RegularConnCount(),
			Logger:          m.logger,
		},
		ReservedPoolConfig: &connpool.Config{
			Name:            "reserved:" + user,
			Capacity:        m.config.UserReservedCapacity(),
			MaxIdleCount:    m.config.UserReservedMaxIdle(),
			IdleTimeout:     m.config.UserReservedIdleTimeout(),
			MaxLifetime:     m.config.UserReservedMaxLifetime(),
			ConnectionCount: m.metrics.ReservedConnCount(),
			Logger:          m.logger,
		},
		ReservedInactivityTimeout: m.config.UserReservedInactivityTimeout(),
		Logger:                    m.logger,
	})

	m.userPools[user] = pool
	m.logger.InfoContext(ctx, "created user pool", "user", user, "total_users", len(m.userPools))

	return pool, nil
}

// Close shuts down all connection pools.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return
	}
	m.closed = true

	// Close all user pools first
	for user, pool := range m.userPools {
		pool.Close()
		m.logger.Debug("closed user pool", "user", user)
	}
	m.userPools = nil

	// Close shared admin pool last
	if m.adminPool != nil {
		m.adminPool.Close()
		m.adminPool = nil
	}

	m.logger.Info("connection pool manager closed")
}

// --- Admin Pool Operations ---

// GetAdminConn acquires an admin connection from the shared pool.
// Admin connections are used for control plane operations like killing queries.
// The caller must call Recycle() on the returned connection to return it to the pool.
func (m *Manager) GetAdminConn(ctx context.Context) (admin.PooledConn, error) {
	return m.adminPool.Get(ctx)
}

// --- Regular Pool Operations ---

// GetRegularConn acquires a regular connection for the specified user.
// The caller must call Recycle() on the returned connection to return it to the pool.
func (m *Manager) GetRegularConn(ctx context.Context, user string) (regular.PooledConn, error) {
	pool, err := m.getOrCreateUserPool(ctx, user)
	if err != nil {
		return nil, err
	}
	return pool.GetRegularConn(ctx)
}

// GetRegularConnWithSettings acquires a regular connection with specific settings for the user.
// Settings are converted via the shared SettingsCache for consistent bucket assignment.
// The caller must call Recycle() on the returned connection to return it to the pool.
func (m *Manager) GetRegularConnWithSettings(ctx context.Context, settings map[string]string, user string) (regular.PooledConn, error) {
	pool, err := m.getOrCreateUserPool(ctx, user)
	if err != nil {
		return nil, err
	}
	// Convert map to *Settings via the shared cache
	s := m.settingsCache.GetOrCreate(settings)
	return pool.GetRegularConnWithSettings(ctx, s)
}

// --- Reserved Pool Operations ---

// NewReservedConn creates a new reserved connection for the specified user.
// Settings are converted via the shared SettingsCache for consistent bucket assignment.
// The connection is assigned a unique ID for client-side tracking.
// The caller must call Release() when done with the connection.
func (m *Manager) NewReservedConn(ctx context.Context, settings map[string]string, user string) (*reserved.Conn, error) {
	pool, err := m.getOrCreateUserPool(ctx, user)
	if err != nil {
		return nil, err
	}
	// Convert map to *Settings via the shared cache
	s := m.settingsCache.GetOrCreate(settings)
	return pool.NewReservedConn(ctx, s)
}

// GetReservedConn retrieves an existing reserved connection by ID for the specified user.
// Returns nil, false if the user pool doesn't exist, the connection is not found, or has timed out.
func (m *Manager) GetReservedConn(connID int64, user string) (*reserved.Conn, bool) {
	m.mu.Lock()
	pool, ok := m.userPools[user]
	m.mu.Unlock()

	if !ok {
		return nil, false
	}

	return pool.GetReservedConn(connID)
}

// --- Stats ---

// Stats returns statistics for all pools.
func (m *Manager) Stats() ManagerStats {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats := ManagerStats{
		Admin:     m.adminPool.Stats(),
		UserPools: make(map[string]UserPoolStats, len(m.userPools)),
	}

	for user, pool := range m.userPools {
		stats.UserPools[user] = pool.Stats()
	}

	return stats
}

// ManagerStats holds statistics for all managed pools.
type ManagerStats struct {
	Admin     connpool.PoolStats       // Shared admin pool stats
	UserPools map[string]UserPoolStats // Per-user pool stats
}
