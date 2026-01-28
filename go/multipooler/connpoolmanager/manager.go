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
	"maps"
	"sync"
	"sync/atomic"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/multipooler/pools/admin"
	"github.com/multigres/multigres/go/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/multipooler/pools/regular"
	"github.com/multigres/multigres/go/multipooler/pools/reserved"
)

// Manager orchestrates per-user connection pools with a shared admin pool.
// Each user gets their own RegularPool and ReservedPool that connect directly
// as that user via trust/peer authentication.
//
// The manager uses an atomic snapshot pattern for lock-free reads on the hot path.
// Most connection requests (for existing users) complete with just an atomic load
// and map lookup. Only new user pool creation requires acquiring a mutex.
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

	// userPoolsSnapshot holds an atomic pointer to an immutable map of user pools.
	// This enables lock-free reads on the hot path (existing users).
	// The map is replaced atomically via copy-on-write when new users are added.
	userPoolsSnapshot atomic.Pointer[map[string]*UserPool]

	// createMu serializes user pool creation (cold path only).
	// This mutex is only acquired when a new user pool needs to be created.
	createMu sync.Mutex

	// closed indicates whether the manager has been closed.
	closed atomic.Bool

	// Rebalancer goroutine management
	rebalancerCtx    context.Context
	rebalancerCancel context.CancelFunc
	rebalancerWg     sync.WaitGroup

	// Fair share allocators (created once in Open)
	regularAllocator  *FairShareAllocator
	reservedAllocator *FairShareAllocator
}

// Open initializes the manager and creates the shared admin pool.
// User pools are created lazily on first connection request.
//
// Parameters:
//   - ctx: Context for pool operations
//   - connConfig: Connection settings (socket file, host, port, database)
func (m *Manager) Open(ctx context.Context, connConfig *ConnectionConfig) {
	m.createMu.Lock()
	defer m.createMu.Unlock()

	m.connConfig = connConfig
	emptyPools := make(map[string]*UserPool)
	m.userPoolsSnapshot.Store(&emptyPools)
	m.settingsCache = connstate.NewSettingsCache(m.config.SettingsCacheSize())
	m.closed.Store(false)

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

	// Create fair share allocators based on global capacity and reserved ratio
	globalCapacity := m.config.GlobalCapacity()
	reservedRatio := m.config.ReservedRatio()
	regularCapacity := int64(float64(globalCapacity) * (1 - reservedRatio))
	reservedCapacity := globalCapacity - regularCapacity

	m.regularAllocator = NewFairShareAllocator(regularCapacity)
	m.reservedAllocator = NewFairShareAllocator(reservedCapacity)

	// Start the rebalancer goroutine
	m.rebalancerCtx, m.rebalancerCancel = context.WithCancel(ctx)
	m.startRebalancer()

	m.logger.InfoContext(ctx, "connection pool manager opened",
		"admin_user", m.config.AdminUser(),
		"admin_capacity", adminPoolConfig.Capacity,
		"user_regular_capacity", m.config.UserRegularCapacity(),
		"user_reserved_capacity", m.config.UserReservedCapacity(),
		"max_users", m.config.MaxUsers(),
		"settings_cache_size", m.config.SettingsCacheSize(),
		"global_capacity", globalCapacity,
		"reserved_ratio", reservedRatio,
		"regular_allocation", regularCapacity,
		"reserved_allocation", reservedCapacity,
		"rebalance_interval", m.config.RebalanceInterval(),
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
//
// This method uses an atomic snapshot pattern for lock-free reads on the hot path.
// For existing users, this completes with just an atomic load and map lookup.
// Only new user creation acquires the createMu mutex.
func (m *Manager) getOrCreateUserPool(ctx context.Context, user string) (*UserPool, error) {
	if user == "" {
		return nil, errors.New("user cannot be empty")
	}

	// Hot path: atomic load + map lookup (no lock)
	if pools := m.userPoolsSnapshot.Load(); pools != nil {
		if pool, ok := (*pools)[user]; ok {
			return pool, nil
		}
	}

	// Check if closed before attempting to create
	if m.closed.Load() {
		return nil, errors.New("manager is closed")
	}

	// Cold path: need to create a new user pool
	return m.createUserPoolSlow(ctx, user)
}

// createUserPoolSlow creates a new user pool. This is the cold path that requires
// acquiring the createMu mutex.
func (m *Manager) createUserPoolSlow(ctx context.Context, user string) (*UserPool, error) {
	m.createMu.Lock()
	defer m.createMu.Unlock()

	// Double-check after acquiring lock
	pools := m.userPoolsSnapshot.Load()
	if pools != nil {
		if pool, ok := (*pools)[user]; ok {
			return pool, nil
		}
	}

	// Check if closed (with lock held)
	if m.closed.Load() {
		return nil, errors.New("manager is closed")
	}

	// Check max users limit
	// TODO: Consider garbage collecting user pools after a period of inactivity
	// to reduce the odds of reaching the user pool maximum.
	currentPools := *pools
	if maxUsers := m.config.MaxUsers(); maxUsers > 0 && int64(len(currentPools)) >= maxUsers {
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
		DemandWindow:              m.config.DemandWindow(),
		DemandSampleInterval:      m.config.DemandSampleInterval(),
		RebalanceInterval:         m.config.RebalanceInterval(),
		Logger:                    m.logger,
	})

	// Copy-on-write: create new map with the new pool
	newPools := make(map[string]*UserPool, len(currentPools)+1)
	maps.Copy(newPools, currentPools)
	newPools[user] = pool

	// Atomic publish
	m.userPoolsSnapshot.Store(&newPools)
	m.logger.InfoContext(ctx, "created user pool", "user", user, "total_users", len(newPools))

	return pool, nil
}

// Close shuts down all connection pools.
func (m *Manager) Close() {
	m.createMu.Lock()
	defer m.createMu.Unlock()

	if m.closed.Load() {
		return
	}
	m.closed.Store(true)

	// Stop the rebalancer goroutine first
	if m.rebalancerCancel != nil {
		m.rebalancerCancel()
		m.rebalancerWg.Wait()
	}

	// Close all user pools
	if pools := m.userPoolsSnapshot.Load(); pools != nil {
		for user, pool := range *pools {
			pool.Close()
			m.logger.Debug("closed user pool", "user", user)
		}
	}
	// Clear the snapshot
	emptyPools := make(map[string]*UserPool)
	m.userPoolsSnapshot.Store(&emptyPools)

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
	// Lock-free read via atomic snapshot
	pools := m.userPoolsSnapshot.Load()
	if pools == nil {
		return nil, false
	}

	pool, ok := (*pools)[user]
	if !ok {
		return nil, false
	}

	return pool.GetReservedConn(connID)
}

// --- Stats ---

// Stats returns statistics for all pools.
// This reads from the atomic snapshot, providing a consistent view of all pools.
func (m *Manager) Stats() ManagerStats {
	pools := m.userPoolsSnapshot.Load()

	var userPoolStats map[string]UserPoolStats
	if pools != nil {
		userPoolStats = make(map[string]UserPoolStats, len(*pools))
		for user, pool := range *pools {
			userPoolStats[user] = pool.Stats()
		}
	} else {
		userPoolStats = make(map[string]UserPoolStats)
	}

	return ManagerStats{
		Admin:     m.adminPool.Stats(),
		UserPools: userPoolStats,
	}
}

// ManagerStats holds statistics for all managed pools.
type ManagerStats struct {
	Admin     connpool.PoolStats       // Shared admin pool stats
	UserPools map[string]UserPoolStats // Per-user pool stats
}

// IsClosed returns whether the manager has been closed.
func (m *Manager) IsClosed() bool {
	return m.closed.Load()
}

// UserPoolCount returns the number of user pools currently managed.
func (m *Manager) UserPoolCount() int {
	pools := m.userPoolsSnapshot.Load()
	if pools == nil {
		return 0
	}
	return len(*pools)
}

// HasUserPool returns whether a pool exists for the given user.
func (m *Manager) HasUserPool(user string) bool {
	pools := m.userPoolsSnapshot.Load()
	if pools == nil {
		return false
	}
	_, ok := (*pools)[user]
	return ok
}
