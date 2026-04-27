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
	"github.com/multigres/multigres/go/services/multipooler/connstate"
	"github.com/multigres/multigres/go/services/multipooler/pools/admin"
	"github.com/multigres/multigres/go/services/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/pools/regular"
	"github.com/multigres/multigres/go/services/multipooler/pools/reserved"
)

const (
	// initialUserPoolCapacity is the initial capacity for new user pools.
	// The rebalancer will adjust this based on demand.
	initialUserPoolCapacity int64 = 10
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

	ctx           context.Context          // Manager lifecycle context, used for lazy pool creation
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

	// generation is incremented on every Open(). Callers that get
	// ErrPoolClosed from an in-flight pool operation can compare the
	// pre-call generation against the current one: if it advanced, a
	// reopen swapped the pools underneath them and the call is safe to
	// retry against the fresh snapshot.
	generation atomic.Uint64

	// Rebalancer goroutine management
	rebalancerCtx    context.Context
	rebalancerCancel context.CancelFunc
	rebalancerWg     sync.WaitGroup

	// Fair share allocators (created once in Open)
	regularAllocator  *FairShareAllocator
	reservedAllocator *FairShareAllocator

	// Drain tracking: counts connections currently lent out across all pools.
	// Used for graceful drain during state transitions (NOT_SERVING).
	drainMu   sync.Mutex
	lentCount int64
	zeroCh    chan struct{}
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

	m.ctx = ctx
	m.connConfig = connConfig
	emptyPools := make(map[string]*UserPool)
	m.userPoolsSnapshot.Store(&emptyPools)

	// Initialize zeroCh as closed: starts at zero lent connections (drained).
	zeroCh := make(chan struct{})
	close(zeroCh)
	m.zeroCh = zeroCh
	m.settingsCache = connstate.NewSettingsCache(m.config.SettingsCacheSize())
	m.closed.Store(false)
	m.generation.Add(1)

	// Build admin client config
	adminClientConfig := m.buildClientConfig(m.config.PgUser(), m.config.PgPassword())

	// Build admin pool config
	connectTimeout := 2 * m.config.DialTimeout()
	adminPoolConfig := &connpool.Config{
		Name:           "admin",
		Capacity:       m.config.AdminCapacity(),
		ConnectTimeout: connectTimeout,
		Logger:         m.logger,
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
	minPerUser := m.config.MinCapacityPerUser()
	regularCapacity := int64(float64(globalCapacity) * (1 - reservedRatio))
	reservedCapacity := globalCapacity - regularCapacity
	regularMinPerUser := max(int64(float64(minPerUser)*(1-reservedRatio)), 1)
	reservedMinPerUser := max(minPerUser-regularMinPerUser, 1)

	// Use configurable minCapacityPerUser as the minimum per-user floor.
	// This ensures light users always have enough capacity for burst demand.
	m.regularAllocator = NewFairShareAllocator(regularCapacity, regularMinPerUser)
	m.reservedAllocator = NewFairShareAllocator(reservedCapacity, reservedMinPerUser)

	// Start the rebalancer goroutine
	m.rebalancerCtx, m.rebalancerCancel = context.WithCancel(ctx)
	m.startRebalancer()

	// Register observable metric callbacks for pool statistics.
	if err := m.metrics.RegisterManagerCallbacks(m.Stats, m.UserPoolCount, m.config.GlobalCapacity, m.IsClosed); err != nil {
		m.logger.WarnContext(ctx, "failed to register pool metrics callbacks", "error", err)
	}

	m.logger.InfoContext(ctx, "connection pool manager opened",
		"pg_user", m.config.PgUser(),
		"admin_capacity", adminPoolConfig.Capacity,
		"initial_user_capacity", initialUserPoolCapacity,
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
		SocketFile:  m.connConfig.SocketFile,
		Host:        m.connConfig.Host,
		Port:        m.connConfig.Port,
		Database:    m.connConfig.Database,
		User:        user,
		Password:    password,
		DialTimeout: m.config.DialTimeout(),
	}
}

// getOrCreateUserPool returns the pool for the given user, creating it if needed.
//
// This method uses an atomic snapshot pattern for lock-free reads on the hot path.
// For existing users, this completes with just an atomic load and map lookup.
// Only new user creation acquires the createMu mutex.
func (m *Manager) getOrCreateUserPool(user string) (*UserPool, error) {
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

	// Cold path: need to create a new user pool.
	// Use the manager's lifecycle context so the pool is tied to the manager's
	// lifetime, not the caller's request context.
	return m.createUserPoolSlow(m.ctx, user)
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

	currentPools := *pools

	// Calculate initial capacities proportional to the global split.
	// Regular pools get (1 - reservedRatio) of initial capacity, reserved pools get reservedRatio.
	reservedRatio := m.config.ReservedRatio()
	initialRegularCap := max(int64(float64(initialUserPoolCapacity)*(1-reservedRatio)), 1)
	initialReservedCap := max(int64(float64(initialUserPoolCapacity)*reservedRatio), 1)

	// Create drain tracking callbacks for this user pool.
	// These are called on every borrow/recycle/reserve/release to track lent connections.
	onBorrow := func() { m.lentAdd(1) }
	onRecycle := func() { m.lentAdd(-1) }
	onReserve := func() { m.lentAdd(1) }
	onRelease := func() { m.lentAdd(-1) }

	// Create new user pool with per-user pool names for metric cardinality.
	// Note: Including username in pool names enables per-user monitoring but increases
	// metric cardinality. If this becomes an issue with many users, we can make it configurable.
	// Create new user pool with initial capacity. The rebalancer will adjust
	// the capacity based on demand within a few seconds.
	userConnectTimeout := 2 * m.config.DialTimeout()
	pool, err := NewUserPool(ctx, &UserPoolConfig{
		ClientConfig: m.buildClientConfig(user, ""), // Trust auth - no password
		AdminPool:    m.adminPool,
		RegularPoolConfig: &connpool.Config{
			Name:            "regular:" + user,
			Capacity:        initialRegularCap,
			IdleTimeout:     m.config.UserRegularIdleTimeout(),
			MaxLifetime:     m.config.UserRegularMaxLifetime(),
			ConnectTimeout:  userConnectTimeout,
			ConnectionCount: m.metrics.RegularConnCount(),
			Logger:          m.logger,
		},
		ReservedPoolConfig: &connpool.Config{
			Name:            "reserved:" + user,
			Capacity:        initialReservedCap,
			IdleTimeout:     m.config.UserReservedIdleTimeout(),
			MaxLifetime:     m.config.UserReservedMaxLifetime(),
			ConnectTimeout:  userConnectTimeout,
			ConnectionCount: m.metrics.ReservedConnCount(),
			Logger:          m.logger,
		},
		ReservedInactivityTimeout: m.config.UserReservedInactivityTimeout(),
		DemandWindow:              m.config.DemandWindow(),
		RebalanceInterval:         m.config.RebalanceInterval(),
		Logger:                    m.logger,
		OnBorrow:                  onBorrow,
		OnRecycle:                 onRecycle,
		OnReserve:                 onReserve,
		OnRelease:                 onRelease,
	})
	if err != nil {
		return nil, fmt.Errorf("create user pool for %q: %w", user, err)
	}

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

	// Unregister observable metric callbacks so the OTel SDK stops invoking
	// them against closed pool state.
	if err := m.metrics.Close(); err != nil {
		m.logger.Warn("failed to unregister pool metrics callbacks", "error", err)
	}

	m.logger.Info("connection pool manager closed")
}

// PgUser returns the configured PostgreSQL user for system queries.
func (m *Manager) PgUser() string {
	return m.config.PgUser()
}

// --- Admin Pool Operations ---

// GetAdminConn acquires an admin connection from the shared pool.
// Admin connections are used for control plane operations like killing queries.
// The caller must call Recycle() on the returned connection to return it to the pool.
func (m *Manager) GetAdminConn(ctx context.Context) (admin.PooledConn, error) {
	// Read adminPool under createMu to avoid a nil-pointer panic if Close() is
	// racing with this call. Close() sets m.adminPool = nil while holding
	// createMu, so a snapshot taken here is either the valid pool or nil.
	//
	// We do not use the defer pattern used in other methods because that would
	// mean that we hold the mutex while calling Get() below. If the Get() call
	// block waiting for I/O, no other action will be able to get a connection.
	m.createMu.Lock()
	adminPool := m.adminPool
	m.createMu.Unlock()
	if adminPool == nil {
		return nil, errors.New("admin pool is closed")
	}
	return adminPool.Get(ctx)
}

// --- Regular Pool Operations ---

// GetRegularConn acquires a regular connection for the specified user.
// The caller must call Recycle() on the returned connection to return it to the pool.
func (m *Manager) GetRegularConn(ctx context.Context, user string) (regular.PooledConn, error) {
	return withReopenRetry(m, user, func(pool *UserPool) (regular.PooledConn, error) {
		return pool.GetRegularConn(ctx)
	})
}

// GetRegularConnWithSettings acquires a regular connection with specific settings for the user.
// Settings are converted via the shared SettingsCache for consistent bucket assignment.
// The caller must call Recycle() on the returned connection to return it to the pool.
func (m *Manager) GetRegularConnWithSettings(ctx context.Context, settings map[string]string, user string) (regular.PooledConn, error) {
	s := m.settingsCache.GetOrCreate(settings)
	return withReopenRetry(m, user, func(pool *UserPool) (regular.PooledConn, error) {
		return pool.GetRegularConnWithSettings(ctx, s)
	})
}

// --- Reserved Pool Operations ---

// NewReservedConn creates a new reserved connection for the specified user.
// Settings are converted via the shared SettingsCache for consistent bucket assignment.
// The connection is assigned a unique ID for client-side tracking.
// The caller must call Release() when done with the connection.
func (m *Manager) NewReservedConn(ctx context.Context, settings map[string]string, user string, opts ...reserved.ReservedConnOption) (*reserved.Conn, error) {
	s := m.settingsCache.GetOrCreate(settings)
	return withReopenRetry(m, user, func(pool *UserPool) (*reserved.Conn, error) {
		return pool.NewReservedConn(ctx, s, opts...)
	})
}

// withReopenRetry runs op against the current user pool and, if op returns
// ErrPoolClosed because a concurrent reopenConnections() swapped the pools
// mid-flight, retries once against the fresh pool. Genuine closed-pool errors
// (manager shutting down for real) are surfaced to the caller unchanged
// because the generation will not have advanced.
func withReopenRetry[T any](m *Manager, user string, op func(*UserPool) (T, error)) (T, error) {
	var zero T
	startGen := m.generation.Load()
	pool, err := m.getOrCreateUserPool(user)
	if err != nil {
		return zero, err
	}
	result, err := op(pool)
	if err == nil || !errors.Is(err, connpool.ErrPoolClosed) {
		return result, err
	}
	if m.generation.Load() == startGen {
		return zero, err
	}
	pool, err2 := m.getOrCreateUserPool(user)
	if err2 != nil {
		return zero, err2
	}
	return op(pool)
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

// ApplySettingsToConn ensures the connection's settings match the given session
// settings. ApplySettings handles the diff internally: it resets removed
// variables via individual RESET commands (safe inside transactions, unlike
// RESET ALL) and applies desired variables via SET SESSION.
func (m *Manager) ApplySettingsToConn(ctx context.Context, conn *regular.Conn, settings map[string]string) error {
	desired := m.settingsCache.GetOrCreate(settings)
	current := conn.Settings()

	// Pointer equality — same *Settings means same settings (via cache interning)
	if desired == current {
		return nil
	}

	return conn.ApplySettings(ctx, desired)
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

// lentAdd adjusts the lent connection count by n.
// When the count transitions to zero, zeroCh is closed to unblock WaitForDrain.
// When it transitions away from zero, a new zeroCh is created.
func (m *Manager) lentAdd(n int64) {
	m.drainMu.Lock()
	defer m.drainMu.Unlock()

	if m.lentCount+n < 0 {
		m.logger.Error("lentCount going negative, likely a bug in borrow/recycle or reserve/release callbacks",
			"current", m.lentCount, "delta", n)
	}
	m.lentCount += n
	if m.lentCount == 0 {
		// Signal drain complete by closing the channel.
		select {
		case <-m.zeroCh:
			// Already closed, nothing to do.
		default:
			close(m.zeroCh)
		}
	} else if m.lentCount == n && n > 0 {
		// Transitioned from 0 to positive: create a new open channel.
		m.zeroCh = make(chan struct{})
	}
}

// WaitForDrain blocks until all lent connections have been returned or ctx is cancelled.
func (m *Manager) WaitForDrain(ctx context.Context) error {
	m.drainMu.Lock()
	if m.lentCount == 0 {
		m.drainMu.Unlock()
		return nil
	}
	ch := m.zeroCh
	m.drainMu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CloseReservedConnections kills all active reserved connections across all user pools.
// Used after drain grace period expires to prevent reserved connections from being
// used in a non-serving state.
func (m *Manager) CloseReservedConnections(ctx context.Context) int {
	pools := m.userPoolsSnapshot.Load()
	if pools == nil {
		return 0
	}

	total := 0
	for _, pool := range *pools {
		total += pool.CloseReservedConnections(ctx)
	}
	return total
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
