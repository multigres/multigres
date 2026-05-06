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

	"github.com/multigres/multigres/go/common/mterrors"
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

	// Warn operators when running with an empty admin password. The shipped
	// pg_hba template retains trust on the local socket only for the configured
	// admin user, so this path continues to work during the bootstrap window.
	// Once that trust exception is closed (tracked as a TODO in the template),
	// an empty password will cause admin dials to fail.
	if m.config.PgPassword() == "" {
		m.logger.WarnContext(ctx, "admin password is empty; multipooler relies on the local-socket trust exception in pg_hba.conf. Configure CONNPOOL_ADMIN_PASSWORD (or POSTGRES_PASSWORD) before the trust line is removed.",
			"pg_user", m.config.PgUser(),
		)
	}

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
// Used by the admin pool and by user pools when SCRAM passthrough is disabled.
//
// SSLMode/TLSConfig from the ConnectionConfig propagate to every dial built by
// this manager. They are honored only on TCP connections (libpq parity); the
// client startup code skips SSLRequest when SocketFile is set.
func (m *Manager) buildClientConfig(user, password string) *client.Config {
	return &client.Config{
		SocketFile:  m.connConfig.SocketFile,
		Host:        m.connConfig.Host,
		Port:        m.connConfig.Port,
		Database:    m.connConfig.Database,
		User:        user,
		Password:    password,
		SSLMode:     m.connConfig.SSLMode,
		TLSConfig:   m.connConfig.TLSConfig,
		DialTimeout: m.config.DialTimeout(),
	}
}

// buildUserClientConfig creates a client.Config for a per-user pool dial.
// When the session carried ClientKey / ServerKey (forwarded via
// ExecuteOptions.UserAuth), the returned config authenticates to PostgreSQL
// using those keys. Absent keys fall back to the empty-password path, which
// only succeeds against a pg_hba.conf that still trusts the local socket for
// the dialing user — the template's narrow admin-user trust exception.
func (m *Manager) buildUserClientConfig(user string, clientKey, serverKey []byte) *client.Config {
	cfg := m.buildClientConfig(user, "")
	if len(clientKey) > 0 && len(serverKey) > 0 {
		cfg.ScramClientKey = clientKey
		cfg.ScramServerKey = serverKey
	}
	return cfg
}

// getOrCreateUserPool returns the pool for the given user, creating it if needed.
//
// This method uses an atomic snapshot pattern for lock-free reads on the hot path.
// For existing users, this completes with just an atomic load and map lookup.
// Only new user creation acquires the createMu mutex.
//
// clientKey and serverKey are the SCRAM passthrough keys forwarded by the
// gateway on the triggering RPC. They are consumed only when a new pool is
// created (cold path); subsequent RPCs for the same user reuse the existing
// pool regardless of the keys they carry. ClientKey is deterministic per
// (user, password) in PostgreSQL's SCRAM scheme, so cached keys stay valid
// until the user's password rotates; password rotation is surfaced via SCRAM
// auth failure on the next dial.
func (m *Manager) getOrCreateUserPool(user string, clientKey, serverKey []byte) (*UserPool, error) {
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
	return m.createUserPoolSlow(m.ctx, user, clientKey, serverKey)
}

// createUserPoolSlow creates a new user pool. This is the cold path that requires
// acquiring the createMu mutex.
func (m *Manager) createUserPoolSlow(ctx context.Context, user string, clientKey, serverKey []byte) (*UserPool, error) {
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
		ClientConfig: m.buildUserClientConfig(user, clientKey, serverKey),
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

// GetRegularConn acquires a regular connection for the specified user,
// optionally carrying SCRAM passthrough keys from the caller's session. Keys
// may be nil for admin/internal callers that dial via the local-trust line in
// pg_hba.conf. When non-nil, keys are consumed only when this call triggers
// first-time pool creation for the user; subsequent calls reuse the existing
// pool regardless of the keys they pass. The caller must call Recycle() on
// the returned connection to return it to the pool.
func (m *Manager) GetRegularConn(ctx context.Context, user string, clientKey, serverKey []byte) (regular.PooledConn, error) {
	return withReopenRetry(m, user, clientKey, serverKey, func(pool *UserPool) (regular.PooledConn, error) {
		return pool.GetRegularConn(ctx)
	})
}

// GetRegularConnWithSettings is GetRegularConn that additionally applies
// per-session settings. Settings are converted via the shared SettingsCache
// for consistent bucket assignment.
func (m *Manager) GetRegularConnWithSettings(ctx context.Context, settings map[string]string, user string, clientKey, serverKey []byte) (regular.PooledConn, error) {
	s := m.settingsCache.GetOrCreate(settings)
	return withReopenRetry(m, user, clientKey, serverKey, func(pool *UserPool) (regular.PooledConn, error) {
		return pool.GetRegularConnWithSettings(ctx, s)
	})
}

// --- Reserved Pool Operations ---

// NewReservedConn creates a new reserved connection for the specified user,
// optionally carrying SCRAM passthrough keys. Settings are converted via the
// shared SettingsCache for consistent bucket assignment. The connection is
// assigned a unique ID for client-side tracking. Optional ReservedConnOption
// values configure validate-with-retry behavior. Key-consumption semantics
// match GetRegularConn. The caller must call Release() when done with the
// connection.
func (m *Manager) NewReservedConn(ctx context.Context, settings map[string]string, user string, clientKey, serverKey []byte, opts ...reserved.ReservedConnOption) (*reserved.Conn, error) {
	s := m.settingsCache.GetOrCreate(settings)
	return withReopenRetry(m, user, clientKey, serverKey, func(pool *UserPool) (*reserved.Conn, error) {
		return pool.NewReservedConn(ctx, s, opts...)
	})
}

// evictUserPool removes stale from the snapshot and closes it. Used when a
// pool's cached SCRAM keys no longer match pg_authid — typically because the
// user rotated their PostgreSQL password — so the next getOrCreateUserPool
// call rebuilds from the triggering session's fresh keys.
//
// Eviction is a no-op (returns false) if a racing caller already swapped the
// snapshot to a different pool for this user, or dropped the entry entirely.
// Callers should still retry against whatever the snapshot now holds: the
// racing caller's fresh pool is the right target, and if the entry is gone
// getOrCreateUserPool creates a new one.
func (m *Manager) evictUserPool(user string, stale *UserPool) bool {
	m.createMu.Lock()
	defer m.createMu.Unlock()

	pools := m.userPoolsSnapshot.Load()
	if pools == nil {
		return false
	}
	current, ok := (*pools)[user]
	if !ok || current != stale {
		return false
	}

	newPools := make(map[string]*UserPool, len(*pools)-1)
	for u, p := range *pools {
		if u == user {
			continue
		}
		newPools[u] = p
	}
	m.userPoolsSnapshot.Store(&newPools)

	// Close outside the createMu critical section would be safer for latency
	// but Close is cheap and racing creators already passed the double-check
	// by now, so finishing synchronously keeps invariants simple.
	stale.Close()
	m.logger.InfoContext(m.ctx, "evicted user pool after stale credentials detected", "user", user)
	return true
}

// withReopenRetry runs op against the current user pool with two single-shot
// retry paths for transient, self-healable failures:
//
//  1. ErrPoolClosed after a generation bump: reopenConnections() swapped the
//     pools mid-flight (PostgreSQL auto-restart recovery). Retry against the
//     fresh pool. A closed-pool error with no generation bump means the
//     manager is genuinely shutting down — surface it unchanged.
//
//  2. Class-28 SQLSTATE from PostgreSQL: the cached user pool's ClientConfig
//     carries stale SCRAM keys (password rotated in pg_authid). Evict the
//     pool and recreate from the triggering session's keys, which are
//     known-current — they were derived moments ago during the session's
//     SCRAM handshake at MultiGateway against whatever verifier pg_authid
//     holds right now. If the retry also auth-fails (retrier itself used the
//     old password at the gateway), we surface the clean 28xxx error and the
//     client reconnects to re-derive keys against the new verifier.
//
// clientKey and serverKey forward the SCRAM passthrough material to
// getOrCreateUserPool so that a first-time pool creation triggered during
// this call (including after an eviction or reopen swap) gets the session's
// keys.
func withReopenRetry[T any](m *Manager, user string, clientKey, serverKey []byte, op func(*UserPool) (T, error)) (T, error) {
	var zero T
	startGen := m.generation.Load()
	pool, err := m.getOrCreateUserPool(user, clientKey, serverKey)
	if err != nil {
		return zero, err
	}
	result, err := op(pool)
	if err == nil {
		return result, nil
	}

	// Manager-restart race: reopenConnections swapped pools mid-flight.
	if errors.Is(err, connpool.ErrPoolClosed) {
		if m.generation.Load() == startGen {
			return zero, err
		}
		pool2, err2 := m.getOrCreateUserPool(user, clientKey, serverKey)
		if err2 != nil {
			return zero, err2
		}
		return op(pool2)
	}

	// Stale-key self-heal. evictUserPool is best-effort; even if it returns
	// false (racing eviction), getOrCreateUserPool returns whatever is in the
	// snapshot now, which is either a freshly-created pool with fresh keys
	// or absent (and we create one).
	if mterrors.IsAuthenticationError(err) {
		m.evictUserPool(user, pool)
		pool2, err2 := m.getOrCreateUserPool(user, clientKey, serverKey)
		if err2 != nil {
			return zero, err2
		}
		return op(pool2)
	}

	return zero, err
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
