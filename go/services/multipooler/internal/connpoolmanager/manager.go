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
	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/admin"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
)

const (
	// initialUserPoolCapacity is the initial capacity for new user pools.
	// The rebalancer will adjust this based on demand.
	initialUserPoolCapacity int64 = 10

	// maxPoolClosedRetries bounds how many times withReopenRetry retries a
	// transient closed-pool failure (a reopen swap or a stale-credentials
	// eviction) before surfacing it. It protects callers from unbounded retry
	// loops if closes keep racing acquisition.
	maxPoolClosedRetries = 4
)

// ErrManagerClosed is returned by connection-acquisition paths when the
// manager is closed. withReopenRetry treats it as transient (waits for the
// paired Open, then retries) while a reopen window is open, and as terminal
// otherwise.
var ErrManagerClosed = errors.New("manager is closed")

// managerLifecycle is the manager's phase. It is the single source of truth for
// retry decisions in withReopenRetry: only lifecycleRunning permits new pool
// creation, lifecycleReopening is a transient close that callers wait out, and
// lifecycleClosed is terminal.
type managerLifecycle uint32

const (
	// lifecycleRunning is the normal serving state. It is the zero value, so a
	// freshly-allocated Manager must be explicitly stored as lifecycleClosed
	// until Open() runs (see NewManager).
	lifecycleRunning managerLifecycle = iota
	// lifecycleReopening marks a CloseForReopen->Open window. Pool creation is
	// refused (the pools are torn down) but the close is transient: callers
	// block on reopenDone and retry once the paired Open completes.
	lifecycleReopening
	// lifecycleClosed is a terminal shutdown. Pool creation is refused and
	// callers surface the close instead of retrying.
	lifecycleClosed
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

	// lifecycle is the manager's phase (running/reopening/closed) and the single
	// source of truth for retry decisions. It is read lock-free on the hot path
	// and distinguishes a transient reopen from a terminal shutdown without a
	// second flag. See managerLifecycle.
	lifecycle atomic.Uint32

	// reopenDone is the wait handle for a reopen window, guarded by reopenMu.
	// CloseForReopen creates it (and moves lifecycle to reopening) before tearing
	// the pools down; the paired Open — or a terminal Close that interrupts the
	// reopen — closes it via endReopen. While lifecycle is reopening,
	// withReopenRetry blocks on this channel before retrying a closed-pool error
	// instead of surfacing the transient failure to the client.
	reopenMu   sync.Mutex
	reopenDone chan struct{}

	// Rebalancer goroutine management
	rebalancerCtx    context.Context
	rebalancerCancel context.CancelFunc
	rebalancerWg     sync.WaitGroup

	// Fair share allocators (created once in Open)
	regularAllocator  *FairShareAllocator
	reservedAllocator *FairShareAllocator

	// Drain tracking: counts connections currently lent out across all pools.
	// Used for graceful drain during state transitions (NOT_SERVING).
	//
	// regularCount tracks regular (single-query) borrows; reservedCount tracks
	// reserved connections (transactions, temp tables, portals, COPY, etc.). They
	// are kept separate so a borrow/recycle or reserve/release touches exactly one
	// counter under a single lock acquisition. The combined total (regularCount +
	// reservedCount) drives zeroCh; reservedCount alone drives reservedZeroCh. The
	// two-stage drain waits on reservedZeroCh first (let transactions finish while
	// still serving single queries), then on zeroCh (let the remaining in-flight
	// single queries finish). All guarded by drainMu.
	drainMu        sync.Mutex
	regularCount   int64
	reservedCount  int64
	zeroCh         chan struct{} // closed when regularCount + reservedCount == 0
	reservedZeroCh chan struct{} // closed when reservedCount == 0
}

// CredentialQueryRecorder returns a narrow recorder for the gRPC service's
// credential-query observations. Returns nil when the manager is unopened
// or metric init failed; the *Metrics receiver is nil-safe, so callers
// can treat a nil return as the noop sink.
func (m *Manager) CredentialQueryRecorder() CredentialQueryRecorder {
	if m == nil {
		return nil
	}
	return m.metrics
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

	// Same for the reserved-only drain channel.
	reservedZeroCh := make(chan struct{})
	close(reservedZeroCh)
	m.reservedZeroCh = reservedZeroCh
	m.settingsCache = connstate.NewSettingsCache(m.config.SettingsCacheSize())
	m.setLifecycle(lifecycleRunning)

	// Build admin client config. pwSourceNone signals that ResolvePgPassword
	// was never called — production startup in services/multipooler/init.go
	// enforces it before reaching Open, so an unset source here is strictly
	// a programmer error.
	adminPassword, source := m.config.PgPassword()
	if source == pwSourceNone {
		panic("connpoolmanager: Open called before ResolvePgPassword; no password source configured")
	}
	adminClientConfig := m.buildClientConfig(m.config.PgUser(), adminPassword)

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

	// If this Open is the second half of a reopen, end the window now that the
	// fresh pools are ready, waking any withReopenRetry waiters so they retry.
	m.endReopen()
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
	// If SCRAM passthrough keys are present, use them — no password needed.
	// Otherwise we need a password to authenticate. When the requested user
	// matches the configured admin user, fall back to the admin password
	// (this covers internal queries like heartbeat reads that don't carry a
	// SCRAM session). For any other user without keys, return an empty
	// password and let the dial fail with a clear auth error.
	password := ""
	if len(clientKey) == 0 || len(serverKey) == 0 {
		if user == m.config.PgUser() {
			pw, source := m.config.PgPassword()
			if source == pwSourceNone {
				// Invariant violation: production startup in
				// services/multipooler/init.go calls
				// ResolvePgPassword before any user pool is created.
				panic("connpoolmanager: buildUserClientConfig called before ResolvePgPassword; no password source configured")
			}
			password = pw
		}
	}
	cfg := m.buildClientConfig(user, password)
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

	// Refuse to create a pool unless the manager is running. Reopening counts as
	// closed here: the pools (and admin pool) are torn down mid-reopen, so a pool
	// built now would be wired to a nil admin pool. withReopenRetry waits the
	// reopen window out and retries against the fresh pools.
	if m.lifecycleState() != lifecycleRunning {
		return nil, ErrManagerClosed
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

	// Re-check lifecycle with the lock held (see getOrCreateUserPool).
	if m.lifecycleState() != lifecycleRunning {
		return nil, ErrManagerClosed
	}

	currentPools := *pools

	// Calculate initial capacities proportional to the global split.
	// Regular pools get (1 - reservedRatio) of initial capacity, reserved pools get reservedRatio.
	reservedRatio := m.config.ReservedRatio()
	initialRegularCap := max(int64(float64(initialUserPoolCapacity)*(1-reservedRatio)), 1)
	initialReservedCap := max(int64(float64(initialUserPoolCapacity)*reservedRatio), 1)

	// Create drain tracking callbacks for this user pool.
	// These are called on every borrow/recycle/reserve/release to track lent connections.
	// Regular borrows and reserved connections bump separate counters, so each
	// event takes the drain lock exactly once. The combined total drives the
	// full drain; the reserved count alone drives the reserved-only drain.
	onBorrow := func() { m.regularAdd(1) }
	onRecycle := func() { m.regularAdd(-1) }
	onReserve := func() { m.reservedAdd(1) }
	onRelease := func() { m.reservedAdd(-1) }

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

// Close shuts down all connection pools. This is a terminal close: callers
// acquiring connections afterward receive ErrManagerClosed until a fresh Open.
func (m *Manager) Close() {
	m.createMu.Lock()
	defer m.createMu.Unlock()
	if m.lifecycleState() == lifecycleClosed {
		return
	}
	m.setLifecycle(lifecycleClosed)
	// If a reopen was in progress, this terminal close preempts it: wake any
	// withReopenRetry waiters so they observe lifecycleClosed and surface the
	// error instead of blocking on a paired Open that will never come.
	m.endReopen()
	m.teardownLocked()
}

// CloseForReopen closes all pools as the first half of a reopen — a Close
// immediately followed by an Open, e.g. to refresh stale file descriptors
// after PostgreSQL restarts. It opens a reopen window so that connection
// requests racing the close wait for the matching Open and retry against the
// fresh pools, rather than receiving the transient ErrManagerClosed /
// ErrPoolClosed error. It MUST be paired with a subsequent Open, which closes
// the window. See withReopenRetry.
func (m *Manager) CloseForReopen() {
	m.createMu.Lock()
	defer m.createMu.Unlock()
	// beginReopen moves lifecycle to reopening (and arms reopenDone) before the
	// teardown, so a racing acquisition sees "reopening" rather than a torn-down
	// "running" manager.
	m.beginReopen()
	m.teardownLocked()
}

// teardownLocked tears down all pools and shared resources. It does NOT touch
// lifecycle — the caller sets that (Close -> closed, CloseForReopen -> reopening)
// before calling. It is idempotent: each resource is guarded so a repeated or
// post-reopen call is a cheap no-op. The caller must hold createMu.
func (m *Manager) teardownLocked() {
	// Stop the rebalancer goroutine first
	if m.rebalancerCancel != nil {
		m.rebalancerCancel()
		m.rebalancerWg.Wait()
		m.rebalancerCancel = nil
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
	// them against closed pool state. Metrics.Close is safe to call repeatedly.
	if err := m.metrics.Close(); err != nil {
		m.logger.Warn("failed to unregister pool metrics callbacks", "error", err)
	}

	m.logger.Info("connection pool manager closed")
}

// PgUser returns the configured PostgreSQL user for system queries.
func (m *Manager) PgUser() string {
	return m.config.PgUser()
}

// PgPassword returns the resolved PostgreSQL superuser password and an "ok"
// flag indicating whether ResolvePgPassword ran successfully and produced a
// source. A false ok means no password source was configured (or Resolve was
// never called); production startup in services/multipooler/init.go calls
// Resolve first and surfaces its error, so callers reaching this point can
// treat !ok as a programmer-error invariant violation.
func (m *Manager) PgPassword() (string, bool) {
	pw, source := m.config.PgPassword()
	return pw, source != pwSourceNone
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
	return withReopenRetry(ctx, m, user, clientKey, serverKey, func(pool *UserPool) (regular.PooledConn, error) {
		return pool.GetRegularConn(ctx)
	})
}

// GetRegularConnWithSettings is GetRegularConn that additionally applies
// per-session settings. Settings are converted via the shared SettingsCache
// for consistent bucket assignment.
func (m *Manager) GetRegularConnWithSettings(ctx context.Context, settings map[string]string, user string, clientKey, serverKey []byte) (regular.PooledConn, error) {
	s := m.settingsCache.GetOrCreate(settings)
	return withReopenRetry(ctx, m, user, clientKey, serverKey, func(pool *UserPool) (regular.PooledConn, error) {
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
	return withReopenRetry(ctx, m, user, clientKey, serverKey, func(pool *UserPool) (*reserved.Conn, error) {
		return pool.NewReservedConn(ctx, s, opts...)
	})
}

// NewLogicalReplicationConn returns a Postgres connection opened in
// replication mode (replication=database) and tagged with
// ReasonLogicalReplication, on the specified user's reserved pool. SCRAM
// passthrough key semantics match NewReservedConn.
func (m *Manager) NewLogicalReplicationConn(ctx context.Context, user string, clientKey, serverKey []byte) (*reserved.Conn, error) {
	return withReopenRetry(ctx, m, user, clientKey, serverKey, func(pool *UserPool) (*reserved.Conn, error) {
		return pool.NewLogicalReplicationConn(ctx)
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

// withReopenRetry runs op against the current user pool, retrying transient,
// self-healable failures:
//
//  1. Closed pool while the manager is being reopened: reopenConnections()
//     runs Close (via CloseForReopen) immediately followed by Open to refresh
//     pools (PostgreSQL auto-restart recovery). A request racing that window
//     sees ErrManagerClosed from getOrCreateUserPool or ErrPoolClosed from the
//     op. Because CloseForReopen marked a reopen window, we wait for the paired
//     Open to finish (honoring ctx) and retry against the fresh pools instead
//     of surfacing the transient error. A closed pool with no reopen window
//     means the manager is genuinely shutting down — surface it unchanged.
//
//  2. Closed pool while the manager stays up: evictUserPool() replaced a
//     stale-credentials pool. Retry against the freshly created pool. Both this
//     and the reopen path are bounded by maxPoolClosedRetries.
//
//  3. Class-28 SQLSTATE from PostgreSQL: the cached user pool's ClientConfig
//     carries stale SCRAM keys (password rotated in pg_authid). Evict the
//     pool and recreate from the triggering session's keys, which are
//     known-current — they were derived moments ago during the session's
//     SCRAM handshake at MultiGateway against whatever verifier pg_authid
//     holds right now. If the retry also auth-fails (retrier itself used the
//     old password at the gateway), we surface the clean 28xxx error and the
//     client reconnects to re-derive keys against the new verifier. This path
//     performs at most one eviction + retry.
//
// clientKey and serverKey forward the SCRAM passthrough material to
// getOrCreateUserPool so that a first-time pool creation triggered during
// this call (including after an eviction or reopen swap) gets the session's
// keys.
func withReopenRetry[T any](ctx context.Context, m *Manager, user string, clientKey, serverKey []byte, op func(*UserPool) (T, error)) (T, error) {
	var zero T
	authRetried := false

	for closedAttempts := 0; ; {
		var err error
		pool, lookupErr := m.getOrCreateUserPool(user, clientKey, serverKey)
		if lookupErr != nil {
			err = lookupErr
		} else {
			result, opErr := op(pool)
			if opErr == nil {
				return result, nil
			}

			// Stale-key self-heal applies only to op failures, since it needs the
			// pool to evict. evictUserPool is best-effort; even if it returns
			// false (racing eviction), getOrCreateUserPool on the next iteration
			// returns whatever is in the snapshot now — a freshly-created pool
			// with fresh keys, or absent (and we create one).
			if mterrors.IsAuthenticationError(opErr) {
				if authRetried {
					return zero, opErr
				}
				authRetried = true
				m.evictUserPool(user, pool)
				continue
			}
			err = opErr
		}

		// A closed manager (ErrManagerClosed from the lookup) and a closed pool
		// (ErrPoolClosed from the op) are the same situation seen from two sides:
		// transient while a reopen is in progress — wait it out and retry — and
		// terminal otherwise. Bound the retries so repeated transient closes
		// can't loop forever.
		if errors.Is(err, ErrManagerClosed) || errors.Is(err, connpool.ErrPoolClosed) {
			if closedAttempts >= maxPoolClosedRetries {
				return zero, err
			}
			closedAttempts++
			retry, werr := m.handleClosed(ctx)
			if werr != nil {
				return zero, werr
			}
			if retry {
				continue
			}
		}

		return zero, err
	}
}

// handleClosed decides how withReopenRetry should react to a closed-pool
// failure (ErrPoolClosed from an op, or ErrManagerClosed from the lookup),
// switching on the manager's lifecycle:
//
//   - reopening: a CloseForReopen->Open window is in progress. Block on the
//     reopen channel (honoring ctx) for the paired Open to finish, then retry
//     against the fresh pools.
//   - closed: terminal shutdown. Report retry=false so the caller surfaces the
//     error unchanged.
//   - running: the reopen already completed, or a pool was evicted/swapped
//     while the manager stayed up. Retry against the current snapshot.
//
// It returns a non-nil error only when ctx is cancelled while waiting. Retry
// bounding is the caller's responsibility.
func (m *Manager) handleClosed(ctx context.Context) (retry bool, err error) {
	switch m.lifecycleState() {
	case lifecycleReopening:
		// done is non-nil whenever lifecycle is reopening; reopenWaitCh may
		// still return nil if the window closed between the load above and the
		// lock below, in which case the reopen is done and we just retry.
		if done := m.reopenWaitCh(); done != nil {
			select {
			case <-done:
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
		return true, nil
	case lifecycleClosed:
		return false, nil
	default: // lifecycleRunning
		return true, nil
	}
}

// lifecycleState returns the manager's current phase.
func (m *Manager) lifecycleState() managerLifecycle {
	return managerLifecycle(m.lifecycle.Load())
}

// setLifecycle stores the manager's phase.
func (m *Manager) setLifecycle(s managerLifecycle) {
	m.lifecycle.Store(uint32(s))
}

// beginReopen arms the reopen wait handle and moves lifecycle to reopening. The
// channel is created before the lifecycle store so any reader that observes
// "reopening" is guaranteed a non-nil channel to wait on. The caller must hold
// createMu.
func (m *Manager) beginReopen() {
	m.reopenMu.Lock()
	if m.reopenDone == nil {
		m.reopenDone = make(chan struct{})
	}
	m.reopenMu.Unlock()
	m.setLifecycle(lifecycleReopening)
}

// endReopen closes the current reopen wait handle, if any, waking withReopenRetry
// waiters so they re-check lifecycle and retry (against the freshly reopened
// pools after Open, or surface the error after a terminal Close). It is a no-op
// when no window is armed (e.g. a plain startup Open). It does not change
// lifecycle — the caller sets that first.
func (m *Manager) endReopen() {
	m.reopenMu.Lock()
	done := m.reopenDone
	m.reopenDone = nil
	m.reopenMu.Unlock()
	if done != nil {
		close(done)
	}
}

// reopenWaitCh returns the channel endReopen will close when the current reopen
// window completes, or nil if no window is armed.
func (m *Manager) reopenWaitCh() <-chan struct{} {
	m.reopenMu.Lock()
	defer m.reopenMu.Unlock()
	return m.reopenDone
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

// UpdateReservedConnSessionState records the latest authoritative gateway
// session settings on a reserved connection without executing SQL. A nil/empty
// settings map is a known-clean desired state, distinct from unknown.
func (m *Manager) UpdateReservedConnSessionState(conn *reserved.Conn, settings map[string]string) {
	if conn == nil {
		return
	}
	conn.SetAuthoritativeSettings(m.settingsCache.GetOrCreate(settings), true)
}

// PrepareReservedConn records the latest authoritative settings and reconciles
// an existing reserved connection before user-visible backend work. If the
// connection is marked untrusted, force reconciliation avoids stale connstate
// pointer-equality no-ops.
func (m *Manager) PrepareReservedConn(ctx context.Context, conn *reserved.Conn, settings map[string]string) error {
	if conn == nil {
		return nil
	}
	desired := m.settingsCache.GetOrCreate(settings)
	conn.SetAuthoritativeSettings(desired, true)

	if conn.SessionStateUntrusted() {
		if err := conn.Conn().ForceApplySettings(ctx, desired); err != nil {
			return err
		}
		conn.ClearSessionStateUntrusted()
		return nil
	}

	current := conn.Conn().Settings()
	if desired == current {
		return nil
	}
	return conn.Conn().ApplySettings(ctx, desired)
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

// signalZeroChanLocked reconciles a drain channel with its counter total: it
// closes ch when total has reached zero (releasing WaitFor* callers), or re-arms
// a fresh open channel when total is positive but ch is still closed from a prior
// drain. Caller must hold drainMu.
func signalZeroChanLocked(total int64, ch *chan struct{}) {
	if total == 0 {
		select {
		case <-*ch:
			// Already closed, nothing to do.
		default:
			close(*ch)
		}
		return
	}
	// total > 0: ensure an open channel for future waiters.
	select {
	case <-*ch:
		// Was closed (drained); re-arm.
		*ch = make(chan struct{})
	default:
		// Already open.
	}
}

// regularAdd adjusts the regular (single-query) borrow count by n. It touches
// only zeroCh (the combined drain) — regular borrows never affect the
// reserved-only drain.
func (m *Manager) regularAdd(n int64) {
	m.drainMu.Lock()
	defer m.drainMu.Unlock()

	if m.regularCount+n < 0 {
		m.logger.Error("regularCount going negative, likely a bug in borrow/recycle callbacks",
			"current", m.regularCount, "delta", n)
	}
	m.regularCount += n
	signalZeroChanLocked(m.regularCount+m.reservedCount, &m.zeroCh)
}

// reservedAdd adjusts the reserved connection count by n. A reserved connection
// affects both drains, so it reconciles reservedZeroCh and the combined zeroCh —
// under a single lock acquisition (unlike calling two separate *Add methods).
func (m *Manager) reservedAdd(n int64) {
	m.drainMu.Lock()
	defer m.drainMu.Unlock()

	if m.reservedCount+n < 0 {
		m.logger.Error("reservedCount going negative, likely a bug in reserve/release callbacks",
			"current", m.reservedCount, "delta", n)
	}
	m.reservedCount += n
	signalZeroChanLocked(m.reservedCount, &m.reservedZeroCh)
	signalZeroChanLocked(m.regularCount+m.reservedCount, &m.zeroCh)
}

// WaitForDrain blocks until all lent connections (regular + reserved) have been
// returned or ctx is cancelled.
func (m *Manager) WaitForDrain(ctx context.Context) error {
	m.drainMu.Lock()
	if m.regularCount+m.reservedCount == 0 {
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

// WaitForReservedDrain blocks until all RESERVED connections (transactions, temp
// tables, portals, COPY, etc.) have been released or ctx is cancelled. Unlike
// WaitForDrain it ignores transient single-query borrows, so the first stage of
// a graceful drain can wait for in-flight transactions to finish while the
// pooler keeps serving single autocommit queries.
func (m *Manager) WaitForReservedDrain(ctx context.Context) error {
	m.drainMu.Lock()
	if m.reservedCount == 0 {
		m.drainMu.Unlock()
		return nil
	}
	ch := m.reservedZeroCh
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

// IsClosed reports whether the manager is terminally closed. It returns false
// during a reopen window: the manager is mid-refresh, not shut down.
func (m *Manager) IsClosed() bool {
	return m.lifecycleState() == lifecycleClosed
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
