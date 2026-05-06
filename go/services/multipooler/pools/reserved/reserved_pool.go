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

package reserved

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/connstate"
	"github.com/multigres/multigres/go/services/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/pools/regular"
)

// PoolConfig holds configuration for the reserved pool.
type PoolConfig struct {
	// InactivityTimeout is the maximum duration a reserved connection can be inactive
	// (no client activity) before being killed. A value of 0 means no timeout.
	// Default: 30s
	InactivityTimeout time.Duration

	// Logger for pool operations.
	Logger *slog.Logger

	// RegularPoolConfig is the configuration for the underlying regular pool.
	// The reserved pool creates and manages this pool internally.
	RegularPoolConfig *regular.PoolConfig

	// OnReserve is called after a new reserved connection is created (optional).
	OnReserve func()

	// OnRelease is called after a reserved connection is released or killed (optional).
	OnRelease func()
}

// Pool manages reserved connections with ID-based tracking.
// It wraps a regular connection pool and adds:
//   - Unique connection IDs for client-side tracking
//   - Transaction state management
//   - Portal reservation tracking
//   - Lock/unlock semantics for concurrent access
//   - Background killer for idle connections
type Pool struct {
	config *PoolConfig
	logger *slog.Logger

	// conns is the underlying pool of regular connections.
	conns *regular.Pool

	// mu protects active map and closed flag.
	mu sync.Mutex

	// active tracks reserved connections by their unique ID.
	active map[int64]*Conn

	// lastID generates unique connection IDs. Initialized with current Unix nanoseconds
	// to prevent ID collisions after multipooler restarts.
	lastID atomic.Int64

	// closed indicates whether the pool has been closed.
	closed bool

	// ctx is the pool's context, derived from the context passed to NewPool.
	// It is canceled when the pool is closed.
	ctx context.Context

	// cancel cancels the pool's context, signaling the background killer to stop.
	cancel context.CancelFunc

	// Metrics
	reserveCount    atomic.Int64
	releaseCount    atomic.Int64
	killCount       atomic.Int64
	timeoutCount    atomic.Int64
	txCommitCount   atomic.Int64
	txRollbackCount atomic.Int64
}

// NewPool creates a new reserved connection pool.
// The pool creates and manages its own underlying regular connection pool.
// Starts a background goroutine to kill idle connections.
// The provided context is used to derive the pool's lifecycle context.
func NewPool(ctx context.Context, config *PoolConfig) *Pool {
	if config.InactivityTimeout <= 0 {
		config.InactivityTimeout = 30 * time.Second
	}

	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	poolCtx, cancel := context.WithCancel(ctx)

	// Create the underlying regular pool.
	regularPool := regular.NewPool(ctx, config.RegularPoolConfig)
	regularPool.Open()

	p := &Pool{
		config: config,
		logger: logger,
		conns:  regularPool,
		active: make(map[int64]*Conn),
		ctx:    poolCtx,
		cancel: cancel,
	}

	// Initialize lastID with current Unix nanoseconds to prevent ID collisions
	// after multipooler restarts. Sequential IDs from this starting point
	// won't collide with IDs from previous pool instances.
	p.lastID.Store(time.Now().UnixNano())

	// Start background killer goroutine.
	// Ticker interval is 1/10th the idle timeout (like Vitess).
	interval := p.config.InactivityTimeout / 10
	go p.idleKiller(interval)

	return p
}

// idleKiller periodically scans for and kills timed out connections.
func (p *Pool) idleKiller(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.KillTimedOut(p.ctx)
		}
	}
}

// NewConn acquires a new reserved connection.
// The connection is assigned a unique ID for client-side tracking.
// The connection is already authenticated as the pool's configured user.
//
// If WithValidate is supplied, the callback runs against the freshly
// acquired *regular.Conn before the reserved connection is registered.
// On a connection error from validate (e.g. the pool handed back a socket
// that PostgreSQL has silently closed), the underlying connection is
// tainted and a replacement is fetched, up to constants.MaxConnPoolRetryAttempts total.
func (p *Pool) NewConn(ctx context.Context, settings *connstate.Settings, opts ...ReservedConnOption) (*Conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("reserved pool is closed")
	}
	p.mu.Unlock()

	o := reservedConnOpts{}
	for _, opt := range opts {
		opt(&o)
	}

	pooled, err := p.acquireValidated(ctx, settings, o.validate)
	if err != nil {
		return nil, err
	}

	// Generate unique ID. Since lastID is initialized with Unix nanoseconds,
	// IDs won't collide with previous pool instances after restarts.
	connID := p.lastID.Add(1)

	// Create reserved connection.
	rc := newConn(pooled, connID, p)
	rc.SetInactivityTimeout(p.config.InactivityTimeout)

	// Register in active map.
	p.mu.Lock()
	p.active[connID] = rc
	p.mu.Unlock()

	p.reserveCount.Add(1)
	if p.config.OnReserve != nil {
		p.config.OnReserve()
	}

	p.logger.DebugContext(ctx, "reserved connection created",
		"conn_id", connID,
		"process_id", rc.ProcessID())

	return rc, nil
}

// acquireValidated borrows a regular connection from the underlying pool
// and, if validate is non-nil, runs it against the connection. A
// connection error from validate (stale socket exposed by the first
// user-issued write) triggers another attempt on a fresh socket, up to
// constants.MaxConnPoolRetryAttempts.
//
// Any non-nil error from validate — connection or otherwise — taints
// the pooled conn before returning or retrying. Validate hooks perform
// real state-modifying work on the conn (Parse, BEGIN, COPY initiation),
// so a failure in one of those steps may leave the conn in a partially
// modified state (e.g. BEGIN succeeded, then InitiateCopyFromStdin
// failed — the conn is now stuck in failed-transaction 'E' state).
// Recycling such a conn back to the pool would poison the next user;
// discarding it is the safe default and matches the pre-primitive
// behavior of every caller (Release(ReleaseError) on failure).
//
// Connection errors from GetWithSettings itself (stale socket exposed
// while applying SETs) are handled inside regular.Pool.GetWithSettings,
// which retries with the same regime; this layer does not double-retry.
func (p *Pool) acquireValidated(
	ctx context.Context,
	settings *connstate.Settings,
	validate func(context.Context, *regular.Conn) error,
) (regular.PooledConn, error) {
	if validate == nil {
		pooled, err := p.conns.GetWithSettings(ctx, settings)
		if err != nil {
			return nil, fmt.Errorf("failed to get connection: %w", err)
		}
		return pooled, nil
	}

	var lastErr error
	for attempt := 1; attempt <= constants.MaxConnPoolRetryAttempts; attempt++ {
		pooled, err := p.conns.GetWithSettings(ctx, settings)
		if err != nil {
			return nil, fmt.Errorf("failed to get connection: %w", err)
		}

		validateErr := validate(ctx, pooled.Conn)
		if validateErr == nil {
			return pooled, nil
		}

		// Any validate failure discards the conn. Taint nils out
		// pooled.pool, so the subsequent Recycle takes the pool==nil
		// branch and closes the orphaned socket immediately rather than
		// waiting on the idle killer.
		pooled.Taint()
		pooled.Recycle()

		if !mterrors.IsConnectionError(validateErr) {
			return nil, validateErr
		}
		lastErr = validateErr

		if attempt == constants.MaxConnPoolRetryAttempts {
			break
		}
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		backoffTimer := time.NewTimer(constants.ConnPoolRetryBackoff)
		select {
		case <-backoffTimer.C:
		case <-ctx.Done():
			backoffTimer.Stop()
			return nil, context.Cause(ctx)
		}
	}
	return nil, fmt.Errorf("reserved connection validate failed after %d attempts: %w", constants.MaxConnPoolRetryAttempts, lastErr)
}

// Get retrieves a reserved connection by ID and resets its expiry time.
// Returns nil, false if the connection is not found or has timed out.
func (p *Pool) Get(connID int64) (*Conn, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, false
	}

	rc, ok := p.active[connID]
	if !ok {
		return nil, false
	}

	// Check if the connection has timed out.
	if rc.IsTimedOut() {
		p.timeoutCount.Add(1)
		return nil, false
	}

	// Reset expiry time since the connection is being used.
	rc.ResetExpiryTime()

	return rc, true
}

// KillConnection kills a reserved connection by ID.
func (p *Pool) KillConnection(ctx context.Context, connID int64) error {
	p.mu.Lock()
	rc, ok := p.active[connID]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("connection %d not found", connID)
	}
	delete(p.active, connID)
	p.mu.Unlock()

	// Kill the backend process.
	if err := rc.Kill(ctx); err != nil {
		p.logger.WarnContext(ctx, "failed to kill connection",
			"conn_id", connID,
			"error", err)
	}

	// Taint the connection - it's dead after kill.
	rc.pooled.Taint()

	// Release handles OnRelease, Recycle, and metrics. The CAS inside
	// Release prevents double-release if an in-flight request also calls
	// Release after Kill causes it to fail.
	rc.Release(ReleaseKill)

	p.logger.InfoContext(ctx, "connection killed",
		"conn_id", connID,
		"process_id", rc.ProcessID())

	return nil
}

// release is called when a reserved connection is released.
func (p *Pool) release(rc *Conn, reason ReleaseReason) {
	p.mu.Lock()
	delete(p.active, rc.ConnID())
	p.mu.Unlock()

	p.releaseCount.Add(1)
	if p.config.OnRelease != nil {
		p.config.OnRelease()
	}

	// Update metrics based on reason.
	switch reason {
	case ReleaseCommit:
		p.txCommitCount.Add(1)
	case ReleaseRollback:
		p.txRollbackCount.Add(1)
	case ReleaseTimeout:
		p.timeoutCount.Add(1)
	case ReleaseKill:
		p.killCount.Add(1)
	case ReleaseError:
		rc.pooled.Taint()
	}

	// Return the underlying connection to the pool.
	// If the connection is in a bad state, the caller should have tainted it.
	rc.pooled.Recycle()

	p.logger.Debug("reserved connection released",
		"conn_id", rc.ConnID(),
		"reason", reason.String())
}

// Close closes all reserved connections, the underlying regular pool, and the pool itself.
func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true

	// Cancel the pool's context to stop the background killer.
	p.cancel()

	// Collect all connections to taint.
	conns := make([]*Conn, 0, len(p.active))
	for _, rc := range p.active {
		conns = append(conns, rc)
	}
	p.active = make(map[int64]*Conn)
	p.mu.Unlock()

	// Taint all connections since they may be in an inconsistent state.
	for _, rc := range conns {
		rc.pooled.Taint()
		rc.pooled.Close()
	}

	// Close the underlying regular pool.
	p.conns.Close()

	p.logger.Info("reserved pool closed")
}

// Stats returns current pool statistics.
func (p *Pool) Stats() PoolStats {
	p.mu.Lock()
	active := len(p.active)
	p.mu.Unlock()

	return PoolStats{
		Active:          active,
		ReserveCount:    p.reserveCount.Load(),
		ReleaseCount:    p.releaseCount.Load(),
		KillCount:       p.killCount.Load(),
		TimeoutCount:    p.timeoutCount.Load(),
		TxCommitCount:   p.txCommitCount.Load(),
		TxRollbackCount: p.txRollbackCount.Load(),
		RegularPool:     p.conns.Stats(),
	}
}

// SetCapacity changes the pool's maximum capacity.
// If reducing capacity, may block waiting for borrowed connections to return.
func (p *Pool) SetCapacity(ctx context.Context, newcap int64) error {
	return p.conns.SetCapacity(ctx, newcap)
}

// Requested returns the number of currently requested connections (borrowed + waiters).
// Used for demand tracking in the rebalancer.
func (p *Pool) Requested() int64 {
	return p.conns.Requested()
}

// PeakRequestedAndReset returns the peak demand since the last reset and resets the peak.
// This captures burst demand that point-in-time sampling might miss.
func (p *Pool) PeakRequestedAndReset() int64 {
	return p.conns.PeakRequestedAndReset()
}

// WaitCount returns the total number of times a client had to wait for a connection.
func (p *Pool) WaitCount() int64 {
	return p.conns.WaitCount()
}

// WaitTime returns the total time clients spent waiting for a connection.
func (p *Pool) WaitTime() time.Duration {
	return p.conns.WaitTime()
}

// GetCount returns the total number of Get() calls (connections borrowed).
func (p *Pool) GetCount() int64 {
	return p.conns.GetCount()
}

// PoolStats contains pool statistics for reserved connections.
type PoolStats struct {
	// Active is the number of currently reserved connections.
	Active int

	// ReserveCount is the total number of reservations.
	ReserveCount int64

	// ReleaseCount is the total number of releases.
	ReleaseCount int64

	// KillCount is the total number of killed connections.
	KillCount int64

	// TimeoutCount is the total number of timed out connections.
	TimeoutCount int64

	// TxCommitCount is the total number of committed transactions.
	TxCommitCount int64

	// TxRollbackCount is the total number of rolled back transactions.
	TxRollbackCount int64

	// RegularPool contains statistics for the underlying regular connection pool.
	RegularPool connpool.PoolStats
}

// ForEachActive calls fn for each active reserved connection.
// This is useful for monitoring and cleanup operations.
func (p *Pool) ForEachActive(fn func(connID int64, rc *Conn) bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for id, rc := range p.active {
		if !fn(id, rc) {
			return
		}
	}
}

// KillAll kills all active reserved connections.
// Used during graceful shutdown when the drain grace period has expired.
func (p *Pool) KillAll(ctx context.Context) int {
	p.mu.Lock()
	ids := make([]int64, 0, len(p.active))
	for id := range p.active {
		ids = append(ids, id)
	}
	p.mu.Unlock()

	for _, connID := range ids {
		if err := p.KillConnection(ctx, connID); err != nil {
			p.logger.WarnContext(ctx, "failed to kill connection during drain",
				"conn_id", connID, "error", err)
		}
	}

	return len(ids)
}

// KillTimedOut kills all connections that have exceeded their timeout.
// This should be called periodically by a background goroutine.
func (p *Pool) KillTimedOut(ctx context.Context) int {
	var timedOutIDs []int64

	// Find all timed out connections.
	p.mu.Lock()
	for id, rc := range p.active {
		if rc.IsTimedOut() {
			timedOutIDs = append(timedOutIDs, id)
		}
	}
	p.mu.Unlock()

	// Kill them.
	for _, connID := range timedOutIDs {
		if err := p.KillConnection(ctx, connID); err != nil {
			p.logger.WarnContext(ctx, "failed to kill timed out connection",
				"conn_id", connID,
				"error", err)
		}
	}

	return len(timedOutIDs)
}
