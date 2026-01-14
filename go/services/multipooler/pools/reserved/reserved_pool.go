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
func (p *Pool) NewConn(ctx context.Context, settings *connstate.Settings) (*Conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("reserved pool is closed")
	}
	p.mu.Unlock()

	// Get a regular connection from the pool.
	pooled, err := p.conns.GetWithSettings(ctx, settings)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
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

	p.logger.DebugContext(ctx, "reserved connection created",
		"conn_id", connID,
		"process_id", rc.ProcessID())

	return rc, nil
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

	p.killCount.Add(1)

	// Kill the backend process.
	if err := rc.Kill(ctx); err != nil {
		p.logger.WarnContext(ctx, "failed to kill connection",
			"conn_id", connID,
			"error", err)
	}

	// Taint the connection - it's dead after kill.
	rc.pooled.Taint()

	p.logger.InfoContext(ctx, "connection killed",
		"conn_id", connID,
		"process_id", rc.ProcessID())

	return nil
}

// release is called when a reserved connection is released.
func (p *Pool) release(rc *Conn, reason ReleaseReason) {
	p.mu.Lock()
	delete(p.active, rc.ConnID)
	p.mu.Unlock()

	p.releaseCount.Add(1)

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
	}

	// Return the underlying connection to the pool.
	// If the connection is in a bad state, the caller should have tainted it.
	rc.pooled.Recycle()

	p.logger.Debug("reserved connection released",
		"conn_id", rc.ConnID,
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
