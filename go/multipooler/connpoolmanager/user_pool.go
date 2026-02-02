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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/multipooler/pools/admin"
	"github.com/multigres/multigres/go/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/multipooler/pools/regular"
	"github.com/multigres/multigres/go/multipooler/pools/reserved"
)

// UserPool manages connection pools for a specific user.
// Each user gets their own RegularPool and ReservedPool that connect
// directly as that user (using trust/peer auth), eliminating the need
// for SET ROLE.
type UserPool struct {
	username     string
	regularPool  *regular.Pool
	reservedPool *reserved.Pool
	adminPool    *admin.Pool // Shared reference for kill operations
	logger       *slog.Logger

	// Demand tracking for rebalancer
	regularDemandTracker  *DemandTracker
	reservedDemandTracker *DemandTracker

	// Last activity timestamp (Unix nanos) for garbage collection
	lastActivity atomic.Int64

	mu     sync.Mutex
	closed bool
}

// UserPoolConfig holds configuration for creating a UserPool.
type UserPoolConfig struct {
	// ClientConfig is the PostgreSQL connection configuration.
	// The User field specifies the PostgreSQL user for this pool.
	ClientConfig *client.Config

	// AdminPool is the shared admin pool for kill operations.
	AdminPool *admin.Pool

	// RegularPoolConfig is the configuration for the regular connection pool.
	RegularPoolConfig *connpool.Config

	// ReservedPoolConfig is the configuration for the reserved pool's underlying connection pool.
	ReservedPoolConfig *connpool.Config

	// ReservedInactivityTimeout is how long a reserved connection can be inactive (no client activity)
	// before being killed. This is typically more aggressive (e.g., 30s) than pool idle timeout.
	ReservedInactivityTimeout time.Duration

	// DemandWindow is how far back to consider when calculating peak demand.
	// Set to 0 to disable demand tracking.
	// Example: 30s means "allocate based on peak demand over the last 30 seconds"
	DemandWindow time.Duration

	// RebalanceInterval is how often the rebalancer runs.
	// Number of demand tracking buckets = DemandWindow / RebalanceInterval.
	RebalanceInterval time.Duration

	// Logger for pool operations.
	Logger *slog.Logger
}

// NewUserPool creates a new UserPool for the given user.
// The pool connects directly as the user using trust/peer authentication.
func NewUserPool(ctx context.Context, config *UserPoolConfig) *UserPool {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("user", config.ClientConfig.User)

	// Create regular pool for this user
	regularPool := regular.NewPool(ctx, &regular.PoolConfig{
		ClientConfig:   config.ClientConfig,
		ConnPoolConfig: config.RegularPoolConfig,
		AdminPool:      config.AdminPool,
	})
	regularPool.Open()

	// Create reserved pool for this user (it manages its own internal regular pool)
	// InactivityTimeout kills reserved connections if the client that reserved them
	// hasn't used them for this duration (typically aggressive, e.g., 30s).
	// The ReservedPoolConfig.IdleTimeout is for the underlying pool (less aggressive, e.g., 5min).
	reservedPool := reserved.NewPool(ctx, &reserved.PoolConfig{
		InactivityTimeout: config.ReservedInactivityTimeout,
		Logger:            logger,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig:   config.ClientConfig,
			ConnPoolConfig: config.ReservedPoolConfig,
			AdminPool:      config.AdminPool,
		},
	})

	// Create demand trackers for rebalancer (if demand tracking is enabled).
	// We use PeakRequestedAndReset instead of Requested to capture burst demand that
	// point-in-time sampling would miss (e.g., short-lived queries that complete between samples).
	var regularDemandTracker, reservedDemandTracker *DemandTracker
	if config.DemandWindow > 0 && config.RebalanceInterval > 0 {
		// NewDemandTracker validates config and panics on invalid values
		regularDemandTracker = NewDemandTracker(&DemandTrackerConfig{
			DemandWindow:      config.DemandWindow,
			RebalanceInterval: config.RebalanceInterval,
			Sampler:           regularPool.PeakRequestedAndReset,
		})

		reservedDemandTracker = NewDemandTracker(&DemandTrackerConfig{
			DemandWindow:      config.DemandWindow,
			RebalanceInterval: config.RebalanceInterval,
			Sampler:           reservedPool.PeakRequestedAndReset,
		})
	}

	logger.InfoContext(ctx, "user pool created",
		"regular_capacity", config.RegularPoolConfig.Capacity,
		"reserved_capacity", config.ReservedPoolConfig.Capacity)

	up := &UserPool{
		username:              config.ClientConfig.User,
		regularPool:           regularPool,
		reservedPool:          reservedPool,
		adminPool:             config.AdminPool,
		logger:                logger,
		regularDemandTracker:  regularDemandTracker,
		reservedDemandTracker: reservedDemandTracker,
	}
	up.lastActivity.Store(time.Now().UnixNano())
	return up
}

// Username returns the username for this pool.
func (p *UserPool) Username() string {
	return p.username
}

// touchActivity updates the last activity timestamp.
// Called internally when a connection is acquired.
func (p *UserPool) touchActivity() {
	p.lastActivity.Store(time.Now().UnixNano())
}

// LastActivity returns the last activity timestamp (Unix nanos).
// Used by the rebalancer for garbage collection.
func (p *UserPool) LastActivity() int64 {
	return p.lastActivity.Load()
}

// RegularDemand returns the peak demand for regular connections over the sliding window.
// It also rotates the demand tracker to the next bucket, aging out old data.
// This should be called once per rebalance cycle. Returns 0 if demand tracking is disabled.
func (p *UserPool) RegularDemand() int64 {
	if p.regularDemandTracker == nil {
		return 0
	}
	return p.regularDemandTracker.GetPeakAndRotate()
}

// ReservedDemand returns the peak demand for reserved connections over the sliding window.
// It also rotates the demand tracker to the next bucket, aging out old data.
// This should be called once per rebalance cycle. Returns 0 if demand tracking is disabled.
func (p *UserPool) ReservedDemand() int64 {
	if p.reservedDemandTracker == nil {
		return 0
	}
	return p.reservedDemandTracker.GetPeakAndRotate()
}

// GetRegularConn acquires a regular connection from the pool.
// The connection is already authenticated as the pool's user.
func (p *UserPool) GetRegularConn(ctx context.Context) (regular.PooledConn, error) {
	p.touchActivity()
	return p.regularPool.Get(ctx)
}

// GetRegularConnWithSettings acquires a regular connection with the given settings.
// The connection is already authenticated as the pool's user.
func (p *UserPool) GetRegularConnWithSettings(ctx context.Context, settings *connstate.Settings) (regular.PooledConn, error) {
	p.touchActivity()
	return p.regularPool.GetWithSettings(ctx, settings)
}

// NewReservedConn creates a new reserved connection for transactions or portal operations.
// The connection is already authenticated as the pool's user.
func (p *UserPool) NewReservedConn(ctx context.Context, settings *connstate.Settings) (*reserved.Conn, error) {
	p.touchActivity()
	return p.reservedPool.NewConn(ctx, settings)
}

// GetReservedConn retrieves an existing reserved connection by ID.
// Returns nil, false if the connection is not found or has timed out.
func (p *UserPool) GetReservedConn(connID int64) (*reserved.Conn, bool) {
	p.touchActivity()
	return p.reservedPool.Get(connID)
}

// Close closes both regular and reserved pools.
func (p *UserPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.closed = true

	// Close demand trackers first
	if p.regularDemandTracker != nil {
		p.regularDemandTracker.Close()
	}
	if p.reservedDemandTracker != nil {
		p.reservedDemandTracker.Close()
	}

	// Close reserved pool first (it has its own internal regular pool)
	p.reservedPool.Close()

	// Close regular pool
	p.regularPool.Close()

	p.logger.Info("user pool closed")
}

// Stats returns statistics for both pools.
func (p *UserPool) Stats() UserPoolStats {
	var regularDemand, reservedDemand int64
	if p.regularDemandTracker != nil {
		regularDemand = p.regularDemandTracker.Peak()
	}
	if p.reservedDemandTracker != nil {
		reservedDemand = p.reservedDemandTracker.Peak()
	}

	return UserPoolStats{
		Username:       p.username,
		Regular:        p.regularPool.Stats(),
		Reserved:       p.reservedPool.Stats(),
		RegularDemand:  regularDemand,
		ReservedDemand: reservedDemand,
		LastActivity:   p.lastActivity.Load(),
	}
}

// SetCapacity updates the capacity of both regular and reserved pools.
// This is a non-blocking operation: capacity is set immediately, idle connections
// are closed aggressively, and any remaining over-capacity connections are closed
// when they are recycled back to the pool.
func (p *UserPool) SetCapacity(ctx context.Context, regularCap, reservedCap int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errors.New("pool is closed")
	}

	// Set regular pool capacity
	if err := p.regularPool.SetCapacity(ctx, regularCap); err != nil {
		return fmt.Errorf("regular pool: %w", err)
	}

	// Set reserved pool capacity
	if err := p.reservedPool.SetCapacity(ctx, reservedCap); err != nil {
		return fmt.Errorf("reserved pool: %w", err)
	}

	p.logger.InfoContext(ctx, "user pool capacity updated",
		"regular_capacity", regularCap,
		"reserved_capacity", reservedCap)

	return nil
}

// UserPoolStats holds statistics for a user's pools.
type UserPoolStats struct {
	Username       string
	Regular        connpool.PoolStats
	Reserved       reserved.PoolStats
	RegularDemand  int64 // Peak demand from tracker (0 if tracking not enabled)
	ReservedDemand int64 // Peak demand from tracker (0 if tracking not enabled)
	LastActivity   int64 // Unix nanos of last activity
}
