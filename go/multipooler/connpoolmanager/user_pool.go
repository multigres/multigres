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
	"log/slog"
	"sync"
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

	logger.InfoContext(ctx, "user pool created",
		"regular_capacity", config.RegularPoolConfig.Capacity,
		"reserved_capacity", config.ReservedPoolConfig.Capacity)

	return &UserPool{
		username:     config.ClientConfig.User,
		regularPool:  regularPool,
		reservedPool: reservedPool,
		adminPool:    config.AdminPool,
		logger:       logger,
	}
}

// Username returns the username for this pool.
func (p *UserPool) Username() string {
	return p.username
}

// GetRegularConn acquires a regular connection from the pool.
// The connection is already authenticated as the pool's user.
func (p *UserPool) GetRegularConn(ctx context.Context) (regular.PooledConn, error) {
	return p.regularPool.Get(ctx)
}

// GetRegularConnWithSettings acquires a regular connection with the given settings.
// The connection is already authenticated as the pool's user.
func (p *UserPool) GetRegularConnWithSettings(ctx context.Context, settings *connstate.Settings) (regular.PooledConn, error) {
	return p.regularPool.GetWithSettings(ctx, settings)
}

// NewReservedConn creates a new reserved connection for transactions or portal operations.
// The connection is already authenticated as the pool's user.
func (p *UserPool) NewReservedConn(ctx context.Context, settings *connstate.Settings) (*reserved.Conn, error) {
	return p.reservedPool.NewConn(ctx, settings)
}

// GetReservedConn retrieves an existing reserved connection by ID.
// Returns nil, false if the connection is not found or has timed out.
func (p *UserPool) GetReservedConn(connID int64) (*reserved.Conn, bool) {
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

	// Close reserved pool first (it has its own internal regular pool)
	p.reservedPool.Close()

	// Close regular pool
	p.regularPool.Close()

	p.logger.Info("user pool closed")
}

// Stats returns statistics for both pools.
func (p *UserPool) Stats() UserPoolStats {
	return UserPoolStats{
		Username: p.username,
		Regular:  p.regularPool.Stats(),
		Reserved: p.reservedPool.Stats(),
	}
}

// UserPoolStats holds statistics for a user's pools.
type UserPoolStats struct {
	Username string
	Regular  connpool.PoolStats
	Reserved reserved.PoolStats
}
