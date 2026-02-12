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

package regular

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/multipooler/pools/admin"
	"github.com/multigres/multigres/go/multipooler/pools/connpool"
)

// PooledConn is an alias for a pooled regular connection.
type PooledConn = *connpool.Pooled[*Conn]

// PoolConfig holds configuration for the regular pool.
type PoolConfig struct {
	// ClientConfig is the PostgreSQL connection configuration.
	ClientConfig *client.Config

	// ConnPoolConfig is the connection pool configuration.
	// Use ConnPoolConfig.Name to set a custom pool name for metrics (defaults to "unnamed").
	ConnPoolConfig *connpool.Config

	// AdminPool is used for kill operations on connections.
	AdminPool *admin.Pool
}

// Pool manages regular connections for query execution.
// It wraps connpool.Pool[*Conn] for convenience.
type Pool struct {
	pool   *connpool.Pool[*Conn]
	config *PoolConfig
}

// NewPool creates a new regular connection pool.
// The context is used for background pool operations and OTel tracking.
// The pool must be opened with Open() before use.
func NewPool(ctx context.Context, config *PoolConfig) *Pool {
	// Set default name if not provided in ConnPoolConfig
	if config.ConnPoolConfig.Name == "" {
		config.ConnPoolConfig.Name = "unnamed"
	}

	pool := connpool.NewPool[*Conn](ctx, config.ConnPoolConfig)

	return &Pool{
		pool:   pool,
		config: config,
	}
}

// Open opens the pool and starts background workers.
// Must be called before using the pool.
func (p *Pool) Open() {
	connector := func(ctx context.Context, poolCtx context.Context) (*Conn, error) {
		conn, err := client.Connect(ctx, poolCtx, p.config.ClientConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create regular connection: %w", err)
		}
		return NewConn(conn, p.config.AdminPool), nil
	}

	p.pool.Open(connector, nil)
}

// Get returns a connection from the pool.
// The connection is already authenticated as the pool's configured user.
func (p *Pool) Get(ctx context.Context) (PooledConn, error) {
	return p.pool.Get(ctx)
}

// GetWithSettings returns a connection from the pool with the given settings applied.
// The connection is already authenticated as the pool's configured user.
func (p *Pool) GetWithSettings(ctx context.Context, settings *connstate.Settings) (PooledConn, error) {
	return p.pool.GetWithSettings(ctx, settings)
}

// Close closes all connections in the pool.
func (p *Pool) Close() {
	p.pool.Close()
}

// Stats returns current pool statistics.
func (p *Pool) Stats() connpool.PoolStats {
	return p.pool.Stats()
}

// SetCapacity changes the pool's maximum capacity.
// If reducing capacity, may block waiting for borrowed connections to return.
func (p *Pool) SetCapacity(ctx context.Context, newcap int64) error {
	return p.pool.SetCapacity(ctx, newcap)
}

// InnerPool returns the underlying connpool.Pool.
// Use with caution - prefer the wrapped methods.
func (p *Pool) InnerPool() *connpool.Pool[*Conn] {
	return p.pool
}

// Requested returns the number of currently requested connections (borrowed + waiters).
// Used for demand tracking in the rebalancer.
func (p *Pool) Requested() int64 {
	return p.pool.Requested()
}

// PeakRequestedAndReset returns the peak demand since the last reset and resets the peak.
// This captures burst demand that point-in-time sampling might miss.
func (p *Pool) PeakRequestedAndReset() int64 {
	return p.pool.PeakRequestedAndReset()
}
