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

	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/multipooler/pools/admin"
	"github.com/multigres/multigres/go/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/pgprotocol/client"
)

// PooledConn is an alias for a pooled regular connection.
type PooledConn = *connpool.Pooled[*Conn]

// PoolConfig holds configuration for the regular pool.
type PoolConfig struct {
	// ClientConfig is the PostgreSQL connection configuration.
	ClientConfig *client.Config

	// ConnPoolConfig is the connection pool configuration.
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
func NewPool(config *PoolConfig) *Pool {
	pool := connpool.NewPool[*Conn](config.ConnPoolConfig)
	pool.Name = "regular"

	return &Pool{
		pool:   pool,
		config: config,
	}
}

// Open opens the pool and starts background workers.
// Must be called before using the pool.
func (p *Pool) Open(ctx context.Context) {
	connector := func(ctx context.Context) (*Conn, error) {
		conn, err := client.Connect(ctx, p.config.ClientConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create regular connection: %w", err)
		}
		return NewConn(conn, p.config.AdminPool), nil
	}

	p.pool.Open(ctx, connector, nil)
}

// Get returns a connection from the pool with the specified user role.
// If user is non-empty and doesn't match the connection's current role, SET ROLE is executed.
func (p *Pool) Get(ctx context.Context, user string) (PooledConn, error) {
	pooled, err := p.pool.Get(ctx)
	if err != nil {
		return nil, err
	}

	if err := p.ensureRole(ctx, pooled, user); err != nil {
		pooled.Taint()
		return nil, err
	}

	return pooled, nil
}

// GetWithSettings returns a connection from the pool with the given settings and user role applied.
// If user is non-empty and doesn't match the connection's current role, SET ROLE is executed.
func (p *Pool) GetWithSettings(ctx context.Context, settings *connstate.Settings, user string) (PooledConn, error) {
	pooled, err := p.pool.GetWithSettings(ctx, settings)
	if err != nil {
		return nil, err
	}

	if err := p.ensureRole(ctx, pooled, user); err != nil {
		pooled.Taint()
		return nil, err
	}

	return pooled, nil
}

// ensureRole ensures the connection has the correct role set.
func (p *Pool) ensureRole(ctx context.Context, pooled PooledConn, user string) error {
	if user == "" {
		return nil
	}

	if pooled.Conn.CurrentUser() == user {
		return nil
	}

	return pooled.Conn.SetRole(ctx, user)
}

// Close closes all connections in the pool.
func (p *Pool) Close() {
	p.pool.Close()
}

// Stats returns current pool statistics.
func (p *Pool) Stats() connpool.PoolStats {
	return p.pool.Stats()
}

// InnerPool returns the underlying connpool.Pool.
// Use with caution - prefer the wrapped methods.
func (p *Pool) InnerPool() *connpool.Pool[*Conn] {
	return p.pool
}
