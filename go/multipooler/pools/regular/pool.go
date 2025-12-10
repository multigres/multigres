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

// Get returns a connection from the pool with no settings applied.
func (p *Pool) Get(ctx context.Context) (*connpool.Pooled[*Conn], error) {
	return p.pool.Get(ctx)
}

// GetWithSettings returns a connection from the pool with the given settings applied.
func (p *Pool) GetWithSettings(ctx context.Context, settings *connstate.Settings) (*connpool.Pooled[*Conn], error) {
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

// InnerPool returns the underlying connpool.Pool.
// Use with caution - prefer the wrapped methods.
func (p *Pool) InnerPool() *connpool.Pool[*Conn] {
	return p.pool
}
