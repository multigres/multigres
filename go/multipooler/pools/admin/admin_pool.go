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

package admin

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/pgprotocol/client"
)

// PoolConfig holds configuration for the admin pool.
type PoolConfig struct {
	// ClientConfig is the PostgreSQL connection configuration.
	ClientConfig *client.Config

	// ConnPoolConfig is the connection pool configuration.
	ConnPoolConfig *connpool.Config
}

// PooledConn is an alias for a pooled admin connection.
type PooledConn = *connpool.Pooled[*Conn]

// Pool manages a pool of administrative connections using connpool.Pool.
// These connections are used for terminating/canceling other backend connections
// via pg_terminate_backend() and pg_cancel_backend().
//
// The pool is intentionally small (typically 2-5 connections) and always has
// connections available for kill operations.
type Pool struct {
	pool   *connpool.Pool[*Conn]
	config *PoolConfig
}

// NewPool creates a new admin connection pool.
func NewPool(config *PoolConfig) *Pool {
	pool := connpool.NewPool[*Conn](config.ConnPoolConfig)
	pool.Name = "admin"

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
			return nil, fmt.Errorf("failed to create admin connection: %w", err)
		}
		return NewConn(conn), nil
	}

	p.pool.Open(ctx, connector, nil)
}

// Get acquires an admin connection from the pool.
// The caller must call Recycle() on the returned PooledConn to return the connection.
func (p *Pool) Get(ctx context.Context) (PooledConn, error) {
	return p.pool.Get(ctx)
}

// TerminateBackend terminates a backend process using pg_terminate_backend().
// This is a convenience method that handles getting/returning connections.
// Returns true if the backend was terminated, false if it was not found or
// the caller lacks permission.
func (p *Pool) TerminateBackend(ctx context.Context, processID uint32) (bool, error) {
	pooled, err := p.pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer pooled.Recycle()

	return pooled.Conn.TerminateBackend(ctx, processID)
}

// CancelBackend cancels the current query on a backend process using pg_cancel_backend().
// This is a convenience method that handles getting/returning connections.
// Returns true if the signal was sent, false if the backend was not found or
// the caller lacks permission.
func (p *Pool) CancelBackend(ctx context.Context, processID uint32) (bool, error) {
	pooled, err := p.pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer pooled.Recycle()

	return pooled.Conn.CancelBackend(ctx, processID)
}

// Close closes all connections in the pool.
func (p *Pool) Close() {
	p.pool.Close()
}

// Stats returns current pool statistics.
func (p *Pool) Stats() connpool.PoolStats {
	return p.pool.Stats()
}
