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

package connpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/pools/connstack"
)

var (
	// ErrPoolClosed is returned when operating on a closed pool.
	ErrPoolClosed = errors.New("pool is closed")

	// ErrPoolExhausted is returned when the pool has reached capacity.
	ErrPoolExhausted = errors.New("pool exhausted")

	// ErrTimeout is returned when an operation times out.
	ErrTimeout = errors.New("timeout waiting for connection")
)

// Pool is a connection pool that organizes connections by their state hash.
// Connections with the same state (settings, prepared statements, portals)
// are stored in the same stack for efficient reuse.
type Pool[C Connection] struct {
	// stacks maps state hash to a stack of connections with that state.
	// Protected by mu.
	stacks map[uint64]*connstack.Stack[*Pooled[C]]

	// reservations maps client ID to their reserved connection.
	// When a client reserves a connection, it's removed from stacks and stored here.
	// Protected by mu.
	reservations map[uint64]*Pooled[C]

	// mu protects the stacks and reservations maps.
	mu sync.RWMutex

	// factory creates new connections.
	factory func(context.Context) (C, error)

	// Configuration
	capacity    int           // Maximum number of connections
	maxIdle     int           // Maximum idle connections to keep
	idleTimeout time.Duration // How long before idle connections are closed
	maxLifetime time.Duration // Maximum connection lifetime

	// Atomic counters
	active   atomic.Int64 // Total connections (borrowed + idle + reserved)
	borrowed atomic.Int64 // Connections currently borrowed by clients
	reserved atomic.Int64 // Connections reserved (pinned) to clients

	// Lifecycle
	closed atomic.Bool
}

// Config holds configuration for the connection pool.
type Config struct {
	// Capacity is the maximum number of connections in the pool.
	Capacity int

	// MaxIdle is the maximum number of idle connections to keep.
	// If 0, defaults to Capacity.
	MaxIdle int

	// IdleTimeout is how long a connection can be idle before being closed.
	// If 0, connections are never closed due to idle time.
	IdleTimeout time.Duration

	// MaxLifetime is the maximum lifetime of a connection.
	// If 0, connections are never closed due to age.
	MaxLifetime time.Duration
}

// NewPool creates a new connection pool with the given configuration and factory.
// The factory function is called to create new connections when needed.
func NewPool[C Connection](factory func(context.Context) (C, error), cfg Config) *Pool[C] {
	if cfg.Capacity <= 0 {
		cfg.Capacity = 100 // Default capacity
	}
	if cfg.MaxIdle <= 0 {
		cfg.MaxIdle = cfg.Capacity
	}

	return &Pool[C]{
		stacks:       make(map[uint64]*connstack.Stack[*Pooled[C]]),
		reservations: make(map[uint64]*Pooled[C]),
		factory:      factory,
		capacity:     cfg.Capacity,
		maxIdle:      cfg.MaxIdle,
		idleTimeout:  cfg.IdleTimeout,
		maxLifetime:  cfg.MaxLifetime,
	}
}

// getStack returns the stack for the given state hash, creating it if necessary.
// Must be called with mu held (at least RLock).
func (p *Pool[C]) getStack(hash uint64) *connstack.Stack[*Pooled[C]] {
	// Try read lock first for fast path
	p.mu.RLock()
	stack, ok := p.stacks[hash]
	p.mu.RUnlock()

	if ok {
		return stack
	}

	// Need to create stack - upgrade to write lock
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	stack, ok = p.stacks[hash]
	if ok {
		return stack
	}

	// Create new stack
	stack = &connstack.Stack[*Pooled[C]]{}
	p.stacks[hash] = stack
	return stack
}

// Get returns any available connection from the pool, preferring clean connections.
// If no connection is available and the pool is under capacity, creates a new one.
func (p *Pool[C]) Get(ctx context.Context) (*Pooled[C], error) {
	if p.closed.Load() {
		return nil, ErrPoolClosed
	}

	// Try to get a connection with clean state (hash 0)
	cleanState := NewEmptyConnectionState()
	return p.GetWithState(ctx, cleanState)
}

// GetWithState returns a connection with the specified state.
// If no matching connection is available and the pool is under capacity, creates a new one.
func (p *Pool[C]) GetWithState(ctx context.Context, state *ConnectionState) (*Pooled[C], error) {
	if p.closed.Load() {
		return nil, ErrPoolClosed
	}

	hash := state.Hash()
	stack := p.getStack(hash)

	// Try to pop a connection from the matching stack
	for {
		pooled := stack.Pop()
		if pooled == nil {
			break
		}

		// Check if connection is still valid
		if pooled.Conn().IsClosed() {
			p.closeConnection(pooled)
			continue
		}

		// Check max lifetime
		if p.maxLifetime > 0 && pooled.Age() > p.maxLifetime {
			p.closeConnection(pooled)
			continue
		}

		// Check if state matches (handle hash collisions)
		if !pooled.State().Equals(state) {
			// State mismatch - reset and apply the requested state
			if err := pooled.Conn().ResetState(ctx); err != nil {
				// If reset fails, close the connection
				p.closeConnection(pooled)
				continue
			}

			if err := pooled.Conn().ApplyState(ctx, state); err != nil {
				// If apply fails, close the connection
				p.closeConnection(pooled)
				continue
			}
		}

		// Connection is valid and has the right state
		pooled.UpdateLastUsed()
		p.borrowed.Add(1)
		return pooled, nil
	}

	// No matching connection available, try to create new one
	if p.active.Load() >= int64(p.capacity) {
		return nil, ErrPoolExhausted
	}

	// Create new connection
	conn, err := p.factory(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	pooled := NewPooled(conn)
	p.active.Add(1)
	p.borrowed.Add(1)

	return pooled, nil
}

// Put returns a connection to the pool.
// The connection is placed in the stack corresponding to its current state.
// Reserved connections cannot be put back - they must be unreserved first.
func (p *Pool[C]) Put(pooled *Pooled[C]) error {
	if p.closed.Load() {
		p.closeConnection(pooled)
		return ErrPoolClosed
	}

	if pooled == nil {
		return nil
	}

	p.borrowed.Add(-1)

	// Check if connection is still valid
	if pooled.Conn().IsClosed() {
		p.closeConnection(pooled)
		return nil
	}

	// Check max lifetime
	if p.maxLifetime > 0 && pooled.Age() > p.maxLifetime {
		p.closeConnection(pooled)
		return nil
	}

	// Check idle timeout
	if p.idleTimeout > 0 && pooled.IdleTime() > p.idleTimeout {
		p.closeConnection(pooled)
		return nil
	}

	// Get the stack for this connection's current state
	state := pooled.State()
	if state == nil {
		state = NewEmptyConnectionState()
	}
	hash := state.Hash()
	stack := p.getStack(hash)

	// Return to pool
	pooled.UpdateLastUsed()
	stack.Push(pooled)

	return nil
}

// Reserve marks a connection as reserved (pinned to a client).
// The connection is moved from the borrowed state to reserved state
// and stored in the reservations map for this client.
// If the client already has a reserved connection, this returns an error.
// If the connection is already reserved by a different client, this returns an error.
func (p *Pool[C]) Reserve(pooled *Pooled[C], clientID uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if client already has a reservation
	if existing, ok := p.reservations[clientID]; ok {
		if existing == pooled {
			return nil // Already reserved by this client (idempotent)
		}
		return fmt.Errorf("client %d already has a different reserved connection", clientID)
	}

	// Check if this connection is already reserved by another client
	for otherClientID, existingPooled := range p.reservations {
		if existingPooled == pooled {
			return fmt.Errorf("connection already reserved by client %d", otherClientID)
		}
	}

	// Store reservation
	p.reservations[clientID] = pooled
	p.borrowed.Add(-1) // No longer counted as borrowed
	p.reserved.Add(1)

	return nil
}

// Unreserve marks a connection as no longer reserved and returns it to the pool.
// The connection is removed from the client's reservation and returned to the pool.
func (p *Pool[C]) Unreserve(clientID uint64) error {
	p.mu.Lock()
	pooled, ok := p.reservations[clientID]
	if !ok {
		p.mu.Unlock()
		return nil // Not reserved
	}

	// Remove from reservations
	delete(p.reservations, clientID)
	p.mu.Unlock()

	if p.closed.Load() {
		p.closeConnection(pooled)
		return ErrPoolClosed
	}

	p.reserved.Add(-1)

	// Check if connection is still valid
	if pooled.Conn().IsClosed() {
		p.closeConnection(pooled)
		return nil
	}

	// Check max lifetime
	if p.maxLifetime > 0 && pooled.Age() > p.maxLifetime {
		p.closeConnection(pooled)
		return nil
	}

	// Check idle timeout
	if p.idleTimeout > 0 && pooled.IdleTime() > p.idleTimeout {
		p.closeConnection(pooled)
		return nil
	}

	// Get the stack for this connection's current state
	state := pooled.State()
	if state == nil {
		state = NewEmptyConnectionState()
	}
	hash := state.Hash()
	stack := p.getStack(hash)

	// Return to pool
	pooled.UpdateLastUsed()
	stack.Push(pooled)

	return nil
}

// GetReserved returns the reserved connection for a client, if any.
// Returns nil if the client has no reserved connection.
func (p *Pool[C]) GetReserved(clientID uint64) *Pooled[C] {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.reservations[clientID]
}

// closeConnection closes a connection and decrements the active counter.
func (p *Pool[C]) closeConnection(pooled *Pooled[C]) {
	if pooled != nil && !pooled.Conn().IsClosed() {
		pooled.Conn().Close()
	}
	p.active.Add(-1)
}

// Close closes all connections in the pool.
func (p *Pool[C]) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return ErrPoolClosed
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Close all connections in all stacks
	for _, stack := range p.stacks {
		for {
			pooled := stack.Pop()
			if pooled == nil {
				break
			}
			pooled.Conn().Close()
		}
	}

	// Close all reserved connections
	for _, pooled := range p.reservations {
		pooled.Conn().Close()
	}

	// Clear maps
	p.stacks = nil
	p.reservations = nil

	return nil
}

// Stats returns pool statistics.
func (p *Pool[C]) Stats() PoolStats {
	return PoolStats{
		Active:   p.active.Load(),
		Borrowed: p.borrowed.Load(),
		Reserved: p.reserved.Load(),
		Idle:     p.active.Load() - p.borrowed.Load() - p.reserved.Load(),
	}
}

// PoolStats contains pool statistics.
type PoolStats struct {
	Active   int64 // Total connections
	Borrowed int64 // Connections borrowed by clients
	Reserved int64 // Connections reserved (pinned)
	Idle     int64 // Connections available in pool
}
