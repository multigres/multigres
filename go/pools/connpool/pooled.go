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
	"sync/atomic"
	"time"
)

// Pooled wraps a connection with metadata for pool management.
// It tracks creation time and last used time.
// The next pointer enables lock-free stack operations.
// State is accessed via conn.State().
type Pooled[C Connection] struct {
	// conn is the underlying connection.
	conn C

	// next is the next element in the lock-free stack.
	// This is manipulated atomically by the stack implementation.
	next atomic.Pointer[*Pooled[C]]

	// createdAt is the time when this connection was created.
	createdAt time.Time

	// lastUsedAt is the time when this connection was last used.
	// Updated atomically when returning to pool or borrowing from pool.
	lastUsedAt atomic.Int64 // Unix timestamp in nanoseconds
}

// NewPooled creates a new Pooled wrapper around a connection.
func NewPooled[C Connection](conn C) *Pooled[C] {
	now := time.Now()
	p := &Pooled[C]{
		conn:      conn,
		createdAt: now,
	}
	p.lastUsedAt.Store(now.UnixNano())
	return p
}

// NextPtr returns a pointer to the atomic next pointer.
// This is required by the connstack.Node interface.
func (p *Pooled[C]) NextPtr() *atomic.Pointer[*Pooled[C]] {
	return &p.next
}

// Conn returns the underlying connection.
func (p *Pooled[C]) Conn() C {
	return p.conn
}

// State returns the current state of the connection from the underlying connection.
func (p *Pooled[C]) State() *ConnectionState {
	return p.conn.State()
}

// CreatedAt returns the time when this connection was created.
func (p *Pooled[C]) CreatedAt() time.Time {
	return p.createdAt
}

// LastUsedAt returns the time when this connection was last used.
func (p *Pooled[C]) LastUsedAt() time.Time {
	ns := p.lastUsedAt.Load()
	if ns == 0 {
		return p.createdAt
	}
	return time.Unix(0, ns)
}

// UpdateLastUsed updates the last used timestamp to now.
func (p *Pooled[C]) UpdateLastUsed() {
	p.lastUsedAt.Store(time.Now().UnixNano())
}

// Age returns the duration since this connection was created.
func (p *Pooled[C]) Age() time.Duration {
	return time.Since(p.createdAt)
}

// IdleTime returns the duration since this connection was last used.
func (p *Pooled[C]) IdleTime() time.Duration {
	return time.Since(p.LastUsedAt())
}
