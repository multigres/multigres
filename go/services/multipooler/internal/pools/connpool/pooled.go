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

import "github.com/multigres/multigres/go/services/multipooler/internal/connstate"

// Pooled wraps a connection with metadata for pool management.
// It tracks creation time and last used time using monotonic timestamps.
// The next pointer enables stack operations within the pool.
type Pooled[C Connection] struct {
	// next is the next element in the stack.
	// This is only accessed while holding the stack's mutex.
	next *Pooled[C]

	// timeCreated is the monotonic time when this connection was created.
	timeCreated timestamp

	// timeUsed is the monotonic time when this connection was last used.
	// This is used for idle timeout tracking.
	timeUsed timestamp

	// pool is a reference to the pool that owns this connection.
	// Used for the Recycle pattern.
	pool *Pool[C]

	// taintOnRecycle marks a borrowed connection as unusable without releasing
	// its pool slot until Recycle. This is for connections that must remain
	// capacity-accounted while active but must never return to the idle pool.
	taintOnRecycle bool

	// Conn is the underlying connection.
	Conn C
}

// Close closes the underlying connection.
func (p *Pooled[C]) Close() {
	p.Conn.Close()
}

// Recycle returns the connection to its pool.
// If the connection is closed or marked with TaintOnRecycle, a new connection
// will be created to replace it. If the pool reference is nil, the connection
// is closed instead.
func (p *Pooled[C]) Recycle() {
	switch {
	case p.pool == nil:
		p.Conn.Close()
	case p.Conn.IsClosed():
		p.recycleDiscarded()
	case p.taintOnRecycle:
		p.Conn.Close()
		p.recycleDiscarded()
	default:
		p.pool.put(p)
	}
}

func (p *Pooled[C]) recycleDiscarded() {
	pool := p.pool
	p.pool = nil
	pool.put(nil)
}

// TaintOnRecycle marks this borrowed connection as unusable while keeping its
// pool slot borrowed until Recycle. Recycle will close this connection and
// replace it instead of returning it to the idle pool.
func (p *Pooled[C]) TaintOnRecycle() {
	p.taintOnRecycle = true
}

// Taint marks this connection as unusable and immediately releases/replaces its
// pool slot. Use TaintOnRecycle when the slot must remain borrowed until Recycle.
func (p *Pooled[C]) Taint() {
	if p.pool == nil {
		return
	}
	p.pool.put(nil)
	p.pool = nil
}

// Settings returns the current settings of the connection from the underlying connection.
func (p *Pooled[C]) Settings() *connstate.Settings {
	return p.Conn.Settings()
}
