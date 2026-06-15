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

	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"
)

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

	// generation is the pool's defaults-generation at the time this connection
	// was (re)established. When the pool's generation is bumped (see
	// Pool.InvalidateDefaults), a connection whose generation is older is stale:
	// its backend cached per-database/role GUC defaults (pg_db_role_setting) that
	// have since changed (ALTER DATABASE/ROLE ... SET, or an extension that runs
	// one). The pool lazily reconnects such a connection on its next borrow so the
	// fresh backend re-reads those defaults. Owned by whoever currently holds the
	// connection (single owner at a time), so it needs no synchronization.
	generation int64

	// pool is a reference to the pool that owns this connection.
	// Used for the Recycle pattern.
	pool *Pool[C]

	// Conn is the underlying connection.
	Conn C
}

// Close closes the underlying connection.
func (p *Pooled[C]) Close() {
	p.Conn.Close()
}

// Recycle returns the connection to its pool.
// If the connection is closed, a new connection will be created to replace it.
// If the pool reference is nil, the connection is closed instead.
func (p *Pooled[C]) Recycle() {
	switch {
	case p.pool == nil:
		p.Conn.Close()
	case p.Conn.IsClosed():
		p.pool.put(nil)
	default:
		p.pool.put(p)
	}
}

// Taint marks this connection as unusable and removes it from the pool.
// The connection will be closed and a new one created when recycled.
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

// RefreshIfStale reconnects the underlying backend when its defaults-generation
// predates the pool's current generation. Used for checked-out connections
// (e.g. reserved) that bypass the pool's normal borrow-time refresh path.
func (p *Pooled[C]) RefreshIfStale(ctx context.Context) error {
	return p.pool.refreshIfStale(ctx, p)
}
