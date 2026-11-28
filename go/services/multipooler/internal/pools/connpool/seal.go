// Copyright 2026 Supabase, Inc.
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
	"errors"
	"sync"
)

// Sealing
//
// A drain that runs out of time ends by sealing its pools, which works like
// closing them while keeping them open: a sealed pool lends nothing. Get
// fails with the seal's error, the clients already waiting for a connection
// are woken with it, the idle connections are closed, and a connection
// returned while the pool is sealed is closed rather than kept. The
// connections still lent stay open until their borrower returns them; the
// drain ends what they run through the pool's open connections (OpenConns),
// which the pool records as it dials them. Unseal lets the pool lend again; it
// refills as clients ask for connections, as after Open.
//
// A Get that was already past its check when the pool was sealed checks again
// once it holds a connection, and closes it rather than lend it: a sealed pool
// never hands out a connection, whatever point a Get had reached.

// errWaitSealed is what waitForConn returns to a waiter that the pool woke,
// or refused to queue, because it is sealed.
var errWaitSealed = errors.New("connection pool is sealed")

// Seal stops the pool from lending connections, until Unseal: Get fails with
// err, clients already waiting for a connection get err, and the idle
// connections are closed. Sealing a sealed pool replaces its error.
func (pool *Pool[C]) Seal(err error) {
	pool.sealErr.Store(&err)
	pool.wait.seal()
	for {
		conn := pool.getFromSettingsStack(nil)
		if conn == nil {
			conn = pool.pop(&pool.clean)
		}
		if conn == nil {
			break
		}
		conn.Close()
		pool.closedConn()
	}
}

// Unseal lets a sealed pool lend connections again.
func (pool *Pool[C]) Unseal() {
	pool.wait.unseal()
	pool.sealErr.Store(nil)
}

// sealError returns the error a sealed pool refuses connections with, or nil
// when it is not sealed.
func (pool *Pool[C]) sealError() error {
	if err := pool.sealErr.Load(); err != nil {
		return *err
	}
	return nil
}

// refuseIfSealed closes conn, which a Get acquired but has not lent yet, and
// returns the seal's error when the pool is sealed; otherwise it returns nil
// and the Get lends conn.
func (pool *Pool[C]) refuseIfSealed(conn *Pooled[C]) error {
	err := pool.sealError()
	if err == nil {
		return nil
	}
	conn.Close()
	pool.closedConn()
	return err
}

// waitErr maps the error waitForConn returned to the one the Get returns.
func (pool *Pool[C]) waitErr(err error) error {
	if errors.Is(err, errWaitSealed) {
		if sealErr := pool.sealError(); sealErr != nil {
			return sealErr
		}
	}
	return ErrTimeout
}

// openConns records the connections a pool has dialed, so a drain can reach
// the ones still lent (OpenConns). It is written when a connection is dialed,
// not when one is lent or returned, so it costs nothing on the borrow path.
// Closed connections are dropped from it lazily.
type openConns[C Connection] struct {
	mu    sync.Mutex
	conns []C
}

// add records a newly dialed connection. When the record has grown to twice
// the connections still open, it drops the closed ones first.
func (o *openConns[C]) add(conn C) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.conns) >= 16 {
		open := 0
		for _, c := range o.conns {
			if !c.IsClosed() {
				open++
			}
		}
		if 2*open <= len(o.conns) {
			kept := o.conns[:0]
			for _, c := range o.conns {
				if !c.IsClosed() {
					kept = append(kept, c)
				}
			}
			clear(o.conns[len(kept):])
			o.conns = kept
		}
	}
	o.conns = append(o.conns, conn)
}

// snapshot returns the recorded connections that are still open.
func (o *openConns[C]) snapshot() []C {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]C, 0, len(o.conns))
	for _, c := range o.conns {
		if !c.IsClosed() {
			out = append(out, c)
		}
	}
	return out
}

// OpenConns returns the pool's open connections: lent or idle, or being
// dialed. In a sealed pool, which keeps none idle, they are the connections
// still lent.
func (pool *Pool[C]) OpenConns() []C {
	return pool.dialed.snapshot()
}
