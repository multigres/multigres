// Copyright 2021 The Vitess Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Modifications Copyright 2025 Supabase, Inc.

package rpcclient

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	"github.com/multigres/multigres/go/viperutil"
)

const defaultCapacity = 100

// ConnConfig holds configuration for multipooler RPC client connections.
type ConnConfig struct {
	cert viperutil.Value[string]
	key  viperutil.Value[string]
	ca   viperutil.Value[string]
	crl  viperutil.Value[string]
	name viperutil.Value[string]
}

// NewConnConfig creates a new ConnConfig with default values.
func NewConnConfig(reg *viperutil.Registry) *ConnConfig {
	return &ConnConfig{
		cert: viperutil.Configure(reg, "multipooler-grpc-cert", viperutil.Options[string]{
			Default:  "",
			FlagName: "multipooler-grpc-cert",
			Dynamic:  false,
		}),
		key: viperutil.Configure(reg, "multipooler-grpc-key", viperutil.Options[string]{
			Default:  "",
			FlagName: "multipooler-grpc-key",
			Dynamic:  false,
		}),
		ca: viperutil.Configure(reg, "multipooler-grpc-ca", viperutil.Options[string]{
			Default:  "",
			FlagName: "multipooler-grpc-ca",
			Dynamic:  false,
		}),
		crl: viperutil.Configure(reg, "multipooler-grpc-crl", viperutil.Options[string]{
			Default:  "",
			FlagName: "multipooler-grpc-crl",
			Dynamic:  false,
		}),
		name: viperutil.Configure(reg, "multipooler-grpc-server-name", viperutil.Options[string]{
			Default:  "",
			FlagName: "multipooler-grpc-server-name",
			Dynamic:  false,
		}),
	}
}

// RegisterFlags registers all multipooler RPC client flags with the given FlagSet.
func (cc *ConnConfig) RegisterFlags(fs *pflag.FlagSet) {
	fs.String("multipooler-grpc-cert", cc.cert.Default(), "the cert to use to connect to multipooler (not yet implemented)")
	fs.String("multipooler-grpc-key", cc.key.Default(), "the key to use to connect to multipooler (not yet implemented)")
	fs.String("multipooler-grpc-ca", cc.ca.Default(), "the server ca to use to validate multipooler servers when connecting (not yet implemented)")
	fs.String("multipooler-grpc-crl", cc.crl.Default(), "the server crl to use to validate multipooler server certificates when connecting (not yet implemented)")
	fs.String("multipooler-grpc-server-name", cc.name.Default(), "the server name to use to validate multipooler server certificate (not yet implemented)")

	viperutil.BindFlags(fs, cc.cert, cc.key, cc.ca, cc.crl, cc.name)
}

// validateTLSConfig checks if TLS is configured and returns an error if so (not yet implemented).
func (cc *ConnConfig) validateTLSConfig() error {
	if cc.cert.Get() != "" || cc.key.Get() != "" || cc.ca.Get() != "" || cc.crl.Get() != "" || cc.name.Get() != "" {
		return fmt.Errorf("TLS configuration for multipooler RPC client is not yet implemented")
	}
	return nil
}

// closeFunc allows a standalone function to implement io.Closer, similar to
// how http.HandlerFunc allows standalone functions to implement http.Handler.
type closeFunc func() error

func (fn closeFunc) Close() error {
	return fn()
}

var _ io.Closer = (*closeFunc)(nil)

// cachedConn holds a cached gRPC connection to a single multipooler along with
// clients for both consensus and manager services.
type cachedConn struct {
	consensusClient consensuspb.MultiPoolerConsensusClient
	managerClient   multipoolermanagerpb.MultiPoolerManagerClient
	cc              *grpc.ClientConn

	addr           string
	lastAccessTime time.Time
	refs           int
}

// connCache manages a cache of gRPC connections with LRU eviction and capacity limiting.
// This implementation is based on Vitess's cachedConnDialer for vttablet connections.
type connCache struct {
	m            sync.Mutex
	conns        map[string]*cachedConn
	evict        []*cachedConn
	evictSorted  bool
	connWaitSema *semaphore.Weighted
	capacity     int
	metrics      *Metrics
}

// newConnCache creates a new connection cache with the default capacity.
func newConnCache() *connCache {
	return newConnCacheWithCapacity(defaultCapacity)
}

// newConnCacheWithCapacity creates a new connection cache with a specified capacity.
func newConnCacheWithCapacity(capacity int) *connCache {
	cc := &connCache{
		conns:        make(map[string]*cachedConn, capacity),
		evict:        make([]*cachedConn, 0, capacity),
		connWaitSema: semaphore.NewWeighted(int64(capacity)),
		capacity:     capacity,
		metrics:      NewMetrics(),
	}

	// Register callback for cache size observable gauge
	_ = cc.metrics.RegisterCacheSizeCallback(func() int {
		cc.m.Lock()
		defer cc.m.Unlock()
		return len(cc.conns)
	})

	return cc
}

// sortEvictionsLocked sorts the eviction queue by refs (descending) then by
// lastAccessTime (ascending). This ensures unreferenced connections (refs=0)
// are at the front of the queue for efficient eviction.
func (cc *connCache) sortEvictionsLocked() {
	if !cc.evictSorted {
		sort.Slice(cc.evict, func(i, j int) bool {
			left, right := cc.evict[i], cc.evict[j]
			if left.refs == right.refs {
				return right.lastAccessTime.After(left.lastAccessTime)
			}
			return right.refs > left.refs
		})
		cc.evictSorted = true
	}
}

// getOrDial gets an existing connection from the cache or creates a new one.
// Returns the connection and a closer function that must be called when done.
//
// This follows Vitess's three-path dial strategy:
//  1. cache_fast: Try to get from cache without blocking
//  2. sema_fast: Acquire semaphore without blocking and dial new connection
//  3. sema_poll: Poll for evictable connections while waiting for capacity
func (cc *connCache) getOrDial(ctx context.Context, addr string) (*cachedConn, closeFunc, error) {
	start := time.Now()

	// Fast path: try to get from cache without blocking
	if client, closer, found, err := cc.tryFromCache(ctx, addr, &cc.m); found {
		cc.metrics.RecordDialDuration(ctx, time.Since(start), DialPathCacheFast)
		return client, closer, err
	}

	// Try to acquire semaphore without blocking (fast path for new connections)
	if cc.connWaitSema.TryAcquire(1) {
		defer func() {
			cc.metrics.RecordDialDuration(ctx, time.Since(start), DialPathSemaFast)
		}()

		// Check if another goroutine managed to dial a conn for the same addr
		// while we were waiting for the write lock. This is identical to the
		// read-lock section above, except we release the connWaitSema if we
		// are able to use the cache, allowing another goroutine to dial a new
		// conn instead.
		if client, closer, found, err := cc.tryFromCache(ctx, addr, &cc.m); found {
			cc.connWaitSema.Release(1)
			return client, closer, err
		}
		return cc.newDial(ctx, addr)
	}

	// Slow path: poll for evictable connections
	defer func() {
		cc.metrics.RecordDialDuration(ctx, time.Since(start), DialPathSemaPoll)
	}()

	for {
		select {
		case <-ctx.Done():
			cc.metrics.AddDialTimeout(ctx)
			return nil, nil, ctx.Err()
		default:
			if client, closer, found, err := cc.pollOnce(ctx, addr); found {
				return client, closer, err
			}
		}
	}
}

// tryFromCache tries to get a connection from the cache, performing a redial
// on that connection if it exists. It returns a connection, a closer, a flag
// to indicate whether a connection was found in the cache, and an error.
//
// In addition to the addr being dialed, tryFromCache takes a sync.Locker which,
// if not nil, will be used to wrap the lookup and redial in that lock. This
// function can be called in situations where the conns map is locked
// externally (like in pollOnce), so we do not want to manage the locks here. In
// other cases (like in the cache_fast path of getOrDial()), we pass in the cc.m
// to ensure we have a lock on the cache for the duration of the call.
func (cc *connCache) tryFromCache(ctx context.Context, addr string, locker sync.Locker) (client *cachedConn, closer closeFunc, found bool, err error) {
	if locker != nil {
		locker.Lock()
		defer locker.Unlock()
	}

	if conn, ok := cc.conns[addr]; ok {
		client, closer, err := cc.redialLocked(ctx, conn)
		return client, closer, ok, err
	}

	return nil, nil, false, nil
}

// pollOnce is called on each iteration of the polling loop in getOrDial(). It:
//   - locks the conns cache for writes
//   - attempts to get a connection from the cache. If found, redial() it and exit.
//   - peeks at the head of the eviction queue. if the peeked conn has no refs, it
//     is unused, and can be evicted to make room for the new connection to addr.
//     If the peeked conn has refs, exit.
//   - pops the conn we just peeked from the queue, deletes it from the cache, and
//     close the underlying ClientConn for that conn.
//   - attempt a newDial. if the newDial fails, it will release a slot on the
//     connWaitSema, so another getOrDial() call can successfully acquire it to dial
//     a new conn. if the newDial succeeds, we will have evicted one conn, but
//     added another, so the net change is 0, and no changes to the connWaitSema
//     are made.
//
// It returns a connection, a closer, a flag to indicate whether the getOrDial()
// poll loop should exit, and an error.
func (cc *connCache) pollOnce(ctx context.Context, addr string) (client *cachedConn, closer closeFunc, found bool, err error) {
	cc.m.Lock()

	if client, closer, found, err := cc.tryFromCache(ctx, addr, nil); found {
		cc.m.Unlock()
		return client, closer, found, err
	}

	cc.sortEvictionsLocked()

	conn := cc.evict[0]
	if conn.refs != 0 {
		cc.m.Unlock()
		return nil, nil, false, nil
	}

	cc.evict = cc.evict[1:]
	delete(cc.conns, conn.addr)
	if conn.cc != nil {
		conn.cc.Close()
	}
	cc.m.Unlock()

	client, closer, err = cc.newDial(ctx, addr)
	return client, closer, true, err
}

// newDial creates a new cached connection, and updates the cache and eviction
// queue accordingly. If newDial fails to create the underlying gRPC connection,
// it will make a call to Release the connWaitSema for other newDial calls.
//
// It returns the two-tuple of connection and closer that getOrDial returns.
func (cc *connCache) newDial(ctx context.Context, addr string) (*cachedConn, closeFunc, error) {
	// TODO: Add proper TLS configuration for production
	grpcConn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		cc.connWaitSema.Release(1)
		return nil, nil, err
	}

	cc.m.Lock()
	defer cc.m.Unlock()

	if conn, existing := cc.conns[addr]; existing {
		// race condition: some other goroutine has dialed our multipooler before we have;
		// this is not great, but shouldn't happen often (if at all), so we're going to
		// close this connection and reuse the existing one. by doing this, we can keep
		// the actual Dial out of the global lock and significantly increase throughput
		grpcConn.Close()
		cc.connWaitSema.Release(1)
		return cc.redialLocked(ctx, conn)
	}

	// Record new connection metric
	cc.metrics.AddConnNew(ctx)

	conn := &cachedConn{
		consensusClient: consensuspb.NewMultiPoolerConsensusClient(grpcConn),
		managerClient:   multipoolermanagerpb.NewMultiPoolerManagerClient(grpcConn),
		cc:              grpcConn,
		lastAccessTime:  time.Now(),
		refs:            1,
		addr:            addr,
	}

	// NOTE: we deliberately do not set cc.evictSorted=false here. Since
	// cachedConns are evicted from the front of the queue, and we are appending
	// to the end, if there is already a second evictable connection, it will be
	// at the front of the queue, so we can speed up the edge case where we need
	// to evict multiple connections in a row.
	cc.evict = append(cc.evict, conn)
	cc.conns[addr] = conn

	return cc.connWithCloser(conn)
}

// redialLocked takes an already-dialed connection in the cache does all the
// work of lending that connection out to one more caller. It returns the
// two-tuple of connection and closer that getOrDial returns.
func (cc *connCache) redialLocked(ctx context.Context, conn *cachedConn) (*cachedConn, closeFunc, error) {
	// Record connection reuse metric
	cc.metrics.AddConnReuse(ctx)

	conn.lastAccessTime = time.Now()
	conn.refs++
	cc.evictSorted = false
	return cc.connWithCloser(conn)
}

// connWithCloser returns the two-tuple expected by getOrDial, where
// the closer handles the correct state management for updating the conns place
// in the eviction queue.
func (cc *connCache) connWithCloser(conn *cachedConn) (*cachedConn, closeFunc, error) {
	return conn, closeFunc(func() error {
		cc.m.Lock()
		defer cc.m.Unlock()
		if conn.refs > 0 {
			conn.refs--
			cc.evictSorted = false
		}
		return nil
	}), nil
}

// close closes a specific connection and removes it from the cache.
func (cc *connCache) close(addr string) {
	cc.m.Lock()
	defer cc.m.Unlock()

	conn, ok := cc.conns[addr]
	if !ok {
		return
	}

	// Close the connection
	if conn.cc != nil {
		conn.cc.Close()
	}

	// Remove from cache
	delete(cc.conns, addr)

	// Remove from eviction queue
	for i, ec := range cc.evict {
		if ec == conn {
			cc.evict = append(cc.evict[:i], cc.evict[i+1:]...)
			break
		}
	}

	cc.connWaitSema.Release(1)
}

// closeAll closes all currently cached connections, ***regardless of whether
// those connections are in use***. Calling closeAll therefore will fail any RPCs
// using currently lent-out connections, and, furthermore, will invalidate the
// io.Closer that was returned for that connection from cc.getOrDial(). When
// calling those io.Closers, they will still lock the cache's mutex, and then
// perform needless operations that will slow down dial throughput, but not
// actually impact the correctness of the internal state of the cache.
//
// As a result, while it is safe to reuse a connCache after calling closeAll,
// it will be less performant than getting a new one by calling
// newConnCache or newConnCacheWithCapacity directly.
func (cc *connCache) closeAll() {
	cc.m.Lock()
	defer cc.m.Unlock()

	for _, conn := range cc.evict {
		if conn.cc != nil {
			conn.cc.Close()
		}
		delete(cc.conns, conn.addr)
		cc.connWaitSema.Release(1)
	}
	cc.evict = make([]*cachedConn, 0, cc.capacity)
}
