// Copyright 2025 Supabase, Inc.
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

// Package topowrapper provides a connection wrapper for topo.Conn that handles
// automatic reconnection and error recovery.
package topowrapper

import (
	"context"
	"sync"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/tools/timertools"
)

// Conn wraps a topo.Conn with automatic reconnection and error handling.
// It provides transparent retry logic for retriable connection errors.
type Conn struct {
	newFunc func() (topo.Conn, error)
	// mu protects wrapped and closed.
	mu      sync.Mutex
	wrapped topo.Conn
	closed  bool
}

// NewConn creates a new connection wrapper that uses newFunc to establish connections.
// If the initial connection attempt fails, it starts automatic retry logic in a goroutine.
func NewConn(newFunc func() (topo.Conn, error)) *Conn {
	c := &Conn{newFunc: newFunc}

	conn, err := newFunc()
	if err != nil {
		go c.retryConnection(nil)
	} else {
		c.wrapped = conn
	}

	return c
}

// retryConnection goes into a retry loop until a connection is established.
// It ensures that it goes into the loop only if the input (failed) conn
// matches the wrapped conn. If they don't match, it means the connection was
// already replaced and no action is needed.
// retryConnection terminates if Conn is closed.
func (c *Conn) retryConnection(conn topo.Conn) {
	// Use defer to protect us from unexpected panics
	mustReturn := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.closed {
			return true
		}
		// If two functions fail simultaneously, they will both trigger a retryConnection.
		// The first invocation may successfully reopen a new connection. If so, we have to
		// prevent the second retry from replacing that good connection.
		// We check this by comparing the wrapped connection with the input connection.
		if c.wrapped != conn {
			return true
		}
		if c.wrapped != nil {
			// Close the connection in a goroutine to prevent blocking.
			go c.wrapped.Close()
			c.wrapped = nil
		}
		return false
	}()
	if mustReturn {
		return
	}

	ticker := timertools.NewBackoffTicker(1*time.Millisecond, 30*time.Second)
	defer ticker.Stop()

	for range ticker.C {
		conn, err := c.newFunc()
		mustContinue := func() bool {
			// We have to do this entire operation within a lock:
			// Once we check the value of c.closed, it should not be allowed
			// to change until we also set the value of c.wrapped. Otherwise,
			// it will conflict with c.Close.
			c.mu.Lock()
			defer c.mu.Unlock()

			if c.closed {
				// If the wrapper was closed, we have to close this extra
				// connection and stop retrying.
				if conn != nil {
					_ = conn.Close()
				}
				return false
			}
			if err != nil {
				return true
			}
			c.wrapped = conn
			return false
		}()
		if !mustContinue {
			return
		}
	}
}

func (c *Conn) getConnection() (topo.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.wrapped == nil {
		return nil, mterrors.Errorf(mtrpc.Code_UNAVAILABLE, "no connection available")
	}
	return c.wrapped, nil
}

// handleConnectionError examines the error and triggers reconnection for retriable errors.
// Only specific error codes (UNAVAILABLE, FAILED_PRECONDITION, CLUSTER_EVENT) trigger retries.
func (c *Conn) handleConnectionError(conn topo.Conn, err error) {
	if err == nil {
		return
	}
	switch mterrors.Code(err) {
	case mtrpc.Code_UNAVAILABLE, mtrpc.Code_FAILED_PRECONDITION, mtrpc.Code_CLUSTER_EVENT:
		go c.retryConnection(conn)
	}
}

// ListDir lists the contents of a directory in the topology server.
func (c *Conn) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.ListDir(ctx, dirPath, full)
	c.handleConnectionError(conn, err)
	return result, err
}

// Create creates a new file in the topology server with the given contents.
func (c *Conn) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.Create(ctx, filePath, contents)
	c.handleConnectionError(conn, err)
	return result, err
}

// Update updates an existing file in the topology server with new contents and version.
func (c *Conn) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.Update(ctx, filePath, contents, version)
	c.handleConnectionError(conn, err)
	return result, err
}

// Get retrieves the contents and version of a file from the topology server.
func (c *Conn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, nil, err
	}
	data, version, err := conn.Get(ctx, filePath)
	c.handleConnectionError(conn, err)
	return data, version, err
}

// GetVersion retrieves the contents of a specific version of a file from the topology server.
func (c *Conn) GetVersion(ctx context.Context, filePath string, version int64) ([]byte, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.GetVersion(ctx, filePath, version)
	c.handleConnectionError(conn, err)
	return result, err
}

// List returns key-value information for all files matching the given path prefix.
func (c *Conn) List(ctx context.Context, filePathPrefix string) ([]topo.KVInfo, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.List(ctx, filePathPrefix)
	c.handleConnectionError(conn, err)
	return result, err
}

// Delete removes a file from the topology server with the specified version.
func (c *Conn) Delete(ctx context.Context, filePath string, version topo.Version) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	err = conn.Delete(ctx, filePath, version)
	c.handleConnectionError(conn, err)
	return err
}

// Lock acquires a distributed lock on the specified directory path.
func (c *Conn) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.Lock(ctx, dirPath, contents)
	c.handleConnectionError(conn, err)
	return result, err
}

// LockWithTTL acquires a distributed lock with a time-to-live on the specified directory path.
func (c *Conn) LockWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (topo.LockDescriptor, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.LockWithTTL(ctx, dirPath, contents, ttl)
	c.handleConnectionError(conn, err)
	return result, err
}

// LockName acquires a named distributed lock on the specified directory path.
func (c *Conn) LockName(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.LockName(ctx, dirPath, contents)
	c.handleConnectionError(conn, err)
	return result, err
}

// TryLock attempts to acquire a distributed lock without blocking.
func (c *Conn) TryLock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.TryLock(ctx, dirPath, contents)
	c.handleConnectionError(conn, err)
	return result, err
}

// Watch monitors a file for changes and returns the current state and a channel for updates.
func (c *Conn) Watch(ctx context.Context, filePath string) (current *topo.WatchData, changes <-chan *topo.WatchData, err error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, nil, err
	}
	current, changes, err = conn.Watch(ctx, filePath)
	c.handleConnectionError(conn, err)
	return current, changes, err
}

// WatchRecursive monitors a directory recursively for changes and returns current state and update channel.
func (c *Conn) WatchRecursive(ctx context.Context, path string) ([]*topo.WatchDataRecursive, <-chan *topo.WatchDataRecursive, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, nil, err
	}
	current, changes, err := conn.WatchRecursive(ctx, path)
	c.handleConnectionError(conn, err)
	return current, changes, err
}

// Close closes the connection wrapper and terminates any ongoing retry operations.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	if c.wrapped != nil {
		err := c.wrapped.Close()
		c.wrapped = nil
		return err
	}
	return nil
}
