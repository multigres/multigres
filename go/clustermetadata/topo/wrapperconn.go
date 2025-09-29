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

package topo

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/tools/timertools"
)

// WrapperConn wraps a Conn with automatic reconnection and error handling.
// It provides transparent retry logic for retriable connection errors.
type WrapperConn struct {
	newFunc func() (Conn, error)

	mu       sync.Mutex
	wrapped  Conn
	retrying bool
	closed   bool
}

// NewWrapperConn creates a new connection wrapper that uses newFunc to establish connections.
// If the initial connection attempt fails, it starts automatic retry logic in a goroutine.
func NewWrapperConn(newFunc func() (Conn, error)) *WrapperConn {
	c := &WrapperConn{newFunc: newFunc}

	conn, err := newFunc()
	if err != nil {
		c.handleConnectionError(nil, err)
	} else {
		c.wrapped = conn
	}

	return c
}

// handleConnectionError examines the error and triggers reconnection for retriable errors.
func (c *WrapperConn) handleConnectionError(conn Conn, err error) {
	// If there is no connection, we want to retry irrespective of the error.
	if conn == nil {
		slog.Error("Connection error, will keep retrying", "err", err)
		go c.retryConnection()
		return
	}
	if err == nil {
		return
	}
	if strings.Contains(err.Error(), "context deadline exceeded") {
		slog.Error("Connection error, will keep retrying", "err", err)
		go c.retryConnection()
		return
	}
	if strings.Contains(err.Error(), "context canceled") {
		slog.Error("Connection error, will keep retrying", "err", err)
		go c.retryConnection()
		return
	}
	switch mterrors.Code(err) {
	case mtrpc.Code_UNAVAILABLE, mtrpc.Code_FAILED_PRECONDITION, mtrpc.Code_CLUSTER_EVENT:
		slog.Error("Connection error, will keep retrying", "err", err)
		go c.retryConnection()
	}
}

// retryConnection goes into a retry loop until a connection is established.
// It ensures that it goes into the loop only if it's already not retrying.
// retryConnection terminates if Conn is closed.
func (c *WrapperConn) retryConnection() {
	// Use defer to protect us from unexpected panics
	mustReturn := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.closed {
			return true
		}
		if c.retrying {
			return true
		}
		c.retrying = true
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

	// There is a race condition:
	// - Connection gets successfully established.
	// - Lock is released, but the defer below is not executed yet.
	// - Someone uses the new connection.
	// - The connection fails, and causes a retry.
	// - This will cause the second retry to return early, because the flag is not reset yet.
	// This is a near impossible race condition, and it's also harmless.
	// Eventually, someone will retry and this will trigger the retry logic.
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.retrying = false
	}()

	ticker := timertools.NewBackoffTicker(10*time.Millisecond, 30*time.Second)
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

func (c *WrapperConn) getConnection() (Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.wrapped == nil {
		return nil, mterrors.Errorf(mtrpc.Code_UNAVAILABLE, "no connection available")
	}
	return c.wrapped, nil
}

// ListDir lists the contents of a directory in the topology server.
func (c *WrapperConn) ListDir(ctx context.Context, dirPath string, full bool) ([]DirEntry, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.ListDir(ctx, dirPath, full)
	c.handleConnectionError(conn, err)
	return result, err
}

// Create creates a new file in the topology server with the given contents.
func (c *WrapperConn) Create(ctx context.Context, filePath string, contents []byte) (Version, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.Create(ctx, filePath, contents)
	c.handleConnectionError(conn, err)
	return result, err
}

// Update updates an existing file in the topology server with new contents and version.
func (c *WrapperConn) Update(ctx context.Context, filePath string, contents []byte, version Version) (Version, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.Update(ctx, filePath, contents, version)
	c.handleConnectionError(conn, err)
	return result, err
}

// Get retrieves the contents and version of a file from the topology server.
func (c *WrapperConn) Get(ctx context.Context, filePath string) ([]byte, Version, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, nil, err
	}
	data, version, err := conn.Get(ctx, filePath)
	c.handleConnectionError(conn, err)
	return data, version, err
}

// GetVersion retrieves the contents of a specific version of a file from the topology server.
func (c *WrapperConn) GetVersion(ctx context.Context, filePath string, version int64) ([]byte, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.GetVersion(ctx, filePath, version)
	c.handleConnectionError(conn, err)
	return result, err
}

// List returns key-value information for all files matching the given path prefix.
func (c *WrapperConn) List(ctx context.Context, filePathPrefix string) ([]KVInfo, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.List(ctx, filePathPrefix)
	c.handleConnectionError(conn, err)
	return result, err
}

// Delete removes a file from the topology server with the specified version.
func (c *WrapperConn) Delete(ctx context.Context, filePath string, version Version) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	err = conn.Delete(ctx, filePath, version)
	c.handleConnectionError(conn, err)
	return err
}

// Lock acquires a distributed lock on the specified directory path.
func (c *WrapperConn) Lock(ctx context.Context, dirPath, contents string) (LockDescriptor, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.Lock(ctx, dirPath, contents)
	c.handleConnectionError(conn, err)
	return result, err
}

// LockWithTTL acquires a distributed lock with a time-to-live on the specified directory path.
func (c *WrapperConn) LockWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (LockDescriptor, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.LockWithTTL(ctx, dirPath, contents, ttl)
	c.handleConnectionError(conn, err)
	return result, err
}

// LockName acquires a named distributed lock on the specified directory path.
func (c *WrapperConn) LockName(ctx context.Context, dirPath, contents string) (LockDescriptor, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.LockName(ctx, dirPath, contents)
	c.handleConnectionError(conn, err)
	return result, err
}

// TryLock attempts to acquire a distributed lock without blocking.
func (c *WrapperConn) TryLock(ctx context.Context, dirPath, contents string) (LockDescriptor, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	result, err := conn.TryLock(ctx, dirPath, contents)
	c.handleConnectionError(conn, err)
	return result, err
}

// Watch monitors a file for changes and returns the current state and a channel for updates.
func (c *WrapperConn) Watch(ctx context.Context, filePath string) (current *WatchData, changes <-chan *WatchData, err error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, nil, err
	}
	current, changes, err = conn.Watch(ctx, filePath)
	c.handleConnectionError(conn, err)
	return current, changes, err
}

// WatchRecursive monitors a directory recursively for changes and returns current state and update channel.
func (c *WrapperConn) WatchRecursive(ctx context.Context, path string) ([]*WatchDataRecursive, <-chan *WatchDataRecursive, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, nil, err
	}
	current, changes, err := conn.WatchRecursive(ctx, path)
	c.handleConnectionError(conn, err)
	return current, changes, err
}

// Close closes the connection wrapper and terminates any ongoing retry operations.
func (c *WrapperConn) Close() error {
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
