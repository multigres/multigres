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

package memorytopo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
)

// convertError converts a context error into a topo error.
func convertError(err error, nodePath string) error {
	switch {
	case errors.Is(err, context.Canceled):
		return topoclient.NewError(topoclient.Interrupted, nodePath)
	case errors.Is(err, context.DeadlineExceeded):
		return topoclient.NewError(topoclient.Timeout, nodePath)
	}
	return err
}

// memoryTopoLockDescriptor implements topoclient.LockDescriptor.
type memoryTopoLockDescriptor struct {
	c       *conn
	dirPath string
}

// TryLock is part of the topoclient.Conn interface.
func (c *conn) TryLock(ctx context.Context, dirPath, contents string) (topoclient.LockDescriptor, error) {
	c.factory.mu.Lock()
	err := c.factory.getOperationError(TryLock, dirPath)
	c.factory.mu.Unlock()
	if err != nil {
		return nil, err
	}

	if err := c.checkLockExistence(ctx, dirPath, false); err != nil {
		return nil, err
	}

	return c.Lock(ctx, dirPath, contents)
}

// checkLockExistence is a private helper method that checks if a lock already exists for the given path.
// It returns nil if no lock exists, or &topoclient.TopoError{Code: topoclient.NodeExists} if a lock already exists.
func (c *conn) checkLockExistence(ctx context.Context, dirPath string, named bool) error {
	if err := c.dial(ctx); err != nil {
		return err
	}

	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	var n *node
	if named {
		n = c.factory.getOrCreatePath(c.cell, dirPath)
	} else {
		n = c.factory.nodeByPath(c.cell, dirPath)
	}
	if n == nil {
		return topoclient.NewError(topoclient.NoNode, dirPath)
	}

	// Check if a lock exists
	if n.lock != nil {
		return &topoclient.TopoError{Code: topoclient.NodeExists}
	}

	// No lock exists
	return nil
}

// Lock is part of the topoclient.Conn interface.
func (c *conn) Lock(ctx context.Context, dirPath, contents string) (topoclient.LockDescriptor, error) {
	c.factory.mu.Lock()
	err := c.factory.getOperationError(Lock, dirPath)
	c.factory.mu.Unlock()
	if err != nil {
		return nil, err
	}

	return c.lock(ctx, dirPath, contents, false)
}

// LockWithTTL is part of the topoclient.Conn interface.
func (c *conn) LockWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (topoclient.LockDescriptor, error) {
	c.factory.mu.Lock()
	err := c.factory.getOperationError(Lock, dirPath)
	c.factory.mu.Unlock()
	if err != nil {
		return nil, err
	}

	return c.lockWithTTL(ctx, dirPath, contents, false, ttl)
}

// LockName is part of the topoclient.Conn interface.
func (c *conn) LockName(ctx context.Context, dirPath, contents string) (topoclient.LockDescriptor, error) {
	return c.lock(ctx, dirPath, contents, true)
}

// LockNameWithTTL is part of the topoclient.Conn interface.
func (c *conn) LockNameWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (topoclient.LockDescriptor, error) {
	return c.lockWithTTL(ctx, dirPath, contents, true, ttl)
}

// TryLockName is part of the topoclient.Conn interface.
func (c *conn) TryLockName(ctx context.Context, dirPath, contents string) (topoclient.LockDescriptor, error) {
	c.factory.mu.Lock()
	err := c.factory.getOperationError(TryLock, dirPath)
	c.factory.mu.Unlock()
	if err != nil {
		return nil, err
	}

	// Check if lock exists, using named=true so the path is created if needed
	if err := c.checkLockExistence(ctx, dirPath, true); err != nil {
		return nil, err
	}

	return c.lock(ctx, dirPath, contents, true)
}

// lock acquires a lock without TTL.
func (c *conn) lock(ctx context.Context, dirPath, contents string, named bool) (topoclient.LockDescriptor, error) {
	return c.lockWithTTL(ctx, dirPath, contents, named, 0)
}

// lockWithTTL acquires a lock with an optional TTL. If ttl is 0, the lock does not expire.
func (c *conn) lockWithTTL(ctx context.Context, dirPath, contents string, named bool, ttl time.Duration) (topoclient.LockDescriptor, error) {
	for {
		if err := c.dial(ctx); err != nil {
			return nil, err
		}

		c.factory.mu.Lock()

		if c.factory.err != nil {
			c.factory.mu.Unlock()
			return nil, c.factory.err
		}

		var n *node
		if named {
			n = c.factory.getOrCreatePath(c.cell, dirPath)
		} else {
			n = c.factory.nodeByPath(c.cell, dirPath)
		}
		if n == nil {
			c.factory.mu.Unlock()
			return nil, topoclient.NewError(topoclient.NoNode, dirPath)
		}

		if l := n.lock; l != nil {
			// Someone else has the lock. Just wait for it.
			c.factory.mu.Unlock()
			select {
			case <-l:
				// Node was unlocked, try again to grab it.
				continue
			case <-ctx.Done():
				// Done waiting
				return nil, convertError(ctx.Err(), dirPath)
			}
		}

		// No one has the lock, grab it.
		n.lock = make(chan struct{})
		n.lockContents = contents

		// Set up TTL expiration if specified
		if ttl > 0 {
			n.lockTTLTimer = time.AfterFunc(ttl, func() {
				c.factory.mu.Lock()
				defer c.factory.mu.Unlock()
				// Only expire if the lock is still held (not already unlocked)
				if n.lock != nil {
					close(n.lock)
					n.lock = nil
					n.lockContents = ""
					n.lockTTLTimer = nil
				}
			})
		}

		for _, w := range n.watches {
			if w.lock == nil {
				continue
			}
			w.lock <- contents
		}
		c.factory.mu.Unlock()
		return &memoryTopoLockDescriptor{
			c:       c,
			dirPath: dirPath,
		}, nil
	}
}

// Check is part of the topoclient.LockDescriptor interface.
// We can never lose a lock in this implementation.
func (ld *memoryTopoLockDescriptor) Check(ctx context.Context) error {
	return nil
}

// Unlock is part of the topoclient.LockDescriptor interface.
func (ld *memoryTopoLockDescriptor) Unlock(ctx context.Context) error {
	return ld.c.unlock(ctx, ld.dirPath)
}

func (c *conn) unlock(ctx context.Context, dirPath string) error {
	if c.closed.Load() {
		return ErrConnectionClosed
	}

	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	n := c.factory.nodeByPath(c.cell, dirPath)
	if n == nil {
		return topoclient.NewError(topoclient.NoNode, dirPath)
	}
	if n.lock == nil {
		return fmt.Errorf("node %v is not locked", dirPath)
	}
	// Stop the TTL timer if one exists
	if n.lockTTLTimer != nil {
		n.lockTTLTimer.Stop()
		n.lockTTLTimer = nil
	}
	close(n.lock)
	n.lock = nil
	n.lockContents = ""
	return nil
}
