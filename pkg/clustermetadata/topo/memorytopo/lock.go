package memorytopo

import (
	"context"
	"fmt"
	"time"

	"github.com/multigres/multigres/pkg/clustermetadata/topo"
)

// convertError converts a context error into a topo error.
func convertError(err error, nodePath string) error {
	switch err {
	case context.Canceled:
		return topo.NewError(topo.Interrupted, nodePath)
	case context.DeadlineExceeded:
		return topo.NewError(topo.Timeout, nodePath)
	}
	return err
}

// memoryTopoLockDescriptor implements topo.LockDescriptor.
type memoryTopoLockDescriptor struct {
	c       *conn
	dirPath string
}

// TryLock is part of the topo.Conn interface. Its implementation is same as Lock
func (c *conn) TryLock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// c.factory.callstats.Add([]string{"TryLock"}, 1)

	c.factory.mu.Lock()
	err := c.factory.getOperationError(TryLock, dirPath)
	c.factory.mu.Unlock()
	if err != nil {
		return nil, err
	}

	return c.Lock(ctx, dirPath, contents)
}

// Lock is part of the topo.Conn interface.
func (c *conn) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// c.factory.callstats.Add([]string{"Lock"}, 1)

	c.factory.mu.Lock()
	err := c.factory.getOperationError(Lock, dirPath)
	c.factory.mu.Unlock()
	if err != nil {
		return nil, err
	}

	return c.lock(ctx, dirPath, contents, false)
}

// LockWithTTL is part of the topo.Conn interface. It behaves the same as Lock
// as TTLs are not supported in memorytopo.
func (c *conn) LockWithTTL(ctx context.Context, dirPath, contents string, _ time.Duration) (topo.LockDescriptor, error) {
	// c.factory.callstats.Add([]string{"LockWithTTL"}, 1)

	c.factory.mu.Lock()
	err := c.factory.getOperationError(Lock, dirPath)
	c.factory.mu.Unlock()
	if err != nil {
		return nil, err
	}

	return c.lock(ctx, dirPath, contents, false)
}

// LockName is part of the topo.Conn interface.
func (c *conn) LockName(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// c.factory.callstats.Add([]string{"LockName"}, 1)
	return c.lock(ctx, dirPath, contents, true)
}

// Lock is part of the topo.Conn interface.
func (c *conn) lock(ctx context.Context, dirPath, contents string, named bool) (topo.LockDescriptor, error) {
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
			return nil, topo.NewError(topo.NoNode, dirPath)
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

// Check is part of the topo.LockDescriptor interface.
// We can never lose a lock in this implementation.
func (ld *memoryTopoLockDescriptor) Check(ctx context.Context) error {
	return nil
}

// Unlock is part of the topo.LockDescriptor interface.
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
		return topo.NewError(topo.NoNode, dirPath)
	}
	if n.lock == nil {
		return fmt.Errorf("node %v is not locked", dirPath)
	}
	close(n.lock)
	n.lock = nil
	n.lockContents = ""
	return nil
}
