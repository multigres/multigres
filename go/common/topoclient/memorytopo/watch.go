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
	"fmt"

	"github.com/multigres/multigres/go/common/topoclient"
)

// Watch is part of the topoclient.Conn interface.
func (c *conn) Watch(ctx context.Context, filePath string) (*topoclient.WatchData, <-chan *topoclient.WatchData, error) {
	// c.factory.callstats.Add([]string{"Watch"}, 1)

	if c.closed.Load() {
		return nil, nil, ErrConnectionClosed
	}

	c.factory.Lock()
	defer c.factory.Unlock()

	if c.factory.err != nil {
		return nil, nil, c.factory.err
	}
	if err := c.factory.getOperationError(Watch, filePath); err != nil {
		return nil, nil, err
	}

	n := c.factory.nodeByPath(c.cell, filePath)
	if n == nil {
		return nil, nil, topoclient.NewError(topoclient.NoNode, filePath)
	}
	if n.contents == nil {
		// it's a directory
		return nil, nil, fmt.Errorf("cannot watch directory %v in cell %v", filePath, c.cell)
	}
	current := &topoclient.WatchData{
		Contents: n.contents,
		Version:  NodeVersion(n.version),
	}

	notifications := make(chan *topoclient.WatchData, 100)

	// Create a cancellable context so we can forcibly close the watch
	watchCtx, watchCancel := context.WithCancel(ctx)
	watchIndex := n.addWatch(watch{
		contents: notifications,
		cancel:   watchCancel,
	})

	go func() {
		<-watchCtx.Done()
		// This function can be called at any point, so we first need
		// to make sure the watch is still valid.
		c.factory.Lock()
		defer c.factory.Unlock()

		n := c.factory.nodeByPath(c.cell, filePath)
		if n == nil {
			return
		}

		if w, ok := n.watches[watchIndex]; ok {
			delete(n.watches, watchIndex)
			w.contents <- &topoclient.WatchData{Err: topoclient.NewError(topoclient.Interrupted, "watch")}
			close(w.contents)
		}
	}()
	return current, notifications, nil
}

// WatchRecursive is part of the topoclient.Conn interface.
func (c *conn) WatchRecursive(ctx context.Context, dirpath string) ([]*topoclient.WatchDataRecursive, <-chan *topoclient.WatchDataRecursive, error) {
	// c.factory.callstats.Add([]string{"WatchRecursive"}, 1)

	if c.closed.Load() {
		return nil, nil, ErrConnectionClosed
	}

	c.factory.Lock()
	defer c.factory.Unlock()

	if c.factory.err != nil {
		return nil, nil, c.factory.err
	}
	if err := c.factory.getOperationError(WatchRecursive, dirpath); err != nil {
		return nil, nil, err
	}

	n := c.factory.getOrCreatePath(c.cell, dirpath)
	if n == nil {
		return nil, nil, topoclient.NewError(topoclient.NoNode, dirpath)
	}

	var initialwd []*topoclient.WatchDataRecursive
	n.recurseContents(func(n *node) {
		initialwd = append(initialwd, &topoclient.WatchDataRecursive{
			Path: n.fullPath(),
			WatchData: topoclient.WatchData{
				Contents: n.contents,
				Version:  NodeVersion(n.version),
			},
		})
	})

	notifications := make(chan *topoclient.WatchDataRecursive, 100)

	// Create a cancellable context so we can forcibly close the watch
	watchCtx, watchCancel := context.WithCancel(ctx)
	watchIndex := n.addWatch(watch{
		recursive: notifications,
		cancel:    watchCancel,
	})

	go func() {
		defer close(notifications)

		<-watchCtx.Done()

		c.factory.Lock()
		f := c.factory
		defer f.Unlock()

		n := f.nodeByPath(c.cell, dirpath)
		if n != nil {
			delete(n.watches, watchIndex)
		}

		notifications <- &topoclient.WatchDataRecursive{WatchData: topoclient.WatchData{Err: topoclient.NewError(topoclient.Interrupted, "watch")}}
	}()

	return initialwd, notifications, nil
}
