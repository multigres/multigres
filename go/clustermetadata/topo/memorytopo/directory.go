// Copyright 2025 The Multigres Authors.
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

	"github.com/multigres/multigres/go/pb/mtrpc"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/mterrors"
)

// ListDir is part of the topo.Conn interface.
func (c *conn) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	// c.factory.callstats.Add([]string{"ListDir"}, 1)

	if err := c.dial(ctx); err != nil {
		return nil, err
	}

	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	if c.factory.err != nil {
		return nil, c.factory.err
	}
	if err := c.factory.getOperationError(ListDir, dirPath); err != nil {
		return nil, err
	}

	isRoot := dirPath == "" || dirPath == "/"

	// Get the node to list.
	n := c.factory.nodeByPath(c.cell, dirPath)
	if n == nil {
		return nil, topo.NewError(topo.NoNode, dirPath)
	}

	// Check it's a directory.
	if !n.isDirectory() {
		return nil, mterrors.Errorf(mtrpc.Code_INVALID_ARGUMENT, "node %v in cell %v is not a directory", dirPath, c.cell)
	}

	result := make([]topo.DirEntry, 0, len(n.children))
	for name, child := range n.children {
		e := topo.DirEntry{
			Name: name,
		}
		if full {
			e.Type = topo.TypeFile
			if child.isDirectory() {
				e.Type = topo.TypeDirectory
			}
			if isRoot && name == electionsPath {
				e.Ephemeral = true
			}
		}
		result = append(result, e)
	}
	topo.DirEntriesSortByName(result)
	return result, nil
}
