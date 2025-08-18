package memorytopo

import (
	"context"

	"github.com/multigres/multigres/pb/mtrpc"

	"github.com/multigres/multigres/pkg/clustermetadata/topo"
	"github.com/multigres/multigres/pkg/mterrors"
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
