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

package topo

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"

	"github.com/multigres/multigres/go/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"google.golang.org/protobuf/proto"
)

// This file provides the utility methods to save / retrieve Cell
// in the topology server.
//
// Cell records are not meant to be changed while the system is
// running.  In a running system, a Cell can be added, and
// topology server implementations should be able to read them to
// access the cells upon demand. Topology server implementations can
// also read the available Cell at startup to build a list of
// available cells, if necessary. A Cell can only be removed if no
// Shard record references the corresponding cell in its Cells list.

func pathForCell(cell string) string {
	return path.Join(CellsPath, cell, CellFile)
}

// GetCellNames returns the names of the existing cells. They are
// sorted by name.
func (ts *store) GetCellNames(ctx context.Context) ([]string, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	entries, err := ts.globalTopo.ListDir(ctx, CellsPath, false /*full*/)
	switch {
	case errors.Is(err, &TopoError{Code: NoNode}):
		return nil, nil
	case err == nil:
		return DirEntriesToStringArray(entries), nil
	default:
		return nil, err
	}
}

// GetCell reads a Cell from the global Conn.
func (ts *store) GetCell(ctx context.Context, cell string) (*clustermetadatapb.Cell, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	conn := ts.globalTopo
	// Read the file.
	filePath := pathForCell(cell)
	contents, _, err := conn.Get(ctx, filePath)
	if err != nil {
		return nil, err
	}

	// Unpack the contents.
	ci := &clustermetadatapb.Cell{}
	if err := proto.Unmarshal(contents, ci); err != nil {
		return nil, err
	}
	return ci, nil
}

// CreateCell creates a new Cell with the provided content.
func (ts *store) CreateCell(ctx context.Context, cell string, ci *clustermetadatapb.Cell) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Pack the content.
	contents, err := proto.Marshal(ci)
	if err != nil {
		return err
	}

	// Save it.
	filePath := pathForCell(cell)
	_, err = ts.globalTopo.Create(ctx, filePath, contents)
	return err
}

// UpdateCellFields is a high level helper method to read a Cell
// object, update its fields, and then write it back.  If the write fails due to
// a version mismatch, it will re-read the record and retry the update.
// If the update method returns ErrNoUpdateNeeded, nothing is written,
// and nil is returned.
func (ts *store) UpdateCellFields(ctx context.Context, cell string, update func(*clustermetadatapb.Cell) error) error {
	filePath := pathForCell(cell)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		ci := &clustermetadatapb.Cell{}

		// Read the file, unpack the contents.
		contents, version, err := ts.globalTopo.Get(ctx, filePath)
		switch {
		case err == nil:
			if err := proto.Unmarshal(contents, ci); err != nil {
				return err
			}
		case errors.Is(err, &TopoError{Code: NoNode}):
			// Nothing to do.
		default:
			return err
		}

		// Call update method.
		if err = update(ci); err != nil {
			if errors.Is(err, &TopoError{Code: NoUpdateNeeded}) {
				return nil
			}
			return err
		}

		// Pack and save.
		contents, err = proto.Marshal(ci)
		if err != nil {
			return err
		}
		if _, err = ts.globalTopo.Update(ctx, filePath, contents, version); !errors.Is(err, &TopoError{Code: BadVersion}) {
			// This includes the 'err=nil' case.
			return err
		}
	}
}

// DeleteCell deletes the specified Cell.
// We first try to make sure no Shard record points to the cell,
// but we'll continue regardless if 'force' is true.
func (ts *store) DeleteCell(ctx context.Context, cell string, force bool) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Check if this cell is being used in any database before deleting it.
	if !force {
		databaseNames, err := ts.GetDatabaseNames(ctx)
		if err != nil {
			return err
		}

		for _, dbName := range databaseNames {
			db, err := ts.GetDatabase(ctx, dbName)
			if err != nil {
				return mterrors.Wrap(err, fmt.Sprintf("failed to get database %s", dbName))
			}

			// Check if this database references the cell to be deleted
			if slices.Contains(db.Cells, cell) {
				return NewError(NodeNotEmpty, fmt.Sprintf("cell %s is referenced by database %s. This could create serving issues in the cluster. Either remove the cell from the database or use force=true to delete the cell anyway.", cell, dbName))
			}
		}
	}

	filePath := pathForCell(cell)
	return ts.globalTopo.Delete(ctx, filePath, nil)
}
