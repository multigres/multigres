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
	"path"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"google.golang.org/protobuf/proto"
)

// This file provides the utility methods to save / retrieve CellLocation
// in the topology server.
//
// CellLocation records are not meant to be changed while the system is
// running.  In a running system, a CellLocation can be added, and
// topology server implementations should be able to read them to
// access the cells upon demand. Topology server implementations can
// also read the available CellLocation at startup to build a list of
// available cells, if necessary. A CellLocation can only be removed if no
// Shard record references the corresponding cell in its Cells list.

func pathForCellLocation(cell string) string {
	return path.Join(CellsPath, cell, CellLocationFile)
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

// GetCellLocation reads a CellLocation from the global Conn.
func (ts *store) GetCellLocation(ctx context.Context, cell string) (*clustermetadatapb.CellLocation, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	conn := ts.globalTopo
	// Read the file.
	filePath := pathForCellLocation(cell)
	contents, _, err := conn.Get(ctx, filePath)
	if err != nil {
		return nil, err
	}

	// Unpack the contents.
	ci := &clustermetadatapb.CellLocation{}
	if err := proto.Unmarshal(contents, ci); err != nil {
		return nil, err
	}
	return ci, nil
}

// CreateCellLocation creates a new CellLocation with the provided content.
func (ts *store) CreateCellLocation(ctx context.Context, cell string, ci *clustermetadatapb.CellLocation) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Pack the content.
	contents, err := proto.Marshal(ci)
	if err != nil {
		return err
	}

	// Save it.
	filePath := pathForCellLocation(cell)
	_, err = ts.globalTopo.Create(ctx, filePath, contents)
	return err
}

// UpdateCellLocationFields is a high level helper method to read a CellLocation
// object, update its fields, and then write it back.  If the write fails due to
// a version mismatch, it will re-read the record and retry the update.
// If the update method returns ErrNoUpdateNeeded, nothing is written,
// and nil is returned.
func (ts *store) UpdateCellLocationFields(ctx context.Context, cell string, update func(*clustermetadatapb.CellLocation) error) error {
	filePath := pathForCellLocation(cell)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		ci := &clustermetadatapb.CellLocation{}

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

// DeleteCellLocation deletes the specified CellLocation.
// We first try to make sure no Shard record points to the cell,
// but we'll continue regardless if 'force' is true.
func (ts *store) DeleteCellLocation(ctx context.Context, cell string, force bool) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// TODO: Check if this cell is being used in any database before deleting it.

	filePath := pathForCellLocation(cell)
	return ts.globalTopo.Delete(ctx, filePath, nil)
}
