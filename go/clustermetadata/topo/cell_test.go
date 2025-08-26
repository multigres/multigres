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

package topo_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
)

func TestCellCRUDOperations(t *testing.T) {
	ctx := context.Background()
	cell := "test-cell-1"
	cell2 := "test-cell-2"

	tests := []struct {
		name string
		test func(t *testing.T, ts topo.Store)
	}{
		{
			name: "Create and Get Cell",
			test: func(t *testing.T, ts topo.Store) {
				cl := &clustermetadatapb.Cell{
					ServerAddresses: []string{"server1:2181", "server2:2181"},
					Root:            "/topo",
				}
				err := ts.CreateCell(ctx, cell, cl)
				require.NoError(t, err)

				retrieved, err := ts.GetCell(ctx, cell)
				require.NoError(t, err)
				require.Equal(t, cl.ServerAddresses, retrieved.ServerAddresses)
				require.Equal(t, cl.Root, retrieved.Root)
			},
		},
		{
			name: "Get nonexistent Cell",
			test: func(t *testing.T, ts topo.Store) {
				_, err := ts.GetCell(ctx, "nonexistent")
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}))
			},
		},
		{
			name: "Update Cell Fields",
			test: func(t *testing.T, ts topo.Store) {
				cl := &clustermetadatapb.Cell{
					ServerAddresses: []string{"server1:2181"},
					Root:            "/topo",
				}
				err := ts.CreateCell(ctx, cell, cl)
				require.NoError(t, err)

				// Update the cell location
				err = ts.UpdateCellFields(ctx, cell, func(cl *clustermetadatapb.Cell) error {
					cl.ServerAddresses = append(cl.ServerAddresses, "server2:2181")
					cl.Root = "/new_topo"
					return nil
				})
				require.NoError(t, err)

				// Verify the update
				retrieved, err := ts.GetCell(ctx, cell)
				require.NoError(t, err)
				require.Contains(t, retrieved.ServerAddresses, "server2:2181")
				require.Equal(t, "/new_topo", retrieved.Root)
			},
		},
		{
			name: "Update Cell Fields with failing update function",
			test: func(t *testing.T, ts topo.Store) {
				cl := &clustermetadatapb.Cell{
					ServerAddresses: []string{"server1:2181"},
					Root:            "/topo",
				}
				err := ts.CreateCell(ctx, cell, cl)
				require.NoError(t, err)

				// Update function that fails
				updateErr := errors.New("update failed")
				err = ts.UpdateCellFields(ctx, cell, func(cl *clustermetadatapb.Cell) error {
					return updateErr
				})
				require.Error(t, err)
				require.Equal(t, updateErr, err)

				// Verify cell location was not modified
				retrieved, err := ts.GetCell(ctx, cell)
				require.NoError(t, err)
				require.Equal(t, []string{"server1:2181"}, retrieved.ServerAddresses)
				require.Equal(t, "/topo", retrieved.Root)
			},
		},
		{
			name: "Get Cell Names",
			test: func(t *testing.T, ts topo.Store) {
				// Create multiple cell locations
				cl1 := &clustermetadatapb.Cell{
					ServerAddresses: []string{"server1:2181"},
					Root:            "/topo1",
				}
				cl2 := &clustermetadatapb.Cell{
					ServerAddresses: []string{"server2:2181"},
					Root:            "/topo2",
				}

				err := ts.CreateCell(ctx, cell, cl1)
				require.NoError(t, err)
				err = ts.CreateCell(ctx, cell2, cl2)
				require.NoError(t, err)

				// Get all cell names - should include the pre-created zone-1 and our new cells
				names, err := ts.GetCellNames(ctx)
				require.NoError(t, err)
				require.Len(t, names, 3) // zone-1 + test-cell-1 + test-cell-2
				require.Contains(t, names, "zone-1")
				require.Contains(t, names, cell)
				require.Contains(t, names, cell2)

				// Should be sorted alphabetically
				require.Equal(t, []string{cell, cell2, "zone-1"}, names)
			},
		},
		{
			name: "Delete Cell",
			test: func(t *testing.T, ts topo.Store) {
				cl := &clustermetadatapb.Cell{
					ServerAddresses: []string{"server1:2181"},
					Root:            "/topo",
				}
				err := ts.CreateCell(ctx, cell, cl)
				require.NoError(t, err)

				// Delete the cell location
				err = ts.DeleteCell(ctx, cell, true)
				require.NoError(t, err)

				// Verify it's gone
				_, err = ts.GetCell(ctx, cell)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}))
			},
		},
		{
			name: "Update Cell Fields retries on BadVersion error",
			test: func(t *testing.T, ts topo.Store) {
				// Use NewServerAndFactory to get direct access to the factory
				tsWithFactory, factory := memorytopo.NewServerAndFactory(ctx, "zone-1")
				defer tsWithFactory.Close()

				cl := &clustermetadatapb.Cell{
					ServerAddresses: []string{"server1:2181"},
					Root:            "/topo",
				}
				err := tsWithFactory.CreateCell(ctx, cell, cl)
				require.NoError(t, err)

				// Inject a BadVersion error that will only occur once
				badVersionErr := &topo.TopoError{Code: topo.BadVersion}
				factory.AddOneTimeOperationError(memorytopo.Update, "cells/"+cell+"/Cell", badVersionErr)

				// Track how many times the update function is called
				updateCallCount := 0

				err = tsWithFactory.UpdateCellFields(ctx, cell, func(cl *clustermetadatapb.Cell) error {
					updateCallCount++
					cl.ServerAddresses = append(cl.ServerAddresses, "server2:2181")
					cl.Root = "/new_topo"
					return nil
				})
				require.NoError(t, err)

				// Verify the update function was called twice (retry happened)
				require.Equal(t, 2, updateCallCount)

				// Verify the update was successful
				retrieved, err := tsWithFactory.GetCell(ctx, cell)
				require.NoError(t, err)
				require.Contains(t, retrieved.ServerAddresses, "server2:2181")
				require.Equal(t, "/new_topo", retrieved.Root)
			},
		},
		{
			name: "Delete Cell with database reference",
			test: func(t *testing.T, ts topo.Store) {
				// Create a cell
				cl := &clustermetadatapb.Cell{
					ServerAddresses: []string{"server1:2181"},
					Name:            cell,
					Root:            "/cell-root-abc",
				}
				err := ts.CreateCell(ctx, cell, cl)
				require.NoError(t, err)

				// Create a database that references the cell
				db := &clustermetadatapb.Database{
					Name:  "test-db",
					Cells: []string{cell},
				}
				err = ts.CreateDatabase(ctx, "test-db", db)
				require.NoError(t, err)

				// Try to delete the cell without force - should fail
				err = ts.DeleteCell(ctx, cell, false)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NodeNotEmpty}))

				// Verify the error message contains the expected information
				require.Contains(t, err.Error(), fmt.Sprintf("cell %s is referenced by database %s", cell, "test-db"))

				// Verify the cell still exists
				_, err = ts.GetCell(ctx, cell)
				require.NoError(t, err)

				// Delete with force=true - should succeed
				err = ts.DeleteCell(ctx, cell, true)
				require.NoError(t, err)

				// Verify the cell is gone
				_, err = ts.GetCell(ctx, cell)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}))
			},
		},
		{
			name: "Delete Cell with multiple database references",
			test: func(t *testing.T, ts topo.Store) {
				// Create a cell
				cl := &clustermetadatapb.Cell{
					ServerAddresses: []string{"server1:2181"},
					Root:            "/topo",
				}
				err := ts.CreateCell(ctx, cell, cl)
				require.NoError(t, err)

				// Create multiple databases that reference the cell
				db1 := &clustermetadatapb.Database{
					Name:  "test-db-1",
					Cells: []string{cell, "other-cell"},
				}
				db2 := &clustermetadatapb.Database{
					Name:  "test-db-2",
					Cells: []string{"other-cell", cell},
				}

				err = ts.CreateDatabase(ctx, "test-db-1", db1)
				require.NoError(t, err)
				err = ts.CreateDatabase(ctx, "test-db-2", db2)
				require.NoError(t, err)

				// Try to delete the cell without force - should fail
				err = ts.DeleteCell(ctx, cell, false)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NodeNotEmpty}))

				// Verify the error message contains information about one of the databases
				// (the first one found will be mentioned in the error)
				require.Contains(t, err.Error(), "This could create serving issues in the cluster")

				// Verify the cell still exists
				_, err = ts.GetCell(ctx, cell)
				require.NoError(t, err)

				// Delete with force=true - should succeed
				err = ts.DeleteCell(ctx, cell, true)
				require.NoError(t, err)

				// Verify the cell is gone
				_, err = ts.GetCell(ctx, cell)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}))
			},
		},
		{
			name: "Delete Cell with no database references",
			test: func(t *testing.T, ts topo.Store) {
				// Create a cell
				cl := &clustermetadatapb.Cell{
					ServerAddresses: []string{"server1:2181"},
					Root:            "/topo",
				}
				err := ts.CreateCell(ctx, cell, cl)
				require.NoError(t, err)

				// Create a database that doesn't reference the cell
				db := &clustermetadatapb.Database{
					Name:  "test-db",
					Cells: []string{"other-cell"},
				}
				err = ts.CreateDatabase(ctx, "test-db", db)
				require.NoError(t, err)

				// Try to delete the cell without force - should succeed
				err = ts.DeleteCell(ctx, cell, false)
				require.NoError(t, err)

				// Verify the cell is gone
				_, err = ts.GetCell(ctx, cell)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(ctx, "zone-1")
			defer ts.Close()
			tt.test(t, ts)
		})
	}
}
