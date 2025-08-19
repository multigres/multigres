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
	"testing"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
)

func TestDatabaseOperations(t *testing.T) {
	ctx := context.Background()
	cell := "zone-1"
	cell2 := "zone-2"
	database_a := "db_a"
	database_b := "db_b"

	tests := []struct {
		name string
		test func(t *testing.T, ts topo.Store)
	}{
		{
			name: "Basic database lifecycle",
			test: func(t *testing.T, ts topo.Store) {
				// Initially no databases
				databases, err := ts.GetDatabaseNames(ctx)
				require.NoError(t, err)
				require.Empty(t, databases)

				// Create a database
				db := &clustermetadatapb.Database{
					Name:             database_a,
					BackupLocation:   "/backups",
					DurabilityPolicy: "semi_sync",
					Cells:            []string{cell},
				}
				err = ts.CreateDatabase(ctx, database_a, db)
				require.NoError(t, err)

				// Verify it exists
				databases, err = ts.GetDatabaseNames(ctx)
				require.NoError(t, err)
				require.Len(t, databases, 1)
				require.Equal(t, database_a, databases[0])

				// Get the full database
				retrieved, err := ts.GetDatabase(ctx, database_a)
				require.NoError(t, err)
				require.Equal(t, db.Name, retrieved.Name)
				require.Equal(t, db.BackupLocation, retrieved.BackupLocation)
				require.Equal(t, db.DurabilityPolicy, retrieved.DurabilityPolicy)
				require.Equal(t, db.Cells, retrieved.Cells)

				// Delete the database
				err = ts.DeleteDatabase(ctx, database_a, true)
				require.NoError(t, err)

				// Verify it's gone
				databases, err = ts.GetDatabaseNames(ctx)
				require.NoError(t, err)
				require.Empty(t, databases)
			},
		},
		{
			name: "Multiple databases management",
			test: func(t *testing.T, ts topo.Store) {
				// Create multiple databases
				db1 := &clustermetadatapb.Database{
					Name:             database_a,
					BackupLocation:   "/backups",
					DurabilityPolicy: "semi_sync",
					Cells:            []string{cell},
				}
				db2 := &clustermetadatapb.Database{
					Name:             database_b,
					BackupLocation:   "/backups2",
					DurabilityPolicy: "async",
					Cells:            []string{cell2},
				}

				err := ts.CreateDatabase(ctx, database_b, db2)
				require.NoError(t, err)

				err = ts.CreateDatabase(ctx, database_a, db1)
				require.NoError(t, err)

				// Verify both exist and are sorted
				databases, err := ts.GetDatabaseNames(ctx)
				require.NoError(t, err)
				require.Len(t, databases, 2)
				// They are returned in alphabetical order.
				require.Equal(t, []string{database_a, database_b}, databases)

				// Delete only one
				err = ts.DeleteDatabase(ctx, database_a, true)
				require.NoError(t, err)

				// Verify the other remains
				db, err := ts.GetDatabase(ctx, database_b)
				require.NoError(t, err)
				require.Equal(t, database_b, db.Name)

				// Verify first is gone
				_, err = ts.GetDatabase(ctx, database_a)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}))
			},
		},
		{
			name: "Updating fields",
			test: func(t *testing.T, ts topo.Store) {
				// Create databases with different configurations
				db1 := &clustermetadatapb.Database{
					Name:             database_a,
					BackupLocation:   "/backups",
					DurabilityPolicy: "semi_sync",
					Cells:            []string{cell, cell2},
				}
				db2 := &clustermetadatapb.Database{
					Name:             database_b,
					BackupLocation:   "/backups2",
					DurabilityPolicy: "async",
					Cells:            []string{cell},
				}

				err := ts.CreateDatabase(ctx, database_a, db1)
				require.NoError(t, err)
				err = ts.CreateDatabase(ctx, database_b, db2)
				require.NoError(t, err)

				// Update one database
				err = ts.UpdateDatabaseFields(ctx, database_a, func(db *clustermetadatapb.Database) error {
					db.BackupLocation = "/new_backups"
					return nil
				})
				require.NoError(t, err)

				// Verify first database was updated
				retrieved1, err := ts.GetDatabase(ctx, database_a)
				require.NoError(t, err)
				require.Equal(t, "/new_backups", retrieved1.BackupLocation)
				require.Equal(t, "semi_sync", retrieved1.DurabilityPolicy)

				// You can update multiple fields at once.
				err = ts.UpdateDatabaseFields(ctx, database_a, func(db *clustermetadatapb.Database) error {
					db.BackupLocation = "/new_backups_3"
					db.DurabilityPolicy = "sync"
					return nil
				})
				require.NoError(t, err)
				retrieved1, err = ts.GetDatabase(ctx, database_a)
				require.NoError(t, err)
				require.Equal(t, "/new_backups_3", retrieved1.BackupLocation)
				require.Equal(t, "sync", retrieved1.DurabilityPolicy)

				// Verify second database was not affected
				retrieved2, err := ts.GetDatabase(ctx, database_b)
				require.NoError(t, err)
				require.Equal(t, "/backups2", retrieved2.BackupLocation)
				require.Equal(t, "async", retrieved2.DurabilityPolicy)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(ctx, cell, cell2)
			defer ts.Close()
			tt.test(t, ts)
		})
	}
}

func TestDatabaseCRUDOperations(t *testing.T) {
	ctx := context.Background()
	cell := "zone-1"
	database := "test_db"

	tests := []struct {
		name string
		test func(t *testing.T, ts topo.Store)
	}{
		{
			name: "Create and Get Database",
			test: func(t *testing.T, ts topo.Store) {
				db := &clustermetadatapb.Database{
					Name:             database,
					BackupLocation:   "/backups/test_db",
					DurabilityPolicy: "semi_sync",
					Cells:            []string{cell, "zone-2"},
				}
				err := ts.CreateDatabase(ctx, database, db)
				require.NoError(t, err)

				retrieved, err := ts.GetDatabase(ctx, database)
				require.NoError(t, err)
				require.Equal(t, db.Name, retrieved.Name)
				require.Equal(t, db.BackupLocation, retrieved.BackupLocation)
				require.Equal(t, db.DurabilityPolicy, retrieved.DurabilityPolicy)
				require.Equal(t, db.Cells, retrieved.Cells)
			},
		},
		{
			name: "Get nonexistent Database",
			test: func(t *testing.T, ts topo.Store) {
				_, err := ts.GetDatabase(ctx, "nonexistent")
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}))
			},
		},
		{
			name: "Create duplicate Database fails",
			test: func(t *testing.T, ts topo.Store) {
				db := &clustermetadatapb.Database{
					Name:             database,
					BackupLocation:   "/backups/test_db",
					DurabilityPolicy: "semi_sync",
					Cells:            []string{cell},
				}
				err := ts.CreateDatabase(ctx, database, db)
				require.NoError(t, err)

				// Try to create again should fail
				err = ts.CreateDatabase(ctx, database, db)
				require.Error(t, err)
			},
		},
		{
			name: "Update Database Fields",
			test: func(t *testing.T, ts topo.Store) {
				db := &clustermetadatapb.Database{
					Name:             database,
					BackupLocation:   "/backups/test_db",
					DurabilityPolicy: "semi_sync",
					Cells:            []string{cell},
				}
				err := ts.CreateDatabase(ctx, database, db)
				require.NoError(t, err)

				// Update the database
				err = ts.UpdateDatabaseFields(ctx, database, func(db *clustermetadatapb.Database) error {
					db.BackupLocation = "/new_backups/test_db"
					db.DurabilityPolicy = "async"
					db.Cells = append(db.Cells, "zone-2")
					return nil
				})
				require.NoError(t, err)

				// Verify the update
				retrieved, err := ts.GetDatabase(ctx, database)
				require.NoError(t, err)
				require.Equal(t, "/new_backups/test_db", retrieved.BackupLocation)
				require.Equal(t, "async", retrieved.DurabilityPolicy)
				require.Contains(t, retrieved.Cells, "zone-2")
			},
		},
		{
			name: "Update nonexistent Database creates it",
			test: func(t *testing.T, ts topo.Store) {
				err := ts.UpdateDatabaseFields(ctx, "new_db", func(db *clustermetadatapb.Database) error {
					db.Name = "new_db"
					db.BackupLocation = "/backups/new_db"
					db.DurabilityPolicy = "sync"
					db.Cells = []string{cell}
					return nil
				})
				require.NoError(t, err)

				// Verify it was created
				retrieved, err := ts.GetDatabase(ctx, "new_db")
				require.NoError(t, err)
				require.Equal(t, "new_db", retrieved.Name)
			},
		},
		{
			name: "Delete Database",
			test: func(t *testing.T, ts topo.Store) {
				db := &clustermetadatapb.Database{
					Name:             database,
					BackupLocation:   "/backups/test_db",
					DurabilityPolicy: "semi_sync",
					Cells:            []string{cell},
				}
				err := ts.CreateDatabase(ctx, database, db)
				require.NoError(t, err)

				// Delete the database
				err = ts.DeleteDatabase(ctx, database, true)
				require.NoError(t, err)

				// Verify it's gone
				_, err = ts.GetDatabase(ctx, database)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}))
			},
		},
		{
			name: "Get Database Names",
			test: func(t *testing.T, ts topo.Store) {
				// Create multiple databases
				db1 := &clustermetadatapb.Database{
					Name:             "db1",
					BackupLocation:   "/backups/db1",
					DurabilityPolicy: "semi_sync",
					Cells:            []string{cell},
				}
				db2 := &clustermetadatapb.Database{
					Name:             "db2",
					BackupLocation:   "/backups/db2",
					DurabilityPolicy: "async",
					Cells:            []string{cell},
				}

				err := ts.CreateDatabase(ctx, "db1", db1)
				require.NoError(t, err)
				err = ts.CreateDatabase(ctx, "db2", db2)
				require.NoError(t, err)

				// Get all database names
				names, err := ts.GetDatabaseNames(ctx)
				require.NoError(t, err)
				require.Len(t, names, 2)
				require.Contains(t, names, "db1")
				require.Contains(t, names, "db2")

				// Should be sorted alphabetically
				require.Equal(t, []string{"db1", "db2"}, names)
			},
		},
		{
			name: "Update Database Fields with failing update function",
			test: func(t *testing.T, ts topo.Store) {
				db := &clustermetadatapb.Database{
					Name:             database,
					BackupLocation:   "/backups/test_db",
					DurabilityPolicy: "semi_sync",
					Cells:            []string{cell},
				}
				err := ts.CreateDatabase(ctx, database, db)
				require.NoError(t, err)

				// Update function that fails
				updateErr := errors.New("update failed")
				err = ts.UpdateDatabaseFields(ctx, database, func(db *clustermetadatapb.Database) error {
					return updateErr
				})
				require.Error(t, err)
				require.Equal(t, updateErr, err)

				// Verify database was not modified
				retrieved, err := ts.GetDatabase(ctx, database)
				require.NoError(t, err)
				require.Equal(t, "/backups/test_db", retrieved.BackupLocation)
				require.Equal(t, "semi_sync", retrieved.DurabilityPolicy)
			},
		},
		{
			name: "Update Database Fields retries on BadVersion error",
			test: func(t *testing.T, ts topo.Store) {
				// Use NewServerAndFactory to get direct access to the factory
				tsWithFactory, factory := memorytopo.NewServerAndFactory(ctx, cell, "zone-2")
				defer tsWithFactory.Close()

				db := &clustermetadatapb.Database{
					Name:             database,
					BackupLocation:   "/backups/test_db",
					DurabilityPolicy: "semi_sync",
					Cells:            []string{cell},
				}
				err := tsWithFactory.CreateDatabase(ctx, database, db)
				require.NoError(t, err)

				// Inject a BadVersion error that will only occur once
				badVersionErr := &topo.TopoError{Code: topo.BadVersion}
				factory.AddOneTimeOperationError(memorytopo.Update, "databases/test_db/Database", badVersionErr)

				// Track how many times the update function is called
				updateCallCount := 0

				err = tsWithFactory.UpdateDatabaseFields(ctx, database, func(db *clustermetadatapb.Database) error {
					updateCallCount++
					db.BackupLocation = "/new_backups/test_db"
					db.DurabilityPolicy = "async"
					return nil
				})
				require.NoError(t, err)

				// Verify the update function was called twice (retry happened)
				require.Equal(t, 2, updateCallCount)

				// Verify the update was successful
				retrieved, err := tsWithFactory.GetDatabase(ctx, database)
				require.NoError(t, err)
				require.Equal(t, "/new_backups/test_db", retrieved.BackupLocation)
				require.Equal(t, "async", retrieved.DurabilityPolicy)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(ctx, cell, "zone-2")
			defer ts.Close()
			tt.test(t, ts)
		})
	}
}
