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
	"cmp"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
)

var (
	cells        = []string{"zone1", "zone2"}
	databases    = []string{"db1", "db2"}
	shards       = []string{"-8", "8-"}
	multipoolers []*clustermetadatapb.MultiPooler
)

func init() {
	uid := uint32(1)
	for _, cell := range cells {
		for _, database := range databases {
			for _, shard := range shards {
				multipooler := getMultiPooler(database, shard, cell, uid)
				multipoolers = append(multipoolers, multipooler)
				uid++
			}
		}
	}
}

func getMultiPooler(database string, shard string, cell string, uid uint32) *clustermetadatapb.MultiPooler {
	return &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Cell: cell,
			Uid:  uid,
		},
		Database: database,
		Shard:    shard,
		Hostname: "host1",
		PortMap: map[string]int32{
			"grpc": int32(uid),
		},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
	}
}

func checkMultiPoolersEqual(t *testing.T, expected, actual *clustermetadatapb.MultiPooler) {
	t.Helper()
	require.Equal(t, expected.Id.String(), actual.Id.String())
	require.Equal(t, expected.Database, actual.Database)
	require.Equal(t, expected.Shard, actual.Shard)
	require.Equal(t, expected.Hostname, actual.Hostname)
	require.Equal(t, expected.Type, actual.Type)
	require.Equal(t, expected.ServingStatus, actual.ServingStatus)
	require.Equal(t, expected.PortMap, actual.PortMap)
}

func checkMultiPoolerInfosEqual(t *testing.T, expected, actual []*topo.MultiPoolerInfo) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for _, actualMP := range actual {
		found := false
		for _, expectedMP := range expected {
			if topo.MultiPoolerIDString(actualMP.Id) == topo.MultiPoolerIDString(expectedMP.Id) {
				checkMultiPoolersEqual(t, expectedMP.MultiPooler, actualMP.MultiPooler)
				found = true
				break
			}
		}
		require.True(t, found, "unexpected multipooler %v", actualMP.IDString())
	}
}

// Test various cases of calls to GetMultiPoolersByCell.
// GetMultiPoolersByCell first tries to get all the multipoolers using List.
// If the response is too large, we will get an error, and fall back to one multipooler at a time.
func TestServerGetMultiPoolersByCell(t *testing.T) {
	const cell = "zone1"
	const database = "testdb"
	const shard = "testshard"

	tests := []struct {
		name                    string
		createShardMultiPoolers int
		expectedMultiPoolers    []*clustermetadatapb.MultiPooler
		opt                     *topo.GetMultiPoolersByCellOptions
		listError               error
		databaseShards          []*topo.DatabaseShard
	}{
		{
			name: "single",
			databaseShards: []*topo.DatabaseShard{
				{Database: database, Shard: shard},
			},
			createShardMultiPoolers: 1,
			expectedMultiPoolers: []*clustermetadatapb.MultiPooler{
				{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  uint32(1),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(1),
					},
					Database:      database,
					Shard:         shard,
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
			},
			opt: nil,
		},
		{
			name: "multiple",
			databaseShards: []*topo.DatabaseShard{
				{Database: database, Shard: shard},
			},
			createShardMultiPoolers: 4,
			expectedMultiPoolers: []*clustermetadatapb.MultiPooler{
				{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  uint32(1),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(1),
					},
					Database:      database,
					Shard:         shard,
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  uint32(2),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(2),
					},
					Database:      database,
					Shard:         shard,
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  uint32(3),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(3),
					},
					Database:      database,
					Shard:         shard,
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  uint32(4),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(4),
					},
					Database:      database,
					Shard:         shard,
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
			},
		},
		{
			name: "filtered by database and shard",
			databaseShards: []*topo.DatabaseShard{
				{Database: database, Shard: shard},
				{Database: "filtered", Shard: "-"},
			},
			createShardMultiPoolers: 2,
			expectedMultiPoolers: []*clustermetadatapb.MultiPooler{
				{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  uint32(1),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(1),
					},
					Database:      database,
					Shard:         shard,
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  uint32(2),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(2),
					},
					Database:      database,
					Shard:         shard,
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
			},
			opt: &topo.GetMultiPoolersByCellOptions{
				DatabaseShard: &topo.DatabaseShard{
					Database: database,
					Shard:    shard,
				},
			},
		},
		{
			name: "filtered by database and no shard",
			databaseShards: []*topo.DatabaseShard{
				{Database: database, Shard: shard},
				{Database: database, Shard: shard + "2"},
			},
			createShardMultiPoolers: 2,
			expectedMultiPoolers: []*clustermetadatapb.MultiPooler{
				{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  uint32(1),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(1),
					},
					Database:      database,
					Shard:         shard,
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  uint32(2),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(2),
					},
					Database:      database,
					Shard:         shard,
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  uint32(3),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(3),
					},
					Database:      database,
					Shard:         shard + "2",
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  uint32(4),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(4),
					},
					Database:      database,
					Shard:         shard + "2",
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
			},
			opt: &topo.GetMultiPoolersByCellOptions{
				DatabaseShard: &topo.DatabaseShard{
					Database: database,
					Shard:    "",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ts, factory := memorytopo.NewServerAndFactory(ctx, cell)
			defer ts.Close()
			if tt.listError != nil {
				factory.AddOperationError(memorytopo.List, ".*", tt.listError)
			}

			var uid uint32 = 1
			for _, ds := range tt.databaseShards {
				for i := 0; i < tt.createShardMultiPoolers; i++ {
					multipooler := &clustermetadatapb.MultiPooler{
						Id: &clustermetadatapb.ID{
							Cell: cell,
							Uid:  uid,
						},
						Hostname:      "host1",
						PortMap:       map[string]int32{"grpc": int32(uid)},
						Database:      ds.Database,
						Shard:         ds.Shard,
						Type:          clustermetadatapb.PoolerType_PRIMARY,
						ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
					}
					require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))
					uid++
				}
			}

			out, err := ts.GetMultiPoolersByCell(ctx, cell, tt.opt)
			require.NoError(t, err)
			require.Len(t, out, len(tt.expectedMultiPoolers))

			slices.SortFunc(out, func(i, j *topo.MultiPoolerInfo) int {
				return cmp.Compare(i.Id.Uid, j.Id.Uid)
			})
			slices.SortFunc(tt.expectedMultiPoolers, func(i, j *clustermetadatapb.MultiPooler) int {
				return cmp.Compare(i.Id.Uid, j.Id.Uid)
			})

			for i, multipoolerInfo := range out {
				checkMultiPoolersEqual(t, tt.expectedMultiPoolers[i], multipoolerInfo.MultiPooler)
			}
		})
	}
}

// TestMultiPoolerIDString tests the ID string functionality
func TestMultiPoolerIDString(t *testing.T) {
	tests := []struct {
		name     string
		id       *clustermetadatapb.ID
		expected string
	}{
		{
			name:     "simple case",
			id:       &clustermetadatapb.ID{Cell: "zone1", Uid: 100},
			expected: "zone1-100",
		},
		{
			name:     "zero uid",
			id:       &clustermetadatapb.ID{Cell: "prod", Uid: 0},
			expected: "prod-0",
		},
		{
			name:     "large uid",
			id:       &clustermetadatapb.ID{Cell: "test", Uid: 999999},
			expected: "test-999999",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := topo.MultiPoolerIDString(tt.id)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestParseMultiPoolerID tests the ID parsing functionality

// TestMultiPoolerCRUDOperations tests basic CRUD operations for multipoolers
func TestMultiPoolerCRUDOperations(t *testing.T) {
	ctx := context.Background()
	cell := "zone-1"

	tests := []struct {
		name string
		test func(t *testing.T, ts topo.Store)
	}{
		{
			name: "Create and Get MultiPooler",
			test: func(t *testing.T, ts topo.Store) {
				multipooler := &clustermetadatapb.MultiPooler{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  1,
					},
					Database:      "testdb",
					Shard:         "testshard",
					Hostname:      "host1.example.com",
					PortMap:       map[string]int32{"grpc": 8080, "http": 8081},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				err := ts.CreateMultiPooler(ctx, multipooler)
				require.NoError(t, err)

				retrieved, err := ts.GetMultiPooler(ctx, multipooler.Id)
				require.NoError(t, err)
				checkMultiPoolersEqual(t, multipooler, retrieved.MultiPooler)
				require.NotZero(t, retrieved.Version())
			},
		},
		{
			name: "Get nonexistent MultiPooler",
			test: func(t *testing.T, ts topo.Store) {
				id := &clustermetadatapb.ID{Cell: cell, Uid: 999}
				_, err := ts.GetMultiPooler(ctx, id)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}))
			},
		},
		{
			name: "Create duplicate MultiPooler fails",
			test: func(t *testing.T, ts topo.Store) {
				multipooler := &clustermetadatapb.MultiPooler{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  1,
					},
					Database:      "testdb",
					Shard:         "testshard",
					Hostname:      "host1.example.com",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				err := ts.CreateMultiPooler(ctx, multipooler)
				require.NoError(t, err)

				err = ts.CreateMultiPooler(ctx, multipooler)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NodeExists}))
			},
		},
		{
			name: "Update MultiPooler",
			test: func(t *testing.T, ts topo.Store) {
				multipooler := &clustermetadatapb.MultiPooler{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  1,
					},
					Database:      "testdb",
					Shard:         "testshard",
					Hostname:      "host1.example.com",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				err := ts.CreateMultiPooler(ctx, multipooler)
				require.NoError(t, err)

				retrieved, err := ts.GetMultiPooler(ctx, multipooler.Id)
				require.NoError(t, err)
				oldVersion := retrieved.Version()

				retrieved.Hostname = "host2.example.com"
				retrieved.PortMap["http"] = 8081
				retrieved.ServingStatus = clustermetadatapb.PoolerServingStatus_NOT_SERVING

				err = ts.UpdateMultiPooler(ctx, retrieved)
				require.NoError(t, err)

				updated, err := ts.GetMultiPooler(ctx, multipooler.Id)
				require.NoError(t, err)
				require.Equal(t, "host2.example.com", updated.Hostname)
				require.Equal(t, int32(8081), updated.PortMap["http"])
				require.Equal(t, clustermetadatapb.PoolerServingStatus_NOT_SERVING, updated.ServingStatus)
				require.NotEqual(t, oldVersion, updated.Version())
			},
		},
		{
			name: "Delete MultiPooler",
			test: func(t *testing.T, ts topo.Store) {
				multipooler := &clustermetadatapb.MultiPooler{
					Id: &clustermetadatapb.ID{
						Cell: cell,
						Uid:  1,
					},
					Database:      "testdb",
					Shard:         "testshard",
					Hostname:      "host1.example.com",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				err := ts.CreateMultiPooler(ctx, multipooler)
				require.NoError(t, err)

				err = ts.DeleteMultiPooler(ctx, multipooler.Id)
				require.NoError(t, err)

				_, err = ts.GetMultiPooler(ctx, multipooler.Id)
				require.Error(t, err)

				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(ctx, cell)
			defer ts.Close()
			tt.test(t, ts)
		})
	}
}

// TestGetMultiPoolerIDsByCell tests getting multipooler IDs by cell
func TestGetMultiPoolerIDsByCell(t *testing.T) {
	ctx := context.Background()
	cell1 := "zone-1"
	cell2 := "zone-2"

	tests := []struct {
		name string
		test func(t *testing.T, ts topo.Store)
	}{
		{
			name: "Empty cell returns empty list",
			test: func(t *testing.T, ts topo.Store) {
				ids, err := ts.GetMultiPoolerIDsByCell(ctx, cell1)
				require.NoError(t, err)
				require.Empty(t, ids)
			},
		},
		{
			name: "Cell with multipoolers",
			test: func(t *testing.T, ts topo.Store) {
				multipoolers := []*clustermetadatapb.MultiPooler{
					{
						Id:            &clustermetadatapb.ID{Cell: cell1, Uid: 1},
						Database:      "db1",
						Shard:         "shard1",
						Hostname:      "host1",
						PortMap:       map[string]int32{"grpc": 8080},
						Type:          clustermetadatapb.PoolerType_PRIMARY,
						ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
					},
					{
						Id:            &clustermetadatapb.ID{Cell: cell1, Uid: 3},
						Database:      "db2",
						Shard:         "shard2",
						Hostname:      "host3",
						PortMap:       map[string]int32{"grpc": 8083},
						Type:          clustermetadatapb.PoolerType_REPLICA,
						ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
					},
				}

				for _, mp := range multipoolers {
					require.NoError(t, ts.CreateMultiPooler(ctx, mp))
				}

				ids, err := ts.GetMultiPoolerIDsByCell(ctx, cell1)
				require.NoError(t, err)
				require.Len(t, ids, 2)

				expectedIDs := []*clustermetadatapb.ID{
					{Cell: cell1, Uid: 1},
					{Cell: cell1, Uid: 3},
				}

				slices.SortFunc(ids, func(a, b *clustermetadatapb.ID) int {
					return cmp.Compare(a.Uid, b.Uid)
				})

				for i, id := range ids {
					require.Equal(t, expectedIDs[i].Cell, id.Cell)
					require.Equal(t, expectedIDs[i].Uid, id.Uid)
				}

				// Verify cell boundary: multipoolers are NOT accessible from cell2
				cell2Ids, err := ts.GetMultiPoolerIDsByCell(ctx, cell2)
				require.NoError(t, err)
				require.Empty(t, cell2Ids, "multipoolers should not be accessible from other cells")
			},
		},
		{
			name: "Nonexistent cell returns error",
			test: func(t *testing.T, ts topo.Store) {
				_, err := ts.GetMultiPoolerIDsByCell(ctx, "nonexistent")
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(ctx, cell1, cell2)
			defer ts.Close()
			tt.test(t, ts)
		})
	}
}

// TestUpdateMultiPoolerFields tests the update fields functionality with retry logic
func TestUpdateMultiPoolerFields(t *testing.T) {
	ctx := context.Background()
	cell := "zone-1"

	tests := []struct {
		name string
		test func(t *testing.T, ts topo.Store)
	}{
		{
			name: "Successful update",
			test: func(t *testing.T, ts topo.Store) {
				id := &clustermetadatapb.ID{Cell: cell, Uid: 1}
				multipooler := &clustermetadatapb.MultiPooler{
					Id:            id,
					Database:      "testdb",
					Shard:         "testshard",
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

				updated, err := ts.UpdateMultiPoolerFields(ctx, id, func(mp *clustermetadatapb.MultiPooler) error {
					mp.Hostname = "newhost"
					mp.PortMap["http"] = 8081
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, "newhost", updated.Hostname)
				require.Equal(t, int32(8081), updated.PortMap["http"])

				retrieved, err := ts.GetMultiPooler(ctx, id)
				require.NoError(t, err)
				require.Equal(t, "newhost", retrieved.Hostname)
				require.Equal(t, int32(8081), retrieved.PortMap["http"])
			},
		},
		{
			name: "Update function returns error",
			test: func(t *testing.T, ts topo.Store) {
				id := &clustermetadatapb.ID{Cell: cell, Uid: 1}
				multipooler := &clustermetadatapb.MultiPooler{
					Id:            id,
					Database:      "testdb",
					Shard:         "testshard",
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

				updateErr := errors.New("update failed")
				_, err := ts.UpdateMultiPoolerFields(ctx, id, func(mp *clustermetadatapb.MultiPooler) error {
					return updateErr
				})
				require.Error(t, err)
				require.Equal(t, updateErr, err)

				retrieved, err := ts.GetMultiPooler(ctx, id)
				require.NoError(t, err)
				require.Equal(t, "host1", retrieved.Hostname)
			},
		},
		{
			name: "NoUpdateNeeded returns nil",
			test: func(t *testing.T, ts topo.Store) {
				id := &clustermetadatapb.ID{Cell: cell, Uid: 1}
				multipooler := &clustermetadatapb.MultiPooler{
					Id:            id,
					Database:      "testdb",
					Shard:         "testshard",
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

				result, err := ts.UpdateMultiPoolerFields(ctx, id, func(mp *clustermetadatapb.MultiPooler) error {
					return &topo.TopoError{Code: topo.NoUpdateNeeded}
				})
				require.NoError(t, err)
				require.Nil(t, result)
			},
		},
		{
			name: "Retry on BadVersion error",
			test: func(t *testing.T, ts topo.Store) {
				tsWithFactory, factory := memorytopo.NewServerAndFactory(ctx, cell)
				defer tsWithFactory.Close()

				id := &clustermetadatapb.ID{Cell: cell, Uid: 1}
				multipooler := &clustermetadatapb.MultiPooler{
					Id:            id,
					Database:      "testdb",
					Shard:         "testshard",
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				require.NoError(t, tsWithFactory.CreateMultiPooler(ctx, multipooler))

				badVersionErr := &topo.TopoError{Code: topo.BadVersion}
				factory.AddOneTimeOperationError(memorytopo.Update, "poolers/zone-1-1/Pooler", badVersionErr)

				updateCallCount := 0
				updated, err := tsWithFactory.UpdateMultiPoolerFields(ctx, id, func(mp *clustermetadatapb.MultiPooler) error {
					updateCallCount++
					mp.Hostname = "newhost"
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, 2, updateCallCount)
				require.Equal(t, "newhost", updated.Hostname)

				retrieved, err := tsWithFactory.GetMultiPooler(ctx, id)
				require.NoError(t, err)
				require.Equal(t, "newhost", retrieved.Hostname)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(ctx, cell)
			defer ts.Close()
			tt.test(t, ts)
		})
	}
}

// TestInitMultiPooler tests the init multipooler functionality
func TestInitMultiPooler(t *testing.T) {
	ctx := context.Background()
	cell := "zone-1"

	tests := []struct {
		name string
		test func(t *testing.T, ts topo.Store)
	}{
		{
			name: "Create new multipooler",
			test: func(t *testing.T, ts topo.Store) {
				multipooler := &clustermetadatapb.MultiPooler{
					Id:            &clustermetadatapb.ID{Cell: cell, Uid: 1},
					Database:      "testdb",
					Shard:         "testshard",
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}

				err := ts.InitMultiPooler(ctx, multipooler, false, false)
				require.NoError(t, err)

				retrieved, err := ts.GetMultiPooler(ctx, multipooler.Id)
				require.NoError(t, err)
				checkMultiPoolersEqual(t, multipooler, retrieved.MultiPooler)
			},
		},
		{
			name: "Update existing multipooler with allowUpdate=true",
			test: func(t *testing.T, ts topo.Store) {
				original := &clustermetadatapb.MultiPooler{
					Id:            &clustermetadatapb.ID{Cell: cell, Uid: 1},
					Database:      "testdb",
					Shard:         "testshard",
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				require.NoError(t, ts.CreateMultiPooler(ctx, original))

				updated := &clustermetadatapb.MultiPooler{
					Id:            &clustermetadatapb.ID{Cell: cell, Uid: 1},
					Database:      "testdb",
					Shard:         "testshard",
					Hostname:      "newhost",
					PortMap:       map[string]int32{"grpc": 8081},
					Type:          clustermetadatapb.PoolerType_REPLICA,
					ServingStatus: clustermetadatapb.PoolerServingStatus_NOT_SERVING,
				}

				err := ts.InitMultiPooler(ctx, updated, false, true)
				require.NoError(t, err)

				retrieved, err := ts.GetMultiPooler(ctx, original.Id)
				require.NoError(t, err)
				checkMultiPoolersEqual(t, updated, retrieved.MultiPooler)
			},
		},
		{
			name: "Fail to update existing multipooler with allowUpdate=false",
			test: func(t *testing.T, ts topo.Store) {
				original := &clustermetadatapb.MultiPooler{
					Id:            &clustermetadatapb.ID{Cell: cell, Uid: 1},
					Database:      "testdb",
					Shard:         "testshard",
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				require.NoError(t, ts.CreateMultiPooler(ctx, original))

				updated := &clustermetadatapb.MultiPooler{
					Id:            &clustermetadatapb.ID{Cell: cell, Uid: 1},
					Database:      "testdb",
					Shard:         "testshard",
					Hostname:      "newhost",
					PortMap:       map[string]int32{"grpc": 8081},
					Type:          clustermetadatapb.PoolerType_REPLICA,
					ServingStatus: clustermetadatapb.PoolerServingStatus_NOT_SERVING,
				}

				err := ts.InitMultiPooler(ctx, updated, false, false)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NodeExists}))
			},
		},
		{
			name: "Fail to update with different database/shard",
			test: func(t *testing.T, ts topo.Store) {
				original := &clustermetadatapb.MultiPooler{
					Id:            &clustermetadatapb.ID{Cell: cell, Uid: 1},
					Database:      "testdb",
					Shard:         "testshard",
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				require.NoError(t, ts.CreateMultiPooler(ctx, original))

				updated := &clustermetadatapb.MultiPooler{
					Id:            &clustermetadatapb.ID{Cell: cell, Uid: 1},
					Database:      "differentdb",
					Shard:         "testshard",
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}

				err := ts.InitMultiPooler(ctx, updated, false, true)
				require.Error(t, err)
				require.Contains(t, err.Error(), "Cannot override with shard")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(ctx, cell)
			defer ts.Close()
			tt.test(t, ts)
		})
	}
}

// TestNewMultiPooler tests the factory function
func TestNewMultiPooler(t *testing.T) {
	tests := []struct {
		name     string
		uid      uint32
		cell     string
		host     string
		expected *clustermetadatapb.MultiPooler
	}{
		{
			name: "basic creation",
			uid:  100,
			cell: "zone1",
			host: "host.example.com",
			expected: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Cell: "zone1",
					Uid:  100,
				},
				Hostname: "host.example.com",
				PortMap:  map[string]int32{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := topo.NewMultiPooler(tt.uid, tt.cell, tt.host)
			require.Equal(t, tt.expected.Id.Cell, result.Id.Cell)
			require.Equal(t, tt.expected.Id.Uid, result.Id.Uid)
			require.Equal(t, tt.expected.Hostname, result.Hostname)
			require.NotNil(t, result.PortMap)
		})
	}
}

// TestMultiPoolerInfo tests the MultiPoolerInfo methods
func TestMultiPoolerInfo(t *testing.T) {
	multipooler := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Cell: "zone1",
			Uid:  100,
		},
		Hostname: "host.example.com",
		PortMap: map[string]int32{
			"grpc": 8080,
			"http": 8081,
		},
	}
	version := memorytopo.NodeVersion(123)
	info := topo.NewMultiPoolerInfo(multipooler, version)

	t.Run("String method", func(t *testing.T) {
		result := info.String()
		expected := "MultiPooler{zone1-100}"
		require.Equal(t, expected, result)
	})

	t.Run("IDString method", func(t *testing.T) {
		result := info.IDString()
		expected := "zone1-100"
		require.Equal(t, expected, result)
	})

	t.Run("Addr method with grpc port", func(t *testing.T) {
		result := info.Addr()
		expected := "host.example.com:8080"
		require.Equal(t, expected, result)
	})

	t.Run("Addr method without grpc port", func(t *testing.T) {
		multipoolerNoGrpc := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Cell: "zone1",
				Uid:  100,
			},
			Hostname: "host.example.com",
			PortMap: map[string]int32{
				"http": 8081,
			},
		}
		infoNoGrpc := topo.NewMultiPoolerInfo(multipoolerNoGrpc, version)
		result := infoNoGrpc.Addr()
		expected := "host.example.com"
		require.Equal(t, expected, result)
	})

	t.Run("Version method", func(t *testing.T) {
		result := info.Version()
		require.Equal(t, version, result)
	})
}

// TestGetMultiPoolersByCell covers comprehensive scenarios for the GetMultiPoolersByCell method
func TestGetMultiPoolersByCell_Comprehensive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("cell with multiple multipoolers without filtering", func(t *testing.T) {
		// Create fresh topo for this test with multiple cells
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
		defer ts.Close()

		// Setup: Create 4 multipoolers in zone1 (2 databases × 2 shards)
		multipoolers := []*clustermetadatapb.MultiPooler{
			{
				Id:            &clustermetadatapb.ID{Cell: "zone1", Uid: 1},
				Database:      "db1",
				Shard:         "-8",
				Hostname:      "host1",
				PortMap:       map[string]int32{"grpc": 8080},
				Type:          clustermetadatapb.PoolerType_PRIMARY,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
			{
				Id:            &clustermetadatapb.ID{Cell: "zone1", Uid: 2},
				Database:      "db1",
				Shard:         "8-",
				Hostname:      "host2",
				PortMap:       map[string]int32{"grpc": 8081},
				Type:          clustermetadatapb.PoolerType_REPLICA,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
			{
				Id:            &clustermetadatapb.ID{Cell: "zone1", Uid: 3},
				Database:      "db2",
				Shard:         "-8",
				Hostname:      "host3",
				PortMap:       map[string]int32{"grpc": 8082},
				Type:          clustermetadatapb.PoolerType_PRIMARY,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
			{
				Id:            &clustermetadatapb.ID{Cell: "zone1", Uid: 4},
				Database:      "db2",
				Shard:         "8-",
				Hostname:      "host4",
				PortMap:       map[string]int32{"grpc": 8083},
				Type:          clustermetadatapb.PoolerType_REPLICA,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
		}

		// Create all multipoolers
		for _, mp := range multipoolers {
			require.NoError(t, ts.CreateMultiPooler(ctx, mp))
		}

		// Test: Get all multipoolers without filtering
		multipoolerInfos, err := ts.GetMultiPoolersByCell(ctx, "zone1", nil)
		require.NoError(t, err)
		require.Len(t, multipoolerInfos, 4)

		// Verify all multipoolers are returned
		expectedMPs := []*topo.MultiPoolerInfo{
			{MultiPooler: multipoolers[0]}, // db1, -8
			{MultiPooler: multipoolers[1]}, // db1, 8-
			{MultiPooler: multipoolers[2]}, // db2, -8
			{MultiPooler: multipoolers[3]}, // db2, 8-
		}
		checkMultiPoolerInfosEqual(t, expectedMPs, multipoolerInfos)

		// Verify cell boundary: multipoolers are NOT accessible from other cells
		otherCellInfos, err := ts.GetMultiPoolersByCell(ctx, "zone2", nil)
		require.NoError(t, err)
		require.Empty(t, otherCellInfos, "multipoolers should not be accessible from other cells")
	})

	t.Run("cell with database filtering", func(t *testing.T) {
		// Create fresh topo for this test with multiple cells
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
		defer ts.Close()

		// Setup: Create 2 multipoolers for db1 in zone1
		multipoolers := []*clustermetadatapb.MultiPooler{
			{
				Id:            &clustermetadatapb.ID{Cell: "zone1", Uid: 1},
				Database:      "db1",
				Shard:         "-8",
				Hostname:      "host1",
				PortMap:       map[string]int32{"grpc": 8080},
				Type:          clustermetadatapb.PoolerType_PRIMARY,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
			{
				Id:            &clustermetadatapb.ID{Cell: "zone1", Uid: 2},
				Database:      "db1",
				Shard:         "8-",
				Hostname:      "host2",
				PortMap:       map[string]int32{"grpc": 8081},
				Type:          clustermetadatapb.PoolerType_REPLICA,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
		}

		// Create multipoolers
		for _, mp := range multipoolers {
			require.NoError(t, ts.CreateMultiPooler(ctx, mp))
		}

		// Test: Filter by database only (empty shard matches all)
		opts := &topo.GetMultiPoolersByCellOptions{
			DatabaseShard: &topo.DatabaseShard{
				Database: "db1",
				Shard:    "", // empty shard matches all
			},
		}

		multipoolerInfos, err := ts.GetMultiPoolersByCell(ctx, "zone1", opts)
		require.NoError(t, err)
		require.Len(t, multipoolerInfos, 2)

		// Verify only db1 multipoolers are returned
		for _, info := range multipoolerInfos {
			require.Equal(t, "db1", info.Database)
		}

		// Verify cell boundary: multipoolers are NOT accessible from other cells
		otherCellInfos, err := ts.GetMultiPoolersByCell(ctx, "zone2", nil)
		require.NoError(t, err)
		require.Empty(t, otherCellInfos, "multipoolers should not be accessible from other cells")
	})

	t.Run("cell with database and shard filtering", func(t *testing.T) {
		// Create fresh topo for this test with multiple cells
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
		defer ts.Close()

		// Setup: Create 2 multipoolers for db2 in zone1
		multipoolers := []*clustermetadatapb.MultiPooler{
			{
				Id:            &clustermetadatapb.ID{Cell: "zone1", Uid: 1},
				Database:      "db2",
				Shard:         "-8",
				Hostname:      "host1",
				PortMap:       map[string]int32{"grpc": 8080},
				Type:          clustermetadatapb.PoolerType_PRIMARY,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
			{
				Id:            &clustermetadatapb.ID{Cell: "zone1", Uid: 2},
				Database:      "db2",
				Shard:         "8-",
				Hostname:      "host2",
				PortMap:       map[string]int32{"grpc": 8081},
				Type:          clustermetadatapb.PoolerType_REPLICA,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
		}

		// Create multipoolers
		for _, mp := range multipoolers {
			require.NoError(t, ts.CreateMultiPooler(ctx, mp))
		}

		// Test: Filter by specific database and shard
		opts := &topo.GetMultiPoolersByCellOptions{
			DatabaseShard: &topo.DatabaseShard{
				Database: "db2",
				Shard:    "-8",
			},
		}

		multipoolerInfos, err := ts.GetMultiPoolersByCell(ctx, "zone1", opts)
		require.NoError(t, err)
		require.Len(t, multipoolerInfos, 1)

		// Verify correct multipooler is returned
		require.Equal(t, "db2", multipoolerInfos[0].Database)
		require.Equal(t, "-8", multipoolerInfos[0].Shard)

		// Verify cell boundary: multipoolers are NOT accessible from other cells
		otherCellInfos, err := ts.GetMultiPoolersByCell(ctx, "zone2", nil)
		require.NoError(t, err)
		require.Empty(t, otherCellInfos, "multipoolers should not be accessible from other cells")
	})

	t.Run("empty cell returns empty list", func(t *testing.T) {
		// Create fresh topo for this test
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		// Setup: No multipoolers created

		// Test: Get multipoolers from empty cell
		multipoolerInfos, err := ts.GetMultiPoolersByCell(ctx, "zone1", nil)
		require.NoError(t, err)
		require.Empty(t, multipoolerInfos)
	})

	t.Run("nonexistent cell returns NoNode error", func(t *testing.T) {
		// Create fresh topo for this test
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		// Setup: No multipoolers created

		// Test: Try to get multipoolers from nonexistent cell
		_, err := ts.GetMultiPoolersByCell(ctx, "nonexistent", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}))
	})

	t.Run("multipoolers are isolated between cells", func(t *testing.T) {
		// Create fresh topo for this test with multiple cells
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
		defer ts.Close()

		// Setup: Create multipoolers in both cells
		zone1Multipooler := &clustermetadatapb.MultiPooler{
			Id:            &clustermetadatapb.ID{Cell: "zone1", Uid: 1},
			Database:      "db1",
			Shard:         "-8",
			Hostname:      "host1",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_PRIMARY,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		}
		zone2Multipooler := &clustermetadatapb.MultiPooler{
			Id:            &clustermetadatapb.ID{Cell: "zone2", Uid: 1},
			Database:      "db1",
			Shard:         "-8",
			Hostname:      "host2",
			PortMap:       map[string]int32{"grpc": 8081},
			Type:          clustermetadatapb.PoolerType_REPLICA,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		}

		// Create multipoolers in their respective cells
		require.NoError(t, ts.CreateMultiPooler(ctx, zone1Multipooler))
		require.NoError(t, ts.CreateMultiPooler(ctx, zone2Multipooler))

		// Test: Verify zone1 can only see its own multipooler
		zone1Infos, err := ts.GetMultiPoolersByCell(ctx, "zone1", nil)
		require.NoError(t, err)
		require.Len(t, zone1Infos, 1)
		require.Equal(t, "zone1", zone1Infos[0].Id.Cell)
		require.Equal(t, "host1", zone1Infos[0].Hostname)

		// Test: Verify zone2 can only see its own multipooler
		zone2Infos, err := ts.GetMultiPoolersByCell(ctx, "zone2", nil)
		require.NoError(t, err)
		require.Len(t, zone2Infos, 1)
		require.Equal(t, "zone2", zone2Infos[0].Id.Cell)
		require.Equal(t, "host2", zone2Infos[0].Hostname)

		// Test: Verify cross-cell access is properly isolated
		zone1FromZone2, err := ts.GetMultiPooler(ctx, zone1Multipooler.Id)
		require.NoError(t, err, "should be able to get multipooler by ID regardless of current cell context")
		require.Equal(t, "zone1", zone1FromZone2.Id.Cell)

		zone2FromZone1, err := ts.GetMultiPooler(ctx, zone2Multipooler.Id)
		require.NoError(t, err, "should be able to get multipooler by ID regardless of current cell context")
		require.Equal(t, "zone2", zone2FromZone1.Id.Cell)
	})
}
