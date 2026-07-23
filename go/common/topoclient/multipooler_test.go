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

package topoclient_test

import (
	"cmp"
	"context"
	"errors"
	"path"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/test/utils"
)

var (
	cells        = []string{"zone1", "zone2"}
	databases    = []string{"db1", "db2"}
	shards       = []string{"-8", "8-"}
	multipoolers []*clustermetadatapb.Multipooler
)

func init() {
	uid := uint32(1)
	for _, cell := range cells {
		for _, database := range databases {
			for _, shard := range shards {
				multipooler := getMultipooler(database, shard, cell, uid)
				multipoolers = append(multipoolers, multipooler)
				uid++
			}
		}
	}
}

func getMultipooler(database string, shard string, cell string, uid uint32) *clustermetadatapb.Multipooler {
	return &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cell,
			Name:      strconv.FormatUint(uint64(uid), 10),
		},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   database,
			TableGroup: "default",
			Shard:      shard,
		},
		Hostname: "host1",
		PortMap: map[string]int32{
			"grpc": int32(uid),
		},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
	}
}

func checkMultipoolersEqual(t *testing.T, expected, actual *clustermetadatapb.Multipooler) {
	t.Helper()
	require.Equal(t, expected.Id.String(), actual.Id.String())
	require.True(t, proto.Equal(expected.ShardKey, actual.ShardKey), "ShardKey mismatch: expected %v, got %v", expected.ShardKey, actual.ShardKey)
	require.Equal(t, expected.Type, actual.Type)
	require.Equal(t, expected.ServingStatus, actual.ServingStatus)
	require.Equal(t, expected.PortMap, actual.PortMap)
}

func checkMultipoolerInfosEqual(t *testing.T, expected, actual []*topoclient.MultipoolerInfo) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for _, actualMP := range actual {
		found := false
		for _, expectedMP := range expected {
			if topoclient.ComponentIDString(actualMP.Id) == topoclient.ComponentIDString(expectedMP.Id) {
				checkMultipoolersEqual(t, expectedMP.Multipooler, actualMP.Multipooler)
				found = true
				break
			}
		}
		require.True(t, found, "unexpected multipooler %v", topoclient.ComponentIDString(actualMP.Id))
	}
}

// Test various cases of calls to GetMultipoolersByCell.
// GetMultipoolersByCell first tries to get all the multipoolers using List.
// If the response is too large, we will get an error, and fall back to one multipooler at a time.
func TestServerGetMultipoolersByCell(t *testing.T) {
	const cell = "zone1"
	const database = "testdb"
	const shard = "testshard"

	tests := []struct {
		name                    string
		createShardMultipoolers int
		expectedMultipoolers    []*clustermetadatapb.Multipooler
		opt                     *topoclient.GetMultipoolersByCellOptions
		listError               error
		databaseShards          []*topoclient.DatabaseShard
	}{
		{
			name: "single",
			databaseShards: []*topoclient.DatabaseShard{
				{Database: database, Shard: shard},
			},
			createShardMultipoolers: 1,
			expectedMultipoolers: []*clustermetadatapb.Multipooler{
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "alpha",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(1),
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   database,
						TableGroup: "default",
						Shard:      shard,
					},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
			},
			opt: nil,
		},
		{
			name: "multiple",
			databaseShards: []*topoclient.DatabaseShard{
				{Database: database, Shard: shard},
			},
			createShardMultipoolers: 4,
			expectedMultipoolers: []*clustermetadatapb.Multipooler{
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "beta",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(1),
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   database,
						TableGroup: "default",
						Shard:      shard,
					},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "echo",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(2),
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   database,
						TableGroup: "default",
						Shard:      shard,
					},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "foxtrot",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(3),
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   database,
						TableGroup: "default",
						Shard:      shard,
					},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "golf",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(4),
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   database,
						TableGroup: "default",
						Shard:      shard,
					},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
			},
		},
		{
			name: "filtered by database and shard",
			databaseShards: []*topoclient.DatabaseShard{
				{Database: database, TableGroup: "tg1", Shard: shard},
				{Database: "filtered", TableGroup: "tg2", Shard: "-"},
			},
			createShardMultipoolers: 2,
			expectedMultipoolers: []*clustermetadatapb.Multipooler{
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "hotel",
					},
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": int32(1)},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   database,
						TableGroup: "tg1",
						Shard:      shard,
					},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "india",
					},
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": int32(2)},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   database,
						TableGroup: "tg1",
						Shard:      shard,
					},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
			},
			opt: &topoclient.GetMultipoolersByCellOptions{
				DatabaseShard: &topoclient.DatabaseShard{
					Database:   database,
					TableGroup: "tg1",
					Shard:      shard,
				},
			},
		},
		{
			name: "filtered by database and no shard",
			databaseShards: []*topoclient.DatabaseShard{
				{Database: database, Shard: shard},
				{Database: database, Shard: shard + "2"},
			},
			createShardMultipoolers: 2,
			expectedMultipoolers: []*clustermetadatapb.Multipooler{
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "juliet",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(1),
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   database,
						TableGroup: "default",
						Shard:      shard,
					},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "kilo",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(2),
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   database,
						TableGroup: "default",
						Shard:      shard,
					},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "lima",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(3),
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   database,
						TableGroup: "default",
						Shard:      shard + "2",
					},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "mike",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(4),
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   database,
						TableGroup: "default",
						Shard:      shard + "2",
					},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
			},
			opt: &topoclient.GetMultipoolersByCellOptions{
				DatabaseShard: &topoclient.DatabaseShard{
					Database: database,
					Shard:    "",
				},
			},
		},
		{
			name: "filtered by database and tablegroup",
			databaseShards: []*topoclient.DatabaseShard{
				{Database: database, TableGroup: "tg1", Shard: "shard1"},
				{Database: database, TableGroup: "tg1", Shard: "shard2"},
				{Database: database, TableGroup: "tg2", Shard: "shard1"},
			},
			createShardMultipoolers: 2,
			expectedMultipoolers: []*clustermetadatapb.Multipooler{
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "laperla",
					},
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": int32(1)},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   database,
						TableGroup: "tg1",
						Shard:      "shard1",
					},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "berghain",
					},
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": int32(2)},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   database,
						TableGroup: "tg1",
						Shard:      "shard2",
					},
					Type:          clustermetadatapb.PoolerType_REPLICA,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
			},
			opt: &topoclient.GetMultipoolersByCellOptions{
				DatabaseShard: &topoclient.DatabaseShard{
					Database:   database,
					TableGroup: "tg1",
					Shard:      "",
				},
			},
		},
		{
			name: "filtered by database, tablegroup, and shard",
			databaseShards: []*topoclient.DatabaseShard{
				{Database: database, TableGroup: "tg1", Shard: "shard1"},
				{Database: database, TableGroup: "tg1", Shard: "shard2"},
			},
			createShardMultipoolers: 1,
			expectedMultipoolers: []*clustermetadatapb.Multipooler{
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "papa",
					},
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": int32(1)},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   database,
						TableGroup: "tg1",
						Shard:      "shard1",
					},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				},
			},
			opt: &topoclient.GetMultipoolersByCellOptions{
				DatabaseShard: &topoclient.DatabaseShard{
					Database:   database,
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
		},
	}

	// Tests for validation errors
	validationTests := []struct {
		name      string
		opt       *topoclient.GetMultipoolersByCellOptions
		expectErr string
	}{
		{
			name: "error: shard without tablegroup",
			opt: &topoclient.GetMultipoolersByCellOptions{
				DatabaseShard: &topoclient.DatabaseShard{
					Database:   database,
					TableGroup: "",
					Shard:      "shard1",
				},
			},
			expectErr: "cannot filter by Shard without specifying TableGroup",
		},
		{
			name: "error: tablegroup without database",
			opt: &topoclient.GetMultipoolersByCellOptions{
				DatabaseShard: &topoclient.DatabaseShard{
					Database:   "",
					TableGroup: "tg1",
					Shard:      "",
				},
			},
			expectErr: "cannot filter by TableGroup without specifying Database",
		},
		{
			name: "error: shard and tablegroup without database",
			opt: &topoclient.GetMultipoolersByCellOptions{
				DatabaseShard: &topoclient.DatabaseShard{
					Database:   "",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			expectErr: "cannot filter by TableGroup without specifying Database",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			ts, factory := memorytopo.NewServerAndFactory(ctx, cell)
			defer ts.Close()
			if tt.listError != nil {
				factory.AddOperationError(memorytopo.List, ".*", tt.listError)
			}

			// Create multipoolers with names from expected results
			for i, expectedMP := range tt.expectedMultipoolers {
				multipooler := &clustermetadatapb.Multipooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      expectedMP.Id.Name,
					},
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": int32(i + 1)},
					ShardKey:      expectedMP.GetShardKey(),
					Type:          expectedMP.Type,
					ServingStatus: expectedMP.ServingStatus,
				}
				require.NoError(t, ts.CreateMultipooler(ctx, multipooler))
			}

			out, err := ts.GetMultipoolersByCell(ctx, cell, tt.opt)
			require.NoError(t, err)
			require.Len(t, out, len(tt.expectedMultipoolers))

			slices.SortFunc(out, func(i, j *topoclient.MultipoolerInfo) int {
				return cmp.Compare(i.Id.Name, j.Id.Name)
			})
			slices.SortFunc(tt.expectedMultipoolers, func(i, j *clustermetadatapb.Multipooler) int {
				return cmp.Compare(i.Id.Name, j.Id.Name)
			})

			for i, multipoolerInfo := range out {
				checkMultipoolersEqual(t, tt.expectedMultipoolers[i], multipoolerInfo.Multipooler)
			}
		})
	}

	// Run validation error tests
	for _, tt := range validationTests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			ts, _ := memorytopo.NewServerAndFactory(ctx, cell)
			defer ts.Close()

			_, err := ts.GetMultipoolersByCell(ctx, cell, tt.opt)
			require.Error(t, err)
			require.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.BadInput}), "expected BadInput error, got: %v", err)
			require.Contains(t, err.Error(), tt.expectErr)
		})
	}
}

// TestMultipoolerIDString tests the ID string functionality
func TestMultipoolerIDString(t *testing.T) {
	tests := []struct {
		name     string
		id       *clustermetadatapb.ID
		expected string
	}{
		{
			name:     "simple case",
			id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "100"},
			expected: "multipooler-zone1-100",
		},
		{
			name:     "you can use name as numbers",
			id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "prod", Name: "0"},
			expected: "multipooler-prod-0",
		},
		{
			name:     "funny name",
			id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "prod", Name: "sleepy"},
			expected: "multipooler-prod-sleepy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := string(topoclient.ComponentIDString(tt.id))
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestParseMultipoolerID tests the ID parsing functionality

// TestMultipoolerCRUDOperations tests basic CRUD operations for multipoolers
func TestMultipoolerCRUDOperations(t *testing.T) {
	ctx := context.Background()
	cell := "zone-1"

	tests := []struct {
		name string
		test func(t *testing.T, ts topoclient.Store)
	}{
		{
			name: "Create and Get Multipooler",
			test: func(t *testing.T, ts topoclient.Store) {
				multipooler := &clustermetadatapb.Multipooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "november",
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "host1.example.com",
					PortMap:       map[string]int32{"grpc": 8080, "http": 8081},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				err := ts.CreateMultipooler(ctx, multipooler)
				require.NoError(t, err)

				retrieved, err := ts.GetMultipooler(ctx, multipooler.Id)
				require.NoError(t, err)
				checkMultipoolersEqual(t, multipooler, retrieved.Multipooler)
				require.NotZero(t, retrieved.Version())
			},
		},
		{
			name: "Get nonexistent Multipooler",
			test: func(t *testing.T, ts topoclient.Store) {
				id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: cell, Name: "999"}
				_, err := ts.GetMultipooler(ctx, id)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}))
			},
		},
		{
			name: "Create duplicate Multipooler fails",
			test: func(t *testing.T, ts topoclient.Store) {
				multipooler := &clustermetadatapb.Multipooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "oscar",
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "host1.example.com",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				err := ts.CreateMultipooler(ctx, multipooler)
				require.NoError(t, err)

				err = ts.CreateMultipooler(ctx, multipooler)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}))
			},
		},
		{
			name: "Update Multipooler",
			test: func(t *testing.T, ts topoclient.Store) {
				multipooler := &clustermetadatapb.Multipooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "papa",
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "host1.example.com",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				err := ts.CreateMultipooler(ctx, multipooler)
				require.NoError(t, err)

				retrieved, err := ts.GetMultipooler(ctx, multipooler.Id)
				require.NoError(t, err)
				oldVersion := retrieved.Version()

				retrieved.Hostname = "host2.example.com"
				retrieved.PortMap["http"] = 8081
				retrieved.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED

				err = ts.UpdateMultipooler(ctx, retrieved)
				require.NoError(t, err)

				updated, err := ts.GetMultipooler(ctx, multipooler.Id)
				require.NoError(t, err)
				require.Equal(t, "host2.example.com", updated.Hostname)
				require.Equal(t, int32(8081), updated.PortMap["http"])
				require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, updated.ServingStatus)
				require.NotEqual(t, oldVersion, updated.Version())
			},
		},
		{
			name: "Delete Multipooler",
			test: func(t *testing.T, ts topoclient.Store) {
				multipooler := &clustermetadatapb.Multipooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "quebec",
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "host1.example.com",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				err := ts.CreateMultipooler(ctx, multipooler)
				require.NoError(t, err)

				err = ts.UnregisterMultipooler(ctx, multipooler.Id)
				require.NoError(t, err)

				_, err = ts.GetMultipooler(ctx, multipooler.Id)
				require.Error(t, err)

				require.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}))
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

// TestGetMultipoolerIDsByCell tests getting multipooler IDs by cell
func TestGetMultipoolerIDsByCell(t *testing.T) {
	ctx := context.Background()
	cell1 := "zone-1"
	cell2 := "zone-2"

	tests := []struct {
		name string
		test func(t *testing.T, ts topoclient.Store)
	}{
		{
			name: "Empty cell returns empty list",
			test: func(t *testing.T, ts topoclient.Store) {
				ids, err := ts.GetMultipoolerIDsByCell(ctx, cell1)
				require.NoError(t, err)
				require.Empty(t, ids)
			},
		},
		{
			name: "Cell with multipoolers",
			test: func(t *testing.T, ts topoclient.Store) {
				multipoolers := []*clustermetadatapb.Multipooler{
					{
						Id: &clustermetadatapb.ID{
							Component: clustermetadatapb.ID_MULTIPOOLER,
							Cell:      cell1,
							Name:      "bravo",
						},
						ShardKey: &clustermetadatapb.ShardKey{
							Database:   "db1",
							TableGroup: "default",
							Shard:      "shard1",
						},
						Hostname:      "host1",
						PortMap:       map[string]int32{"grpc": 8080},
						Type:          clustermetadatapb.PoolerType_PRIMARY,
						ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
					},
					{
						Id: &clustermetadatapb.ID{
							Component: clustermetadatapb.ID_MULTIPOOLER,
							Cell:      cell1,
							Name:      "charlie",
						},
						ShardKey: &clustermetadatapb.ShardKey{
							Database:   "db2",
							TableGroup: "default",
							Shard:      "shard2",
						},
						Hostname:      "host3",
						PortMap:       map[string]int32{"grpc": 8083},
						Type:          clustermetadatapb.PoolerType_REPLICA,
						ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
					},
				}

				for _, mp := range multipoolers {
					require.NoError(t, ts.CreateMultipooler(ctx, mp))
				}

				ids, err := ts.GetMultipoolerIDsByCell(ctx, cell1)
				require.NoError(t, err)
				require.Len(t, ids, 2)

				expectedIDs := []*clustermetadatapb.ID{
					{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell1,
						Name:      "bravo",
					},
					{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell1,
						Name:      "charlie",
					},
				}

				slices.SortFunc(ids, func(a, b *clustermetadatapb.ID) int {
					return cmp.Compare(a.Name, b.Name)
				})

				for i, id := range ids {
					require.Equal(t, expectedIDs[i].Cell, id.Cell)
					require.Equal(t, expectedIDs[i].Name, id.Name)
				}

				// Verify cell boundary: multipoolers are NOT accessible from cell2
				cell2Ids, err := ts.GetMultipoolerIDsByCell(ctx, cell2)
				require.NoError(t, err)
				require.Empty(t, cell2Ids, "multipoolers should not be accessible from other cells")
			},
		},
		{
			name: "Nonexistent cell returns error",
			test: func(t *testing.T, ts topoclient.Store) {
				_, err := ts.GetMultipoolerIDsByCell(ctx, "nonexistent")
				require.Error(t, err)
				require.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}))
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

// TestUpdateMultipoolerFields tests the update fields functionality with retry logic
func TestUpdateMultipoolerFields(t *testing.T) {
	ctx := context.Background()
	cell := "zone-1"

	tests := []struct {
		name string
		test func(t *testing.T, ts topoclient.Store)
	}{
		{
			name: "Successful update",
			test: func(t *testing.T, ts topoclient.Store) {
				id := &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      cell,
					Name:      "tango",
				}
				multipooler := &clustermetadatapb.Multipooler{
					Id: id,
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				require.NoError(t, ts.CreateMultipooler(ctx, multipooler))

				updated, err := ts.UpdateMultipoolerFields(ctx, id, func(mp *clustermetadatapb.Multipooler) error {
					mp.Hostname = "newhost"
					mp.PortMap["http"] = 8081
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, "newhost", updated.Hostname)
				require.Equal(t, int32(8081), updated.PortMap["http"])

				retrieved, err := ts.GetMultipooler(ctx, id)
				require.NoError(t, err)
				require.Equal(t, "newhost", retrieved.Hostname)
				require.Equal(t, int32(8081), retrieved.PortMap["http"])
			},
		},
		{
			name: "Update function returns error",
			test: func(t *testing.T, ts topoclient.Store) {
				id := &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      cell,
					Name:      "uniform",
				}
				multipooler := &clustermetadatapb.Multipooler{
					Id: id,
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				require.NoError(t, ts.CreateMultipooler(ctx, multipooler))

				updateErr := errors.New("update failed")
				_, err := ts.UpdateMultipoolerFields(ctx, id, func(mp *clustermetadatapb.Multipooler) error {
					return updateErr
				})
				require.Error(t, err)
				require.Equal(t, updateErr, err)

				retrieved, err := ts.GetMultipooler(ctx, id)
				require.NoError(t, err)
				require.Equal(t, "host1", retrieved.Hostname)
			},
		},
		{
			name: "NoUpdateNeeded returns nil",
			test: func(t *testing.T, ts topoclient.Store) {
				id := &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      cell,
					Name:      "victor",
				}
				multipooler := &clustermetadatapb.Multipooler{
					Id: id,
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				require.NoError(t, ts.CreateMultipooler(ctx, multipooler))

				result, err := ts.UpdateMultipoolerFields(ctx, id, func(mp *clustermetadatapb.Multipooler) error {
					return &topoclient.TopoError{Code: topoclient.NoUpdateNeeded}
				})
				require.NoError(t, err)
				require.Nil(t, result)
			},
		},
		{
			name: "Retry on BadVersion error",
			test: func(t *testing.T, ts topoclient.Store) {
				tsWithFactory, factory := memorytopo.NewServerAndFactory(ctx, cell)
				defer tsWithFactory.Close()

				id := &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      cell,
					Name:      "whiskey",
				}
				multipooler := &clustermetadatapb.Multipooler{
					Id: id,
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				require.NoError(t, tsWithFactory.CreateMultipooler(ctx, multipooler))

				badVersionErr := &topoclient.TopoError{Code: topoclient.BadVersion}
				poolerPath := path.Join(topoclient.PoolersPath, string(topoclient.ComponentIDString(id)), topoclient.PoolerFile)
				factory.AddOneTimeOperationError(memorytopo.Update, poolerPath, badVersionErr)

				updateCallCount := 0
				updated, err := tsWithFactory.UpdateMultipoolerFields(ctx, id, func(mp *clustermetadatapb.Multipooler) error {
					updateCallCount++
					mp.Hostname = "newhost"
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, 2, updateCallCount)
				require.Equal(t, "newhost", updated.Hostname)

				retrieved, err := tsWithFactory.GetMultipooler(ctx, id)
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

// TestInitMultipooler tests the init multipooler functionality
func TestInitMultipooler(t *testing.T) {
	ctx := context.Background()
	cell := "zone-1"

	tests := []struct {
		name string
		test func(t *testing.T, ts topoclient.Store)
	}{
		{
			name: "Create new multipooler",
			test: func(t *testing.T, ts topoclient.Store) {
				multipooler := &clustermetadatapb.Multipooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "zulu",
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}

				err := ts.RegisterMultipooler(ctx, multipooler, false)
				require.NoError(t, err)

				retrieved, err := ts.GetMultipooler(ctx, multipooler.Id)
				require.NoError(t, err)
				checkMultipoolersEqual(t, multipooler, retrieved.Multipooler)
			},
		},
		{
			name: "Update existing multipooler with allowUpdate=true",
			test: func(t *testing.T, ts topoclient.Store) {
				original := &clustermetadatapb.Multipooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "xray",
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				require.NoError(t, ts.CreateMultipooler(ctx, original))

				updated := &clustermetadatapb.Multipooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "xray",
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "newhost",
					PortMap:       map[string]int32{"grpc": 8081},
					Type:          clustermetadatapb.PoolerType_REPLICA,
					ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED,
				}

				err := ts.RegisterMultipooler(ctx, updated, true)
				require.NoError(t, err)

				retrieved, err := ts.GetMultipooler(ctx, original.Id)
				require.NoError(t, err)
				checkMultipoolersEqual(t, updated, retrieved.Multipooler)
			},
		},
		{
			name: "Fail to update existing multipooler with allowUpdate=false",
			test: func(t *testing.T, ts topoclient.Store) {
				original := &clustermetadatapb.Multipooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "whiskey",
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "host1",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				require.NoError(t, ts.CreateMultipooler(ctx, original))

				updated := &clustermetadatapb.Multipooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "whiskey",
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "newhost",
					PortMap:       map[string]int32{"grpc": 8081},
					Type:          clustermetadatapb.PoolerType_REPLICA,
					ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED,
				}

				err := ts.RegisterMultipooler(ctx, updated, false)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}))
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

// TestNewMultipooler tests the factory function
func TestNewMultipooler(t *testing.T) {
	tests := []struct {
		testName string
		name     string
		cell     string
		host     string
		expected *clustermetadatapb.Multipooler
	}{
		{
			testName: "basic creation",
			name:     "100",
			cell:     "zone1",
			host:     "host.example.com",
			expected: &clustermetadatapb.Multipooler{
				Id: &clustermetadatapb.ID{
					Cell: "zone1",
					Name: "100",
				},
				Hostname: "host.example.com",
				PortMap:  map[string]int32{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			result := topoclient.NewMultipooler(tt.name, tt.cell, tt.host)
			require.Equal(t, tt.expected.Id.Cell, result.Id.Cell)
			require.Equal(t, tt.expected.Id.Name, result.Id.Name)
			require.Equal(t, tt.expected.Hostname, result.Hostname)
			require.Nil(t, result.ShardKey, "factory leaves ShardKey unset; caller fills it")
			require.NotNil(t, result.PortMap)
		})
	}

	// Test that empty name is passed through as-is (caller is responsible for generating IDs)
	t.Run("empty name is passed through", func(t *testing.T) {
		result := topoclient.NewMultipooler("", "zone2", "host2.example.com")

		// Verify basic properties
		require.Equal(t, "zone2", result.Id.Cell)
		require.Equal(t, "host2.example.com", result.Hostname)
		require.NotNil(t, result.PortMap)

		// Verify empty name is preserved (caller should use servenv.GenerateRandomServiceID if needed)
		require.Empty(t, result.Id.Name, "empty name should be passed through as-is")
	})
}

// TestMultipoolerDatabaseField tests database field handling
func TestMultipoolerDatabaseField(t *testing.T) {
	ctx := context.Background()
	cell := "zone-1"

	tests := []struct {
		name string
		test func(t *testing.T, ts topoclient.Store)
	}{
		{
			name: "Create Multipooler with database field",
			test: func(t *testing.T, ts topoclient.Store) {
				multipooler := &clustermetadatapb.Multipooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "db-test1",
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "host1.example.com",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				err := ts.CreateMultipooler(ctx, multipooler)
				require.NoError(t, err)

				retrieved, err := ts.GetMultipooler(ctx, multipooler.Id)
				require.NoError(t, err)
				require.Equal(t, "testdb", retrieved.ShardKey.Database)
			},
		},
		{
			name: "Update Multipooler preserves database field",
			test: func(t *testing.T, ts topoclient.Store) {
				multipooler := &clustermetadatapb.Multipooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "db-test2",
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "originaldb",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "host1.example.com",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				err := ts.CreateMultipooler(ctx, multipooler)
				require.NoError(t, err)

				retrieved, err := ts.GetMultipooler(ctx, multipooler.Id)
				require.NoError(t, err)
				oldVersion := retrieved.Version()

				// Update hostname but keep database
				retrieved.Hostname = "host2.example.com"

				err = ts.UpdateMultipooler(ctx, retrieved)
				require.NoError(t, err)

				updated, err := ts.GetMultipooler(ctx, multipooler.Id)
				require.NoError(t, err)
				require.Equal(t, "originaldb", updated.ShardKey.Database) // Database preserved
				require.Equal(t, "host2.example.com", updated.Hostname)
				require.NotEqual(t, oldVersion, updated.Version())
			},
		},
		{
			name: "Create Multipooler with empty database field",
			test: func(t *testing.T, ts topoclient.Store) {
				multipooler := &clustermetadatapb.Multipooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      cell,
						Name:      "db-test3",
					},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "",
						TableGroup: "default",
						Shard:      "testshard",
					},
					Hostname:      "host1.example.com",
					PortMap:       map[string]int32{"grpc": 8080},
					Type:          clustermetadatapb.PoolerType_PRIMARY,
					ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				}
				err := ts.CreateMultipooler(ctx, multipooler)
				require.NoError(t, err)

				retrieved, err := ts.GetMultipooler(ctx, multipooler.Id)
				require.NoError(t, err)
				require.Equal(t, "", retrieved.ShardKey.Database)
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

// TestMultipoolerInfo tests the MultipoolerInfo methods
func TestMultipoolerInfo(t *testing.T) {
	multipooler := &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "100",
		},
		Hostname: "host.example.com",
		PortMap: map[string]int32{
			"grpc": 8080,
			"http": 8081,
		},
	}
	version := memorytopo.NodeVersion(123)
	info := topoclient.NewMultipoolerInfo(multipooler, version)

	t.Run("String method", func(t *testing.T) {
		result := info.String()
		expected := "Multipooler{multipooler-zone1-100}"
		require.Equal(t, expected, result)
	})

	t.Run("IDString method", func(t *testing.T) {
		result := string(topoclient.ComponentIDString(info.Id))
		expected := "multipooler-zone1-100"
		require.Equal(t, expected, result)
	})

	t.Run("Addr method with grpc port", func(t *testing.T) {
		result := info.Addr()
		expected := "host.example.com:8080"
		require.Equal(t, expected, result)
	})

	t.Run("Addr method without grpc port", func(t *testing.T) {
		multipoolerNoGrpc := &clustermetadatapb.Multipooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "100",
			},
			Hostname: "host.example.com",
			PortMap: map[string]int32{
				"http": 8081,
			},
		}
		infoNoGrpc := topoclient.NewMultipoolerInfo(multipoolerNoGrpc, version)
		result := infoNoGrpc.Addr()
		expected := "host.example.com"
		require.Equal(t, expected, result)
	})

	t.Run("Version method", func(t *testing.T) {
		result := info.Version()
		require.Equal(t, version, result)
	})
}

// TestGetMultipoolersByCell covers comprehensive scenarios for the GetMultipoolersByCell method
func TestGetMultipoolersByCell_Comprehensive(t *testing.T) {
	ctx := utils.WithTimeout(t, 10*time.Second)

	t.Run("cell with multiple multipoolers without filtering", func(t *testing.T) {
		// Create fresh topo for this test with multiple cells
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
		defer ts.Close()

		// Setup: Create 4 multipoolers in zone1 (2 databases × 2 shards)
		multipoolers := []*clustermetadatapb.Multipooler{
			{
				Id: &clustermetadatapb.ID{Cell: "zone1", Name: "1"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "default",
					Shard:      "-8",
				},
				Hostname:      "host1",
				PortMap:       map[string]int32{"grpc": 8080},
				Type:          clustermetadatapb.PoolerType_PRIMARY,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
			{
				Id: &clustermetadatapb.ID{Cell: "zone1", Name: "2"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "default",
					Shard:      "8-",
				},
				Hostname:      "host2",
				PortMap:       map[string]int32{"grpc": 8081},
				Type:          clustermetadatapb.PoolerType_REPLICA,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
			{
				Id: &clustermetadatapb.ID{Cell: "zone1", Name: "3"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db2",
					TableGroup: "default",
					Shard:      "-8",
				},
				Hostname:      "host3",
				PortMap:       map[string]int32{"grpc": 8082},
				Type:          clustermetadatapb.PoolerType_PRIMARY,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
			{
				Id: &clustermetadatapb.ID{Cell: "zone1", Name: "4"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db2",
					TableGroup: "default",
					Shard:      "8-",
				},
				Hostname:      "host4",
				PortMap:       map[string]int32{"grpc": 8083},
				Type:          clustermetadatapb.PoolerType_REPLICA,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
		}

		// Create all multipoolers
		for _, mp := range multipoolers {
			require.NoError(t, ts.CreateMultipooler(ctx, mp))
		}

		// Test: Get all multipoolers without filtering
		multipoolerInfos, err := ts.GetMultipoolersByCell(ctx, "zone1", nil)
		require.NoError(t, err)
		require.Len(t, multipoolerInfos, 4)

		// Verify all multipoolers are returned
		expectedMPs := []*topoclient.MultipoolerInfo{
			{Multipooler: multipoolers[0]}, // db1, -8
			{Multipooler: multipoolers[1]}, // db1, 8-
			{Multipooler: multipoolers[2]}, // db2, -8
			{Multipooler: multipoolers[3]}, // db2, 8-
		}
		checkMultipoolerInfosEqual(t, expectedMPs, multipoolerInfos)

		// Verify cell boundary: multipoolers are NOT accessible from other cells
		otherCellInfos, err := ts.GetMultipoolersByCell(ctx, "zone2", nil)
		require.NoError(t, err)
		require.Empty(t, otherCellInfos, "multipoolers should not be accessible from other cells")
	})

	t.Run("cell with database filtering", func(t *testing.T) {
		// Create fresh topo for this test with multiple cells
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
		defer ts.Close()

		// Setup: Create 2 multipoolers for db1 in zone1
		multipoolers := []*clustermetadatapb.Multipooler{
			{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "1",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "default",
					Shard:      "-8",
				},
				Hostname:      "host1",
				PortMap:       map[string]int32{"grpc": 8080},
				Type:          clustermetadatapb.PoolerType_PRIMARY,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
			{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "2",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "default",
					Shard:      "8-",
				},
				Hostname:      "host2",
				PortMap:       map[string]int32{"grpc": 8081},
				Type:          clustermetadatapb.PoolerType_REPLICA,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
		}

		// Create multipoolers
		for _, mp := range multipoolers {
			require.NoError(t, ts.CreateMultipooler(ctx, mp))
		}

		// Test: Filter by database only (empty shard matches all)
		opts := &topoclient.GetMultipoolersByCellOptions{
			DatabaseShard: &topoclient.DatabaseShard{
				Database: "db1",
				Shard:    "", // empty shard matches all
			},
		}

		multipoolerInfos, err := ts.GetMultipoolersByCell(ctx, "zone1", opts)
		require.NoError(t, err)
		require.Len(t, multipoolerInfos, 2)

		// Verify only db1 multipoolers are returned
		for _, info := range multipoolerInfos {
			require.Equal(t, "db1", info.GetShardKey().GetDatabase())
		}

		// Verify cell boundary: multipoolers are NOT accessible from other cells
		otherCellInfos, err := ts.GetMultipoolersByCell(ctx, "zone2", nil)
		require.NoError(t, err)
		require.Empty(t, otherCellInfos, "multipoolers should not be accessible from other cells")
	})

	t.Run("cell with database and shard filtering", func(t *testing.T) {
		// Create fresh topo for this test with multiple cells
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
		defer ts.Close()

		// Setup: Create 2 multipoolers for db2 in zone1
		multipoolers := []*clustermetadatapb.Multipooler{
			{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "1",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db2",
					TableGroup: "tg1",
					Shard:      "-8",
				},
				Hostname:      "host1",
				PortMap:       map[string]int32{"grpc": 8080},
				Type:          clustermetadatapb.PoolerType_PRIMARY,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
			{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "2",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db2",
					TableGroup: "tg1",
					Shard:      "8-",
				},
				Hostname:      "host2",
				PortMap:       map[string]int32{"grpc": 8081},
				Type:          clustermetadatapb.PoolerType_REPLICA,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			},
		}

		// Create multipoolers
		for _, mp := range multipoolers {
			require.NoError(t, ts.CreateMultipooler(ctx, mp))
		}

		// Test: Filter by specific database, tablegroup, and shard
		opts := &topoclient.GetMultipoolersByCellOptions{
			DatabaseShard: &topoclient.DatabaseShard{
				Database:   "db2",
				TableGroup: "tg1",
				Shard:      "-8",
			},
		}

		multipoolerInfos, err := ts.GetMultipoolersByCell(ctx, "zone1", opts)
		require.NoError(t, err)
		require.Len(t, multipoolerInfos, 1)

		// Verify correct multipooler is returned
		require.Equal(t, "db2", multipoolerInfos[0].GetShardKey().GetDatabase())
		require.Equal(t, "tg1", multipoolerInfos[0].GetShardKey().GetTableGroup())
		require.Equal(t, "-8", multipoolerInfos[0].GetShardKey().GetShard())

		// Verify cell boundary: multipoolers are NOT accessible from other cells
		otherCellInfos, err := ts.GetMultipoolersByCell(ctx, "zone2", nil)
		require.NoError(t, err)
		require.Empty(t, otherCellInfos, "multipoolers should not be accessible from other cells")
	})

	t.Run("empty cell returns empty list", func(t *testing.T) {
		// Create fresh topo for this test
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		// Setup: No multipoolers created

		// Test: Get multipoolers from empty cell
		multipoolerInfos, err := ts.GetMultipoolersByCell(ctx, "zone1", nil)
		require.NoError(t, err)
		require.Empty(t, multipoolerInfos)
	})

	t.Run("nonexistent cell returns NoNode error", func(t *testing.T) {
		// Create fresh topo for this test
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		// Setup: No multipoolers created

		// Test: Try to get multipoolers from nonexistent cell
		_, err := ts.GetMultipoolersByCell(ctx, "nonexistent", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}))
	})

	t.Run("multipoolers are isolated between cells", func(t *testing.T) {
		// Create fresh topo for this test with multiple cells
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
		defer ts.Close()

		// Setup: Create multipoolers in both cells
		zone1Multipooler := &clustermetadatapb.Multipooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "1",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "default",
				Shard:      "-8",
			},
			Hostname:      "host1",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_PRIMARY,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		}
		zone2Multipooler := &clustermetadatapb.Multipooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone2",
				Name:      "1",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "default",
				Shard:      "-8",
			},
			Hostname:      "host2",
			PortMap:       map[string]int32{"grpc": 8081},
			Type:          clustermetadatapb.PoolerType_REPLICA,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		}

		// Create multipoolers in their respective cells
		require.NoError(t, ts.CreateMultipooler(ctx, zone1Multipooler))
		require.NoError(t, ts.CreateMultipooler(ctx, zone2Multipooler))

		// Test: Verify zone1 can only see its own multipooler
		zone1Infos, err := ts.GetMultipoolersByCell(ctx, "zone1", nil)
		require.NoError(t, err)
		require.Len(t, zone1Infos, 1)
		require.Equal(t, "zone1", zone1Infos[0].Id.Cell)
		require.Equal(t, "host1", zone1Infos[0].Hostname)

		// Test: Verify zone2 can only see its own multipooler
		zone2Infos, err := ts.GetMultipoolersByCell(ctx, "zone2", nil)
		require.NoError(t, err)
		require.Len(t, zone2Infos, 1)
		require.Equal(t, "zone2", zone2Infos[0].Id.Cell)
		require.Equal(t, "host2", zone2Infos[0].Hostname)

		// Test: Verify cross-cell access is properly isolated
		zone1FromZone2, err := ts.GetMultipooler(ctx, zone1Multipooler.Id)
		require.NoError(t, err, "should be able to get multipooler by ID regardless of current cell context")
		require.Equal(t, "zone1", zone1FromZone2.Id.Cell)

		zone2FromZone1, err := ts.GetMultipooler(ctx, zone2Multipooler.Id)
		require.NoError(t, err, "should be able to get multipooler by ID regardless of current cell context")
		require.Equal(t, "zone2", zone2FromZone1.Id.Cell)
	})
}
