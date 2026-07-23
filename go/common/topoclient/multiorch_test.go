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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/test/utils"
)

var multiorchs []*clustermetadatapb.Multiorch

func init() {
	uid := uint32(1)
	for _, cell := range cells {
		multiorch := getMultiorch(cell, uid)
		multiorchs = append(multiorchs, multiorch)
		uid++
	}
}

func getMultiorch(cell string, uid uint32) *clustermetadatapb.Multiorch {
	return &clustermetadatapb.Multiorch{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIORCH,
			Cell:      cell,
			Name:      strconv.FormatUint(uint64(uid), 10),
		},
		Hostname: "host1",
		PortMap: map[string]int32{
			"grpc": int32(uid),
			"http": int32(uid + 8080),
		},
	}
}

func checkMultiorchsEqual(t *testing.T, expected, actual *clustermetadatapb.Multiorch) {
	t.Helper()
	require.Equal(t, expected.Id.String(), actual.Id.String())
	require.Equal(t, expected.Hostname, actual.Hostname)
	require.Equal(t, expected.PortMap, actual.PortMap)
}

func checkMultiorchInfosEqual(t *testing.T, expected, actual []*topoclient.MultiorchInfo) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for _, actualMO := range actual {
		found := false
		for _, expectedMO := range expected {
			if topoclient.ComponentIDString(actualMO.Id) == topoclient.ComponentIDString(expectedMO.Id) {
				checkMultiorchsEqual(t, expectedMO.Multiorch, actualMO.Multiorch)
				found = true
				break
			}
		}
		require.True(t, found, "unexpected multiorch %v", topoclient.ComponentIDString(actualMO.Id))
	}
}

// Test various cases of calls to GetMultiorchsByCell.
func TestServerGetMultiorchsByCell(t *testing.T) {
	const cell = "zone1"

	tests := []struct {
		name                string
		createCellMultiorch int
		expectedMultiorch   []*clustermetadatapb.Multiorch
		listError           error
	}{
		{
			name:                "single",
			createCellMultiorch: 1,
			expectedMultiorch: []*clustermetadatapb.Multiorch{
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      "alpha",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": 1,
						"http": 8081,
					},
				},
			},
		},
		{
			name:                "multiple",
			createCellMultiorch: 4,
			expectedMultiorch: []*clustermetadatapb.Multiorch{
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      "beta",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": 1,
						"http": 8081,
					},
				},
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      "echo",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": 2,
						"http": 8082,
					},
				},
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      "foxtrot",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": 3,
						"http": 8083,
					},
				},
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      "golf",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": 4,
						"http": 8084,
					},
				},
			},
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

			// Create multiorchs with names from expected results
			for i, expectedMO := range tt.expectedMultiorch {
				multiorch := &clustermetadatapb.Multiorch{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      expectedMO.Id.Name,
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc": int32(i + 1),
						"http": int32(i + 1 + 8080),
					},
				}
				require.NoError(t, ts.CreateMultiorch(ctx, multiorch))
			}

			out, err := ts.GetMultiorchsByCell(ctx, cell)
			require.NoError(t, err)
			require.Len(t, out, len(tt.expectedMultiorch))

			slices.SortFunc(out, func(i, j *topoclient.MultiorchInfo) int {
				return cmp.Compare(i.Id.Name, j.Id.Name)
			})
			slices.SortFunc(tt.expectedMultiorch, func(i, j *clustermetadatapb.Multiorch) int {
				return cmp.Compare(i.Id.Name, j.Id.Name)
			})

			for i, multiorchInfo := range out {
				checkMultiorchsEqual(t, tt.expectedMultiorch[i], multiorchInfo.Multiorch)
			}
		})
	}
}

// TestMultiorchIDString tests the ID string functionality
func TestMultiorchIDString(t *testing.T) {
	tests := []struct {
		name     string
		id       *clustermetadatapb.ID
		expected string
	}{
		{
			name:     "simple case",
			id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "zone1", Name: "100"},
			expected: "multiorch-zone1-100",
		},
		{
			name:     "you can use name as numbers",
			id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "prod", Name: "0"},
			expected: "multiorch-prod-0",
		},
		{
			name:     "funny name",
			id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "prod", Name: "sleepy"},
			expected: "multiorch-prod-sleepy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := string(topoclient.ComponentIDString(tt.id))
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestMultiorchCRUDOperations tests basic CRUD operations for multiorchs
func TestMultiorchCRUDOperations(t *testing.T) {
	ctx := context.Background()
	cell := "zone-1"

	tests := []struct {
		name string
		test func(t *testing.T, ts topoclient.Store)
	}{
		{
			name: "Create and Get Multiorch",
			test: func(t *testing.T, ts topoclient.Store) {
				multiorch := &clustermetadatapb.Multiorch{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      "november",
					},
					Hostname: "host1.example.com",
					PortMap:  map[string]int32{"grpc": 8080, "http": 9090},
				}
				err := ts.CreateMultiorch(ctx, multiorch)
				require.NoError(t, err)

				retrieved, err := ts.GetMultiorch(ctx, multiorch.Id)
				require.NoError(t, err)
				checkMultiorchsEqual(t, multiorch, retrieved.Multiorch)
				require.NotZero(t, retrieved.Version())
			},
		},
		{
			name: "Get nonexistent Multiorch",
			test: func(t *testing.T, ts topoclient.Store) {
				id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: cell, Name: "999"}
				_, err := ts.GetMultiorch(ctx, id)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}))
			},
		},
		{
			name: "Create duplicate Multiorch fails",
			test: func(t *testing.T, ts topoclient.Store) {
				multiorch := &clustermetadatapb.Multiorch{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      "oscar",
					},
					Hostname: "host1.example.com",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				err := ts.CreateMultiorch(ctx, multiorch)
				require.NoError(t, err)

				err = ts.CreateMultiorch(ctx, multiorch)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}))
			},
		},
		{
			name: "Update Multiorch",
			test: func(t *testing.T, ts topoclient.Store) {
				multiorch := &clustermetadatapb.Multiorch{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      "papa",
					},
					Hostname: "host1.example.com",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				err := ts.CreateMultiorch(ctx, multiorch)
				require.NoError(t, err)

				retrieved, err := ts.GetMultiorch(ctx, multiorch.Id)
				require.NoError(t, err)
				oldVersion := retrieved.Version()

				retrieved.Hostname = "host2.example.com"
				retrieved.PortMap["http"] = 9090

				err = ts.UpdateMultiorch(ctx, retrieved)
				require.NoError(t, err)

				updated, err := ts.GetMultiorch(ctx, multiorch.Id)
				require.NoError(t, err)
				require.Equal(t, "host2.example.com", updated.Hostname)
				require.Equal(t, int32(9090), updated.PortMap["http"])
				require.NotEqual(t, oldVersion, updated.Version())
			},
		},
		{
			name: "Delete Multiorch",
			test: func(t *testing.T, ts topoclient.Store) {
				multiorch := &clustermetadatapb.Multiorch{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      "quebec",
					},
					Hostname: "host1.example.com",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				err := ts.CreateMultiorch(ctx, multiorch)
				require.NoError(t, err)

				err = ts.UnregisterMultiorch(ctx, multiorch.Id)
				require.NoError(t, err)

				_, err = ts.GetMultiorch(ctx, multiorch.Id)
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

// TestGetMultiorchIDsByCell tests getting multiorch IDs by cell
func TestGetMultiorchIDsByCell(t *testing.T) {
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
				ids, err := ts.GetMultiorchIDsByCell(ctx, cell1)
				require.NoError(t, err)
				require.Empty(t, ids)
			},
		},
		{
			name: "Cell with multiorchs",
			test: func(t *testing.T, ts topoclient.Store) {
				multiorchs := []*clustermetadatapb.Multiorch{
					{
						Id: &clustermetadatapb.ID{
							Component: clustermetadatapb.ID_MULTIORCH,
							Cell:      cell1,
							Name:      "bravo",
						},
						Hostname: "host1",
						PortMap:  map[string]int32{"grpc": 8080},
					},
					{
						Id: &clustermetadatapb.ID{
							Component: clustermetadatapb.ID_MULTIORCH,
							Cell:      cell1,
							Name:      "charlie",
						},
						Hostname: "host3",
						PortMap:  map[string]int32{"grpc": 8083},
					},
				}

				for _, mo := range multiorchs {
					require.NoError(t, ts.CreateMultiorch(ctx, mo))
				}

				ids, err := ts.GetMultiorchIDsByCell(ctx, cell1)
				require.NoError(t, err)
				require.Len(t, ids, 2)

				expectedIDs := []*clustermetadatapb.ID{
					{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell1,
						Name:      "bravo",
					},
					{
						Component: clustermetadatapb.ID_MULTIORCH,
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

				// Verify cell boundary: multiorchs are NOT accessible from cell2
				cell2Ids, err := ts.GetMultiorchIDsByCell(ctx, cell2)
				require.NoError(t, err)
				require.Empty(t, cell2Ids, "multiorchs should not be accessible from other cells")
			},
		},
		{
			name: "Nonexistent cell returns error",
			test: func(t *testing.T, ts topoclient.Store) {
				_, err := ts.GetMultiorchIDsByCell(ctx, "nonexistent")
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

// TestUpdateMultiorchFields tests the update fields functionality with retry logic
func TestUpdateMultiorchFields(t *testing.T) {
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
					Component: clustermetadatapb.ID_MULTIORCH,
					Cell:      cell,
					Name:      "tango",
				}
				multiorch := &clustermetadatapb.Multiorch{
					Id:       id,
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				require.NoError(t, ts.CreateMultiorch(ctx, multiorch))

				updated, err := ts.UpdateMultiorchFields(ctx, id, func(mo *clustermetadatapb.Multiorch) error {
					mo.Hostname = "newhost"
					mo.PortMap["http"] = 9090
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, "newhost", updated.Hostname)
				require.Equal(t, int32(9090), updated.PortMap["http"])

				retrieved, err := ts.GetMultiorch(ctx, id)
				require.NoError(t, err)
				require.Equal(t, "newhost", retrieved.Hostname)
				require.Equal(t, int32(9090), retrieved.PortMap["http"])
			},
		},
		{
			name: "Update function returns error",
			test: func(t *testing.T, ts topoclient.Store) {
				id := &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIORCH,
					Cell:      cell,
					Name:      "uniform",
				}
				multiorch := &clustermetadatapb.Multiorch{
					Id:       id,
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				require.NoError(t, ts.CreateMultiorch(ctx, multiorch))

				updateErr := errors.New("update failed")
				_, err := ts.UpdateMultiorchFields(ctx, id, func(mo *clustermetadatapb.Multiorch) error {
					return updateErr
				})
				require.Error(t, err)
				require.Equal(t, updateErr, err)

				retrieved, err := ts.GetMultiorch(ctx, id)
				require.NoError(t, err)
				require.Equal(t, "host1", retrieved.Hostname)
			},
		},
		{
			name: "NoUpdateNeeded returns nil",
			test: func(t *testing.T, ts topoclient.Store) {
				id := &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIORCH,
					Cell:      cell,
					Name:      "victor",
				}
				multiorch := &clustermetadatapb.Multiorch{
					Id:       id,
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				require.NoError(t, ts.CreateMultiorch(ctx, multiorch))

				result, err := ts.UpdateMultiorchFields(ctx, id, func(mo *clustermetadatapb.Multiorch) error {
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
					Component: clustermetadatapb.ID_MULTIORCH,
					Cell:      cell,
					Name:      "whiskey",
				}
				multiorch := &clustermetadatapb.Multiorch{
					Id:       id,
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				require.NoError(t, tsWithFactory.CreateMultiorch(ctx, multiorch))

				badVersionErr := &topoclient.TopoError{Code: topoclient.BadVersion}
				orchPath := path.Join(topoclient.OrchsPath, string(topoclient.ComponentIDString(id)), topoclient.OrchFile)
				factory.AddOneTimeOperationError(memorytopo.Update, orchPath, badVersionErr)

				updateCallCount := 0
				updated, err := tsWithFactory.UpdateMultiorchFields(ctx, id, func(mo *clustermetadatapb.Multiorch) error {
					updateCallCount++
					mo.Hostname = "newhost"
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, 2, updateCallCount)
				require.Equal(t, "newhost", updated.Hostname)

				retrieved, err := tsWithFactory.GetMultiorch(ctx, id)
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

// TestInitMultiorch tests the init multiorch functionality
func TestInitMultiorch(t *testing.T) {
	ctx := context.Background()
	cell := "zone-1"

	tests := []struct {
		name string
		test func(t *testing.T, ts topoclient.Store)
	}{
		{
			name: "Create new multiorch",
			test: func(t *testing.T, ts topoclient.Store) {
				multiorch := &clustermetadatapb.Multiorch{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      "zulu",
					},
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": 8080},
				}

				err := ts.RegisterMultiorch(ctx, multiorch, false)
				require.NoError(t, err)

				retrieved, err := ts.GetMultiorch(ctx, multiorch.Id)
				require.NoError(t, err)
				checkMultiorchsEqual(t, multiorch, retrieved.Multiorch)
			},
		},
		{
			name: "Update existing multiorch with allowUpdate=true",
			test: func(t *testing.T, ts topoclient.Store) {
				original := &clustermetadatapb.Multiorch{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      "xray",
					},
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				require.NoError(t, ts.CreateMultiorch(ctx, original))

				updated := &clustermetadatapb.Multiorch{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      "xray",
					},
					Hostname: "newhost",
					PortMap:  map[string]int32{"grpc": 8081, "http": 9090},
				}

				err := ts.RegisterMultiorch(ctx, updated, true)
				require.NoError(t, err)

				retrieved, err := ts.GetMultiorch(ctx, original.Id)
				require.NoError(t, err)
				checkMultiorchsEqual(t, updated, retrieved.Multiorch)
			},
		},
		{
			name: "Fail to update existing multiorch with allowUpdate=false",
			test: func(t *testing.T, ts topoclient.Store) {
				original := &clustermetadatapb.Multiorch{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      "whiskey",
					},
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				require.NoError(t, ts.CreateMultiorch(ctx, original))

				updated := &clustermetadatapb.Multiorch{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIORCH,
						Cell:      cell,
						Name:      "whiskey",
					},
					Hostname: "newhost",
					PortMap:  map[string]int32{"grpc": 8081},
				}

				err := ts.RegisterMultiorch(ctx, updated, false)
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

// TestNewMultiorch tests the factory function
func TestNewMultiorch(t *testing.T) {
	tests := []struct {
		testName string
		name     string
		cell     string
		host     string
		expected *clustermetadatapb.Multiorch
	}{
		{
			testName: "basic creation",
			name:     "100",
			cell:     "zone1",
			host:     "host.example.com",
			expected: &clustermetadatapb.Multiorch{
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
			result := topoclient.NewMultiorch(tt.name, tt.cell, tt.host)
			require.Equal(t, tt.expected.Id.Cell, result.Id.Cell)
			require.Equal(t, tt.expected.Id.Name, result.Id.Name)
			require.Equal(t, tt.expected.Hostname, result.Hostname)
			require.NotNil(t, result.PortMap)
		})
	}

	// Test that empty name is passed through as-is (caller is responsible for generating IDs)
	t.Run("empty name is passed through", func(t *testing.T) {
		result := topoclient.NewMultiorch("", "zone2", "host2.example.com")

		// Verify basic properties
		require.Equal(t, "zone2", result.Id.Cell)
		require.Equal(t, "host2.example.com", result.Hostname)
		require.NotNil(t, result.PortMap)

		// Verify empty name is preserved (caller should use servenv.GenerateRandomServiceID if needed)
		require.Empty(t, result.Id.Name, "empty name should be passed through as-is")
	})
}

// TestMultiorchInfo tests the MultiorchInfo methods
func TestMultiorchInfo(t *testing.T) {
	multiorch := &clustermetadatapb.Multiorch{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIORCH,
			Cell:      "zone1",
			Name:      "100",
		},
		Hostname: "host.example.com",
		PortMap: map[string]int32{
			"grpc": 8080,
			"http": 9090,
		},
	}
	version := memorytopo.NodeVersion(123)
	info := topoclient.NewMultiorchInfo(multiorch, version)

	t.Run("String method", func(t *testing.T) {
		result := info.String()
		expected := "Multiorch{multiorch-zone1-100}"
		require.Equal(t, expected, result)
	})

	t.Run("IDString method", func(t *testing.T) {
		result := string(topoclient.ComponentIDString(info.Id))
		expected := "multiorch-zone1-100"
		require.Equal(t, expected, result)
	})

	t.Run("Addr method with grpc port", func(t *testing.T) {
		result := info.Addr()
		expected := "host.example.com:8080"
		require.Equal(t, expected, result)
	})

	t.Run("Addr method without grpc port", func(t *testing.T) {
		multiorchNoGrpc := &clustermetadatapb.Multiorch{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIORCH,
				Cell:      "zone1",
				Name:      "100",
			},
			Hostname: "host.example.com",
			PortMap: map[string]int32{
				"http": 9090,
			},
		}
		infoNoGrpc := topoclient.NewMultiorchInfo(multiorchNoGrpc, version)
		result := infoNoGrpc.Addr()
		expected := "host.example.com"
		require.Equal(t, expected, result)
	})

	t.Run("Version method", func(t *testing.T) {
		result := info.Version()
		require.Equal(t, version, result)
	})
}

// TestGetMultiorchsByCell covers comprehensive scenarios for the GetMultiorchsByCell method
func TestGetMultiorchsByCell_Comprehensive(t *testing.T) {
	ctx := utils.WithTimeout(t, 10*time.Second)

	t.Run("cell with multiple multiorchs", func(t *testing.T) {
		// Create fresh topo for this test with multiple cells
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
		defer ts.Close()

		// Setup: Create 3 multiorchs in zone1
		multiorchs := []*clustermetadatapb.Multiorch{
			{
				Id:       &clustermetadatapb.ID{Cell: "zone1", Name: "1"},
				Hostname: "host1",
				PortMap:  map[string]int32{"grpc": 8080, "http": 9090},
			},
			{
				Id:       &clustermetadatapb.ID{Cell: "zone1", Name: "2"},
				Hostname: "host2",
				PortMap:  map[string]int32{"grpc": 8081, "http": 9091},
			},
			{
				Id:       &clustermetadatapb.ID{Cell: "zone1", Name: "3"},
				Hostname: "host3",
				PortMap:  map[string]int32{"grpc": 8082, "http": 9092},
			},
		}

		// Create all multiorchs
		for _, mo := range multiorchs {
			require.NoError(t, ts.CreateMultiorch(ctx, mo))
		}

		// Test: Get all multiorchs
		multiorchInfos, err := ts.GetMultiorchsByCell(ctx, "zone1")
		require.NoError(t, err)
		require.Len(t, multiorchInfos, 3)

		// Verify all multiorchs are returned
		expectedMOs := []*topoclient.MultiorchInfo{
			{Multiorch: multiorchs[0]},
			{Multiorch: multiorchs[1]},
			{Multiorch: multiorchs[2]},
		}
		checkMultiorchInfosEqual(t, expectedMOs, multiorchInfos)

		// Verify cell boundary: multiorchs are NOT accessible from other cells
		otherCellInfos, err := ts.GetMultiorchsByCell(ctx, "zone2")
		require.NoError(t, err)
		require.Empty(t, otherCellInfos, "multiorchs should not be accessible from other cells")
	})

	t.Run("empty cell returns empty list", func(t *testing.T) {
		// Create fresh topo for this test
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		// Setup: No multiorchs created

		// Test: Get multiorchs from empty cell
		multiorchInfos, err := ts.GetMultiorchsByCell(ctx, "zone1")
		require.NoError(t, err)
		require.Empty(t, multiorchInfos)
	})

	t.Run("nonexistent cell returns NoNode error", func(t *testing.T) {
		// Create fresh topo for this test
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		// Setup: No multiorchs created

		// Test: Try to get multiorchs from nonexistent cell
		_, err := ts.GetMultiorchsByCell(ctx, "nonexistent")
		require.Error(t, err)
		require.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}))
	})

	t.Run("multiorchs are isolated between cells", func(t *testing.T) {
		// Create fresh topo for this test with multiple cells
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
		defer ts.Close()

		// Setup: Create multiorchs in both cells
		zone1Multiorch := &clustermetadatapb.Multiorch{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIORCH,
				Cell:      "zone1",
				Name:      "1",
			},
			Hostname: "host1",
			PortMap:  map[string]int32{"grpc": 8080, "http": 9090},
		}
		zone2Multiorch := &clustermetadatapb.Multiorch{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIORCH,
				Cell:      "zone2",
				Name:      "1",
			},
			Hostname: "host2",
			PortMap:  map[string]int32{"grpc": 8081, "http": 9091},
		}

		// Create multiorchs in their respective cells
		require.NoError(t, ts.CreateMultiorch(ctx, zone1Multiorch))
		require.NoError(t, ts.CreateMultiorch(ctx, zone2Multiorch))

		// Test: Verify zone1 can only see its own multiorch
		zone1Infos, err := ts.GetMultiorchsByCell(ctx, "zone1")
		require.NoError(t, err)
		require.Len(t, zone1Infos, 1)
		require.Equal(t, "zone1", zone1Infos[0].Id.Cell)
		require.Equal(t, "host1", zone1Infos[0].Hostname)

		// Test: Verify zone2 can only see its own multiorch
		zone2Infos, err := ts.GetMultiorchsByCell(ctx, "zone2")
		require.NoError(t, err)
		require.Len(t, zone2Infos, 1)
		require.Equal(t, "zone2", zone2Infos[0].Id.Cell)
		require.Equal(t, "host2", zone2Infos[0].Hostname)

		// Test: Verify cross-cell access is properly isolated
		zone1FromZone2, err := ts.GetMultiorch(ctx, zone1Multiorch.Id)
		require.NoError(t, err, "should be able to get multiorch by ID regardless of current cell context")
		require.Equal(t, "zone1", zone1FromZone2.Id.Cell)

		zone2FromZone1, err := ts.GetMultiorch(ctx, zone2Multiorch.Id)
		require.NoError(t, err, "should be able to get multiorch by ID regardless of current cell context")
		require.Equal(t, "zone2", zone2FromZone1.Id.Cell)
	})
}
