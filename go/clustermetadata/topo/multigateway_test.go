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
	"fmt"
	"path"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
)

var (
	multigateways []*clustermetadatapb.MultiGateway
)

func init() {
	uid := uint32(1)
	for _, cell := range cells {
		multigateway := getMultiGateway(cell, uid)
		multigateways = append(multigateways, multigateway)
		uid++
	}
}

func getMultiGateway(cell string, uid uint32) *clustermetadatapb.MultiGateway {
	return &clustermetadatapb.MultiGateway{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIGATEWAY,
			Cell:      cell,
			Name:      fmt.Sprintf("%d", uid),
		},
		Hostname: "host1",
		PortMap: map[string]int32{
			"grpc":     int32(uid),
			"postgres": int32(uid + 5432),
		},
	}
}

func checkMultiGatewaysEqual(t *testing.T, expected, actual *clustermetadatapb.MultiGateway) {
	t.Helper()
	require.Equal(t, expected.Id.String(), actual.Id.String())
	require.Equal(t, expected.Hostname, actual.Hostname)
	require.Equal(t, expected.PortMap, actual.PortMap)
}

func checkMultiGatewayInfosEqual(t *testing.T, expected, actual []*topo.MultiGatewayInfo) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for _, actualMG := range actual {
		found := false
		for _, expectedMG := range expected {
			if topo.MultiGatewayIDString(actualMG.Id) == topo.MultiGatewayIDString(expectedMG.Id) {
				checkMultiGatewaysEqual(t, expectedMG.MultiGateway, actualMG.MultiGateway)
				found = true
				break
			}
		}
		require.True(t, found, "unexpected multigateway %v", actualMG.IDString())
	}
}

// Test various cases of calls to GetMultiGatewaysByCell.
func TestServerGetMultiGatewaysByCell(t *testing.T) {
	const cell = "zone1"

	tests := []struct {
		name                    string
		createCellMultiGateways int
		expectedMultiGateways   []*clustermetadatapb.MultiGateway
		listError               error
	}{
		{
			name:                    "single",
			createCellMultiGateways: 1,
			expectedMultiGateways: []*clustermetadatapb.MultiGateway{
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      "alpha",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc":     1,
						"postgres": 5433,
					},
				},
			},
		},
		{
			name:                    "multiple",
			createCellMultiGateways: 4,
			expectedMultiGateways: []*clustermetadatapb.MultiGateway{
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      "beta",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc":     1,
						"postgres": 5433,
					},
				},
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      "echo",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc":     2,
						"postgres": 5434,
					},
				},
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      "foxtrot",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc":     3,
						"postgres": 5435,
					},
				},
				{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      "golf",
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc":     4,
						"postgres": 5436,
					},
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

			// Create multigateways with names from expected results
			for i, expectedMG := range tt.expectedMultiGateways {
				multigateway := &clustermetadatapb.MultiGateway{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      expectedMG.Id.Name,
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"grpc":     int32(i + 1),
						"postgres": int32(i + 1 + 5432),
					},
				}
				require.NoError(t, ts.CreateMultiGateway(ctx, multigateway))
			}

			out, err := ts.GetMultiGatewaysByCell(ctx, cell)
			require.NoError(t, err)
			require.Len(t, out, len(tt.expectedMultiGateways))

			slices.SortFunc(out, func(i, j *topo.MultiGatewayInfo) int {
				return cmp.Compare(i.Id.Name, j.Id.Name)
			})
			slices.SortFunc(tt.expectedMultiGateways, func(i, j *clustermetadatapb.MultiGateway) int {
				return cmp.Compare(i.Id.Name, j.Id.Name)
			})

			for i, multigatewayInfo := range out {
				checkMultiGatewaysEqual(t, tt.expectedMultiGateways[i], multigatewayInfo.MultiGateway)
			}
		})
	}
}

// TestMultiGatewayIDString tests the ID string functionality
func TestMultiGatewayIDString(t *testing.T) {
	tests := []struct {
		name     string
		id       *clustermetadatapb.ID
		expected string
	}{
		{
			name:     "simple case",
			id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIGATEWAY, Cell: "zone1", Name: "100"},
			expected: "multigateway-zone1-100",
		},
		{
			name:     "you can use name as numbers",
			id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIGATEWAY, Cell: "prod", Name: "0"},
			expected: "multigateway-prod-0",
		},
		{
			name:     "funny name",
			id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIGATEWAY, Cell: "prod", Name: "sleepy"},
			expected: "multigateway-prod-sleepy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := topo.MultiGatewayIDString(tt.id)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestMultiGatewayCRUDOperations tests basic CRUD operations for multigateways
func TestMultiGatewayCRUDOperations(t *testing.T) {
	ctx := context.Background()
	cell := "zone-1"

	tests := []struct {
		name string
		test func(t *testing.T, ts topo.Store)
	}{
		{
			name: "Create and Get MultiGateway",
			test: func(t *testing.T, ts topo.Store) {
				multigateway := &clustermetadatapb.MultiGateway{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      "november",
					},
					Hostname: "host1.example.com",
					PortMap:  map[string]int32{"grpc": 8080, "postgres": 5432},
				}
				err := ts.CreateMultiGateway(ctx, multigateway)
				require.NoError(t, err)

				retrieved, err := ts.GetMultiGateway(ctx, multigateway.Id)
				require.NoError(t, err)
				checkMultiGatewaysEqual(t, multigateway, retrieved.MultiGateway)
				require.NotZero(t, retrieved.Version())
			},
		},
		{
			name: "Get nonexistent MultiGateway",
			test: func(t *testing.T, ts topo.Store) {
				id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIGATEWAY, Cell: cell, Name: "999"}
				_, err := ts.GetMultiGateway(ctx, id)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}))
			},
		},
		{
			name: "Create duplicate MultiGateway fails",
			test: func(t *testing.T, ts topo.Store) {
				multigateway := &clustermetadatapb.MultiGateway{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      "oscar",
					},
					Hostname: "host1.example.com",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				err := ts.CreateMultiGateway(ctx, multigateway)
				require.NoError(t, err)

				err = ts.CreateMultiGateway(ctx, multigateway)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NodeExists}))
			},
		},
		{
			name: "Update MultiGateway",
			test: func(t *testing.T, ts topo.Store) {
				multigateway := &clustermetadatapb.MultiGateway{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      "papa",
					},
					Hostname: "host1.example.com",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				err := ts.CreateMultiGateway(ctx, multigateway)
				require.NoError(t, err)

				retrieved, err := ts.GetMultiGateway(ctx, multigateway.Id)
				require.NoError(t, err)
				oldVersion := retrieved.Version()

				retrieved.Hostname = "host2.example.com"
				retrieved.PortMap["postgres"] = 5432

				err = ts.UpdateMultiGateway(ctx, retrieved)
				require.NoError(t, err)

				updated, err := ts.GetMultiGateway(ctx, multigateway.Id)
				require.NoError(t, err)
				require.Equal(t, "host2.example.com", updated.Hostname)
				require.Equal(t, int32(5432), updated.PortMap["postgres"])
				require.NotEqual(t, oldVersion, updated.Version())
			},
		},
		{
			name: "Delete MultiGateway",
			test: func(t *testing.T, ts topo.Store) {
				multigateway := &clustermetadatapb.MultiGateway{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      "quebec",
					},
					Hostname: "host1.example.com",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				err := ts.CreateMultiGateway(ctx, multigateway)
				require.NoError(t, err)

				err = ts.DeleteMultiGateway(ctx, multigateway.Id)
				require.NoError(t, err)

				_, err = ts.GetMultiGateway(ctx, multigateway.Id)
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

// TestGetMultiGatewayIDsByCell tests getting multigateway IDs by cell
func TestGetMultiGatewayIDsByCell(t *testing.T) {
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
				ids, err := ts.GetMultiGatewayIDsByCell(ctx, cell1)
				require.NoError(t, err)
				require.Empty(t, ids)
			},
		},
		{
			name: "Cell with multigateways",
			test: func(t *testing.T, ts topo.Store) {
				multigateways := []*clustermetadatapb.MultiGateway{
					{
						Id: &clustermetadatapb.ID{
							Component: clustermetadatapb.ID_MULTIGATEWAY,
							Cell:      cell1,
							Name:      "bravo",
						},
						Hostname: "host1",
						PortMap:  map[string]int32{"grpc": 8080},
					},
					{
						Id: &clustermetadatapb.ID{
							Component: clustermetadatapb.ID_MULTIGATEWAY,
							Cell:      cell1,
							Name:      "charlie",
						},
						Hostname: "host3",
						PortMap:  map[string]int32{"grpc": 8083},
					},
				}

				for _, mg := range multigateways {
					require.NoError(t, ts.CreateMultiGateway(ctx, mg))
				}

				ids, err := ts.GetMultiGatewayIDsByCell(ctx, cell1)
				require.NoError(t, err)
				require.Len(t, ids, 2)

				expectedIDs := []*clustermetadatapb.ID{
					{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell1,
						Name:      "bravo",
					},
					{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
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

				// Verify cell boundary: multigateways are NOT accessible from cell2
				cell2Ids, err := ts.GetMultiGatewayIDsByCell(ctx, cell2)
				require.NoError(t, err)
				require.Empty(t, cell2Ids, "multigateways should not be accessible from other cells")
			},
		},
		{
			name: "Nonexistent cell returns error",
			test: func(t *testing.T, ts topo.Store) {
				_, err := ts.GetMultiGatewayIDsByCell(ctx, "nonexistent")
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

// TestUpdateMultiGatewayFields tests the update fields functionality with retry logic
func TestUpdateMultiGatewayFields(t *testing.T) {
	ctx := context.Background()
	cell := "zone-1"

	tests := []struct {
		name string
		test func(t *testing.T, ts topo.Store)
	}{
		{
			name: "Successful update",
			test: func(t *testing.T, ts topo.Store) {
				id := &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIGATEWAY,
					Cell:      cell,
					Name:      "tango",
				}
				multigateway := &clustermetadatapb.MultiGateway{
					Id:       id,
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				require.NoError(t, ts.CreateMultiGateway(ctx, multigateway))

				updated, err := ts.UpdateMultiGatewayFields(ctx, id, func(mg *clustermetadatapb.MultiGateway) error {
					mg.Hostname = "newhost"
					mg.PortMap["postgres"] = 5432
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, "newhost", updated.Hostname)
				require.Equal(t, int32(5432), updated.PortMap["postgres"])

				retrieved, err := ts.GetMultiGateway(ctx, id)
				require.NoError(t, err)
				require.Equal(t, "newhost", retrieved.Hostname)
				require.Equal(t, int32(5432), retrieved.PortMap["postgres"])
			},
		},
		{
			name: "Update function returns error",
			test: func(t *testing.T, ts topo.Store) {
				id := &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIGATEWAY,
					Cell:      cell,
					Name:      "uniform",
				}
				multigateway := &clustermetadatapb.MultiGateway{
					Id:       id,
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				require.NoError(t, ts.CreateMultiGateway(ctx, multigateway))

				updateErr := errors.New("update failed")
				_, err := ts.UpdateMultiGatewayFields(ctx, id, func(mg *clustermetadatapb.MultiGateway) error {
					return updateErr
				})
				require.Error(t, err)
				require.Equal(t, updateErr, err)

				retrieved, err := ts.GetMultiGateway(ctx, id)
				require.NoError(t, err)
				require.Equal(t, "host1", retrieved.Hostname)
			},
		},
		{
			name: "NoUpdateNeeded returns nil",
			test: func(t *testing.T, ts topo.Store) {
				id := &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIGATEWAY,
					Cell:      cell,
					Name:      "victor",
				}
				multigateway := &clustermetadatapb.MultiGateway{
					Id:       id,
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				require.NoError(t, ts.CreateMultiGateway(ctx, multigateway))

				result, err := ts.UpdateMultiGatewayFields(ctx, id, func(mg *clustermetadatapb.MultiGateway) error {
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

				id := &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIGATEWAY,
					Cell:      cell,
					Name:      "whiskey",
				}
				multigateway := &clustermetadatapb.MultiGateway{
					Id:       id,
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				require.NoError(t, tsWithFactory.CreateMultiGateway(ctx, multigateway))

				badVersionErr := &topo.TopoError{Code: topo.BadVersion}
				gatewayPath := path.Join(topo.GatewaysPath, topo.MultiGatewayIDString(id), topo.GatewayFile)
				factory.AddOneTimeOperationError(memorytopo.Update, gatewayPath, badVersionErr)

				updateCallCount := 0
				updated, err := tsWithFactory.UpdateMultiGatewayFields(ctx, id, func(mg *clustermetadatapb.MultiGateway) error {
					updateCallCount++
					mg.Hostname = "newhost"
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, 2, updateCallCount)
				require.Equal(t, "newhost", updated.Hostname)

				retrieved, err := tsWithFactory.GetMultiGateway(ctx, id)
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

// TestInitMultiGateway tests the init multigateway functionality
func TestInitMultiGateway(t *testing.T) {
	ctx := context.Background()
	cell := "zone-1"

	tests := []struct {
		name string
		test func(t *testing.T, ts topo.Store)
	}{
		{
			name: "Create new multigateway",
			test: func(t *testing.T, ts topo.Store) {
				multigateway := &clustermetadatapb.MultiGateway{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      "zulu",
					},
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": 8080},
				}

				err := ts.InitMultiGateway(ctx, multigateway, false)
				require.NoError(t, err)

				retrieved, err := ts.GetMultiGateway(ctx, multigateway.Id)
				require.NoError(t, err)
				checkMultiGatewaysEqual(t, multigateway, retrieved.MultiGateway)
			},
		},
		{
			name: "Update existing multigateway with allowUpdate=true",
			test: func(t *testing.T, ts topo.Store) {
				original := &clustermetadatapb.MultiGateway{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      "xray",
					},
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				require.NoError(t, ts.CreateMultiGateway(ctx, original))

				updated := &clustermetadatapb.MultiGateway{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      "xray",
					},
					Hostname: "newhost",
					PortMap:  map[string]int32{"grpc": 8081, "postgres": 5432},
				}

				err := ts.InitMultiGateway(ctx, updated, true)
				require.NoError(t, err)

				retrieved, err := ts.GetMultiGateway(ctx, original.Id)
				require.NoError(t, err)
				checkMultiGatewaysEqual(t, updated, retrieved.MultiGateway)
			},
		},
		{
			name: "Fail to update existing multigateway with allowUpdate=false",
			test: func(t *testing.T, ts topo.Store) {
				original := &clustermetadatapb.MultiGateway{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      "whiskey",
					},
					Hostname: "host1",
					PortMap:  map[string]int32{"grpc": 8080},
				}
				require.NoError(t, ts.CreateMultiGateway(ctx, original))

				updated := &clustermetadatapb.MultiGateway{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIGATEWAY,
						Cell:      cell,
						Name:      "whiskey",
					},
					Hostname: "newhost",
					PortMap:  map[string]int32{"grpc": 8081},
				}

				err := ts.InitMultiGateway(ctx, updated, false)
				require.Error(t, err)
				require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NodeExists}))
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

// TestNewMultiGateway tests the factory function
func TestNewMultiGateway(t *testing.T) {
	tests := []struct {
		testName string
		name     string
		cell     string
		host     string
		expected *clustermetadatapb.MultiGateway
	}{
		{
			testName: "basic creation",
			name:     "100",
			cell:     "zone1",
			host:     "host.example.com",
			expected: &clustermetadatapb.MultiGateway{
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
			result := topo.NewMultiGateway(tt.name, tt.cell, tt.host)
			require.Equal(t, tt.expected.Id.Cell, result.Id.Cell)
			require.Equal(t, tt.expected.Id.Name, result.Id.Name)
			require.Equal(t, tt.expected.Hostname, result.Hostname)
			require.NotNil(t, result.PortMap)
		})
	}

	// Test random name generation when name is empty
	t.Run("empty name generates random name", func(t *testing.T) {
		result := topo.NewMultiGateway("", "zone2", "host2.example.com")

		// Verify basic properties
		require.Equal(t, "zone2", result.Id.Cell)
		require.Equal(t, "host2.example.com", result.Hostname)
		require.NotNil(t, result.PortMap)

		// Verify random name was generated
		require.NotEmpty(t, result.Id.Name, "expected random name to be generated for empty name")
		require.Len(t, result.Id.Name, 8, "expected random name to be 8 characters long")

		// Verify the generated name only contains valid characters
		validChars := "bcdfghjklmnpqrstvwxz2456789"
		for _, char := range result.Id.Name {
			require.Contains(t, validChars, string(char), "generated name should only contain valid characters")
		}

		// Test that multiple calls generate different names
		result2 := topo.NewMultiGateway("", "zone2", "host2.example.com")
		require.NotEqual(t, result.Id.Name, result2.Id.Name, "multiple calls should generate different random names")
	})
}

// TestMultiGatewayInfo tests the MultiGatewayInfo methods
func TestMultiGatewayInfo(t *testing.T) {
	multigateway := &clustermetadatapb.MultiGateway{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIGATEWAY,
			Cell:      "zone1",
			Name:      "100",
		},
		Hostname: "host.example.com",
		PortMap: map[string]int32{
			"grpc":     8080,
			"postgres": 5432,
		},
	}
	version := memorytopo.NodeVersion(123)
	info := topo.NewMultiGatewayInfo(multigateway, version)

	t.Run("String method", func(t *testing.T) {
		result := info.String()
		expected := "MultiGateway{multigateway-zone1-100}"
		require.Equal(t, expected, result)
	})

	t.Run("IDString method", func(t *testing.T) {
		result := info.IDString()
		expected := "multigateway-zone1-100"
		require.Equal(t, expected, result)
	})

	t.Run("Addr method with grpc port", func(t *testing.T) {
		result := info.Addr()
		expected := "host.example.com:8080"
		require.Equal(t, expected, result)
	})

	t.Run("Addr method without grpc port", func(t *testing.T) {
		multigatewayNoGrpc := &clustermetadatapb.MultiGateway{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIGATEWAY,
				Cell:      "zone1",
				Name:      "100",
			},
			Hostname: "host.example.com",
			PortMap: map[string]int32{
				"postgres": 5432,
			},
		}
		infoNoGrpc := topo.NewMultiGatewayInfo(multigatewayNoGrpc, version)
		result := infoNoGrpc.Addr()
		expected := "host.example.com"
		require.Equal(t, expected, result)
	})

	t.Run("Version method", func(t *testing.T) {
		result := info.Version()
		require.Equal(t, version, result)
	})
}

// TestGetMultiGatewaysByCell covers comprehensive scenarios for the GetMultiGatewaysByCell method
func TestGetMultiGatewaysByCell_Comprehensive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("cell with multiple multigateways", func(t *testing.T) {
		// Create fresh topo for this test with multiple cells
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
		defer ts.Close()

		// Setup: Create 3 multigateways in zone1
		multigateways := []*clustermetadatapb.MultiGateway{
			{
				Id:       &clustermetadatapb.ID{Cell: "zone1", Name: "1"},
				Hostname: "host1",
				PortMap:  map[string]int32{"grpc": 8080, "postgres": 5432},
			},
			{
				Id:       &clustermetadatapb.ID{Cell: "zone1", Name: "2"},
				Hostname: "host2",
				PortMap:  map[string]int32{"grpc": 8081, "postgres": 5433},
			},
			{
				Id:       &clustermetadatapb.ID{Cell: "zone1", Name: "3"},
				Hostname: "host3",
				PortMap:  map[string]int32{"grpc": 8082, "postgres": 5434},
			},
		}

		// Create all multigateways
		for _, mg := range multigateways {
			require.NoError(t, ts.CreateMultiGateway(ctx, mg))
		}

		// Test: Get all multigateways
		multigatewayInfos, err := ts.GetMultiGatewaysByCell(ctx, "zone1")
		require.NoError(t, err)
		require.Len(t, multigatewayInfos, 3)

		// Verify all multigateways are returned
		expectedMGs := []*topo.MultiGatewayInfo{
			{MultiGateway: multigateways[0]},
			{MultiGateway: multigateways[1]},
			{MultiGateway: multigateways[2]},
		}
		checkMultiGatewayInfosEqual(t, expectedMGs, multigatewayInfos)

		// Verify cell boundary: multigateways are NOT accessible from other cells
		otherCellInfos, err := ts.GetMultiGatewaysByCell(ctx, "zone2")
		require.NoError(t, err)
		require.Empty(t, otherCellInfos, "multigateways should not be accessible from other cells")
	})

	t.Run("empty cell returns empty list", func(t *testing.T) {
		// Create fresh topo for this test
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		// Setup: No multigateways created

		// Test: Get multigateways from empty cell
		multigatewayInfos, err := ts.GetMultiGatewaysByCell(ctx, "zone1")
		require.NoError(t, err)
		require.Empty(t, multigatewayInfos)
	})

	t.Run("nonexistent cell returns NoNode error", func(t *testing.T) {
		// Create fresh topo for this test
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		// Setup: No multigateways created

		// Test: Try to get multigateways from nonexistent cell
		_, err := ts.GetMultiGatewaysByCell(ctx, "nonexistent")
		require.Error(t, err)
		require.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}))
	})

	t.Run("multigateways are isolated between cells", func(t *testing.T) {
		// Create fresh topo for this test with multiple cells
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
		defer ts.Close()

		// Setup: Create multigateways in both cells
		zone1MultiGateway := &clustermetadatapb.MultiGateway{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIGATEWAY,
				Cell:      "zone1",
				Name:      "1",
			},
			Hostname: "host1",
			PortMap:  map[string]int32{"grpc": 8080, "postgres": 5432},
		}
		zone2MultiGateway := &clustermetadatapb.MultiGateway{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIGATEWAY,
				Cell:      "zone2",
				Name:      "1",
			},
			Hostname: "host2",
			PortMap:  map[string]int32{"grpc": 8081, "postgres": 5433},
		}

		// Create multigateways in their respective cells
		require.NoError(t, ts.CreateMultiGateway(ctx, zone1MultiGateway))
		require.NoError(t, ts.CreateMultiGateway(ctx, zone2MultiGateway))

		// Test: Verify zone1 can only see its own multigateway
		zone1Infos, err := ts.GetMultiGatewaysByCell(ctx, "zone1")
		require.NoError(t, err)
		require.Len(t, zone1Infos, 1)
		require.Equal(t, "zone1", zone1Infos[0].Id.Cell)
		require.Equal(t, "host1", zone1Infos[0].Hostname)

		// Test: Verify zone2 can only see its own multigateway
		zone2Infos, err := ts.GetMultiGatewaysByCell(ctx, "zone2")
		require.NoError(t, err)
		require.Len(t, zone2Infos, 1)
		require.Equal(t, "zone2", zone2Infos[0].Id.Cell)
		require.Equal(t, "host2", zone2Infos[0].Hostname)

		// Test: Verify cross-cell access is properly isolated
		zone1FromZone2, err := ts.GetMultiGateway(ctx, zone1MultiGateway.Id)
		require.NoError(t, err, "should be able to get multigateway by ID regardless of current cell context")
		require.Equal(t, "zone1", zone1FromZone2.Id.Cell)

		zone2FromZone1, err := ts.GetMultiGateway(ctx, zone2MultiGateway.Id)
		require.NoError(t, err, "should be able to get multigateway by ID regardless of current cell context")
		require.Equal(t, "zone2", zone2FromZone1.Id.Cell)
	})
}
