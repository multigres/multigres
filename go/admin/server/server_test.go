// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"log/slog"
	"os"
	"testing"

	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMultiAdminServerGetCell(t *testing.T) {
	// Create a memory topology for testing
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := NewMultiAdminServer(ts, logger)

	// Test getting a non-existent cell
	t.Run("non-existent cell returns NotFound", func(t *testing.T) {
		req := &multiadminpb.GetCellRequest{Name: "nonexistent"}
		resp, err := server.GetCell(ctx, req)

		assert.Nil(t, resp)
		assert.NotNil(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
		assert.Contains(t, st.Message(), "cell 'nonexistent' not found")
	})

	// Test with empty cell name
	t.Run("empty cell name returns InvalidArgument", func(t *testing.T) {
		req := &multiadminpb.GetCellRequest{Name: ""}
		resp, err := server.GetCell(ctx, req)

		assert.Nil(t, resp)
		assert.NotNil(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "cell name cannot be empty")
	})

	// Test getting an existing cell
	t.Run("existing cell returns cell data", func(t *testing.T) {
		// First create a cell
		testCell := &clustermetadatapb.Cell{
			Name:            "testcell",
			ServerAddresses: []string{"localhost:2379"},
			Root:            "/multigres/testcell",
		}

		err := ts.CreateCell(ctx, "testcell", testCell)
		require.NoError(t, err)

		// Now try to get it
		req := &multiadminpb.GetCellRequest{Name: "testcell"}
		resp, err := server.GetCell(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Cell)
		assert.Equal(t, "testcell", resp.Cell.Name)
		assert.Equal(t, []string{"localhost:2379"}, resp.Cell.ServerAddresses)
		assert.Equal(t, "/multigres/testcell", resp.Cell.Root)
	})
}

func TestMultiAdminServerGetDatabase(t *testing.T) {
	// Create a memory topology for testing
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := NewMultiAdminServer(ts, logger)

	// Test getting a non-existent database
	t.Run("non-existent database returns NotFound", func(t *testing.T) {
		req := &multiadminpb.GetDatabaseRequest{Name: "nonexistent"}
		resp, err := server.GetDatabase(ctx, req)

		assert.Nil(t, resp)
		assert.NotNil(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
		assert.Contains(t, st.Message(), "database 'nonexistent' not found")
	})

	// Test with empty database name
	t.Run("empty database name returns InvalidArgument", func(t *testing.T) {
		req := &multiadminpb.GetDatabaseRequest{Name: ""}
		resp, err := server.GetDatabase(ctx, req)

		assert.Nil(t, resp)
		assert.NotNil(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "database name cannot be empty")
	})

	// Test getting an existing database
	t.Run("existing database returns database data", func(t *testing.T) {
		// First create a database
		testDatabase := &clustermetadatapb.Database{
			Name:             "testdb",
			BackupLocation:   "s3://backup-bucket/testdb",
			DurabilityPolicy: "none",
			Cells:            []string{"cell1", "cell2"},
		}

		err := ts.CreateDatabase(ctx, "testdb", testDatabase)
		require.NoError(t, err)

		// Now try to get it
		req := &multiadminpb.GetDatabaseRequest{Name: "testdb"}
		resp, err := server.GetDatabase(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Database)
		assert.Equal(t, "testdb", resp.Database.Name)
		assert.Equal(t, "s3://backup-bucket/testdb", resp.Database.BackupLocation)
		assert.Equal(t, "none", resp.Database.DurabilityPolicy)
		assert.Equal(t, []string{"cell1", "cell2"}, resp.Database.Cells)
	})
}

func TestMultiAdminServerGetCellNames(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := NewMultiAdminServer(ts, logger)

	t.Run("empty topology returns empty list", func(t *testing.T) {
		req := &multiadminpb.GetCellNamesRequest{}
		resp, err := server.GetCellNames(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Names)
	})

	t.Run("returns all cell names", func(t *testing.T) {
		// Create test cells
		cells := []*clustermetadatapb.Cell{
			{Name: "cell1", ServerAddresses: []string{"localhost:2379"}, Root: "/multigres/cell1"},
			{Name: "cell2", ServerAddresses: []string{"localhost:2380"}, Root: "/multigres/cell2"},
		}

		for _, cell := range cells {
			err := ts.CreateCell(ctx, cell.Name, cell)
			require.NoError(t, err)
		}

		req := &multiadminpb.GetCellNamesRequest{}
		resp, err := server.GetCellNames(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.ElementsMatch(t, []string{"cell1", "cell2"}, resp.Names)
	})
}

func TestMultiAdminServerGetDatabaseNames(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := NewMultiAdminServer(ts, logger)

	t.Run("empty topology returns empty list", func(t *testing.T) {
		req := &multiadminpb.GetDatabaseNamesRequest{}
		resp, err := server.GetDatabaseNames(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Names)
	})

	t.Run("returns all database names", func(t *testing.T) {
		// Create test databases
		databases := []*clustermetadatapb.Database{
			{Name: "db1", DurabilityPolicy: "none", Cells: []string{"cell1"}},
			{Name: "db2", DurabilityPolicy: "none", Cells: []string{"cell1"}},
		}

		for _, db := range databases {
			err := ts.CreateDatabase(ctx, db.Name, db)
			require.NoError(t, err)
		}

		req := &multiadminpb.GetDatabaseNamesRequest{}
		resp, err := server.GetDatabaseNames(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.ElementsMatch(t, []string{"db1", "db2"}, resp.Names)
	})
}

func TestMultiAdminServerGetGateways(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := NewMultiAdminServer(ts, logger)

	t.Run("get gateways with empty topology", func(t *testing.T) {
		req := &multiadminpb.GetGatewaysRequest{}
		resp, err := server.GetGateways(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Gateways)
	})

	t.Run("get gateways filtered by non-existent cell", func(t *testing.T) {
		req := &multiadminpb.GetGatewaysRequest{
			Cells: []string{"nonexistent"},
		}
		resp, err := server.GetGateways(ctx, req)

		require.Error(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Gateways)
		assert.Contains(t, err.Error(), "partial results returned due to errors in 1 cell(s)")
		assert.Contains(t, err.Error(), "failed to get gateways for cell nonexistent")
	})
}

func TestMultiAdminServerGetGatewaysMultiCell(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1", "cell2")
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := NewMultiAdminServer(ts, logger)

	// Setup test data
	require.NoError(t, memorytopo.SetupMultiCellTestData(ctx, ts))

	t.Run("get all gateways across all cells", func(t *testing.T) {
		req := &multiadminpb.GetGatewaysRequest{}
		resp, err := server.GetGateways(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Gateways, 3) // gw1, gw2, gw3
	})

	t.Run("get gateways filtered by single cell", func(t *testing.T) {
		req := &multiadminpb.GetGatewaysRequest{
			Cells: []string{"cell1"},
		}
		resp, err := server.GetGateways(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Gateways, 2) // gw1, gw2
		for _, gw := range resp.Gateways {
			assert.Equal(t, "cell1", gw.Id.Cell)
		}
	})

	t.Run("get gateways filtered by multiple cells", func(t *testing.T) {
		req := &multiadminpb.GetGatewaysRequest{
			Cells: []string{"cell1", "cell2"},
		}
		resp, err := server.GetGateways(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Gateways, 3) // gw1, gw2, gw3
	})

	t.Run("get gateways with mixed existing and non-existing cells", func(t *testing.T) {
		req := &multiadminpb.GetGatewaysRequest{
			Cells: []string{"cell1", "nonexistent", "cell2"},
		}
		resp, err := server.GetGateways(ctx, req)

		require.Error(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Gateways, 3) // Should get results from existing cells only
		assert.Contains(t, err.Error(), "partial results returned due to errors in 1 cell(s)")
		assert.Contains(t, err.Error(), "failed to get gateways for cell nonexistent")
	})
}

func TestMultiAdminServerGetPoolers(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := NewMultiAdminServer(ts, logger)

	t.Run("get poolers with empty topology", func(t *testing.T) {
		req := &multiadminpb.GetPoolersRequest{}
		resp, err := server.GetPoolers(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Poolers)
	})

	t.Run("get poolers filtered by non-existent cell", func(t *testing.T) {
		req := &multiadminpb.GetPoolersRequest{
			Cells: []string{"nonexistent"},
		}
		resp, err := server.GetPoolers(ctx, req)

		require.Error(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Poolers)
		assert.Contains(t, err.Error(), "partial results returned due to errors in 1 cell(s)")
		assert.Contains(t, err.Error(), "failed to get poolers for cell nonexistent")
	})
}

func TestMultiAdminServerGetPoolersMultiCell(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1", "cell2")
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := NewMultiAdminServer(ts, logger)

	// Setup test data
	require.NoError(t, memorytopo.SetupMultiCellTestData(ctx, ts))

	t.Run("get all poolers across all cells", func(t *testing.T) {
		req := &multiadminpb.GetPoolersRequest{}
		resp, err := server.GetPoolers(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Poolers, 3) // pool1, pool2, pool3
	})

	t.Run("get poolers filtered by single cell", func(t *testing.T) {
		req := &multiadminpb.GetPoolersRequest{
			Cells: []string{"cell2"},
		}
		resp, err := server.GetPoolers(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Poolers, 2) // pool2, pool3
		for _, pooler := range resp.Poolers {
			assert.Equal(t, "cell2", pooler.Id.Cell)
		}
	})

	t.Run("get poolers filtered by database", func(t *testing.T) {
		req := &multiadminpb.GetPoolersRequest{
			Database: "db1",
		}
		resp, err := server.GetPoolers(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Poolers, 2) // pool1 and pool2
		for _, pooler := range resp.Poolers {
			assert.Equal(t, "db1", pooler.Database)
		}
	})

	t.Run("get poolers filtered by cell and database", func(t *testing.T) {
		req := &multiadminpb.GetPoolersRequest{
			Cells:    []string{"cell2"},
			Database: "db1",
		}
		resp, err := server.GetPoolers(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Poolers, 1) // only pool2
		assert.Equal(t, "cell2", resp.Poolers[0].Id.Cell)
		assert.Equal(t, "db1", resp.Poolers[0].Database)
	})

	t.Run("get poolers filtered by non-existent database", func(t *testing.T) {
		req := &multiadminpb.GetPoolersRequest{
			Database: "nonexistent-db",
		}
		resp, err := server.GetPoolers(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Poolers)
	})

	t.Run("get poolers filtered by database db2", func(t *testing.T) {
		req := &multiadminpb.GetPoolersRequest{
			Database: "db2",
		}
		resp, err := server.GetPoolers(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Poolers, 1) // only pool3
		assert.Equal(t, "db2", resp.Poolers[0].Database)
		assert.Equal(t, "pool3", resp.Poolers[0].Id.Name)
	})

	t.Run("get poolers with empty database filter returns all", func(t *testing.T) {
		req := &multiadminpb.GetPoolersRequest{
			Database: "",
		}
		resp, err := server.GetPoolers(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Poolers, 3) // all poolers
	})
}

func TestMultiAdminServerGetOrchs(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := NewMultiAdminServer(ts, logger)

	t.Run("get orchestrators with empty topology", func(t *testing.T) {
		req := &multiadminpb.GetOrchsRequest{}
		resp, err := server.GetOrchs(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Orchs)
	})

	t.Run("get orchestrators filtered by non-existent cell", func(t *testing.T) {
		req := &multiadminpb.GetOrchsRequest{
			Cells: []string{"nonexistent"},
		}
		resp, err := server.GetOrchs(ctx, req)

		require.Error(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Orchs)
		assert.Contains(t, err.Error(), "partial results returned due to errors in 1 cell(s)")
		assert.Contains(t, err.Error(), "failed to get orchestrators for cell nonexistent")
	})
}

func TestMultiAdminServerGetOrchsMultiCell(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1", "cell2")
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := NewMultiAdminServer(ts, logger)

	// Setup test data
	require.NoError(t, memorytopo.SetupMultiCellTestData(ctx, ts))

	t.Run("get all orchestrators across all cells", func(t *testing.T) {
		req := &multiadminpb.GetOrchsRequest{}
		resp, err := server.GetOrchs(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Orchs, 2) // orch1, orch2
	})

	t.Run("get orchestrators filtered by single cell", func(t *testing.T) {
		req := &multiadminpb.GetOrchsRequest{
			Cells: []string{"cell1"},
		}
		resp, err := server.GetOrchs(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Orchs, 1) // orch1
		assert.Equal(t, "cell1", resp.Orchs[0].Id.Cell)
		assert.Equal(t, "orch1", resp.Orchs[0].Id.Name)
	})

	t.Run("get orchestrators with empty result for non-existent cell", func(t *testing.T) {
		req := &multiadminpb.GetOrchsRequest{
			Cells: []string{"nonexistent"},
		}
		resp, err := server.GetOrchs(ctx, req)

		require.Error(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Orchs)
		assert.Contains(t, err.Error(), "partial results returned due to errors in 1 cell(s)")
		assert.Contains(t, err.Error(), "failed to get orchestrators for cell nonexistent")
	})
}
