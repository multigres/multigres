// Copyright 2025 The Multigres Authors.
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
