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

//go:build integration

package endtoend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/test/utils"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockMultiAdminServer implements a mock multiadmin server for testing
type mockMultiAdminServer struct {
	multiadminpb.UnimplementedMultiAdminServiceServer
}

func (s *mockMultiAdminServer) GetCell(ctx context.Context, req *multiadminpb.GetCellRequest) (*multiadminpb.GetCellResponse, error) {
	switch req.Name {
	case "zone1":
		return &multiadminpb.GetCellResponse{
			Cell: &clustermetadatapb.Cell{
				Name:            "zone1",
				ServerAddresses: []string{"localhost:2379"},
				Root:            "/multigres/zone1",
			},
		}, nil
	case "nonexistent":
		return nil, status.Error(codes.NotFound, fmt.Sprintf("cell '%s' not found", req.Name))
	default:
		return nil, status.Error(codes.Internal, "unexpected cell name")
	}
}

func (s *mockMultiAdminServer) GetDatabase(ctx context.Context, req *multiadminpb.GetDatabaseRequest) (*multiadminpb.GetDatabaseResponse, error) {
	switch req.Name {
	case "testdb":
		return &multiadminpb.GetDatabaseResponse{
			Database: &clustermetadatapb.Database{
				Name:  "testdb",
				Cells: []string{"zone1"},
			},
		}, nil
	case "nonexistent":
		return nil, status.Error(codes.NotFound, fmt.Sprintf("database '%s' not found", req.Name))
	default:
		return nil, status.Error(codes.Internal, "unexpected database name")
	}
}

// startMockServer starts a mock multiadmin gRPC server for testing
func startMockServer(t *testing.T) (string, func()) {
	// Get a free port
	port := utils.GetNextPort()
	address := fmt.Sprintf("localhost:%d", port)

	// Create listener
	lis, err := net.Listen("tcp", address)
	require.NoError(t, err)

	// Create gRPC server
	server := grpc.NewServer()
	multiadminpb.RegisterMultiAdminServiceServer(server, &mockMultiAdminServer{})

	// Start serving in a goroutine
	go func() {
		if err := server.Serve(lis); err != nil {
			t.Logf("Mock gRPC server error: %v", err)
		}
	}()

	// Wait for server to be ready
	time.Sleep(100 * time.Millisecond)

	// Return address and cleanup function
	cleanup := func() {
		server.Stop()
	}

	return address, cleanup
}

func TestGetCellIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Start mock server
	serverAddr, cleanup := startMockServer(t)
	defer cleanup()

	// Create temporary directory for config
	tempDir, err := os.MkdirTemp("", "multigres_integration_test_")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create test config file
	configContent := fmt.Sprintf(`provisioner: local
provisioner-config:
  multiadmin:
    grpc-port: %s
    http-port: 15000
`, serverAddr[len("localhost:"):]) // Extract port from address

	configFile := filepath.Join(tempDir, "multigres.yaml")
	err = os.WriteFile(configFile, []byte(configContent), 0o644)
	require.NoError(t, err)

	t.Run("getcell with admin-server flag", func(t *testing.T) {
		// Create command
		cmd := &cobra.Command{}
		cmd.Flags().String("name", "", "")
		cmd.Flags().String("admin-server", "", "")
		cmd.Flags().StringSlice("config-path", []string{}, "")

		// Set flags
		cmd.Flags().Set("name", "zone1")
		cmd.Flags().Set("admin-server", serverAddr)

		// Capture output
		var buf bytes.Buffer
		cmd.SetOut(&buf)

		// Execute command
		err := runGetCell(cmd, []string{})
		require.NoError(t, err)

		// Parse and verify output
		var response map[string]any
		err = json.Unmarshal(buf.Bytes(), &response)
		require.NoError(t, err)

		cell := response["cell"].(map[string]any)
		assert.Equal(t, "zone1", cell["name"])
		assert.Equal(t, []any{"localhost:2379"}, cell["server_addresses"])
		assert.Equal(t, "/multigres/zone1", cell["root"])
	})

	t.Run("getcell with config-path fallback", func(t *testing.T) {
		// Create command
		cmd := &cobra.Command{}
		cmd.Flags().String("name", "", "")
		cmd.Flags().String("admin-server", "", "")
		cmd.Flags().StringSlice("config-path", []string{}, "")

		// Set flags (no admin-server, only config-path)
		cmd.Flags().Set("name", "zone1")
		cmd.Flags().Set("config-path", tempDir)

		// Capture output
		var buf bytes.Buffer
		cmd.SetOut(&buf)

		// Execute command
		err := runGetCell(cmd, []string{})
		require.NoError(t, err)

		// Parse and verify output
		var response map[string]any
		err = json.Unmarshal(buf.Bytes(), &response)
		require.NoError(t, err)

		cell := response["cell"].(map[string]any)
		assert.Equal(t, "zone1", cell["name"])
	})

	t.Run("getcell with nonexistent cell", func(t *testing.T) {
		// Create command
		cmd := &cobra.Command{}
		cmd.Flags().String("name", "", "")
		cmd.Flags().String("admin-server", "", "")
		cmd.Flags().StringSlice("config-path", []string{}, "")

		// Set flags
		cmd.Flags().Set("name", "nonexistent")
		cmd.Flags().Set("admin-server", serverAddr)

		// Execute command and expect error
		err := runGetCell(cmd, []string{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cell 'nonexistent' not found")
	})
}

func TestGetDatabaseIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Start mock server
	serverAddr, cleanup := startMockServer(t)
	defer cleanup()

	t.Run("getdatabase with existing database", func(t *testing.T) {
		// Create command
		cmd := &cobra.Command{}
		cmd.Flags().String("name", "", "")
		cmd.Flags().String("admin-server", "", "")
		cmd.Flags().StringSlice("config-path", []string{}, "")

		// Set flags
		cmd.Flags().Set("name", "testdb")
		cmd.Flags().Set("admin-server", serverAddr)

		// Capture output
		var buf bytes.Buffer
		cmd.SetOut(&buf)

		// Execute command
		err := runGetDatabase(cmd, []string{})
		require.NoError(t, err)

		// Parse and verify output
		var response map[string]any
		err = json.Unmarshal(buf.Bytes(), &response)
		require.NoError(t, err)

		database := response["database"].(map[string]any)
		assert.Equal(t, "testdb", database["name"])
		assert.Equal(t, []any{"zone1"}, database["cells"])
	})

	t.Run("getdatabase with nonexistent database", func(t *testing.T) {
		// Create command
		cmd := &cobra.Command{}
		cmd.Flags().String("name", "", "")
		cmd.Flags().String("admin-server", "", "")
		cmd.Flags().StringSlice("config-path", []string{}, "")

		// Set flags
		cmd.Flags().Set("name", "nonexistent")
		cmd.Flags().Set("admin-server", serverAddr)

		// Execute command and expect error
		err := runGetDatabase(cmd, []string{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "database 'nonexistent' not found")
	})
}
