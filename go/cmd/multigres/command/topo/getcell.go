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

package topo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

// AddGetCellCommand adds the getcell subcommand
func AddGetCellCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "getcell",
		Short: "Get information about a specific cell",
		Long:  "Retrieve detailed information about a cell from the multiadmin server.",
		RunE:  runGetCell,
	}

	// Add command-specific flags
	cmd.Flags().String("name", "", "Name of the cell to retrieve (required)")
	cmd.Flags().String("admin-server", "", "gRPC address of the multiadmin server (e.g., localhost:15990)")

	// Mark the name flag as required
	_ = cmd.MarkFlagRequired("name")

	return cmd
}

// runGetCell executes the getcell command
func runGetCell(cmd *cobra.Command, args []string) error {
	// Get the cell name
	cellName, _ := cmd.Flags().GetString("name")

	// Resolve admin server address
	adminServer, err := GetAdminServerAddress(cmd)
	if err != nil {
		return err
	}

	// Create gRPC connection
	ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(adminServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to admin server at %s: %w", adminServer, err)
	}
	defer conn.Close()

	// Create client and call GetCell RPC
	client := multiadminpb.NewMultiAdminServiceClient(conn)

	response, err := client.GetCell(ctx, &multiadminpb.GetCellRequest{
		Name: cellName,
	})
	if err != nil {
		return fmt.Errorf("GetCell RPC failed: %w", err)
	}

	// Output the response in JSON format
	jsonData, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal response to JSON: %w", err)
	}

	cmd.Print(string(jsonData))
	return nil
}
