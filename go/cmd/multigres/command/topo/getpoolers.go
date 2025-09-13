/*
Copyright 2025 The Multigres Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topo

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

// runGetPoolers handles the getpoolers command
func runGetPoolers(cmd *cobra.Command, args []string) error {
	// Get admin server address
	adminServer, err := getAdminServerAddress(cmd)
	if err != nil {
		return err
	}

	// Get flag values
	cellsFlag, _ := cmd.Flags().GetString("cells")
	database, _ := cmd.Flags().GetString("database")

	// Parse cells flag
	var cells []string
	if cellsFlag != "" {
		cells = strings.Split(cellsFlag, ",")
		// Trim whitespace from each cell name
		for i, cell := range cells {
			cells[i] = strings.TrimSpace(cell)
		}
	}

	// Create gRPC connection
	conn, err := grpc.NewClient(adminServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to admin server %s: %w", adminServer, err)
	}
	defer conn.Close()

	// Create client and make the request
	client := multiadminpb.NewMultiAdminServiceClient(conn)
	ctx := context.Background()

	request := &multiadminpb.GetPoolersRequest{
		Cells:    cells,
		Database: database,
	}

	response, err := client.GetPoolers(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to get poolers: %w", err)
	}

	// Convert to JSON and output
	jsonData, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal response to JSON: %w", err)
	}

	cmd.Print(string(jsonData))
	return nil
}

// GetPoolersCommand represents the getpoolers command
var GetPoolersCommand = &cobra.Command{
	Use:   "getpoolers",
	Short: "Get poolers filtered by cells and/or database",
	Long:  "Retrieve poolers from specified cells and optionally filter by database. If no cells are specified, all cells will be queried.",
	RunE:  runGetPoolers,
}

func init() {
	GetPoolersCommand.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")
	GetPoolersCommand.Flags().String("cells", "", "Comma-separated list of cell names to query (optional)")
	GetPoolersCommand.Flags().String("database", "", "Database name to filter by (optional)")
}
