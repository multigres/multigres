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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

// runGetGateways handles the getgateways command
func runGetGateways(cmd *cobra.Command, args []string) error {
	// Get admin server address
	adminServer, err := GetAdminServerAddress(cmd)
	if err != nil {
		return err
	}

	// Get flag values
	cellsFlag, err := cmd.Flags().GetString("cells")
	if err != nil {
		return fmt.Errorf("failed to read cells flag: %w", err)
	}

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
	ctx := cmd.Context()

	request := &multiadminpb.GetGatewaysRequest{
		Cells: cells,
	}

	response, err := client.GetGateways(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to get gateways: %w", err)
	}

	// Convert to JSON and output
	jsonData, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal response to JSON: %w", err)
	}

	cmd.Print(string(jsonData))
	return nil
}

// AddGetGatewaysCommand adds the getgateways subcommand
func AddGetGatewaysCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "getgateways",
		Short: "Get gateways filtered by cells",
		Long:  "Retrieve gateways from specified cells. If no cells are specified, all cells will be queried.",
		RunE:  runGetGateways,
	}

	cmd.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")
	cmd.Flags().String("cells", "", "Comma-separated list of cell names to query (optional)")

	return cmd
}
