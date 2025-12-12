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

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

// runGetCellNames handles the getcellnames command
func runGetCellNames(cmd *cobra.Command, args []string) error {
	// Get admin server address
	adminServer, err := admin.GetServerAddress(cmd)
	if err != nil {
		return err
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

	response, err := client.GetCellNames(ctx, &multiadminpb.GetCellNamesRequest{})
	if err != nil {
		return fmt.Errorf("failed to get cell names: %w", err)
	}

	// Convert to JSON and output
	jsonData, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal response to JSON: %w", err)
	}

	cmd.Print(string(jsonData))
	return nil
}

// AddGetCellNamesCommand adds the getcellnames subcommand
func AddGetCellNamesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "getcellnames",
		Short: "Get all cell names in the cluster",
		Long:  "Retrieve a list of all cell names in the Multigres cluster.",
		RunE:  runGetCellNames,
	}

	cmd.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")

	return cmd
}
