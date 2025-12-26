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

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

// runGetOrchs handles the getorchs command
func runGetOrchs(cmd *cobra.Command, args []string) error {
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

	// Create admin client
	client, err := admin.NewClient(cmd)
	if err != nil {
		return err
	}
	defer client.Close()

	request := &multiadminpb.GetOrchsRequest{
		Cells: cells,
	}

	response, err := client.GetOrchs(cmd.Context(), request)
	if err != nil {
		return fmt.Errorf("failed to get orchestrators: %w", err)
	}

	// Convert to JSON and output
	jsonData, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal response to JSON: %w", err)
	}

	cmd.Print(string(jsonData))
	return nil
}

// AddGetOrchsCommand adds the getorchs subcommand
func AddGetOrchsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "getorchs",
		Short: "Get orchestrators filtered by cells",
		Long:  "Retrieve orchestrators from specified cells. If no cells are specified, all cells will be queried.",
		RunE:  runGetOrchs,
	}

	cmd.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")
	cmd.Flags().String("cells", "", "Comma-separated list of cell names to query (optional)")

	return cmd
}
