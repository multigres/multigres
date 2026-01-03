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

package pooler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

// AddGetPoolerStatusCommand adds the getpoolerstatus subcommand
func AddGetPoolerStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "getpoolerstatus",
		Short: "Get status of a specific pooler",
		Long: `Retrieve unified status information from a pooler via the multiadmin server.

Works for both PRIMARY and REPLICA poolers. Returns initialization state,
PostgreSQL process status, replication position, and consensus term.

Key fields returned:
  - pooler_type: Whether this is a PRIMARY or REPLICA pooler
  - postgres_running: Whether the PostgreSQL process is running
  - postgres_role: Actual database role (primary/standby/unknown)
  - is_initialized: Whether the pooler has been fully initialized
  - wal_position: Current WAL position (for replication tracking)
  - consensus_term: Current consensus term for failover coordination

Note: Basic status fields are available even when the database connection
is unavailable. Role-specific details (primary_status, replication_status)
require an active database connection.`,
		RunE: runGetPoolerStatus,
	}

	cmd.Flags().String("cell", "", "Cell name where the pooler resides (required)")
	cmd.Flags().String("service-id", "", "Service ID (name) of the pooler (required)")
	cmd.Flags().String("admin-server", "", "gRPC address of the multiadmin server (e.g., localhost:18070)")

	_ = cmd.MarkFlagRequired("cell")
	_ = cmd.MarkFlagRequired("service-id")

	return cmd
}

// runGetPoolerStatus executes the getpoolerstatus command
func runGetPoolerStatus(cmd *cobra.Command, args []string) error {
	cell, _ := cmd.Flags().GetString("cell")
	serviceID, _ := cmd.Flags().GetString("service-id")

	// Create admin client
	client, err := admin.NewClient(cmd)
	if err != nil {
		return err
	}
	defer client.Close()

	// Create context with timeout and call GetPoolerStatus RPC
	ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
	defer cancel()

	response, err := client.GetPoolerStatus(ctx, &multiadminpb.GetPoolerStatusRequest{
		PoolerId: &clustermetadatapb.ID{
			Cell: cell,
			Name: serviceID,
		},
	})
	if err != nil {
		return fmt.Errorf("GetPoolerStatus RPC failed: %w", err)
	}

	// Output the response in JSON format
	jsonData, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal response to JSON: %w", err)
	}

	cmd.Print(string(jsonData))
	return nil
}
