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
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

// AddDisablePostgresMonitorCommand adds the disablepostgresmonitor subcommand
func AddDisablePostgresMonitorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "disablepostgresmonitor",
		Short: "Disable PostgreSQL monitoring on a pooler",
		Long: `Disable the PostgreSQL monitoring goroutine on a specific pooler.

This stops the monitoring goroutine that checks PostgreSQL process health.
After disabling monitoring, the PostgreSQL process will not be automatically
restarted if it becomes unresponsive. This command is idempotent - calling
it multiple times has no effect if monitoring is already disabled.

The --pooler flag should be the fully-qualified pooler name in the format:
  multipooler-{cell}-{name}

Example:
  multigres disablepostgresmonitor --pooler multipooler-zone1-abc123`,
		RunE: runDisablePostgresMonitor,
	}

	cmd.Flags().String("pooler", "", "Fully qualified pooler name (e.g., multipooler-zone1-abc123)")
	cmd.Flags().String("admin-server", "", "gRPC address of the multiadmin server (e.g., localhost:18070)")

	_ = cmd.MarkFlagRequired("pooler")

	return cmd
}

// runDisablePostgresMonitor executes the disablepostgresmonitor command
func runDisablePostgresMonitor(cmd *cobra.Command, args []string) error {
	poolerName, _ := cmd.Flags().GetString("pooler")

	// Parse the fully-qualified pooler name to extract cell and name
	// Format: multipooler-{cell}-{name}
	parts := strings.SplitN(poolerName, "-", 3)
	if len(parts) != 3 || parts[0] != "multipooler" {
		return fmt.Errorf("invalid pooler name format: expected 'multipooler-{cell}-{name}', got '%s'", poolerName)
	}

	cell := parts[1]
	serviceID := parts[2]

	// Create admin client
	client, err := admin.NewClient(cmd)
	if err != nil {
		return err
	}
	defer client.Close()

	// Create context with timeout and call DisablePostgresMonitor RPC
	ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
	defer cancel()

	_, err = client.DisablePostgresMonitor(ctx, &multiadminpb.DisablePostgresMonitorRequest{
		PoolerId: &clustermetadatapb.ID{
			Cell: cell,
			Name: serviceID,
		},
	})
	if err != nil {
		return fmt.Errorf("DisablePostgresMonitor RPC failed: %w", err)
	}

	cmd.Printf("Successfully disabled PostgreSQL monitoring on pooler %s\n", poolerName)
	return nil
}
