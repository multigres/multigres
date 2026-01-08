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

// AddEnablePostgresMonitorCommand adds the enablepostgresmonitor subcommand
func AddEnablePostgresMonitorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "enablepostgresmonitor",
		Short: "Enable PostgreSQL monitoring on a pooler",
		Long: `Enable the PostgreSQL monitoring goroutine on a specific pooler.

The monitoring goroutine checks the health of the PostgreSQL process and
can automatically restart it if it becomes unresponsive. This command is
idempotent - calling it multiple times has no effect if monitoring is
already enabled.

The --pooler flag should be the fully-qualified pooler name in the format:
  multipooler-{cell}-{name}

Example:
  multigres enablepostgresmonitor --pooler multipooler-zone1-abc123`,
		RunE: runEnablePostgresMonitor,
	}

	cmd.Flags().String("pooler", "", "Fully qualified pooler name (e.g., multipooler-zone1-abc123)")
	cmd.Flags().String("admin-server", "", "gRPC address of the multiadmin server (e.g., localhost:18070)")

	_ = cmd.MarkFlagRequired("pooler")

	return cmd
}

// runEnablePostgresMonitor executes the enablepostgresmonitor command
func runEnablePostgresMonitor(cmd *cobra.Command, args []string) error {
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

	// Create context with timeout and call EnablePostgresMonitor RPC
	ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
	defer cancel()

	_, err = client.EnablePostgresMonitor(ctx, &multiadminpb.EnablePostgresMonitorRequest{
		PoolerId: &clustermetadatapb.ID{
			Cell: cell,
			Name: serviceID,
		},
	})
	if err != nil {
		return fmt.Errorf("EnablePostgresMonitor RPC failed: %w", err)
	}

	cmd.Printf("Successfully enabled PostgreSQL monitoring on pooler %s\n", poolerName)
	return nil
}
