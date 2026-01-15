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

// AddSetPostgresMonitorCommand adds the setpostgresmonitor subcommand
func AddSetPostgresMonitorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "setpostgresmonitor",
		Short: "Set PostgreSQL monitoring on a pooler",
		Long: `Enable or disable the PostgreSQL monitoring goroutine on a specific pooler.

The monitoring goroutine checks the health of the PostgreSQL process and
can automatically restart it if it becomes unresponsive. This command is
idempotent - calling it multiple times has no effect if monitoring is
already in the requested state.

The --pooler flag should be the fully-qualified pooler name in the format:
  multipooler-{cell}-{name}

Example:
  multigres setpostgresmonitor --pooler multipooler-zone1-abc123 --enabled=true`,
		RunE: runSetPostgresMonitor,
	}

	cmd.Flags().String("pooler", "", "Fully qualified pooler name (e.g., multipooler-zone1-abc123)")
	cmd.Flags().Bool("enabled", false, "Whether to enable (true) or disable (false) monitoring")
	cmd.Flags().String("admin-server", "", "gRPC address of the multiadmin server (e.g., localhost:18070)")

	_ = cmd.MarkFlagRequired("pooler")
	_ = cmd.MarkFlagRequired("enabled")

	return cmd
}

// runSetPostgresMonitor executes the setpostgresmonitor command
func runSetPostgresMonitor(cmd *cobra.Command, args []string) error {
	poolerName, _ := cmd.Flags().GetString("pooler")
	enabled, _ := cmd.Flags().GetBool("enabled")

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

	// Create context with timeout and call SetPostgresMonitor RPC
	ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
	defer cancel()

	_, err = client.SetPostgresMonitor(ctx, &multiadminpb.SetPostgresMonitorRequest{
		PoolerId: &clustermetadatapb.ID{
			Cell: cell,
			Name: serviceID,
		},
		Enabled: enabled,
	})
	if err != nil {
		return fmt.Errorf("SetPostgresMonitor RPC failed: %w", err)
	}

	state := "disabled"
	if enabled {
		state = "enabled"
	}
	cmd.Printf("Successfully %s PostgreSQL monitoring on pooler %s\n", state, poolerName)
	return nil
}
