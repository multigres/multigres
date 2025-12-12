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

package cluster

import (
	"fmt"

	"github.com/spf13/cobra"
)

// restart handles the cluster restart command by stopping then starting
func restart(cmd *cobra.Command, args []string) error {
	fmt.Println("Restarting Multigres cluster...")
	fmt.Println()

	// Stop the cluster first (reuse down handler logic)
	if err := down(cmd, args); err != nil {
		return fmt.Errorf("failed to stop cluster: %w", err)
	}

	fmt.Println()

	// Start the cluster (reuse start handler)
	if err := start(cmd, args); err != nil {
		return fmt.Errorf("failed to start cluster: %w", err)
	}

	return nil
}

// AddRestartCommand adds the restart subcommand to the cluster command
func AddRestartCommand(clusterCmd *cobra.Command) {
	restartCmd := &cobra.Command{
		Use:   "restart",
		Short: "Restart local cluster",
		Long:  "Restart the local Multigres cluster by stopping it (if running) then starting it again.",
		RunE:  restart,
	}

	// Add clean flag since down() expects it
	restartCmd.Flags().Bool("clean", false, "Fully tear down all cluster resources before restarting")

	clusterCmd.AddCommand(restartCmd)
}
