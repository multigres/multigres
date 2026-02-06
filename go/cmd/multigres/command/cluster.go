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

package command

import (
	"github.com/multigres/multigres/go/cmd/multigres/command/cluster"

	"github.com/spf13/cobra"
)

// AddClusterCommand adds the cluster subcommand and its subcommands to the root command
func AddClusterCommand(root *cobra.Command, mc *MultigresCommand) {
	clusterCmd := &cobra.Command{
		Use:   "cluster",
		Short: "Manage cluster lifecycle",
		Long:  "Commands for managing the Multigres cluster lifecycle including initialization, startup, shutdown, and status monitoring.",
	}

	// Register cluster subcommands
	cluster.AddInitCommand(clusterCmd)
	cluster.AddStartCommand(clusterCmd)
	cluster.AddStopCommand(clusterCmd)
	cluster.AddRestartCommand(clusterCmd)
	cluster.AddStatusCommand(clusterCmd)
	cluster.AddBackupCommand(clusterCmd)
	cluster.AddListBackupsCommand(clusterCmd)
	cluster.AddCheckBackupConfigCommand(clusterCmd)
	cluster.AddRefreshCredentialsCommand(clusterCmd)

	// Register cluster command with root
	root.AddCommand(clusterCmd)
}
