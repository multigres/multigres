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

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Manage cluster lifecycle",
	Long:  "Commands for managing the Multigres cluster lifecycle including initialization, startup, shutdown, and status monitoring.",
}

func init() {
	// Register cluster subcommands
	clusterCmd.AddCommand(cluster.InitCommand)
	clusterCmd.AddCommand(cluster.StartCommand)
	clusterCmd.AddCommand(cluster.StopCommand)
	clusterCmd.AddCommand(cluster.StatusCommand)

	// Register cluster command with root
	Root.AddCommand(clusterCmd)
}
