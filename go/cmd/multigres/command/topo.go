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
	"github.com/multigres/multigres/go/cmd/multigres/command/topo"

	"github.com/spf13/cobra"
)

// AddTopoCommands adds the topo subcommands to the root command
func AddTopoCommands(root *cobra.Command, mc *MultigresCommand) {
	// Register topo commands with root
	root.AddCommand(topo.AddGetCellCommand())
	root.AddCommand(topo.AddGetDatabaseCommand())
	root.AddCommand(topo.AddGetCellNamesCommand())
	root.AddCommand(topo.AddGetDatabaseNamesCommand())
	root.AddCommand(topo.AddGetGatewaysCommand())
	root.AddCommand(topo.AddGetPoolersCommand())
	root.AddCommand(topo.AddGetOrchsCommand())
}
