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
	"github.com/multigres/multigres/go/cmd/multigres/command/pooler"

	"github.com/spf13/cobra"
)

// AddPoolerCommands adds the pooler subcommands to the root command
func AddPoolerCommands(root *cobra.Command, mc *MultigresCommand) {
	// Register pooler commands with root
	root.AddCommand(pooler.AddGetPoolerStatusCommand())
	root.AddCommand(pooler.AddEnablePostgresMonitorCommand())
	root.AddCommand(pooler.AddDisablePostgresMonitorCommand())
}
