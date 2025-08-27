// Copyright 2025 The Multigres Authors.
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

package down

import (
	"fmt"

	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

var Command = &cobra.Command{
	Use:   "down",
	Short: "Stop local cluster",
	Long:  "Stop the local Multigres cluster. Use --clean to fully tear down all resources.",
	RunE: func(cmd *cobra.Command, args []string) error {
		servenv.FireRunHooks()
		fmt.Println("Stopping Multigres cluster...")
		// TODO: Implement cluster shutdown logic
		fmt.Println("Multigres cluster stopped successfully!")
		return nil
	},
}

func init() {
	Command.Flags().Bool("clean", false, "Fully tear down all cluster resources")
}
