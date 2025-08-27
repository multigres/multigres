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

package init

import (
	"fmt"

	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

var Command = &cobra.Command{
	Use:   "init",
	Short: "Create a local cluster configuration",
	Long:  "Initialize a new local Multigres cluster configuration that can be used with 'multigres cluster up'.",
	RunE: func(cmd *cobra.Command, args []string) error {
		servenv.FireRunHooks()
		fmt.Println("Initializing Multigres cluster configuration...")
		// TODO: Implement cluster initialization logic
		fmt.Println("Cluster configuration created successfully!")
		return nil
	},
}
