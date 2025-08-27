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

package up

import (
	"fmt"

	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

var Command = &cobra.Command{
	Use:   "up",
	Short: "Start local cluster",
	Long:  "Start a local Multigres cluster using the configuration created with 'multigres cluster init'.",
	RunE: func(cmd *cobra.Command, args []string) error {
		servenv.FireRunHooks()
		fmt.Println("Starting Multigres cluster...")
		// TODO: Implement cluster startup logic
		fmt.Println("Multigres cluster started successfully!")
		return nil
	},
}
