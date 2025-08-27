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
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var Command = &cobra.Command{
	Use:   "init",
	Short: "Create a local cluster configuration",
	Long:  "Initialize a new local Multigres cluster configuration that can be used with 'multigres cluster up'.",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Validate config paths exist
		configPaths, err := cmd.Flags().GetStringSlice("config-path")
		if err != nil {
			return fmt.Errorf("failed to get config-path flag: %w", err)
		}
		if len(configPaths) == 0 {
			return fmt.Errorf("no config paths specified")
		}

		for _, configPath := range configPaths {
			absPath, err := filepath.Abs(configPath)
			if err != nil {
				cmd.SilenceUsage = true
				return fmt.Errorf("failed to resolve config path %s: %w", configPath, err)
			}

			if _, err := os.Stat(absPath); os.IsNotExist(err) {
				cmd.SilenceUsage = true
				return fmt.Errorf("config path does not exist: %s", absPath)
			} else if err != nil {
				cmd.SilenceUsage = true
				return fmt.Errorf("failed to access config path %s: %w", absPath, err)
			}

			// Check if it's a directory
			if info, err := os.Stat(absPath); err == nil && !info.IsDir() {
				cmd.SilenceUsage = true
				return fmt.Errorf("config path is not a directory: %s", absPath)
			}
		}

		fmt.Println("Initializing Multigres cluster configuration...")
		// TODO: Implement cluster initialization logic
		fmt.Println("Cluster configuration created successfully!")
		return nil
	},
}
