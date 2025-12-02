// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// multiorch orchestrates cluster operations including consensus protocol management,
// failover detection and repair, and health monitoring of multipooler instances.
package main

import (
	"log/slog"
	"os"

	"github.com/multigres/multigres/go/multiorch"

	"github.com/spf13/cobra"
)

// CreateMultiOrchCommand creates a cobra command with a MultiOrch instance and registers its flags
func CreateMultiOrchCommand() (*cobra.Command, *multiorch.MultiOrch) {
	mo := multiorch.NewMultiOrch()

	cmd := &cobra.Command{
		Use:   "multiorch",
		Short: "Multiorch orchestrates cluster operations including consensus protocol management, failover detection and repair, and health monitoring of multipooler instances.",
		Long:  "Multiorch orchestrates cluster operations including consensus protocol management, failover detection and repair, and health monitoring of multipooler instances.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return mo.CobraPreRunE(cmd)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, args, mo)
		},
	}

	mo.RegisterFlags(cmd.Flags())

	return cmd, mo
}

func main() {
	cmd, _ := CreateMultiOrchCommand()

	if err := cmd.Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string, mo *multiorch.MultiOrch) error {
	if err := mo.Init(); err != nil {
		return err
	}
	mo.RunDefault()
	return nil
}
