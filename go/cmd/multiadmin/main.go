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

// multiadmin provides administrative services for the multigres cluster,
// exposing both HTTP and gRPC endpoints for cluster management operations.
package main

import (
	"log/slog"
	"os"

	"github.com/multigres/multigres/go/multiadmin"

	"github.com/spf13/cobra"
)

// CreateMultiAdminCommand creates a cobra command with a MultiAdmin instance and registers its flags
func CreateMultiAdminCommand() (*cobra.Command, *multiadmin.MultiAdmin) {
	ma := multiadmin.NewMultiAdmin()

	cmd := &cobra.Command{
		Use:   "multiadmin",
		Short: "Multiadmin provides administrative services for the multigres cluster, exposing both HTTP and gRPC endpoints for cluster management operations.",
		Long:  "Multiadmin provides administrative services for the multigres cluster, exposing both HTTP and gRPC endpoints for cluster management operations.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return ma.CobraPreRunE(cmd)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(ma)
		},
	}

	ma.RegisterFlags(cmd.Flags())

	return cmd, ma
}

func main() {
	cmd, _ := CreateMultiAdminCommand()

	if err := cmd.Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}
}

func run(ma *multiadmin.MultiAdmin) error {
	if err := ma.Init(); err != nil {
		return err
	}
	return ma.RunDefault()
}
