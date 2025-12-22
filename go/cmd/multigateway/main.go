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

// multigateway is the top-level proxy that masquerades as a PostgreSQL server,
// handling client connections and routing queries to multipooler instances.
package main

import (
	"log/slog"
	"os"

	"github.com/multigres/multigres/go/multigateway"

	"github.com/spf13/cobra"
)

// CreateMultiGatewayCommand creates a cobra command with a MultiGateway instance and registers its flags
func CreateMultiGatewayCommand() (*cobra.Command, *multigateway.MultiGateway) {
	mg := multigateway.NewMultiGateway()

	cmd := &cobra.Command{
		Use:   "multigateway",
		Short: "Multigateway is a stateless proxy responsible for accepting requests from applications and routing them to the appropriate multipooler server(s) for query execution. It speaks both the PostgresSQL Protocol and a gRPC protocol.",
		Long:  "Multigateway is a stateless proxy responsible for accepting requests from applications and routing them to the appropriate multipooler server(s) for query execution. It speaks both the PostgresSQL Protocol and a gRPC protocol.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return mg.CobraPreRunE(cmd)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(mg)
		},
	}

	mg.RegisterFlags(cmd.Flags())

	return cmd, mg
}

func main() {
	cmd, _ := CreateMultiGatewayCommand()

	if err := cmd.Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}
}

func run(mg *multigateway.MultiGateway) error {
	if err := mg.Init(); err != nil {
		return err
	}
	return mg.RunDefault()
}
