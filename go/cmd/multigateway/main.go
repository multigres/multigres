/*
Copyright 2025 The Multigres Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// multigateway is the top-level proxy that masquerades as a PostgreSQL server,
// handling client connections and routing queries to multipooler instances.
package main

import (
	"log/slog"
	"os"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	_ "github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

var (
	cell string

	Main = &cobra.Command{
		Use:     "multigateway",
		Short:   "Multigateway is a stateless proxy responsible for accepting requests from applications and routing them to the appropriate multipooler server(s) for query execution. It speaks both the PostgresSQL Protocol and a gRPC protocol.",
		Long:    "Multigateway is a stateless proxy responsible for accepting requests from applications and routing them to the appropriate multipooler server(s) for query execution. It speaks both the PostgresSQL Protocol and a gRPC protocol.",
		Args:    cobra.NoArgs,
		PreRunE: servenv.CobraPreRunE,
		RunE:    run,
	}
)

func main() {
	if err := Main.Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {

	servenv.Init()

	// Get the configured logger
	logger := servenv.GetLogger()

	// Ensure we open the topo before we start the context, so that the
	// defer that closes the topo runs after cancelling the context.
	// This ensures that we've properly closed things like the watchers
	// at that point.
	ts := topo.Open()
	defer func() { _ = ts.Close() }()

	servenv.OnRun(func() {
		// Flags are parsed now.
		logger.Info("multigateway starting up",
			"cell", cell,
			"port", servenv.Port(),
			"grpc_port", servenv.GRPCPort(),
		)
		// TODO: OnRun logic
	})
	servenv.OnClose(func() {
		logger.Info("multigateway shutting down")
		//  TODO: adds closing hooks
	})
	servenv.RunDefault()

	return nil
}

func init() {
	// Register flags BEFORE parsing them
	servenv.RegisterDefaultFlags()
	servenv.RegisterFlags()
	servenv.RegisterGRPCServerFlags()
	servenv.RegisterGRPCServerAuthFlags()
	servenv.RegisterServiceMapFlag()

	// Get the flag set from servenv and add it to the cobra command
	servenv.AddFlagSetToCobraCommand(Main)
	Main.Flags().StringVar(&cell, "cell", cell, "cell to use")
}
