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

// multipooler provides connection pooling and communicates with pgctld via gRPC
// to serve queries from multigateway instances.
package main

import (
	"log/slog"
	"os"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

var (
	pgctldAddr string

	Main = &cobra.Command{
		Use:     "multipooler",
		Short:   "Multipooler provides connection pooling and communicates with pgctld via gRPC to serve queries from multigateway instances.",
		Long:    "Multipooler provides connection pooling and communicates with pgctld via gRPC to serve queries from multigateway instances.",
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
		logger.Info("multipooler starting up",
			"pgctld_addr", pgctldAddr,
			"grpc_port", servenv.GRPCPort(),
		)
	})
	servenv.OnClose(func() {
		logger.Info("multipooler shutting down")
		// TODO: adds closing hooks
	})
	// TODO: Initialize gRPC connection to pgctld
	// TODO: Setup health check endpoint
	// TODO: Register with topology service
	servenv.RunDefault()

	return nil
}

func init() {
	servenv.RegisterServiceCmd(Main)

	// Adds multipooler specific flags
	Main.Flags().StringVar(&pgctldAddr, "pgctld-addr", "localhost:15200", "Address of pgctld gRPC service")
}
