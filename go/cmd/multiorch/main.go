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

// multiorch orchestrates cluster operations including consensus protocol management,
// failover detection and repair, and health monitoring of multipooler instances.
package main

import (
	"log/slog"
	"os"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

var (
	Main = &cobra.Command{
		Use:     "multiorch",
		Short:   "Multiorch orchestrates cluster operations including consensus protocol management, failover detection and repair, and health monitoring of multipooler instances.",
		Long:    "Multiorch orchestrates cluster operations including consensus protocol management, failover detection and repair, and health monitoring of multipooler instances.",
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

	ts := topo.Open()
	defer func() { _ = ts.Close() }()

	servenv.OnRun(func() {
		// Flags are parsed now.
		logger.Info("multiorch starting up",
			"grpc_port", servenv.GRPCPort(),
		)

	})

	servenv.OnClose(func() {
		logger.Info("multiorch shutting down")
		// TODO: adds closing hooks
	})

	// TODO: Initialize connection to topology server (etcd)
	// TODO: Setup consensus protocol management
	// TODO: Implement failover detection and repair
	// TODO: Setup health monitoring of multipooler instances
	servenv.RunDefault()

	return nil
}

func init() {
	servenv.RegisterServiceCmd(Main)
}
