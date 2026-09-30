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
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multigateway"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// standaloneMultigateway owns the process-level resources (servenv, gRPC
// server, topology store) for a multigateway running alone, as its own
// process — the same shape cmd/minigres builds, just for one component
// instead of two. The multigateway itself never constructs these; see
// multigateway.NewMultigateway.
type standaloneMultigateway struct {
	reg        *viperutil.Registry
	senv       *servenv.ServEnv
	grpcServer *servenv.GrpcServer
	topoConfig *topoclient.TopoConfig

	mg *multigateway.Multigateway
}

func newStandaloneMultigateway() *standaloneMultigateway {
	reg := viperutil.NewRegistry()
	s := &standaloneMultigateway{
		reg:        reg,
		senv:       servenv.NewServEnv(reg),
		grpcServer: servenv.NewGrpcServer(reg),
		topoConfig: topoclient.NewTopoConfig(reg),
	}
	resources := servenv.ProcessResources{
		ServEnv:    s.senv,
		GrpcServer: s.grpcServer,
	}
	s.mg = multigateway.NewMultigateway(resources, "/")
	return s
}

func (s *standaloneMultigateway) registerFlags(cmd *cobra.Command) {
	fs := cmd.Flags()
	s.senv.RegisterFlags(fs)
	s.grpcServer.RegisterFlags(fs)
	s.topoConfig.RegisterFlags(fs)
	s.mg.RegisterFlags(fs)
}

// preRun runs the multigateway's own PreRun side effect (its config-reload
// channel) before the shared servenv parses configuration.
func (s *standaloneMultigateway) preRun(cmd *cobra.Command) error {
	if err := s.mg.CobraPreRunE(cmd); err != nil {
		return err
	}
	return s.senv.CobraPreRunE(cmd)
}

func (s *standaloneMultigateway) run(cmd *cobra.Command, ctx context.Context) error {
	ts, err := s.topoConfig.Open()
	if err != nil {
		return fmt.Errorf("topo open: %w", err)
	}
	defer ts.Close()

	// service-id defaults to a random value when unset, same as cmd/minigres:
	// generate one and write it back into the flag so ServiceIdentity (and
	// everything downstream that reads mg.serviceID) sees the resolved value.
	if s.mg.ServiceIdentity().ServiceInstanceID == "" {
		if err := cmd.Flags().Set("service-id", servenv.GenerateRandomServiceID()); err != nil {
			return fmt.Errorf("set service-id: %w", err)
		}
	}
	if err := s.senv.Init(s.mg.ServiceIdentity()); err != nil {
		return fmt.Errorf("servenv init: %w", err)
	}

	if err := s.mg.Init(ctx, ts); err != nil {
		return err
	}
	return s.mg.RunDefault()
}

// CreateMultigatewayCommand creates a cobra command with a Multigateway instance and registers its flags
func CreateMultigatewayCommand() (*cobra.Command, *multigateway.Multigateway) {
	s := newStandaloneMultigateway()

	cmd := &cobra.Command{
		Use:   constants.ServiceMultigateway,
		Short: "Multigateway is a stateless proxy responsible for accepting requests from applications and routing them to the appropriate multipooler server(s) for query execution. It speaks both the PostgreSQL Protocol and a gRPC protocol.",
		Long:  "Multigateway is a stateless proxy responsible for accepting requests from applications and routing them to the appropriate multipooler server(s) for query execution. It speaks both the PostgreSQL Protocol and a gRPC protocol.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return s.preRun(cmd)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return s.run(cmd, cmd.Context())
		},
	}
	s.registerFlags(cmd)

	return cmd, s.mg
}

func main() {
	cmd, _ := CreateMultigatewayCommand()

	if err := cmd.Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}
}
