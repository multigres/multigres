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

// multipooler provides connection pooling and communicates with pgctld via gRPC
// to serve queries from multigateway instances.
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
	"github.com/multigres/multigres/go/services/multipooler"
	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// standaloneMultipooler owns the process-level resources (servenv, gRPC
// server, topology store) for a multipooler running alone, as its own
// process — the same shape cmd/minigres builds, just for one component
// instead of two. The multipooler itself never constructs these; see
// multipooler.NewMultipooler.
type standaloneMultipooler struct {
	reg        *viperutil.Registry
	senv       *servenv.ServEnv
	grpcServer *servenv.GrpcServer
	topoConfig *topoclient.TopoConfig

	mp *multipooler.Multipooler
}

func newStandaloneMultipooler(tel *telemetry.Telemetry) *standaloneMultipooler {
	reg := viperutil.NewRegistry()
	s := &standaloneMultipooler{
		reg:        reg,
		senv:       servenv.NewServEnvWithConfig(reg, servenv.NewLogger(reg, tel), viperutil.NewViperConfig(reg), tel),
		grpcServer: servenv.NewGrpcServer(reg),
		topoConfig: topoclient.NewTopoConfig(reg),
	}
	resources := servenv.ProcessResources{
		ServEnv:    s.senv,
		GrpcServer: s.grpcServer,
	}
	s.mp = multipooler.NewMultipooler(tel, resources, "/", false)
	return s
}

func (s *standaloneMultipooler) registerFlags(cmd *cobra.Command) {
	fs := cmd.Flags()
	s.senv.RegisterFlags(fs)
	s.grpcServer.RegisterFlags(fs)
	s.topoConfig.RegisterFlags(fs)
	s.mp.RegisterFlags(fs)
}

func (s *standaloneMultipooler) preRun(cmd *cobra.Command) error {
	return s.senv.CobraPreRunE(cmd)
}

func (s *standaloneMultipooler) run(cmd *cobra.Command, ctx context.Context) error {
	ts, err := s.topoConfig.Open()
	if err != nil {
		return fmt.Errorf("topo open: %w", err)
	}
	defer ts.Close()

	// service-id defaults to a random value when unset, same as cmd/minigres:
	// generate one and write it back into the flag so ServiceIdentity (and
	// everything downstream that reads mp.serviceID, e.g. the topology record)
	// sees the resolved value too, not just servenv.Init.
	if s.mp.ServiceIdentity().ServiceInstanceID == "" {
		if err := cmd.Flags().Set("service-id", servenv.GenerateRandomServiceID()); err != nil {
			return fmt.Errorf("set service-id: %w", err)
		}
	}
	if err := s.senv.Init(s.mp.ServiceIdentity()); err != nil {
		return fmt.Errorf("servenv init: %w", err)
	}

	if err := s.mp.Init(ctx, ts); err != nil {
		return err
	}
	return s.mp.RunDefault()
}

// CreateMultipoolerCommand creates a cobra command with a Multipooler instance and registers its flags
func CreateMultipoolerCommand() (*cobra.Command, *multipooler.Multipooler) {
	s := newStandaloneMultipooler(telemetry.NewTelemetry())

	cmd := &cobra.Command{
		Use:   constants.ServiceMultipooler,
		Short: "Multipooler provides connection pooling and communicates with pgctld via gRPC to serve queries from multigateway instances.",
		Long:  "Multipooler provides connection pooling and communicates with pgctld via gRPC to serve queries from multigateway instances.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return s.preRun(cmd)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return s.run(cmd, cmd.Context())
		},
	}
	s.registerFlags(cmd)

	return cmd, s.mp
}

func main() {
	cmd, _ := CreateMultipoolerCommand()

	if err := cmd.Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}
}
