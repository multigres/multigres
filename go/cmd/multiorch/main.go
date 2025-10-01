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
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/netutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/viperutil"

	"github.com/spf13/cobra"
)

type MultiOrch struct {
	cell viperutil.Value[string]
	// multiorchID stores the ID for deregistration during shutdown
	multiorchID *clustermetadatapb.ID
}

// CheckCellFlags validates the cell flag against available cells in the topology.
// It helps avoid strange behaviors when multiorch runs but actually does not work
// due to referencing non-existent cells.
func CheckCellFlags(ts topo.Store, cell string) error {
	if ts == nil {
		return fmt.Errorf("topo server cannot be nil")
	}

	// Validate cell flag is set
	if cell == "" {
		return fmt.Errorf("cell flag must be set")
	}

	// Create context with timeout for topology operations
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Get all known cells from topology.
	cellsInTopo, err := ts.GetCellNames(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cells from topology (timeout after 2s): %w", err)
	}
	if len(cellsInTopo) == 0 {
		return fmt.Errorf("topo server should have at least one cell configured")
	}

	// Check if the specified cell exists in topology
	hasCell := slices.Contains(cellsInTopo, cell)
	if !hasCell {
		return fmt.Errorf("cell '%s' does not exist in topology. Available cells: [%s]",
			cell, strings.Join(cellsInTopo, ", "))
	}

	return nil
}

func main() {
	mo := &MultiOrch{
		cell: viperutil.Configure("cell", viperutil.Options[string]{
			Default:  "",
			FlagName: "cell",
			Dynamic:  false,
			EnvVars:  []string{"MT_CELL"},
		}),
	}

	main := &cobra.Command{
		Use:     "multiorch",
		Short:   "Multiorch orchestrates cluster operations including consensus protocol management, failover detection and repair, and health monitoring of multipooler instances.",
		Long:    "Multiorch orchestrates cluster operations including consensus protocol management, failover detection and repair, and health monitoring of multipooler instances.",
		Args:    cobra.NoArgs,
		PreRunE: servenv.CobraPreRunE,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, args, mo)
		},
	}

	main.Flags().String("cell", mo.cell.Default(), "cell to use")
	viperutil.BindFlags(main.Flags(), mo.cell)
	servenv.RegisterServiceCmd(main)

	if err := main.Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string, mo *MultiOrch) error {
	servenv.Init()

	// Get the configured logger
	logger := servenv.GetLogger()

	ts := topo.Open()
	defer func() { _ = ts.Close() }()

	// Validate cell configuration early to fail fast if misconfigured
	if err := CheckCellFlags(ts, mo.cell.Get()); err != nil {
		logger.Error("Cell validation failed", "error", err)
		return fmt.Errorf("cell validation failed: %w", err)
	}
	logger.Info("Cell validation passed", "cell", mo.cell)

	servenv.OnRun(func() {
		// Flags are parsed now.
		logger.Info("multiorch starting up",
			"cell", mo.cell,
			"grpc_port", servenv.GRPCPort(),
		)

		// Register with topology service
		hostname, err := netutil.FullyQualifiedHostname()
		if err != nil {
			logger.Warn("Failed to get fully qualified hostname, falling back to simple hostname", "error", err)
			hostname, err = os.Hostname()
			if err != nil {
				logger.Error("Failed to get hostname", "error", err)
				return
			}
		}

		// Create MultiOrch instance for topo registration
		multiorch := topo.NewMultiOrch("", mo.cell.Get(), hostname)
		multiorch.PortMap["grpc"] = int32(servenv.GRPCPort())

		// Store ID for deregistration during shutdown
		mo.multiorchID = multiorch.Id

		// Register with topology
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := ts.InitMultiOrch(ctx, multiorch, true); err != nil {
			logger.Error("Failed to register multiorch with topology", "error", err)
		} else {
			logger.Info("Successfully registered multiorch with topology", "id", multiorch.Id)
		}
	})

	servenv.OnClose(func() {
		logger.Info("multiorch shutting down")

		// Deregister from topology service
		if mo.multiorchID != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := ts.DeleteMultiOrch(ctx, mo.multiorchID); err != nil {
				logger.Error("Failed to deregister multiorch from topology", "error", err, "id", mo.multiorchID)
			} else {
				logger.Info("Successfully deregistered multiorch from topology", "id", mo.multiorchID)
			}
		}
	})

	// TODO: Setup consensus protocol management
	// TODO: Implement failover detection and repair
	// TODO: Setup health monitoring of multipooler instances
	servenv.RunDefault()

	return nil
}
