// Copyright 2025 Supabase, Inc.
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

package command

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/viperutil"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// MultigresCommand holds the configuration for multigres commands
type MultigresCommand struct {
	reg       *viperutil.Registry
	vc        *viperutil.ViperConfig
	telemetry *telemetry.Telemetry
}

// GetRootCommand creates and returns the root command for multigres with all subcommands
func GetRootCommand() *cobra.Command {
	reg := viperutil.NewRegistry()
	telemetry := telemetry.NewTelemetry()
	mc := &MultigresCommand{
		reg:       reg,
		vc:        viperutil.NewViperConfig(reg),
		telemetry: telemetry,
	}

	var span trace.Span

	root := &cobra.Command{
		Use:   "multigres",
		Short: "The command-line companion for managing and developing with Multigres clusters",
		Long: `The Multigres CLI makes distributed Postgres feel as easy as running Postgres locally.

A single binary that gives developers confidence when experimenting,
and operators the tools to keep clusters healthy at scale.

Get started with:
  multigres cluster init    # Create a local cluster configuration
  multigres cluster up      # Start your local cluster

Configuration:
  Multigres automatically searches for configuration files in this order:
  1. File specified by --config-file flag (if provided)
  2. Files named 'multigres' with supported extensions (.yaml, .yml, .json, .toml)
     in directories specified by --config-path flags
  3. Current working directory (default search path)

  Environment variable MT_CONFIG_NAME can override the config filename.
  Use --config-file-not-found-handling to control behavior when no config is found.`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Silence usage for application errors, but allow it for flag errors
			// This gets called after flag parsing, so flag errors will still show usage
			cmd.SilenceUsage = true

			// Set multigres-specific config name
			viper.SetConfigName("multigres")

			// Load config (without the full servenv setup)
			_, err := mc.vc.LoadConfig(mc.reg)
			if err != nil {
				return err
			}

			if span, err = mc.telemetry.InitForCommand(cmd, "multigres-cli", true); err != nil {
				return fmt.Errorf("failed to initialize OpenTelemetry: %w", err)
			}

			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			span.End()

			// Shutdown OpenTelemetry to flush all pending spans
			// This is critical for CLI commands to export traces before process exit
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := mc.telemetry.ShutdownTelemetry(ctx); err != nil {
				return fmt.Errorf("failed to shutdown OpenTelemetry: %w", err)
			}
			return nil
		},
	}

	// Add any other servenv flags
	mc.vc.RegisterFlags(root.PersistentFlags())

	// Override the default display value for multigres
	if flag := root.PersistentFlags().Lookup("config-name"); flag != nil {
		flag.DefValue = "multigres"
	}

	// Add all subcommands
	AddClusterCommand(root, mc)
	AddTopoCommands(root, mc)

	return root
}
