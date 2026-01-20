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

package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/multigres/multigres/config"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/viperutil"

	"github.com/spf13/cobra"
)

// PgCtlCommand holds the configuration for pgctld commands
type PgCtlCommand struct {
	reg                *viperutil.Registry
	pgDatabase         viperutil.Value[string]
	pgUser             viperutil.Value[string]
	poolerDir          viperutil.Value[string]
	timeout            viperutil.Value[int]
	pgPort             viperutil.Value[int]
	pgListenAddresses  viperutil.Value[string]
	pgHbaTemplate      viperutil.Value[string]
	postgresConfigTmpl viperutil.Value[string]
	vc                 *viperutil.ViperConfig
	lg                 *servenv.Logger
	telemetry          *telemetry.Telemetry
}

// GetRootCommand creates and returns the root command for pgctld with all subcommands
func GetRootCommand() (*cobra.Command, *PgCtlCommand) {
	telemetry := telemetry.NewTelemetry()
	reg := viperutil.NewRegistry()
	pc := &PgCtlCommand{
		reg: reg,
		pgDatabase: viperutil.Configure(reg, "pg-database", viperutil.Options[string]{
			Default:  constants.DefaultPostgresDatabase,
			FlagName: "pg-database",
			Dynamic:  false,
		}),
		pgUser: viperutil.Configure(reg, "pg-user", viperutil.Options[string]{
			Default:  constants.DefaultPostgresUser,
			FlagName: "pg-user",
			Dynamic:  false,
		}),
		timeout: viperutil.Configure(reg, "timeout", viperutil.Options[int]{
			Default:  30,
			FlagName: "timeout",
			Dynamic:  false,
		}),
		poolerDir: viperutil.Configure(reg, "pooler-dir", viperutil.Options[string]{
			Default:  "",
			FlagName: "pooler-dir",
			Dynamic:  false,
		}),
		pgPort: viperutil.Configure(reg, "pg-port", viperutil.Options[int]{
			Default:  5432,
			FlagName: "pg-port",
			Dynamic:  false,
		}),
		pgListenAddresses: viperutil.Configure(reg, "pg-listen-addresses", viperutil.Options[string]{
			Default:  "*",
			FlagName: "pg-listen-addresses",
			Dynamic:  false,
		}),
		pgHbaTemplate: viperutil.Configure(reg, "pg-hba-template", viperutil.Options[string]{
			Default:  "",
			FlagName: "pg-hba-template",
			Dynamic:  false,
		}),
		postgresConfigTmpl: viperutil.Configure(reg, "postgres-config-template", viperutil.Options[string]{
			Default:  "",
			FlagName: "postgres-config-template",
			Dynamic:  false,
		}),
		vc:        viperutil.NewViperConfig(reg),
		lg:        servenv.NewLogger(reg, telemetry),
		telemetry: telemetry,
	}

	var span trace.Span

	root := &cobra.Command{
		Use:   constants.ServicePgctld,
		Short: "PostgreSQL control daemon for Multigres",
		Long: `pgctld manages PostgreSQL server instances within the Multigres cluster.
It provides lifecycle management including start, stop, restart, and configuration
management for PostgreSQL servers.`,
		Args: cobra.NoArgs,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			pc.lg.SetupLogging()
			// Initialize telemetry for CLI commands (server command will re-initialize via ServEnv.Init)
			var err error
			if span, err = pc.telemetry.InitForCommand(cmd, constants.ServicePgctld, cmd.Use != "server" /* startSpan */); err != nil {
				return fmt.Errorf("failed to initialize OpenTelemetry: %w", err)
			}

			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			span.End()

			// Shutdown OpenTelemetry to flush all pending spans
			// For server command, this runs after the server has shut down
			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			defer cancel()
			if err := pc.telemetry.ShutdownTelemetry(ctx); err != nil {
				return fmt.Errorf("failed to shutdown OpenTelemetry: %w", err)
			}
			return nil
		},
	}

	root.PersistentFlags().StringP("pg-database", "D", pc.pgDatabase.Default(), "PostgreSQL database name")
	root.PersistentFlags().StringP("pg-user", "U", pc.pgUser.Default(), "PostgreSQL username")
	root.PersistentFlags().IntP("timeout", "t", pc.timeout.Default(), "Operation timeout in seconds")
	root.PersistentFlags().String("pooler-dir", pc.poolerDir.Default(), "The directory to multipooler data")
	root.PersistentFlags().IntP("pg-port", "p", pc.pgPort.Default(), "PostgreSQL port")
	root.PersistentFlags().String("pg-listen-addresses", pc.pgListenAddresses.Default(), "PostgreSQL listen addresses")
	root.PersistentFlags().String("pg-hba-template", pc.pgHbaTemplate.Default(), "Path to custom pg_hba.conf template file")
	root.PersistentFlags().String("postgres-config-template", pc.postgresConfigTmpl.Default(), "Path to custom postgresql.conf template file")
	pc.vc.RegisterFlags(root.PersistentFlags())
	pc.lg.RegisterFlags(root.PersistentFlags())

	viperutil.BindFlags(root.PersistentFlags(),
		pc.pgDatabase,
		pc.pgUser,
		pc.timeout,
		pc.poolerDir,
		pc.pgPort,
		pc.pgListenAddresses,
		pc.pgHbaTemplate,
		pc.postgresConfigTmpl,
	)

	// Add all subcommands
	AddServerCommand(root, pc)
	AddInitCommand(root, pc)
	AddStartCommand(root, pc)
	AddStopCommand(root, pc)
	AddRestartCommand(root, pc)
	AddStatusCommand(root, pc)
	AddVersionCommand(root, pc)
	AddReloadCommand(root, pc)

	return root, pc
}

// validateGlobalFlags validates required global flags for all pgctld commands
func (pc *PgCtlCommand) validateGlobalFlags(cmd *cobra.Command, args []string) error {
	// Validate pooler-dir is required and non-empty for all commands
	poolerDir := pc.GetPoolerDir()
	if poolerDir == "" {
		return errors.New("pooler-dir needs to be set")
	}

	// If pg-hba-template is specified, read and replace the default template
	pgHbaTemplatePath := pc.pgHbaTemplate.Get()
	if pgHbaTemplatePath != "" {
		contents, err := os.ReadFile(pgHbaTemplatePath)
		if err != nil {
			return fmt.Errorf("failed to read pg-hba-template file %s: %w", pgHbaTemplatePath, err)
		}
		config.PostgresHbaDefaultTmpl = string(contents)
		pc.GetLogger().Info("replaced default pg_hba.conf template", "path", pgHbaTemplatePath)
	}

	// If postgres-config-template is specified, read and replace the default template
	postgresConfigTemplatePath := pc.postgresConfigTmpl.Get()
	if postgresConfigTemplatePath != "" {
		contents, err := os.ReadFile(postgresConfigTemplatePath)
		if err != nil {
			return fmt.Errorf("failed to read postgres-config-template file %s: %w", postgresConfigTemplatePath, err)
		}
		config.PostgresConfigDefaultTmpl = string(contents)
		pc.GetLogger().Info("replaced default postgresql.conf template", "path", postgresConfigTemplatePath)
	}

	return nil
}

// GetLogger returns the configured logger instance
func (pc *PgCtlCommand) GetLogger() *slog.Logger {
	return pc.lg.GetLogger()
}

// GetPoolerDir returns the configured pooler directory as an absolute path
func (pc *PgCtlCommand) GetPoolerDir() string {
	poolerDir := pc.poolerDir.Get()
	if poolerDir == "" {
		return ""
	}

	absPath, err := pgctld.ExpandToAbsolutePath(poolerDir)
	if err != nil {
		// If we can't expand the path, return the original to avoid breaking existing behavior
		// This should rarely happen in practice
		return poolerDir
	}

	return absPath
}

// validateInitialized validates that the PostgreSQL data directory has been initialized
// This should be called by all commands except 'init'
func (pc *PgCtlCommand) validateInitialized(cmd *cobra.Command, args []string) error {
	// First run the standard global validation
	if err := pc.validateGlobalFlags(cmd, args); err != nil {
		return err
	}

	// Check if data directory is initialized
	poolerDir := pc.GetPoolerDir()

	if !pgctld.IsDataDirInitialized(poolerDir) {
		dataDir := pgctld.PostgresDataDir(poolerDir)
		return fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	return nil
}
