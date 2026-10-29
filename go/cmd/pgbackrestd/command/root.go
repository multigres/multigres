// Copyright 2026 Supabase, Inc.
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

// Package command holds the pgbackrestd cobra commands.
package command

import (
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/viperutil"
)

const serviceName = "pgbackrestd"

// GetRootCommand returns the root command and a accessor for its logger.
func GetRootCommand() (*cobra.Command, func() *slog.Logger) {
	reg := viperutil.NewRegistry()
	vc := viperutil.NewViperConfig(reg)
	tel := telemetry.NewTelemetry()
	lg := servenv.NewLogger(reg, tel)

	root := &cobra.Command{
		Use:   serviceName,
		Short: "pgBackRest sidecar for Multigres shard pods",
		Args:  cobra.NoArgs,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			cmd.Root().SilenceUsage = true
			lg.SetupLogging()
			return nil
		},
	}
	vc.RegisterFlags(root.PersistentFlags())
	lg.RegisterFlags(root.PersistentFlags())

	root.AddCommand(newServeCommand(lg.GetLogger, tel))
	root.AddCommand(newCredsCmd())
	root.AddCommand(newHealthCmd())
	return root, lg.GetLogger
}
