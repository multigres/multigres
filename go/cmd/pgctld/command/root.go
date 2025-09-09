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

package command

import (
	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

// Flag variables for root command (shared across all commands)
var (
	pgHost       = "localhost"
	pgPort       = 5432
	pgDatabase   = "postgres"
	pgUser       = "postgres"
	pgPassword   = ""
	pgDataDir    = ""
	pgConfigFile = ""
	pgSocketDir  = "/tmp"
	timeout      = 30
)

// Root represents the base command when called without any subcommands
var Root = &cobra.Command{
	Use:   "pgctld",
	Short: "PostgreSQL control daemon for Multigres",
	Long: `pgctld manages PostgreSQL server instances within the Multigres cluster.
It provides lifecycle management including start, stop, restart, and configuration
management for PostgreSQL servers.`,
	Args:    cobra.NoArgs,
	PreRunE: servenv.CobraPreRunE,
}

func init() {
	servenv.RegisterServiceCmd(Root)
	servenv.InitServiceMap("grpc", "pgctld")
	Root.PersistentFlags().StringVarP(&pgHost, "pg-host", "H", pgHost, "PostgreSQL host")
	Root.PersistentFlags().IntVarP(&pgPort, "pg-port", "p", pgPort, "PostgreSQL port")
	Root.PersistentFlags().StringVarP(&pgDatabase, "pg-database", "D", pgDatabase, "PostgreSQL database name")
	Root.PersistentFlags().StringVarP(&pgUser, "pg-user", "U", pgUser, "PostgreSQL username")
	Root.PersistentFlags().StringVar(&pgPassword, "pg-password", pgPassword, "PostgreSQL password")
	Root.PersistentFlags().StringVarP(&pgDataDir, "pg-data-dir", "d", pgDataDir, "PostgreSQL data directory")
	Root.PersistentFlags().StringVar(&pgConfigFile, "pg-config-file", pgConfigFile, "PostgreSQL configuration file")
	Root.PersistentFlags().StringVar(&pgSocketDir, "pg-socket-dir", pgSocketDir, "PostgreSQL socket directory")
	Root.PersistentFlags().IntVarP(&timeout, "timeout", "t", timeout, "Operation timeout in seconds")
}
