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
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
)

// Root represents the base command when called without any subcommands
var Root = &cobra.Command{
	Use:   "pgctld",
	Short: "PostgreSQL control daemon for Multigres",
	Long: `pgctld manages PostgreSQL server instances within the Multigres cluster.
It provides lifecycle management including start, stop, restart, and configuration
management for PostgreSQL servers.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return initConfig()
	},
}

func init() {
	// Global flags
	Root.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.pgctld.yaml)")

	// PostgreSQL connection flags
	Root.PersistentFlags().StringP("pg-host", "H", "localhost", "PostgreSQL host")
	Root.PersistentFlags().IntP("pg-port", "p", 5432, "PostgreSQL port")
	Root.PersistentFlags().StringP("pg-database", "D", "postgres", "PostgreSQL database name")
	Root.PersistentFlags().StringP("pg-user", "U", "postgres", "PostgreSQL username")
	Root.PersistentFlags().String("pg-password", "", "PostgreSQL password")

	// PostgreSQL server management flags
	Root.PersistentFlags().StringP("data-dir", "d", "", "PostgreSQL data directory")
	Root.PersistentFlags().String("config-file", "", "PostgreSQL configuration file")
	Root.PersistentFlags().String("socket-dir", "/tmp", "PostgreSQL socket directory")
	Root.PersistentFlags().IntP("timeout", "t", 30, "Operation timeout in seconds")

	// Logging
	Root.PersistentFlags().String("log-level", "info", "Log level (debug, info, warn, error)")

	// Bind all flags to viper at once
	if err := viper.BindPFlags(Root.PersistentFlags()); err != nil {
		panic(err) // This should never happen during initialization
	}
}

func initConfig() error {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}

		viper.AddConfigPath(home)
		viper.AddConfigPath(".")
		viper.AddConfigPath("./config")
		viper.AddConfigPath("/etc/multigres")
		viper.SetConfigType("yaml")
		viper.SetConfigName(".pgctld")
	}

	viper.SetEnvPrefix("PGCTLD")
	viper.AutomaticEnv()

	// Try to read config, but don't fail if file doesn't exist
	if err := viper.ReadInConfig(); err != nil {
		// Only return error if it's not a "file not found" error
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
		// Config file not found is OK - we'll use defaults and CLI flags
	}

	return nil
}
