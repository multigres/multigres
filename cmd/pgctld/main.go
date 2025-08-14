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

// pgctld provides a gRPC interface for direct communication with PostgreSQL instances,
// handling query execution and database operations.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func setupConfig() {
	// Define flags
	pflag.StringP("grpc-port", "g", "15200", "gRPC port to listen on")
	pflag.StringP("pg-host", "H", "localhost", "PostgreSQL host")
	pflag.StringP("pg-port", "P", "5432", "PostgreSQL port")
	pflag.StringP("pg-database", "d", "postgres", "PostgreSQL database name")
	pflag.StringP("pg-user", "u", "postgres", "PostgreSQL username")
	pflag.StringP("pg-password", "p", "", "PostgreSQL password")
	pflag.StringP("log-level", "l", "info", "Log level (debug, info, warn, error)")
	pflag.StringP("config", "c", "", "Config file path")
	pflag.Parse()

	// Setup viper
	viper.SetDefault("grpc-port", "15200")
	viper.SetDefault("pg-host", "localhost")
	viper.SetDefault("pg-port", "5432")
	viper.SetDefault("pg-database", "postgres")
	viper.SetDefault("pg-user", "postgres")
	viper.SetDefault("pg-password", "")
	viper.SetDefault("log-level", "info")

	// Bind pflags to viper
	viper.BindPFlags(pflag.CommandLine)

	// Set config file path if provided
	if configFile := viper.GetString("config"); configFile != "" {
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName("pgctld")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("./config")
		viper.AddConfigPath("/etc/multigres")
	}

	// Enable environment variables
	viper.SetEnvPrefix("PGCTLD")
	viper.AutomaticEnv()

	// Read config file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			slog.Error("Error reading config file", "error", err)
			os.Exit(1)
		}
	}
}

func main() {
	setupConfig()

	// Setup structured logging
	var level slog.Level
	switch viper.GetString("log-level") {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
	slog.SetDefault(logger)

	logger.Info("starting pgctld",
		"grpc_port", viper.GetString("grpc-port"),
		"pg_host", viper.GetString("pg-host"),
		"pg_port", viper.GetString("pg-port"),
		"pg_database", viper.GetString("pg-database"),
		"pg_user", viper.GetString("pg-user"),
		"log_level", viper.GetString("log-level"),
		"config_file", viper.ConfigFileUsed(),
	)

	// Create context that cancels on interrupt
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// TODO: Setup gRPC server
	// TODO: Implement PostgreSQL query interface
	// TODO: Use pgPassword for database connection
	_ = viper.GetString("pg-password")
	
	logger.Info("pgctld ready to serve gRPC requests")

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Info("shutting down pgctld")
}