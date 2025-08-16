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

// multigateway is the top-level proxy that masquerades as a PostgreSQL server,
// handling client connections and routing queries to multipooler instances.
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
	pflag.StringP("port", "p", "5432", "Port to listen on")
	pflag.StringP("log-level", "l", "info", "Log level (debug, info, warn, error)")
	pflag.StringP("config", "c", "", "Config file path")
	pflag.Parse()

	// Setup viper
	viper.SetDefault("port", "5432")
	viper.SetDefault("log-level", "info")

	// Bind pflags to viper
	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		slog.Error("Failed to bind flags", "error", err)
		os.Exit(1)
	}

	// Set config file path if provided
	if configFile := viper.GetString("config"); configFile != "" {
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName("multigateway")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("./config")
		viper.AddConfigPath("/etc/multigres")
	}

	// Enable environment variables
	viper.SetEnvPrefix("MULTIGATEWAY")
	viper.AutomaticEnv()

	// Read config file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			// Config file was found but another error was produced
			slog.Error("Error reading config file", "error", err)
			os.Exit(1)
		}
		// Config file not found; ignore error
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

	logger.Info("starting multigateway",
		"port", viper.GetString("port"),
		"log_level", viper.GetString("log-level"),
		"config_file", viper.ConfigFileUsed(),
	)

	// Create context that cancels on interrupt
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// TODO: Initialize Postgres protocol server
	// TODO: Setup connections to multipoolers
	// TODO: Implement query routing logic

	logger.Info("multigateway ready to accept connections")

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Info("shutting down multigateway")
}
