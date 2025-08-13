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
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var (
		grpcPort   = flag.String("grpc-port", "15200", "gRPC port to listen on")
		pgHost     = flag.String("pg-host", "localhost", "PostgreSQL host")
		pgPort     = flag.String("pg-port", "5432", "PostgreSQL port")
		pgDatabase = flag.String("pg-database", "postgres", "PostgreSQL database name")
		pgUser     = flag.String("pg-user", "postgres", "PostgreSQL username")
		pgPassword = flag.String("pg-password", "", "PostgreSQL password")
		logLevel   = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	)
	flag.Parse()

	// Setup structured logging
	var level slog.Level
	switch *logLevel {
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
		"grpc_port", *grpcPort,
		"pg_host", *pgHost,
		"pg_port", *pgPort,
		"pg_database", *pgDatabase,
		"pg_user", *pgUser,
		"log_level", *logLevel,
	)

	// Create context that cancels on interrupt
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// TODO: Setup gRPC server
	// TODO: Implement PostgreSQL query interface
	// TODO: Use pgPassword for database connection
	_ = pgPassword
	
	logger.Info("pgctld ready to serve gRPC requests")

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Info("shutting down pgctld")
}