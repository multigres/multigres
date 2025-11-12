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

package manager

import (
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// CreateDBConnection establishes a new connection to PostgreSQL using the config
func CreateDBConnection(logger *slog.Logger, config *Config) (*sql.DB, error) {
	// Debug: Log the configuration we received
	logger.Info("createDBConnection: Configuration received",
		"pooler_dir", config.PoolerDir,
		"pg_port", config.PgPort,
		"socket_file_path", config.SocketFilePath,
		"database", config.Database)

	var dsn string
	if config.PoolerDir != "" && config.PgPort != 0 {
		// Use pooler directory and port to construct socket path
		// PostgreSQL creates socket files as: {poolerDir}/pg_sockets/.s.PGSQL.{port}
		socketDir := filepath.Join(config.PoolerDir, "pg_sockets")
		port := fmt.Sprintf("%d", config.PgPort)

		dsn = fmt.Sprintf("user=postgres dbname=%s host=%s port=%s sslmode=disable",
			config.Database, socketDir, port)

		logger.Info("Unix socket connection via pooler directory",
			"pooler_dir", config.PoolerDir,
			"socket_dir", socketDir,
			"pg_port", config.PgPort,
			"dsn", dsn)
	} else if config.SocketFilePath != "" {
		// Fallback: use socket file path directly
		socketDir := filepath.Dir(config.SocketFilePath)
		socketFile := filepath.Base(config.SocketFilePath)

		// Extract port from socket filename (.s.PGSQL.PORT)
		port := "5432" // default
		if strings.HasPrefix(socketFile, ".s.PGSQL.") {
			if portStr := strings.TrimPrefix(socketFile, ".s.PGSQL."); portStr != "" {
				port = portStr
			}
		}

		dsn = fmt.Sprintf("user=postgres dbname=%s host=%s port=%s sslmode=disable",
			config.Database, socketDir, port)

		logger.Info("Unix socket connection via socket file path (fallback)",
			"original_socket_path", config.SocketFilePath,
			"socket_dir", socketDir,
			"socket_file", socketFile,
			"extracted_port", port,
			"dsn", dsn)
	} else {
		// Use TCP connection (fallback)
		dsn = fmt.Sprintf("user=postgres dbname=%s host=localhost port=5432 sslmode=disable",
			config.Database)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger.Info("Connected to PostgreSQL", "socket_path", config.SocketFilePath, "database", config.Database)
	return db, nil
}

// CreateSidecarSchema creates the multigres sidecar schema and heartbeat table if they don't exist
func CreateSidecarSchema(db *sql.DB) error {
	_, err := db.Exec("CREATE SCHEMA IF NOT EXISTS multigres")
	if err != nil {
		return fmt.Errorf("failed to create multigres schema: %w", err)
	}

	// Create the heartbeat table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS multigres.heartbeat (
			shard_id BYTEA PRIMARY KEY,
			leader_id TEXT NOT NULL,
			ts BIGINT NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create heartbeat table: %w", err)
	}

	return nil
}
