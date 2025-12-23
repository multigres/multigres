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
		// Convert to absolute path - pq driver requires paths starting with '/' for Unix sockets
		absPoolerDir, err := filepath.Abs(config.PoolerDir)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path for pooler dir: %w", err)
		}
		socketDir := filepath.Join(absPoolerDir, "pg_sockets")
		port := fmt.Sprintf("%d", config.PgPort)

		// Use connect_timeout to prevent indefinite blocking on connection attempts.
		// This is necessary because Go's sql.Open() and db.Ping() don't support
		// context-based timeouts without changing function signatures to use PingContext().
		// The 2-second timeout allows quick failure for retry logic while being generous
		// enough for Unix socket connections which should be nearly instant.
		// NOTE: This will be replaced with connection pooling soon, so it's not worth
		// refactoring to pass in a context parameter at this time.
		dsn = fmt.Sprintf("user=postgres dbname=%s host=%s port=%s sslmode=disable connect_timeout=2",
			config.Database, socketDir, port)

		logger.Info("Unix socket connection via pooler directory",
			"pooler_dir", config.PoolerDir,
			"socket_dir", socketDir,
			"pg_port", config.PgPort,
			"dsn", dsn)
	} else if config.SocketFilePath != "" {
		// Fallback: use socket file path directly
		// Convert to absolute path - pq driver requires paths starting with '/' for Unix sockets
		absSocketPath, err := filepath.Abs(config.SocketFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path for socket file: %w", err)
		}
		socketDir := filepath.Dir(absSocketPath)
		socketFile := filepath.Base(absSocketPath)

		// Extract port from socket filename (.s.PGSQL.PORT)
		port := "5432" // default
		if after, ok := strings.CutPrefix(socketFile, ".s.PGSQL."); ok {
			if portStr := after; portStr != "" {
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
