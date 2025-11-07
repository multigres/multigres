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

// Package pgbackrest handles pgBackRest configuration generation
package pgbackrest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// PgHost represents a PostgreSQL host configuration for pgBackRest
type PgHost struct {
	DataPath  string // Path to PostgreSQL data directory
	Host      string // PostgreSQL hostname (use "localhost" for TCP connection)
	Port      int    // PostgreSQL port
	SocketDir string // PostgreSQL Unix socket directory
	User      string // PostgreSQL user for connections
	Database  string // PostgreSQL database for connections
}

// Config holds the configuration parameters for pgBackRest
type Config struct {
	StanzaName      string   // Name of the backup stanza (usually service ID or similar)
	PgDataPath      string   // Path to PostgreSQL data directory (pg1)
	PgHost          string   // PostgreSQL hostname (pg1, use "localhost" for TCP connection)
	PgPort          int      // PostgreSQL port (pg1)
	PgSocketDir     string   // PostgreSQL Unix socket directory (pg1)
	PgUser          string   // PostgreSQL user for connections (pg1)
	PgPassword      string   // PostgreSQL password (for local development only, pg1)
	PgDatabase      string   // PostgreSQL database for connections (pg1)
	AdditionalHosts []PgHost // Additional PostgreSQL hosts (pg2, pg3, etc.) for multi-host setups
	RepoPath        string   // Path to backup repository
	LogPath         string   // Path for pgBackRest logs
	RetentionFull   int      // Number of full backups to retain
}

// GenerateConfig creates a pgBackRest configuration file content
func GenerateConfig(cfg Config) string {
	var sb strings.Builder

	// Global section
	sb.WriteString("[global]\n")
	sb.WriteString(fmt.Sprintf("repo1-path=%s\n", cfg.RepoPath))
	sb.WriteString(fmt.Sprintf("log-path=%s\n", cfg.LogPath))
	sb.WriteString("\n")

	// Stanza section
	sb.WriteString(fmt.Sprintf("[%s]\n", cfg.StanzaName))

	// pg1 (primary host configuration from Config fields)
	// pg1 represents "self" - pgBackRest accesses the local data directory directly
	sb.WriteString(fmt.Sprintf("pg1-path=%s\n", cfg.PgDataPath))

	// For Unix sockets, specify both socket-path and port
	// Port determines the socket filename (e.g., .s.PGSQL.5432)
	if cfg.PgSocketDir != "" {
		sb.WriteString(fmt.Sprintf("pg1-socket-path=%s\n", cfg.PgSocketDir))
	}
	sb.WriteString(fmt.Sprintf("pg1-port=%d\n", cfg.PgPort))

	if cfg.PgUser != "" {
		sb.WriteString(fmt.Sprintf("pg1-user=%s\n", cfg.PgUser))
	}
	if cfg.PgDatabase != "" {
		sb.WriteString(fmt.Sprintf("pg1-database=%s\n", cfg.PgDatabase))
	}

	// Additional hosts (pg2, pg3, etc.)
	// For local tests with Unix sockets, specify path, socket-path, and port
	for i, host := range cfg.AdditionalHosts {
		pgNum := i + 2 // pg2, pg3, etc.
		sb.WriteString(fmt.Sprintf("pg%d-path=%s\n", pgNum, host.DataPath))

		// For remote hosts (TCP), specify host details
		if host.Host != "" {
			sb.WriteString(fmt.Sprintf("pg%d-host=%s\n", pgNum, host.Host))
			sb.WriteString(fmt.Sprintf("pg%d-host-type=tcp\n", pgNum))
		}

		// For Unix sockets, specify both socket-path and port
		// Port determines the socket filename (e.g., .s.PGSQL.5432)
		if host.SocketDir != "" {
			sb.WriteString(fmt.Sprintf("pg%d-socket-path=%s\n", pgNum, host.SocketDir))
		}
		sb.WriteString(fmt.Sprintf("pg%d-port=%d\n", pgNum, host.Port))

		if host.User != "" {
			sb.WriteString(fmt.Sprintf("pg%d-user=%s\n", pgNum, host.User))
		}
		if host.Database != "" {
			sb.WriteString(fmt.Sprintf("pg%d-database=%s\n", pgNum, host.Database))
		}
	}

	// Retention policy
	if cfg.RetentionFull > 0 {
		sb.WriteString(fmt.Sprintf("repo1-retention-full=%d\n", cfg.RetentionFull))
	}

	return sb.String()
}

// WriteConfigFile writes the pgBackRest configuration to a file
func WriteConfigFile(configPath string, cfg Config) error {
	content := GenerateConfig(cfg)

	// Ensure the directory exists
	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Write the config file
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("failed to write config file %s: %w", configPath, err)
	}

	return nil
}

// StanzaCreate executes pgbackrest stanza-create command to initialize a backup stanza
func StanzaCreate(ctx context.Context, stanzaName, configPath string) error {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Build pgbackrest command
	cmd := exec.CommandContext(ctx, "pgbackrest",
		"--stanza="+stanzaName,
		"--config="+configPath,
		"stanza-create")
	fmt.Println("Executing command:", cmd.String())
	// Capture output for logging
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to create stanza %s: %w\nOutput: %s",
			stanzaName, err, string(output))
	}

	return nil
}
