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

	"github.com/multigres/multigres/go/tools/retry"
)

// CommandRunner executes a command and returns its combined output.
// This abstraction allows tests to inject mock command execution.
type CommandRunner func(ctx context.Context, name string, args ...string) ([]byte, error)

// defaultCommandRunner executes commands using exec.CommandContext.
func defaultCommandRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	return cmd.CombinedOutput()
}

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
	LogPath         string   // Path for pgBackRest logs
	SpoolPath       string   // Path for pgBackRest spool directory
	LockPath        string   // Path for pgBackRest lock files
	RetentionFull   int      // Number of full backups to retain
}

// GenerateConfig creates a pgBackRest configuration file content for a backup stanza.
func GenerateConfig(cfg Config) string {
	var sb strings.Builder

	// Global section
	sb.WriteString("[global]\n")
	sb.WriteString(fmt.Sprintf("log-path=%s\n", cfg.LogPath))
	if cfg.SpoolPath != "" {
		sb.WriteString(fmt.Sprintf("spool-path=%s\n", cfg.SpoolPath))
	}
	if cfg.LockPath != "" {
		sb.WriteString(fmt.Sprintf("lock-path=%s\n", cfg.LockPath))
	}
	// Use Zstandard compression for better compression ratios and performance
	sb.WriteString("compress-type=zst\n")
	// Enable hard linking for all files to save space when multiple backups share identical files
	sb.WriteString("link-all=y\n")
	// Set console logging to info level for readable output during operations
	sb.WriteString("log-level-console=info\n")
	// Set file logging to detail level for comprehensive debugging and audit trails
	sb.WriteString("log-level-file=detail\n")
	// Enable subprocess logging to capture output from parallel processes
	sb.WriteString("log-subprocess=y\n")
	// Disable resume to avoid potential issues with partial backups
	sb.WriteString("resume=n\n")
	// Force an immediate checkpoint when starting backups to speed up the backup process
	sb.WriteString("start-fast=y\n")
	// Note: stanza is a command-line only option, not a config file option
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

// StanzaCreate executes pgbackrest stanza-create command to initialize a backup stanza.
// It retries on lock errors (error code 050) since multiple processes may contend for
// the shared backup repository lock during cluster bootstrap.
//
// Normally, post-bootstrap pgbackrest operations don't need this specific retry logic,
// because they execute while holding the ActionLock.
func StanzaCreate(ctx context.Context, stanzaName, configPath, repoPath string) error {
	return StanzaCreateWithRunner(ctx, stanzaName, configPath, repoPath, defaultCommandRunner)
}

// StanzaCreateWithRunner is like StanzaCreate but accepts a custom command runner.
// This is primarily for testing; production code should use StanzaCreate.
func StanzaCreateWithRunner(ctx context.Context, stanzaName, configPath, repoPath string, runner CommandRunner) error {
	const maxAttempts = 5

	var lastErr error
	var lastOutput string

	r := retry.New(500*time.Millisecond, 5*time.Second)
	for attempt, err := range r.Attempts(ctx) {
		if err != nil {
			return fmt.Errorf("context cancelled while retrying stanza-create: %w", err)
		}
		if attempt > maxAttempts {
			return fmt.Errorf("failed to create stanza %s after %d attempts (lock contention): %w\nOutput: %s",
				stanzaName, maxAttempts, lastErr, lastOutput)
		}

		// Create context with timeout for this attempt
		attemptCtx, cancel := context.WithTimeout(ctx, 30*time.Second)

		// Build pgbackrest command args
		args := []string{
			"--stanza=" + stanzaName,
			"--config=" + configPath,
			"--repo1-path=" + repoPath,
			"stanza-create",
		}
		fmt.Printf("Executing command (attempt %d/%d): pgbackrest %v\n", attempt, maxAttempts, args)

		// Execute command using the provided runner
		output, err := runner(attemptCtx, "pgbackrest", args...)
		cancel()

		if err == nil {
			return nil
		}

		lastErr = err
		lastOutput = string(output)

		// Check if this is a lock error (error code 050)
		if !isLockError(lastOutput) {
			return fmt.Errorf("failed to create stanza %s: %w\nOutput: %s",
				stanzaName, err, lastOutput)
		}

		fmt.Printf("Lock contention detected on attempt %d, will retry...\n", attempt)
	}

	return fmt.Errorf("failed to create stanza %s: %w\nOutput: %s",
		stanzaName, lastErr, lastOutput)
}

// isLockError checks if the pgbackrest output indicates a lock error (error code 050).
func isLockError(output string) bool {
	// pgbackrest error code 050 is "unable to acquire lock"
	return strings.Contains(output, "[050]") || strings.Contains(output, "unable to acquire lock")
}
