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

package backup

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"text/template"

	"github.com/multigres/multigres/config"
)

// ClientConfigOpts holds options for generating pgbackrest.conf for client operations.
type ClientConfigOpts struct {
	PoolerDir     string // Base directory for pooler data
	Pg1Port       int    // Local PostgreSQL port
	Pg1SocketPath string // Local PostgreSQL socket directory
	Pg1Path       string // Local PostgreSQL data directory
}

// WriteClientConfig generates pgbackrest.conf for client operations (backup, restore, info).
// Returns the path to the generated config file.
func WriteClientConfig(opts ClientConfigOpts, backupCfg *Config) (string, error) {
	pgbackrestDir := filepath.Join(opts.PoolerDir, "pgbackrest")
	logPath := filepath.Join(pgbackrestDir, "log")
	spoolPath := filepath.Join(pgbackrestDir, "spool")
	lockPath := filepath.Join(pgbackrestDir, "lock")

	// Create directories
	for _, dir := range []string{pgbackrestDir, logPath, spoolPath, lockPath} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Parse the template
	tmpl, err := template.New("pgbackrest").Parse(config.PgBackRestConfigTmpl)
	if err != nil {
		return "", fmt.Errorf("failed to parse pgbackrest config template: %w", err)
	}

	// Generate repo configuration
	repoConfig, err := backupCfg.PgBackRestConfig("multigres")
	if err != nil {
		return "", fmt.Errorf("failed to generate pgBackRest config: %w", err)
	}

	// Get credentials if using environment credentials
	repoCredentials, err := backupCfg.PgBackRestCredentials()
	if err != nil {
		return "", fmt.Errorf("failed to get pgBackRest credentials: %w", err)
	}

	// Prepare template data
	templateData := struct {
		LogPath   string
		SpoolPath string
		LockPath  string

		RepoConfig      map[string]string
		RepoCredentials map[string]string

		Pg1SocketPath string
		Pg1Port       int
		Pg1Path       string
	}{
		LogPath:   logPath,
		SpoolPath: spoolPath,
		LockPath:  lockPath,

		RepoConfig:      repoConfig,
		RepoCredentials: repoCredentials,

		Pg1SocketPath: opts.Pg1SocketPath,
		Pg1Port:       opts.Pg1Port,
		Pg1Path:       opts.Pg1Path,
	}

	// Execute template
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, templateData); err != nil {
		return "", fmt.Errorf("failed to execute pgbackrest config template: %w", err)
	}

	// Write config with appropriate permissions
	configPath := filepath.Join(pgbackrestDir, "pgbackrest.conf")
	fileMode := os.FileMode(0o644)
	if len(repoCredentials) > 0 {
		fileMode = 0o600
	}
	if err := os.WriteFile(configPath, buf.Bytes(), fileMode); err != nil {
		return "", fmt.Errorf("failed to write pgbackrest.conf: %w", err)
	}

	return configPath, nil
}
