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

package pgctld

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"text/template"

	"github.com/multigres/multigres/config"
	"github.com/multigres/multigres/go/common/backup"
)

// PgBackRestConfig holds configuration for generating pgbackrest.conf
type PgBackRestConfig struct {
	PoolerDir     string // Base directory for pooler data
	CertDir       string // Directory containing TLS certificates
	Port          int    // TLS server port
	Pg1Port       int    // Local PostgreSQL port
	Pg1SocketPath string // Local PostgreSQL socket directory
	Pg1Path       string // Local PostgreSQL data directory
}

// GeneratePgBackRestConfig generates pgbackrest.conf for TLS server mode using the standard template
// Returns the path to the generated config file
func GeneratePgBackRestConfig(cfg PgBackRestConfig, backupCfg *backup.Config) (string, error) {
	// Create pgbackrest directory if it doesn't exist
	pgbackrestDir := filepath.Join(cfg.PoolerDir, "pgbackrest")
	if err := os.MkdirAll(pgbackrestDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create pgbackrest directory: %w", err)
	}

	// Create subdirectories
	logPath := filepath.Join(pgbackrestDir, "log")
	spoolPath := filepath.Join(pgbackrestDir, "spool")
	lockPath := filepath.Join(pgbackrestDir, "lock")

	for _, dir := range []string{logPath, spoolPath, lockPath} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Parse the template
	tmpl, err := template.New("pgbackrest").Parse(config.PgBackRestConfigTmpl)
	if err != nil {
		return "", fmt.Errorf("failed to parse pgbackrest config template: %w", err)
	}

	// Generate repo configuration using common backup package
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

		RepoConfig map[string]string

		Pg1SocketPath string
		Pg1Port       int
		Pg1Path       string

		ServerCertFile string
		ServerKeyFile  string
		ServerCAFile   string
		ServerPort     int
		ServerAuths    []string

		RepoCredentials map[string]string
	}{
		LogPath:   logPath,
		SpoolPath: spoolPath,
		LockPath:  lockPath,

		RepoConfig: repoConfig,

		Pg1SocketPath: cfg.Pg1SocketPath,
		Pg1Port:       cfg.Pg1Port,
		Pg1Path:       cfg.Pg1Path,

		ServerCertFile: filepath.Join(cfg.CertDir, "pgbackrest.crt"),
		ServerKeyFile:  filepath.Join(cfg.CertDir, "pgbackrest.key"),
		ServerCAFile:   filepath.Join(cfg.CertDir, "ca.crt"),
		ServerPort:     cfg.Port,
		ServerAuths:    []string{"pgbackrest=*"},

		RepoCredentials: repoCredentials,
	}

	// Execute template
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, templateData); err != nil {
		return "", fmt.Errorf("failed to execute pgbackrest config template: %w", err)
	}

	// Write config file with appropriate permissions
	// Use restrictive permissions (0o600) when credentials are embedded
	configPath := filepath.Join(pgbackrestDir, "pgbackrest.conf")
	fileMode := os.FileMode(0o644)
	if len(repoCredentials) > 0 {
		fileMode = 0o600
	}
	if err := os.WriteFile(configPath, buf.Bytes(), fileMode); err != nil {
		return "", fmt.Errorf("failed to write pgbackrest config: %w", err)
	}

	return configPath, nil
}
