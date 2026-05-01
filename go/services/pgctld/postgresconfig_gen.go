// Copyright 2025 Supabase, Inc.
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
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/multigres/multigres/config"
	"github.com/multigres/multigres/go/common/constants"
)

// ExpandToAbsolutePath converts a relative path to an absolute path.
// If the path is already absolute, it returns the path unchanged.
func ExpandToAbsolutePath(dir string) (string, error) {
	if dir == "" {
		return "", errors.New("directory path cannot be empty")
	}

	// If already absolute, return as-is
	if filepath.IsAbs(dir) {
		return dir, nil
	}

	// Convert relative path to absolute
	absPath, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("failed to convert relative path to absolute: %w", err)
	}

	return absPath, nil
}

// GeneratePostgresServerConfig generates a new PostgreSQL server configuration
// and writes it to disk using the embedded template, then reads it back.
// extraConfFiles is an optional list of paths whose contents are appended verbatim
// after the templated config — last-write-wins per postgres semantics. The four
// settings pgctld pins via command-line args (port, listen_addresses,
// unix_socket_directories, data_directory) are unaffected even if extras try to
// override them, since CLI args beat postgresql.conf.
func GeneratePostgresServerConfig(poolerDir string, pgUser string, extraConfFiles []string) (*PostgresServerConfig, error) {
	// Create minimal config for template generation
	if poolerDir == "" {
		return nil, errors.New("--pooler-dir needs to be set to generate postgres server config")
	}

	// Expand relative path to absolute path for consistent path handling
	absPoolerDir, err := ExpandToAbsolutePath(poolerDir)
	if err != nil {
		return nil, fmt.Errorf("failed to expand pooler directory path: %w", err)
	}
	cnf := &PostgresServerConfig{}
	cnf.Path = PostgresConfigFile()
	cnf.DataDir = PostgresDataDir()
	cnf.HbaFile = path.Join(PostgresDataDir(), "pg_hba.conf")
	cnf.IdentFile = path.Join(PostgresDataDir(), "pg_ident.conf")
	cnf.ClusterName = "default"
	cnf.User = pgUser

	// Ensure Unix socket directory exists. The path itself is pgctld-controlled and
	// passed to postgres via -c unix_socket_directories= at start, not via the conf file.
	if err := os.MkdirAll(PostgresSocketDir(absPoolerDir), 0o755); err != nil {
		return nil, fmt.Errorf("failed to create Unix socket directory: %w", err)
	}

	// Set Multigres default values - starting with Pico instance defaults from Supabase
	// Reference: https://github.com/supabase/supabase-admin-api/blob/3765a153ef6361cb19a1cbd485cdbf93e0a1820a/optimizations/postgres.go#L38
	// These can be changed in the future based on instance size/requirements
	cnf.MaxConnections = 60
	cnf.SharedBuffers = "64MB"
	cnf.MaintenanceWorkMem = "16MB"
	cnf.WorkMem = "1092kB"
	cnf.MaxWorkerProcesses = 6
	// TODO: @rafael - This setting doesn't work for local on macOS environment,
	// so it's not matching exactly what we have in Supabase.
	cnf.EffectiveIoConcurrency = 0
	cnf.MaxParallelWorkers = 2
	cnf.MaxParallelWorkersPerGather = 1
	cnf.MaxParallelMaintenanceWorkers = 1
	cnf.WalBuffers = "1920kB"
	cnf.MinWalSize = "1GB"
	cnf.MaxWalSize = "4GB"
	cnf.CheckpointCompletionTarget = 0.9
	cnf.MaxWalSenders = 5
	cnf.MaxReplicationSlots = 5
	cnf.EffectiveCacheSize = "192MB"
	cnf.RandomPageCost = 1.1
	cnf.DefaultStatisticsTarget = 100

	// Generate config file from template
	if err := cnf.generateConfigFile(); err != nil {
		return nil, err
	}

	// Append user-supplied extra config files. Each is concatenated raw onto the
	// generated postgresql.conf with a "## <path>" header for diagnostics; postgres
	// applies last-write-wins for repeated keys, so extras override template values.
	if err := cnf.appendExtraConfFiles(extraConfFiles); err != nil {
		return nil, err
	}

	// Generate HBA file from template
	if err := cnf.generateHbaFile(); err != nil {
		return nil, err
	}

	// Read the generated config back from disk to get all template values
	return ReadPostgresServerConfig(cnf, 0)
}

// appendExtraConfFiles concatenates the contents of each path onto the generated
// postgresql.conf. Each block is prefixed with "## <path>" so the source is
// recoverable when reading the merged file.
func (cnf *PostgresServerConfig) appendExtraConfFiles(paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	f, err := os.OpenFile(cnf.Path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open postgresql.conf for extras: %w", err)
	}
	defer f.Close()

	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("failed to read extra postgres config file %q: %w", p, err)
		}
		if _, err := fmt.Fprintf(f, "\n## %s\n", p); err != nil {
			return fmt.Errorf("failed to write extras header for %q: %w", p, err)
		}
		if _, err := f.Write(data); err != nil {
			return fmt.Errorf("failed to append extra postgres config file %q: %w", p, err)
		}
		if len(data) > 0 && data[len(data)-1] != '\n' {
			if _, err := f.WriteString("\n"); err != nil {
				return fmt.Errorf("failed to terminate extras block for %q: %w", p, err)
			}
		}
	}
	return nil
}

// generateConfigFile creates the postgresql.conf file using the embedded template
func (cnf *PostgresServerConfig) generateConfigFile() error {
	// Ensure directory exists
	if err := os.MkdirAll(path.Dir(cnf.Path), 0o755); err != nil {
		return err
	}

	// Generate config content from template
	content, err := cnf.MakePostgresConf(config.PostgresConfigDefaultTmpl)
	if err != nil {
		return err
	}

	// Write to file
	return os.WriteFile(cnf.Path, []byte(content), 0o644)
}

// generateHbaFile creates the pg_hba.conf file using the embedded template
func (cnf *PostgresServerConfig) generateHbaFile() error {
	// Generate HBA content from template
	content, err := cnf.MakeHbaConf(config.PostgresHbaDefaultTmpl)
	if err != nil {
		return err
	}

	// Write to file
	return os.WriteFile(cnf.HbaFile, []byte(content), 0o644)
}

// PostgresDataDir returns the PostgreSQL data directory from the PGDATA environment variable.
func PostgresDataDir() string {
	return os.Getenv(constants.PgDataDirEnvVar)
}

// PostgresSocketDir returns the default location of the PostgreSQL Unix sockets.
func PostgresSocketDir(poolerDir string) string {
	return path.Join(poolerDir, "pg_sockets")
}

// PostgresConfigFile returns the location of the postgresql.conf file within PGDATA.
func PostgresConfigFile() string {
	return path.Join(PostgresDataDir(), "postgresql.conf")
}

// MakePostgresConf will substitute values in the template
func (cnf *PostgresServerConfig) MakePostgresConf(templateContent string) (string, error) {
	pgTemplate, err := template.New("").Parse(templateContent)
	if err != nil {
		return "", err
	}
	var configData strings.Builder
	err = pgTemplate.Execute(&configData, cnf)
	if err != nil {
		return "", err
	}
	return configData.String(), nil
}

// MakeHbaConf will substitute values in the HBA template
func (cnf *PostgresServerConfig) MakeHbaConf(templateContent string) (string, error) {
	hbaTemplate, err := template.New("").Parse(templateContent)
	if err != nil {
		return "", err
	}
	var hbaData strings.Builder
	err = hbaTemplate.Execute(&hbaData, cnf)
	if err != nil {
		return "", err
	}
	return hbaData.String(), nil
}
