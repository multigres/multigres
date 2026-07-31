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

// GeneratePostgresServerConfig writes postgresql.conf from the embedded template,
// appends include_if_exists directives for extraConfFiles (postgres
// last-write-wins), then reads it back.
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

	if err := os.MkdirAll(PostgresSocketDir(absPoolerDir), 0o755); err != nil {
		return nil, fmt.Errorf("failed to create Unix socket directory: %w", err)
	}

	// Set Multigres default values tuned for a small instance.
	// These can be changed in the future based on instance size/requirements
	cnf.MaxConnections = 110
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

	// WAL disk-usage settings scale with the volume backing the data
	// directory; fixed defaults let WAL alone fill small volumes (MUL-1021).
	volBytes, err := volumeTotalBytes(cnf.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to size WAL settings for data volume: %w", err)
	}
	segmentBytes, err := walSegmentSizeBytes(cnf.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL segment size: %w", err)
	}
	ws, err := deriveWalSettings(volBytes, segmentBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to derive WAL settings: %w", err)
	}
	cnf.MinWalSize = fmt.Sprintf("%dMB", ws.minWalSizeMB)
	cnf.MaxWalSize = fmt.Sprintf("%dMB", ws.maxWalSizeMB)
	cnf.WalKeepSize = fmt.Sprintf("%dMB", ws.walKeepSizeMB)
	cnf.MaxSlotWalKeepSize = fmt.Sprintf("%dMB", ws.maxSlotWalKeepSizeMB)

	cnf.CheckpointCompletionTarget = 0.9
	cnf.MaxWalSenders = 25
	cnf.MaxReplicationSlots = 25
	cnf.EffectiveCacheSize = "192MB"
	cnf.RandomPageCost = 1.1
	cnf.DefaultStatisticsTarget = 100

	// Generate config file from template
	if err := cnf.generateConfigFile(); err != nil {
		return nil, err
	}

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

// appendExtraConfFiles appends an `include_if_exists '<absolute path>'`
// directive for each path to the END of postgresql.conf, in order. Postgres
// re-reads the referenced files on every server start and every
// pg_reload_conf() SIGHUP, so a file that changes on disk (e.g. an operator
// ConfigMap remounted read-only into the pod) takes effect on the next restart
// or reload — unlike copying the bytes at init time, which baked a stale
// snapshot into PGDATA/postgresql.conf.
//
// Emitting these after the generated template preserves postgres' last-write-
// wins precedence, so the extra conf still overrides the template. include_if_exists
// (not include) means a missing/unmounted file is silently skipped instead of
// failing startup.
func (cnf *PostgresServerConfig) appendExtraConfFiles(paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	f, err := os.OpenFile(cnf.Path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("opening postgresql.conf for extras: %w", err)
	}
	defer f.Close()

	if _, err := f.WriteString("\n# Live-included extra config (edit the referenced file and reload/restart to apply).\n"); err != nil {
		return fmt.Errorf("appending extra config header: %w", err)
	}
	for _, p := range paths {
		abs, err := ExpandToAbsolutePath(p)
		if err != nil {
			return fmt.Errorf("failed when resolving extra postgres config %q: %w", p, err)
		}
		if _, err := fmt.Fprintf(f, "include_if_exists %s\n", quoteConfValue(abs)); err != nil {
			return fmt.Errorf("failed to append include for %q: %w", p, err)
		}
	}
	return nil
}

// quoteConfValue wraps a value in single quotes and escapes embedded single
// quotes by doubling them, per postgresql.conf string syntax.
func quoteConfValue(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
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

// RestoreCommandPIDFile returns the path the restore_command wrapper (see
// `pgctld restore-wrapper`) writes its own PID to. See
// constants.RestoreCommandPIDFile for why the filename lives there instead of
// here: go/services/multipooler needs it too, and can't import this package.
func RestoreCommandPIDFile(poolerDir string) string {
	return path.Join(poolerDir, constants.RestoreCommandPIDFile)
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
