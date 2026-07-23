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

// postgresExtraConfigFileName is the managed file (inside PGDATA) that holds
// --pg-initdb-extra-conf snippets. postgresql.conf pulls it in via include_if_exists
// so the snippets survive a template re-render, which only rewrites postgresql.conf.
const postgresExtraConfigFileName = "multigres_extra.conf"

// PostgresExtraConfigFile returns the path to the managed extras file within PGDATA.
func PostgresExtraConfigFile() string {
	return path.Join(PostgresDataDir(), postgresExtraConfigFileName)
}

// GeneratePostgresServerConfig writes postgresql.conf from the embedded template,
// writes any extraConfFiles into the managed include file (postgres last-write-wins),
// generates pg_hba.conf, then reads the result back. This is the first-time init
// path; to apply template changes on restart see RegenerateConfigFromTemplate.
func GeneratePostgresServerConfig(poolerDir string, pgUser string, extraConfFiles []string) (*PostgresServerConfig, error) {
	if poolerDir == "" {
		return nil, errors.New("--pooler-dir needs to be set to generate postgres server config")
	}

	// Expand relative path to absolute path for consistent path handling
	absPoolerDir, err := ExpandToAbsolutePath(poolerDir)
	if err != nil {
		return nil, fmt.Errorf("failed to expand pooler directory path: %w", err)
	}

	cnf, err := newRenderConfig(pgUser)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(PostgresSocketDir(absPoolerDir), 0o755); err != nil {
		return nil, fmt.Errorf("failed to create Unix socket directory: %w", err)
	}

	if err := cnf.writePostgresConf(config.PostgresConfigDefaultTmpl); err != nil {
		return nil, err
	}

	if err := writeManagedExtraConf(extraConfFiles); err != nil {
		return nil, err
	}

	if err := cnf.generateHbaFile(); err != nil {
		return nil, err
	}

	// Read the generated config back from disk to get all template values
	return ReadPostgresServerConfig(cnf, 0)
}

// RegenerateConfigFromTemplate re-renders postgresql.conf from the template at
// templatePath when it is non-empty; it is a no-op otherwise. This is what lets
// pgctld pick up postgres-config-template (ConfigMap) changes on restart, since the
// data directory — and its stale postgresql.conf — survive across restarts on a
// retained PVC.
//
// It is deliberately surgical and does NOT reuse the full init-time generation: it
// reads templatePath fresh (so an in-place ConfigMap update is honored even without a
// process restart), rewrites ONLY postgresql.conf, and never touches pg_hba.conf or
// the operator's extras (which live in the managed include file). It does not
// re-validate the rendered config — postgres rejects a bad config at startup, where
// validation is authoritative.
func RegenerateConfigFromTemplate(templatePath, pgUser string) error {
	if templatePath == "" {
		return nil
	}
	templateContent, err := os.ReadFile(templatePath)
	if err != nil {
		return fmt.Errorf("reading postgres config template %q: %w", templatePath, err)
	}
	cnf, err := newRenderConfig(pgUser)
	if err != nil {
		return err
	}
	if err := cnf.writePostgresConf(string(templateContent)); err != nil {
		return fmt.Errorf("re-rendering postgresql.conf from template %q: %w", templatePath, err)
	}
	return nil
}

// newRenderConfig builds a PostgresServerConfig populated with the file locations
// and default tuning values the postgresql.conf template needs to render. Shared by
// first-time generation and on-restart regeneration. It sizes WAL settings from the
// data volume, so it reads the filesystem and can fail.
func newRenderConfig(pgUser string) (*PostgresServerConfig, error) {
	cnf := &PostgresServerConfig{
		Path:        PostgresConfigFile(),
		DataDir:     PostgresDataDir(),
		HbaFile:     path.Join(PostgresDataDir(), "pg_hba.conf"),
		IdentFile:   path.Join(PostgresDataDir(), "pg_ident.conf"),
		ClusterName: "default",
		User:        pgUser,
	}
	if err := applyPicoInstanceDefaults(cnf); err != nil {
		return nil, err
	}
	return cnf, nil
}

// applyPicoInstanceDefaults sets the Multigres default tuning values that fill the
// postgresql.conf template's {{.Field}} placeholders. Tuned for a small instance;
// these can change in the future based on instance size. WAL disk-usage settings are
// derived from the data volume, so this reads the filesystem and can fail.
func applyPicoInstanceDefaults(cnf *PostgresServerConfig) error {
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

	// WAL disk-usage settings scale with the volume backing the data
	// directory; fixed defaults let WAL alone fill small volumes (MUL-1021).
	volBytes, err := volumeTotalBytes(cnf.DataDir)
	if err != nil {
		return fmt.Errorf("failed to size WAL settings for data volume: %w", err)
	}
	segmentBytes, err := walSegmentSizeBytes(cnf.DataDir)
	if err != nil {
		return fmt.Errorf("failed to read WAL segment size: %w", err)
	}
	ws, err := deriveWalSettings(volBytes, segmentBytes)
	if err != nil {
		return fmt.Errorf("failed to derive WAL settings: %w", err)
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

	return nil
}

// writePostgresConf renders templateContent into postgresql.conf at cnf.Path and
// appends an include_if_exists directive for the managed extras file. It deliberately
// does NOT touch pg_hba.conf, the extras file, or validate the result — this is the
// shared core of both first-time generation and on-restart regeneration, and keeping
// it this narrow is what makes regeneration safe to run on every start.
func (cnf *PostgresServerConfig) writePostgresConf(templateContent string) error {
	if err := os.MkdirAll(path.Dir(cnf.Path), 0o755); err != nil {
		return err
	}

	content, err := cnf.MakePostgresConf(templateContent)
	if err != nil {
		return err
	}

	// Pull operator extras in last (postgres last-write-wins) from a managed file that
	// regeneration never rewrites, so --pg-initdb-extra-conf snippets survive a template
	// re-render. The relative path resolves against PGDATA; include_if_exists is a no-op
	// when there are no extras.
	content += fmt.Sprintf("\ninclude_if_exists '%s'\n", postgresExtraConfigFileName)
	//nolint:gosec // G703 false positive: cnf.Path is PGDATA/postgresql.conf, an operator-controlled path, not attacker input
	return os.WriteFile(cnf.Path, []byte(content), 0o644)
}

// writeManagedExtraConf writes the --pg-initdb-extra-conf snippets into the managed
// extras file (PostgresExtraConfigFile), which postgresql.conf includes via
// include_if_exists. Each block carries a "## <path>" attribution header. No-op when
// there are no extras. Written once at init and never rewritten by regeneration, so
// the snippets persist across restarts.
func writeManagedExtraConf(paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	var b strings.Builder
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("reading extra postgres config %q: %w", p, err)
		}
		fmt.Fprintf(&b, "## %s\n", p)
		b.Write(data)
		if len(data) > 0 && data[len(data)-1] != '\n' {
			b.WriteByte('\n')
		}
	}
	return os.WriteFile(PostgresExtraConfigFile(), []byte(b.String()), 0o644)
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
