// Copyright 2025 The Multigres Authors.
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
	"fmt"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/spf13/pflag"

	"github.com/multigres/multigres/config"
	"github.com/multigres/multigres/go/servenv"
)

// This file handles the creation of PostgresServerConfig objects for the default
// file structure. These paths are used by the pgctld commands.

var poolerDir string

var FlagBinaries = []string{"pgctld", "multipooler"}

func init() {
	for _, cmd := range FlagBinaries {
		servenv.OnParseFor(cmd, registerPostgresConfigFlags)
	}
}

func registerPostgresConfigFlags(fs *pflag.FlagSet) {
	fs.StringVar(&poolerDir, "pooler-dir", poolerDir, "The directory to multipooler data")
}

// GetPoolerDir returns the configured pooler directory
func GetPoolerDir() string {
	return poolerDir
}

// GeneratePostgresServerConfig generates a new PostgreSQL server configuration
// and writes it to disk using the embedded template, then reads it back.
// poolerId is used for the cluster name and path generation.
// port is the port for the PostgreSQL server.
func GeneratePostgresServerConfig(poolerDir string, port int) (*PostgresServerConfig, error) {
	// Create minimal config for template generation
	if poolerDir == "" {
		return nil, fmt.Errorf("--pooler-dir needs to be set to generate postgres server config")
	}
	cnf := &PostgresServerConfig{}
	cnf.Path = PostgresConfigFile(poolerDir)
	cnf.DataDir = PostgresDataDir(poolerDir)
	cnf.HbaFile = path.Join(PostgresDataDir(poolerDir), "pg_hba.conf")
	cnf.IdentFile = path.Join(PostgresDataDir(poolerDir), "pg_ident.conf")
	cnf.Port = port
	cnf.ListenAddresses = "localhost"
	cnf.UnixSocketDirectories = "/tmp"
	cnf.ClusterName = "default"

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

	// Read the generated config back from disk to get all template values
	return ReadPostgresServerConfig(cnf, 0)
}

// LoadOrCreatePostgresServerConfig loads an existing PostgreSQL server configuration
// from disk, or generates and writes a new one if it doesn't exist.
// port is the port for the PostgreSQL server.
func LoadOrCreatePostgresServerConfig(poolerDir string, port int) (*PostgresServerConfig, error) {
	configPath := PostgresConfigFile(poolerDir)

	// Check if config file already exists
	if _, err := os.Stat(configPath); err == nil {
		// Config file exists, read it
		cnf := &PostgresServerConfig{Path: configPath}
		return ReadPostgresServerConfig(cnf, 0)
	}

	// Config file doesn't exist, let's generate it
	return GeneratePostgresServerConfig(poolerDir, port)
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

// PostgresDataDir returns the default location of the postgresql.conf file.
func PostgresDataDir(poolerDir string) string {
	return path.Join(poolerDir, "pg_data")
}

// PostgresConfigFile returns the default location of the postgresql.conf file.
func PostgresConfigFile(poolerDir string) string {
	return path.Join(PostgresDataDir(poolerDir), "postgresql.conf")
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
