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
	"fmt"
	"os"
	"path/filepath"
)

// PostgresCtlConfig holds all PostgreSQL control configuration parameters
// It contains a PostgresServerConfig for all PostgreSQL-specific settings
// plus additional connection parameters for control operations
type PostgresCtlConfig struct {
	Port               int
	User               string
	Database           string
	Password           string
	PostgresDataDir    string
	PostgresConfigFile string
	Timeout            int
	PoolerDir          string
}

// NewPostgresCtlConfig creates a PostgresCtlConfig with the given parameters
func NewPostgresCtlConfig(port int, user, database, password string, timeout int, postgresDataDir string, postgresConfigFile string, poolerDir string) (*PostgresCtlConfig, error) {
	if postgresDataDir == "" {
		return nil, fmt.Errorf("postgres-data-dir needs to be set")
	}

	if poolerDir == "" {
		return nil, fmt.Errorf("pooler-dir needs to be set")
	}

	if port == 0 {
		return nil, fmt.Errorf("port needs to be set")
	}
	if postgresConfigFile == "" {
		return nil, fmt.Errorf("postgres-config-file needs to be set")
	}

	return &PostgresCtlConfig{
		Port:               port,
		User:               user,
		Database:           database,
		Password:           password,
		PostgresDataDir:    postgresDataDir,
		Timeout:            timeout,
		PostgresConfigFile: postgresConfigFile,
		PoolerDir:          poolerDir,
	}, nil
}

// IsDataDirInitialized checks if a PostgreSQL data directory has been initialized
func IsDataDirInitialized(poolerDir string) bool {
	// Check if PG_VERSION file exists (indicates initialized data directory)
	dataDir := PostgresDataDir(poolerDir)
	pgVersionFile := filepath.Join(dataDir, "PG_VERSION")
	_, err := os.Stat(pgVersionFile)
	return err == nil
}
