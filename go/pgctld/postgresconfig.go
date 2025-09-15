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
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	postgresConfigWaitRetryTime = 100 * time.Millisecond
)

// PostgresServerConfig is a memory structure that contains PostgreSQL server configuration parameters.
// It can be used to read standard postgresql.conf files and can also be populated from an existing postgresql.conf.
type PostgresServerConfig struct {
	// Core connection settings
	Port                  int
	MaxConnections        int
	ListenAddresses       string
	UnixSocketDirectories string

	// File locations (template fields)
	DataDir   string // matches {{.DataDir}} in template
	HbaFile   string // matches {{.HbaFile}} in template
	IdentFile string // matches {{.IdentFile}} in template

	// Memory settings
	SharedBuffers      string
	MaintenanceWorkMem string
	WorkMem            string

	// Worker and parallel settings
	MaxWorkerProcesses            int
	EffectiveIoConcurrency        int
	MaxParallelWorkers            int
	MaxParallelWorkersPerGather   int
	MaxParallelMaintenanceWorkers int

	// WAL settings
	WalBuffers string
	MinWalSize string
	MaxWalSize string

	// Checkpoint settings
	CheckpointCompletionTarget float64

	// Replication settings
	MaxWalSenders       int
	MaxReplicationSlots int

	// Query planner settings
	EffectiveCacheSize      string
	RandomPageCost          float64
	DefaultStatisticsTarget int

	// Other important settings
	ClusterName string
	User        string // PostgreSQL user name for HBA configuration

	configMap map[string]string
	Path      string // the actual path that represents this postgresql.conf
}

func (cnf *PostgresServerConfig) lookup(key string) string {
	return cnf.configMap[key]
}

func (cnf *PostgresServerConfig) lookupWithDefault(key, defaultVal string) (string, error) {
	val := cnf.lookup(key)
	if val == "" {
		if defaultVal == "" {
			return "", fmt.Errorf("value for key '%v' not set and no default value set", key)
		}
		return defaultVal, nil
	}
	return val, nil
}

func (cnf *PostgresServerConfig) lookupInt(key string) (int, error) {
	val, err := cnf.lookupWithDefault(key, "")
	if err != nil {
		return 0, err
	}
	ival, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("failed to convert %s: %v", key, err)
	}
	return ival, nil
}

func (cnf *PostgresServerConfig) lookupFloat(key string) (float64, error) {
	val, err := cnf.lookupWithDefault(key, "")
	if err != nil {
		return 0, err
	}
	fval, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to convert %s to float: %v", key, err)
	}
	return fval, nil
}

// stripQuotes removes surrounding single or double quotes from a string value
// and removes any trailing comments
func stripQuotes(value string) string {
	value = strings.TrimSpace(value)

	// Remove trailing comments (anything after # with optional whitespace)
	if commentIndex := strings.Index(value, "#"); commentIndex != -1 {
		value = strings.TrimSpace(value[:commentIndex])
	}

	if len(value) >= 2 {
		if (strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) ||
			(strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) {
			return value[1 : len(value)-1]
		}
	}
	return value
}

// ReadPostgresServerConfig reads an existing postgresql.conf from disk and updates the passed in PostgresServerConfig object
// with values from the config file on disk.
func ReadPostgresServerConfig(pgConfig *PostgresServerConfig, waitTime time.Duration) (*PostgresServerConfig, error) {
	f, err := os.Open(pgConfig.Path)
	if waitTime != 0 {
		timer := time.NewTimer(waitTime)
		for err != nil {
			select {
			case <-timer.C:
				return nil, err
			default:
				time.Sleep(postgresConfigWaitRetryTime)
				f, err = os.Open(pgConfig.Path)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := bufio.NewReader(f)
	pgConfig.configMap = make(map[string]string)

	for {
		line, _, err := buf.ReadLine()
		if err == io.EOF {
			break
		}
		lineStr := strings.TrimSpace(string(line))

		// Skip comments and empty lines
		if strings.HasPrefix(lineStr, "#") || lineStr == "" {
			continue
		}

		// Handle PostgreSQL config format: key = value (or key value)
		var key, value string
		if strings.Contains(lineStr, "=") {
			parts := strings.SplitN(lineStr, "=", 2)
			if len(parts) == 2 {
				key = strings.TrimSpace(parts[0])
				value = stripQuotes(strings.TrimSpace(parts[1]))
			}
		} else {
			// Handle format without = (space separated)
			parts := strings.Fields(lineStr)
			if len(parts) >= 2 {
				key = parts[0]
				value = stripQuotes(strings.Join(parts[1:], " "))
			}
		}

		if key != "" {
			pgConfig.configMap[key] = value
		}
	}

	// Parse and map configuration values to struct fields
	var parseErr error

	// Connection settings
	if pgConfig.Port, parseErr = pgConfig.lookupInt("port"); parseErr != nil {
		pgConfig.Port = 5432 // default
	}
	if pgConfig.MaxConnections, parseErr = pgConfig.lookupInt("max_connections"); parseErr != nil {
		pgConfig.MaxConnections = 100 // default
	}
	if val, err := pgConfig.lookupWithDefault("listen_addresses", pgConfig.ListenAddresses); err == nil {
		pgConfig.ListenAddresses = val
	}
	if val, err := pgConfig.lookupWithDefault("unix_socket_directories", pgConfig.UnixSocketDirectories); err == nil {
		pgConfig.UnixSocketDirectories = val
	}

	// File locations
	if val, err := pgConfig.lookupWithDefault("data_directory", pgConfig.DataDir); err == nil {
		pgConfig.DataDir = val
	}
	if val, err := pgConfig.lookupWithDefault("hba_file", pgConfig.HbaFile); err == nil {
		pgConfig.HbaFile = val
	}
	if val, err := pgConfig.lookupWithDefault("ident_file", pgConfig.IdentFile); err == nil {
		pgConfig.IdentFile = val
	}

	// Memory settings - required in our controlled config
	if val, err := pgConfig.lookupWithDefault("shared_buffers", ""); err != nil {
		return nil, fmt.Errorf("shared_buffers not found in config file")
	} else {
		pgConfig.SharedBuffers = val
	}
	if val, err := pgConfig.lookupWithDefault("maintenance_work_mem", ""); err != nil {
		return nil, fmt.Errorf("maintenance_work_mem not found in config file")
	} else {
		pgConfig.MaintenanceWorkMem = val
	}
	if val, err := pgConfig.lookupWithDefault("work_mem", ""); err != nil {
		return nil, fmt.Errorf("work_mem not found in config file")
	} else {
		pgConfig.WorkMem = val
	}

	// Worker and parallel settings - required in our controlled config
	if pgConfig.MaxWorkerProcesses, parseErr = pgConfig.lookupInt("max_worker_processes"); parseErr != nil {
		return nil, fmt.Errorf("max_worker_processes not found in config file: %w", parseErr)
	}
	if pgConfig.EffectiveIoConcurrency, parseErr = pgConfig.lookupInt("effective_io_concurrency"); parseErr != nil {
		return nil, fmt.Errorf("effective_io_concurrency not found in config file: %w", parseErr)
	}
	if pgConfig.MaxParallelWorkers, parseErr = pgConfig.lookupInt("max_parallel_workers"); parseErr != nil {
		return nil, fmt.Errorf("max_parallel_workers not found in config file: %w", parseErr)
	}
	if pgConfig.MaxParallelWorkersPerGather, parseErr = pgConfig.lookupInt("max_parallel_workers_per_gather"); parseErr != nil {
		return nil, fmt.Errorf("max_parallel_workers_per_gather not found in config file: %w", parseErr)
	}
	if pgConfig.MaxParallelMaintenanceWorkers, parseErr = pgConfig.lookupInt("max_parallel_maintenance_workers"); parseErr != nil {
		return nil, fmt.Errorf("max_parallel_maintenance_workers not found in config file: %w", parseErr)
	}

	// WAL settings - required in our controlled config
	if val, err := pgConfig.lookupWithDefault("wal_buffers", ""); err != nil {
		return nil, fmt.Errorf("wal_buffers not found in config file")
	} else {
		pgConfig.WalBuffers = val
	}
	if val, err := pgConfig.lookupWithDefault("min_wal_size", ""); err != nil {
		return nil, fmt.Errorf("min_wal_size not found in config file")
	} else {
		pgConfig.MinWalSize = val
	}
	if val, err := pgConfig.lookupWithDefault("max_wal_size", ""); err != nil {
		return nil, fmt.Errorf("max_wal_size not found in config file")
	} else {
		pgConfig.MaxWalSize = val
	}

	// Checkpoint settings - required in our controlled config
	if pgConfig.CheckpointCompletionTarget, parseErr = pgConfig.lookupFloat("checkpoint_completion_target"); parseErr != nil {
		return nil, fmt.Errorf("checkpoint_completion_target not found in config file: %w", parseErr)
	}

	// Replication settings - required in our controlled config
	if pgConfig.MaxWalSenders, parseErr = pgConfig.lookupInt("max_wal_senders"); parseErr != nil {
		return nil, fmt.Errorf("max_wal_senders not found in config file: %w", parseErr)
	}
	if pgConfig.MaxReplicationSlots, parseErr = pgConfig.lookupInt("max_replication_slots"); parseErr != nil {
		return nil, fmt.Errorf("max_replication_slots not found in config file: %w", parseErr)
	}

	// Query planner settings - required in our controlled config
	if val, err := pgConfig.lookupWithDefault("effective_cache_size", ""); err != nil {
		return nil, fmt.Errorf("effective_cache_size not found in config file")
	} else {
		pgConfig.EffectiveCacheSize = val
	}
	if pgConfig.RandomPageCost, parseErr = pgConfig.lookupFloat("random_page_cost"); parseErr != nil {
		return nil, fmt.Errorf("random_page_cost not found in config file: %w", parseErr)
	}
	if pgConfig.DefaultStatisticsTarget, parseErr = pgConfig.lookupInt("default_statistics_target"); parseErr != nil {
		return nil, fmt.Errorf("default_statistics_target not found in config file: %w", parseErr)
	}

	// Other important settings
	if val, err := pgConfig.lookupWithDefault("cluster_name", pgConfig.ClusterName); err == nil {
		pgConfig.ClusterName = val
	}

	return pgConfig, nil
}
