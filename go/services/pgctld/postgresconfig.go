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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	postgresConfigWaitRetryTime = 100 * time.Millisecond
	// maxIncludeDepth bounds recursion through postgres include directives.
	maxIncludeDepth = 10

	directiveInclude         = "include"
	directiveIncludeIfExists = "include_if_exists"
	directiveIncludeDir      = "include_dir"
)

// PostgresServerConfig is a memory structure that contains PostgreSQL server configuration parameters.
// It can be used to read standard postgresql.conf files and can also be populated from an existing postgresql.conf.
//
// port, listen_addresses, unix_socket_directories, and data_directory are intentionally
// absent: pgctld pins them via postgres command-line args at start, which take precedence
// over postgresql.conf.
type PostgresServerConfig struct {
	// Core connection settings
	MaxConnections int

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
		return 0, fmt.Errorf("failed to convert %s: %w", key, err)
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
		return 0, fmt.Errorf("failed to convert %s to float: %w", key, err)
	}
	return fval, nil
}

// parseConfigInto follows postgres include semantics: relative include paths
// resolve against the including file's directory.
func parseConfigInto(path string, configMap map[string]string, depth int) error {
	if depth > maxIncludeDepth {
		return fmt.Errorf("postgres config include depth exceeded reading %s", path)
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening postgres config %s: %w", path, err)
	}
	defer f.Close()

	baseDir := filepath.Dir(path)
	buf := bufio.NewReader(f)

	for {
		line, _, err := buf.ReadLine()
		if err == io.EOF {
			break
		}
		key, value := parseConfigLine(string(line))
		if key == "" {
			continue
		}

		switch key {
		case directiveInclude, directiveIncludeIfExists:
			target := resolveIncludePath(baseDir, value)
			if err := parseConfigInto(target, configMap, depth+1); err != nil {
				if key == directiveIncludeIfExists && errors.Is(err, fs.ErrNotExist) {
					continue
				}
				return fmt.Errorf("processing %s in %s: %w", key, path, err)
			}
		case directiveIncludeDir:
			if err := parseIncludeDir(resolveIncludePath(baseDir, value), configMap, depth+1); err != nil {
				return fmt.Errorf("processing include_dir in %s: %w", path, err)
			}
		default:
			configMap[key] = value
		}
	}
	return nil
}

// parseConfigLine extracts a key/value from a single postgresql.conf line.
// Returns ("", "") for comments, blanks, and malformed lines.
func parseConfigLine(line string) (string, string) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", ""
	}
	if i := strings.Index(line, "="); i >= 0 {
		return strings.TrimSpace(line[:i]), stripQuotes(strings.TrimSpace(line[i+1:]))
	}
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return "", ""
	}
	return parts[0], stripQuotes(strings.Join(parts[1:], " "))
}

// parseIncludeDir mirrors postgres' include_dir: parse every *.conf in
// lexicographic order, skipping dotfiles and directories.
func parseIncludeDir(dir string, configMap map[string]string, depth int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") || !strings.HasSuffix(e.Name(), ".conf") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, n := range names {
		if err := parseConfigInto(filepath.Join(dir, n), configMap, depth); err != nil {
			return err
		}
	}
	return nil
}

func resolveIncludePath(baseDir, p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(baseDir, p)
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

// ReadPostgresServerConfig populates pgConfig from postgresql.conf at
// pgConfig.Path, following postgres include directives so the in-memory struct
// matches what postgres will load at runtime.
func ReadPostgresServerConfig(pgConfig *PostgresServerConfig, waitTime time.Duration) (*PostgresServerConfig, error) {
	_, err := os.Stat(pgConfig.Path)
	if waitTime != 0 {
		timer := time.NewTimer(waitTime)
		for err != nil {
			select {
			case <-timer.C:
				return nil, err
			default:
				time.Sleep(postgresConfigWaitRetryTime)
				_, err = os.Stat(pgConfig.Path)
			}
		}
	}
	if err != nil {
		return nil, err
	}

	pgConfig.configMap = make(map[string]string)
	if err := parseConfigInto(pgConfig.Path, pgConfig.configMap, 0); err != nil {
		return nil, err
	}

	// Parse and map configuration values to struct fields
	var parseErr error

	if pgConfig.MaxConnections, parseErr = pgConfig.lookupInt("max_connections"); parseErr != nil {
		pgConfig.MaxConnections = 100 // default
	}

	// Memory settings - required in our controlled config
	if val, err := pgConfig.lookupWithDefault("shared_buffers", ""); err != nil {
		return nil, errors.New("shared_buffers not found in config file")
	} else {
		pgConfig.SharedBuffers = val
	}
	if val, err := pgConfig.lookupWithDefault("maintenance_work_mem", ""); err != nil {
		return nil, errors.New("maintenance_work_mem not found in config file")
	} else {
		pgConfig.MaintenanceWorkMem = val
	}
	if val, err := pgConfig.lookupWithDefault("work_mem", ""); err != nil {
		return nil, errors.New("work_mem not found in config file")
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
		return nil, errors.New("wal_buffers not found in config file")
	} else {
		pgConfig.WalBuffers = val
	}
	if val, err := pgConfig.lookupWithDefault("min_wal_size", ""); err != nil {
		return nil, errors.New("min_wal_size not found in config file")
	} else {
		pgConfig.MinWalSize = val
	}
	if val, err := pgConfig.lookupWithDefault("max_wal_size", ""); err != nil {
		return nil, errors.New("max_wal_size not found in config file")
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
		return nil, errors.New("effective_cache_size not found in config file")
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
