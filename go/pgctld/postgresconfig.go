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

	// Other important settings
	ClusterName string

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

// stripQuotes removes surrounding single or double quotes from a string value
func stripQuotes(value string) string {
	value = strings.TrimSpace(value)
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

	// Other important settings
	if val, err := pgConfig.lookupWithDefault("cluster_name", pgConfig.ClusterName); err == nil {
		pgConfig.ClusterName = val
	}

	return pgConfig, nil
}
