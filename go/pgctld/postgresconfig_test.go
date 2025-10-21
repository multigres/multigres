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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// configSettings contains all the PostgreSQL configuration settings we want to test.
// This serves as both the test data source and the assertion reference.
var configSettings = map[string]string{
	// Basic integer settings
	"port":                                "5432",
	"max_connections":                     "100",
	"superuser_reserved_connections":      "3",
	"tcp_keepalives_idle":                 "0",
	"tcp_keepalives_interval":             "0",
	"tcp_keepalives_count":                "0",
	"tcp_user_timeout":                    "0",
	"client_connection_check_interval":    "0",
	"vacuum_cost_delay":                   "0",
	"commit_delay":                        "0",
	"statement_timeout":                   "0",
	"lock_timeout":                        "0",
	"idle_in_transaction_session_timeout": "0",
	"idle_session_timeout":                "0",
	"old_snapshot_threshold":              "-1",
	"temp_file_limit":                     "-1",
	"wal_buffers":                         "1920kB",
	"wal_keep_size":                       "0",
	"max_slot_wal_keep_size":              "4096",
	"archive_timeout":                     "0",
	"max_worker_processes":                "6",
	"effective_io_concurrency":            "200",
	"max_parallel_workers":                "2",
	"max_parallel_workers_per_gather":     "1",
	"max_parallel_maintenance_workers":    "1",
	"max_wal_senders":                     "5",
	"max_replication_slots":               "5",
	"default_statistics_target":           "100",

	// Float/decimal settings
	"checkpoint_completion_target": "0.9",
	"hash_mem_multiplier":          "1.0",
	"bgwriter_lru_multiplier":      "2.0",
	"seq_page_cost":                "1.0",
	"random_page_cost":             "1.1",
	"cpu_tuple_cost":               "0.01",

	// Boolean settings (stored as strings)
	"ssl":                       "on",
	"ssl_prefer_server_ciphers": "off",
	"bonjour":                   "off",
	"db_user_namespace":         "off",
	"krb_caseins_users":         "off",

	// String settings
	"password_encryption":           "scram-sha-256",
	"ssl_ciphers":                   "HIGH:MEDIUM:+3DES:!aNULL",
	"ssl_ecdh_curve":                "prime256v1",
	"ssl_min_protocol_version":      "TLSv1.2",
	"ssl_max_protocol_version":      "",
	"wal_level":                     "logical",
	"wal_sync_method":               "fsync",
	"recovery_target_timeline":      "latest",
	"recovery_target_action":        "pause",
	"log_statement":                 "ddl",
	"log_timezone":                  "UTC",
	"cluster_name":                  "main",
	"default_text_search_config":    "pg_catalog.english",
	"jit_provider":                  "llvmjit",
	"datestyle":                     "iso, mdy",
	"intervalstyle":                 "postgres",
	"timezone_abbreviations":        "Default",
	"client_encoding":               "sql_ascii",
	"default_table_access_method":   "heap",
	"default_toast_compression":     "pglz",
	"default_transaction_isolation": "read committed",
	"session_replication_role":      "origin",
	"bytea_output":                  "hex",
	"xmlbinary":                     "base64",
	"xmloption":                     "content",
	"constraint_exclusion":          "partition",
	"plan_cache_mode":               "auto",
	"track_functions":               "none",
	"compute_query_id":              "auto",
	"log_error_verbosity":           "default",
	"search_path":                   `"$user", public`,
	"ssl_ca_file":                   "",
	"dynamic_library_path":          "$libdir",
	"stats_temp_directory":          "pg_stat_tmp",

	// Path settings - these map to our struct fields
	"data_directory":          "/var/lib/postgresql/data",
	"hba_file":                "/etc/postgresql/pg_hba.conf",
	"ident_file":              "/etc/postgresql/pg_ident.conf",
	"unix_socket_directories": "/var/run/postgresql",
	"listen_addresses":        "*",

	// Include settings
	"include":           "/etc/postgresql/logging.conf",
	"include_dir":       "/etc/postgresql-custom",
	"include_if_exists": "/etc/postgresql-custom/custom.conf",

	// Memory/size settings (with units)
	"shared_buffers":               "64MB",
	"temp_buffers":                 "8MB",
	"work_mem":                     "1092kB",
	"maintenance_work_mem":         "16MB",
	"logical_decoding_work_mem":    "64MB",
	"max_stack_depth":              "2MB",
	"min_dynamic_shared_memory":    "0MB",
	"wal_writer_flush_after":       "1MB",
	"wal_skip_threshold":           "2MB",
	"checkpoint_flush_after":       "256kB",
	"max_wal_size":                 "4GB",
	"min_wal_size":                 "1GB",
	"effective_cache_size":         "192MB",
	"min_parallel_table_scan_size": "8MB",
	"min_parallel_index_scan_size": "512kB",
	"gin_pending_list_limit":       "4MB",

	// Time settings (with units)
	"authentication_timeout":       "1min",
	"bgwriter_delay":               "200ms",
	"wal_writer_delay":             "200ms",
	"checkpoint_timeout":           "5min",
	"checkpoint_warning":           "30s",
	"wal_sender_timeout":           "60s",
	"max_standby_archive_delay":    "30s",
	"max_standby_streaming_delay":  "30s",
	"wal_receiver_status_interval": "10s",
	"wal_receiver_timeout":         "60s",
	"wal_retrieve_retry_interval":  "5s",
	"recovery_min_apply_delay":     "0",
	"autovacuum_naptime":           "1min",
	"autovacuum_vacuum_cost_delay": "2ms",
	"deadlock_timeout":             "1s",

	// Extension settings (with dots in names)
	"auto_explain.log_min_duration": "10s",
	"cron.database_name":            "postgres",
	"pgsodium.getkey_script":        "/usr/lib/postgresql/bin/pgsodium_getkey.sh",
	"vault.getkey_script":           "/usr/lib/postgresql/bin/pgsodium_getkey.sh",

	// Library settings
	"session_preload_libraries": "supautils",
	"shared_preload_libraries":  "pg_stat_statements, pgaudit, plpgsql, plpgsql_check, pg_cron, pg_net, pgsodium, auto_explain, pg_tle, plan_filter, supabase_vault",
}

// generateConfigFromMap creates a PostgreSQL configuration file content from the settings map
func generateConfigFromMap(settings map[string]string) string {
	var lines []string

	// Add header
	lines = append(lines, "# PostgreSQL Configuration Test File")
	lines = append(lines, "# Generated from test settings map")
	lines = append(lines, "")

	// Generate config lines from map - just output key = value
	for key, value := range settings {
		lines = append(lines, fmt.Sprintf("%s = %s", key, value))
	}

	return strings.Join(lines, "\n")
}

func TestReadPostgresServerConfig(t *testing.T) {
	// Generate config file content from our test settings map
	configContent := generateConfigFromMap(configSettings)

	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "postgresql.conf")

	err := os.WriteFile(configPath, []byte(configContent), 0o644)
	require.NoError(t, err, "Failed to write test config file")

	// Create config struct and read the file
	config := &PostgresServerConfig{Path: configPath}
	result, err := ReadPostgresServerConfig(config, 0)
	require.NoError(t, err, "ReadPostgresServerConfig should not return error")

	// Test that all expected settings were read correctly into configMap
	for key, expectedValue := range configSettings {
		actualValue := result.lookup(key)
		assert.Equal(t, expectedValue, actualValue, "Config field %s should match expected value", key)
	}

	// Test that extracted struct fields were populated correctly
	assert.Equal(t, 100, result.MaxConnections, "MaxConnections should be extracted correctly")
	assert.Equal(t, configSettings["cluster_name"], result.ClusterName, "ClusterName should be extracted correctly")

	// Port, listen_addresses, unix_socket_directories, data_directory, hba_file, and ident_file
	// are NOT read from config files - they are set via flags/code and passed as command-line parameters
}

func TestReadPostgresServerConfigEmptyFile(t *testing.T) {
	// Create a temporary empty config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "empty.conf")

	err := os.WriteFile(configPath, []byte(""), 0o644)
	require.NoError(t, err, "Failed to write empty config file")

	// Create config struct and read the file
	config := &PostgresServerConfig{Path: configPath}
	_, err = ReadPostgresServerConfig(config, 0)
	require.Error(t, err, "ReadPostgresServerConfig should return error for empty file")
	assert.Contains(t, err.Error(), "not found in config file", "Error should mention missing parameter")
}

func TestReadPostgresServerConfigCommentsOnly(t *testing.T) {
	// Create a config file with only comments
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "comments.conf")

	configContent := `
# This is a comment
# port = 9999
# Another comment
  # Indented comment
#max_connections = 50
`

	err := os.WriteFile(configPath, []byte(configContent), 0o644)
	require.NoError(t, err, "Failed to write comments-only config file")

	// Create config struct and read the file
	config := &PostgresServerConfig{Path: configPath}
	_, err = ReadPostgresServerConfig(config, 0)
	require.Error(t, err, "ReadPostgresServerConfig should return error for comments-only file")
	assert.Contains(t, err.Error(), "not found in config file", "Error should mention missing parameter")
}

func TestReadPostgresServerConfigFileNotFound(t *testing.T) {
	// Try to read a non-existent file
	config := &PostgresServerConfig{Path: "/nonexistent/file.conf"}
	_, err := ReadPostgresServerConfig(config, 0)
	assert.Error(t, err, "ReadPostgresServerConfig should return error for non-existent file")
}

func TestReadPostgresServerConfigQuoteRemoval(t *testing.T) {
	// Test that quoted values have their quotes removed
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "quoted.conf")

	configContent := `
# Test single quotes
data_directory = 'multigres_local/test-rafa/pg_data'
cluster_name = 'test_cluster'
listen_addresses = 'localhost'

# Test double quotes
hba_file = "/etc/postgresql/pg_hba.conf"
ident_file = "/etc/postgresql/pg_ident.conf"
unix_socket_directories = "/var/run/postgresql"

# Test without quotes (should remain unchanged)
port = 5432
max_connections = 200

# Required parameters for validation (using default/test values)
shared_buffers = 64MB
maintenance_work_mem = 16MB
work_mem = 1092kB
max_worker_processes = 6
effective_io_concurrency = 200
max_parallel_workers = 2
max_parallel_workers_per_gather = 1
max_parallel_maintenance_workers = 1
wal_buffers = 1920kB
min_wal_size = 1GB
max_wal_size = 4GB
checkpoint_completion_target = 0.9
max_wal_senders = 5
max_replication_slots = 5
effective_cache_size = 192MB
random_page_cost = 1.1
default_statistics_target = 100

# Test mixed formats
ssl_cert_file = 'server.crt'
ssl_key_file = "server.key"

# Test space-separated format (without =)
shared_preload_libraries 'pg_stat_statements, auto_explain'
`

	err := os.WriteFile(configPath, []byte(configContent), 0o644)
	require.NoError(t, err, "Failed to write quoted config file")

	// Create config struct and read the file
	config := &PostgresServerConfig{Path: configPath}
	result, err := ReadPostgresServerConfig(config, 0)
	require.NoError(t, err, "ReadPostgresServerConfig should not return error")

	// Test that single quotes were removed
	assert.Equal(t, "test_cluster", result.ClusterName, "Single quotes should be removed from cluster_name")

	// Test that unquoted values remain unchanged
	assert.Equal(t, 200, result.MaxConnections, "Unquoted max_connections should remain unchanged")

	// Port, listen_addresses, unix_socket_directories, data_directory, hba_file, and ident_file
	// are NOT read from config files - they are set via flags/code and passed as command-line parameters

	// Test mixed quote types
	assert.Equal(t, "server.crt", result.lookup("ssl_cert_file"), "Single quotes should be removed from ssl_cert_file")
	assert.Equal(t, "server.key", result.lookup("ssl_key_file"), "Double quotes should be removed from ssl_key_file")

	// Test space-separated format
	assert.Equal(t, "pg_stat_statements, auto_explain", result.lookup("shared_preload_libraries"), "Single quotes should be removed from space-separated values")
}

func TestGenerateAndReadConfigRoundTrip(t *testing.T) {
	// Test the complete round-trip: generate config using template, then read it back
	// and validate that all required template fields are populated correctly
	tmpDir := t.TempDir()

	// Generate config using the template
	generatedConfig, err := GeneratePostgresServerConfig(tmpDir, 5433, "postgres")
	require.NoError(t, err, "GeneratePostgresServerConfig should not return error")

	// Read the generated config back from disk
	readConfig := &PostgresServerConfig{Path: generatedConfig.Path}
	result, err := ReadPostgresServerConfig(readConfig, 0)
	require.NoError(t, err, "ReadPostgresServerConfig should not return error for generated config")

	// Validate that all required template fields are populated (not empty/zero values)
	// Port, ListenAddresses, and UnixSocketDirectories are set during generation and preserved in memory
	// but are NOT read back from the config file (they're passed as CLI parameters)
	assert.NotZero(t, result.MaxConnections, "MaxConnections should be set")
	assert.NotEmpty(t, result.SharedBuffers, "SharedBuffers should be set")
	assert.NotEmpty(t, result.MaintenanceWorkMem, "MaintenanceWorkMem should be set")
	assert.NotEmpty(t, result.WorkMem, "WorkMem should be set")
	assert.NotZero(t, result.MaxWorkerProcesses, "MaxWorkerProcesses should be set")
	// This is an exception, the default is set to zero
	assert.Zero(t, result.EffectiveIoConcurrency, "EffectiveIoConcurrency should be set")
	assert.NotZero(t, result.MaxParallelWorkers, "MaxParallelWorkers should be set")
	assert.NotZero(t, result.MaxParallelWorkersPerGather, "MaxParallelWorkersPerGather should be set")
	assert.NotZero(t, result.MaxParallelMaintenanceWorkers, "MaxParallelMaintenanceWorkers should be set")
	assert.NotEmpty(t, result.WalBuffers, "WalBuffers should be set")
	assert.NotEmpty(t, result.MinWalSize, "MinWalSize should be set")
	assert.NotEmpty(t, result.MaxWalSize, "MaxWalSize should be set")
	assert.NotZero(t, result.CheckpointCompletionTarget, "CheckpointCompletionTarget should be set")
	assert.NotZero(t, result.MaxWalSenders, "MaxWalSenders should be set")
	assert.NotZero(t, result.MaxReplicationSlots, "MaxReplicationSlots should be set")
	assert.NotEmpty(t, result.EffectiveCacheSize, "EffectiveCacheSize should be set")
	assert.NotZero(t, result.RandomPageCost, "RandomPageCost should be set")
	assert.NotZero(t, result.DefaultStatisticsTarget, "DefaultStatisticsTarget should be set")

	// Other settings
	assert.NotEmpty(t, result.ClusterName, "ClusterName should be set")
	configFileContent, err := os.ReadFile(generatedConfig.Path)
	require.NoError(t, err, "Should be able to read generated config file")
	configString := string(configFileContent)

	// These should NOT be in the config file - they're passed as command-line parameters
	assert.NotContains(t, configString, "port =", "Config file should NOT contain port")
	assert.NotContains(t, configString, "listen_addresses =", "Config file should NOT contain listen_addresses")
	assert.NotContains(t, configString, "unix_socket_directories =", "Config file should NOT contain unix_socket_directories")
	assert.NotContains(t, configString, "data_directory =", "Config file should NOT contain data_directory")
	assert.NotContains(t, configString, "hba_file =", "Config file should NOT contain hba_file")
	assert.NotContains(t, configString, "ident_file =", "Config file should NOT contain ident_file")

	assert.NotContains(t, configString, "{{.", "Config file should not contain unprocessed template variables")
}
