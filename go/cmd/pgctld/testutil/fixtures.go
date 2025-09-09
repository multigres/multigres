/*
Copyright 2025 The Multigres Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testutil

import (
	"testing"

	"github.com/spf13/viper"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// SetupTestViper configures viper with test-specific settings
func SetupTestViper(t *testing.T, dataDir string) func() {
	t.Helper()

	// Store original values
	originalValues := make(map[string]any)
	keys := []string{
		"data-dir", "pg-port", "pg-host", "pg-database", "pg-user",
		"socket-dir", "timeout", "log-level",
	}

	for _, key := range keys {
		originalValues[key] = viper.Get(key)
	}

	// Set test values
	viper.Set("data-dir", dataDir)
	viper.Set("pg-port", 5432)
	viper.Set("pg-host", "localhost")
	viper.Set("pg-database", "postgres")
	viper.Set("pg-user", "postgres")
	viper.Set("socket-dir", "/tmp")
	viper.Set("timeout", 30)
	viper.Set("log-level", "error") // Reduce noise during tests

	// Return cleanup function
	return func() {
		for key, value := range originalValues {
			viper.Set(key, value)
		}
	}
}

// TestConfig provides test configuration constants
type TestConfig struct {
	DataDir   string
	Port      int32
	Host      string
	Database  string
	User      string
	SocketDir string
	Timeout   int32
}

// DefaultTestConfig returns default test configuration
func DefaultTestConfig(dataDir string) TestConfig {
	return TestConfig{
		DataDir:   dataDir,
		Port:      5432,
		Host:      "localhost",
		Database:  "postgres",
		User:      "postgres",
		SocketDir: "/tmp",
		Timeout:   30,
	}
}

// CreateStartRequest creates a test StartRequest
func CreateStartRequest(config TestConfig) *pb.StartRequest {
	return &pb.StartRequest{
		DataDir:    config.DataDir,
		Port:       config.Port,
		SocketDir:  config.SocketDir,
		ConfigFile: "",
		ExtraArgs:  []string{},
	}
}

// CreateStopRequest creates a test StopRequest
func CreateStopRequest(config TestConfig, mode string) *pb.StopRequest {
	return &pb.StopRequest{
		Mode:    mode,
		Timeout: config.Timeout,
		DataDir: config.DataDir,
	}
}

// CreateRestartRequest creates a test RestartRequest
func CreateRestartRequest(config TestConfig, mode string) *pb.RestartRequest {
	return &pb.RestartRequest{
		Mode:       mode,
		Timeout:    config.Timeout,
		DataDir:    config.DataDir,
		Port:       config.Port,
		SocketDir:  config.SocketDir,
		ConfigFile: "",
		ExtraArgs:  []string{},
	}
}

// CreateStatusRequest creates a test StatusRequest
func CreateStatusRequest(config TestConfig) *pb.StatusRequest {
	return &pb.StatusRequest{
		DataDir: config.DataDir,
	}
}

// CreateVersionRequest creates a test VersionRequest
func CreateVersionRequest(config TestConfig) *pb.VersionRequest {
	return &pb.VersionRequest{
		Host:     config.Host,
		Port:     config.Port,
		Database: config.Database,
		User:     config.User,
	}
}

// CreateInitDataDirRequest creates a test InitDataDirRequest
func CreateInitDataDirRequest(config TestConfig) *pb.InitDataDirRequest {
	return &pb.InitDataDirRequest{
		DataDir:   config.DataDir,
		AuthLocal: "trust",
		AuthHost:  "md5",
		ExtraArgs: []string{},
	}
}

// ExpectedStartResponse creates expected StartResponse for testing
func ExpectedStartResponse(pid int32, message string) *pb.StartResponse {
	return &pb.StartResponse{
		Pid:     pid,
		Message: message,
	}
}

// ExpectedStopResponse creates expected StopResponse for testing
func ExpectedStopResponse(message string) *pb.StopResponse {
	return &pb.StopResponse{
		Message: message,
	}
}

// ExpectedStatusResponse creates expected StatusResponse for testing
func ExpectedStatusResponse(config TestConfig, status pb.ServerStatus, pid int32, ready bool) *pb.StatusResponse {
	return &pb.StatusResponse{
		Status:        status,
		Pid:           pid,
		Version:       "PostgreSQL 15.0",
		UptimeSeconds: 0,
		DataDir:       config.DataDir,
		Port:          config.Port,
		Host:          config.Host,
		Ready:         ready,
		Message:       "Test status",
	}
}

// MockPostgreSQLCommands returns a map of command patterns to their expected results
func MockPostgreSQLCommands() map[string]MockCommandResult {
	return map[string]MockCommandResult{
		"initdb": {
			ExitCode: 0,
			Stdout:   "Success. You can now start the database server using:\n",
		},
		"postgres": {
			ExitCode: 0,
			Stdout:   "",
		},
		"pg_isready": {
			ExitCode: 0,
			Stdout:   "localhost:5432 - accepting connections\n",
		},
		"pg_ctl stop": {
			ExitCode: 0,
			Stdout:   "waiting for server to shut down.... done\nserver stopped\n",
		},
		"pg_ctl reload": {
			ExitCode: 0,
			Stdout:   "server signaled\n",
		},
		"psql": {
			ExitCode: 0,
			Stdout:   " PostgreSQL 15.0 on x86_64-pc-linux-gnu\n",
		},
	}
}
