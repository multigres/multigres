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

package init

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// executeInitCommand builds and runs the actual multigres binary with "cluster init" command
func executeInitCommand(t *testing.T, args []string) (string, error) {
	// Create a separate temp directory for the binary to avoid conflicts
	binaryDir, err := os.MkdirTemp("", "multigres_binary")
	if err != nil {
		t.Fatalf("Failed to create temp directory for binary: %v", err)
	}
	defer os.RemoveAll(binaryDir)

	// Build multigres binary for testing (following pgctld pattern)
	multigresBinary := filepath.Join(binaryDir, "multigres")
	buildCmd := exec.Command("go", "build", "-o", multigresBinary, "../../..")

	// Set working directory to avoid issues with temp paths
	wd, _ := os.Getwd()
	buildCmd.Dir = wd

	buildOutput, err := buildCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to build multigres binary: %v\nOutput: %s", err, string(buildOutput))
	}

	// Prepare the full command: "multigres cluster init <args>"
	cmdArgs := append([]string{"cluster", "init"}, args...)
	cmd := exec.Command(multigresBinary, cmdArgs...)

	output, err := cmd.CombinedOutput()
	return string(output), err
}

func TestInitCommand(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	tests := []struct {
		name           string
		setupDirs      func(*testing.T) ([]string, func()) // returns config paths and cleanup
		expectError    bool
		errorContains  string
		outputContains []string
	}{
		{
			name: "successful init with current directory",
			setupDirs: func(t *testing.T) ([]string, func()) {
				tempDir, err := os.MkdirTemp("", "multigres_init_test")
				require.NoError(t, err)
				return []string{tempDir}, func() { os.RemoveAll(tempDir) }
			},
			expectError:    false,
			outputContains: []string{"Initializing Multigres cluster configuration", "successfully"},
		},
		{
			name: "error with non-existent config path",
			setupDirs: func(t *testing.T) ([]string, func()) {
				return []string{"/nonexistent/path/that/should/not/exist"}, func() {}
			},
			expectError:   true,
			errorContains: "config path does not exist",
		},
		{
			name: "error with file instead of directory",
			setupDirs: func(t *testing.T) ([]string, func()) {
				tempFile, err := os.CreateTemp("", "multigres_init_test_file")
				require.NoError(t, err)
				tempFile.Close()
				return []string{tempFile.Name()}, func() { os.Remove(tempFile.Name()) }
			},
			expectError:   true,
			errorContains: "config path is not a directory",
		},
		{
			name: "successful init with multiple valid paths",
			setupDirs: func(t *testing.T) ([]string, func()) {
				tempDir1, err := os.MkdirTemp("", "multigres_init_test1")
				require.NoError(t, err)
				tempDir2, err := os.MkdirTemp("", "multigres_init_test2")
				require.NoError(t, err)
				return []string{tempDir1, tempDir2}, func() {
					os.RemoveAll(tempDir1)
					os.RemoveAll(tempDir2)
				}
			},
			expectError:    false,
			outputContains: []string{"Initializing Multigres cluster configuration", "successfully"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test directories
			configPaths, cleanup := tt.setupDirs(t)
			defer cleanup()

			// Build command arguments
			args := []string{}
			for _, path := range configPaths {
				args = append(args, "--config-path", path)
			}

			// Execute command using the actual binary
			output, err := executeInitCommand(t, args)

			// Check results
			if tt.expectError {
				require.Error(t, err)
				// Error message should be in stderr, but exec.CombinedOutput captures both
				errorOutput := output
				if err != nil {
					errorOutput = err.Error() + "\n" + output
				}
				assert.Contains(t, strings.ToLower(errorOutput), strings.ToLower(tt.errorContains))
			} else {
				require.NoError(t, err, "Command failed with output: %s", output)
				for _, expectedOutput := range tt.outputContains {
					assert.Contains(t, output, expectedOutput)
				}
			}
		})
	}
}

func TestInitCommandConfigFileCreation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	// Setup test directory
	tempDir, err := os.MkdirTemp("", "multigres_init_config_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Execute command using the actual binary
	output, err := executeInitCommand(t, []string{"--config-path", tempDir})

	// Command should succeed
	require.NoError(t, err, "Command failed with output: %s", output)

	// Check output contains expected messages
	assert.Contains(t, output, "Initializing Multigres cluster configuration")

	// Check config file was created
	configFile := filepath.Join(tempDir, "multigres.yaml")
	_, err = os.Stat(configFile)
	require.NoError(t, err, "Config file should exist")

	// Read and validate config content
	configData, err := os.ReadFile(configFile)
	require.NoError(t, err)

	var config MultigressConfig
	err = yaml.Unmarshal(configData, &config)
	require.NoError(t, err)

	// Verify config values
	assert.Equal(t, "local", config.Provisioner)
	assert.Equal(t, "etcd2", config.Topology.Backend)
	assert.Equal(t, "/multigres/global", config.Topology.GlobalRootPath)
	assert.Equal(t, "zone1", config.Topology.DefaultCellName)
	assert.Equal(t, "/multigres/zone1", config.Topology.DefaultCellRootPath)
}

func TestInitCommandConfigFileAlreadyExists(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	// Setup test directory
	tempDir, err := os.MkdirTemp("", "multigres_init_exists_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create existing config file
	existingConfig := filepath.Join(tempDir, "multigres.yaml")
	err = os.WriteFile(existingConfig, []byte("existing: config"), 0644)
	require.NoError(t, err)

	// Execute command using the actual binary
	output, err := executeInitCommand(t, []string{"--config-path", tempDir})

	// Should fail with appropriate error
	require.Error(t, err)
	errorOutput := err.Error() + "\n" + output
	assert.Contains(t, errorOutput, "config file already exists")
	assert.Contains(t, errorOutput, existingConfig)
}

func TestInitCommandCustomFlags(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	// Setup test directory
	tempDir, err := os.MkdirTemp("", "multigres_init_flags_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Execute command with custom flags using the actual binary
	args := []string{
		"--config-path", tempDir,
		"--provisioner", "local",
		"--topo-backend", "etcd2",
		"--topo-global-root-path", "/custom/global",
		"--topo-default-cell-name", "custom-cell",
		"--topo-default-cell-root-path", "/custom/cell",
	}
	output, err := executeInitCommand(t, args)

	// Command should succeed
	require.NoError(t, err, "Command failed with output: %s", output)

	// Check config file was created
	configFile := filepath.Join(tempDir, "multigres.yaml")
	_, err = os.Stat(configFile)
	require.NoError(t, err, "Config file should exist")

	// Read and validate config content
	configData, err := os.ReadFile(configFile)
	require.NoError(t, err)

	var config MultigressConfig
	err = yaml.Unmarshal(configData, &config)
	require.NoError(t, err)

	// Verify custom config values
	assert.Equal(t, "local", config.Provisioner)
	assert.Equal(t, "etcd2", config.Topology.Backend)
	assert.Equal(t, "/custom/global", config.Topology.GlobalRootPath)
	assert.Equal(t, "custom-cell", config.Topology.DefaultCellName)
	assert.Equal(t, "/custom/cell", config.Topology.DefaultCellRootPath)
}

func TestInitCommandInvalidTopoBackend(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	// Setup test directory
	tempDir, err := os.MkdirTemp("", "multigres_init_invalid_backend_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Execute command with invalid topo-backend using the actual binary
	args := []string{"--config-path", tempDir, "--topo-backend", "invalid"}
	output, err := executeInitCommand(t, args)

	// Should fail with validation error
	require.Error(t, err)
	errorOutput := err.Error() + "\n" + output
	assert.Contains(t, errorOutput, "invalid topo backend: invalid")
	assert.Contains(t, errorOutput, "available: [etcd2")

	// No config file should be created
	configFile := filepath.Join(tempDir, "multigres.yaml")
	_, err = os.Stat(configFile)
	assert.True(t, os.IsNotExist(err), "Config file should not exist")
}

func TestInitCommandInvalidProvisioner(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	// Setup test directory
	tempDir, err := os.MkdirTemp("", "multigres_init_invalid_provisioner_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Execute command with invalid provisioner using the actual binary
	args := []string{"--config-path", tempDir, "--provisioner", "invalid"}
	output, err := executeInitCommand(t, args)

	// Should fail with validation error
	require.Error(t, err)
	errorOutput := err.Error() + "\n" + output
	assert.Contains(t, errorOutput, "invalid provisioner: invalid")
	assert.Contains(t, errorOutput, "only 'local' is supported")

	// No config file should be created
	configFile := filepath.Join(tempDir, "multigres.yaml")
	_, err = os.Stat(configFile)
	assert.True(t, os.IsNotExist(err), "Config file should not exist")
}

func TestInitCommandAllCustomFlags(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	// Setup test directory
	tempDir, err := os.MkdirTemp("", "multigres_init_all_flags_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Execute command with all custom flags using the actual binary
	args := []string{
		"--config-path", tempDir,
		"--provisioner", "local",
		"--topo-backend", "etcd2",
		"--topo-global-root-path", "/test/global",
		"--topo-default-cell-name", "test-zone",
		"--topo-default-cell-root-path", "/test/zone",
	}
	output, err := executeInitCommand(t, args)
	require.NoError(t, err, "Command failed with output: %s", output)

	// Read config file
	configFile := filepath.Join(tempDir, "multigres.yaml")
	configData, err := os.ReadFile(configFile)
	require.NoError(t, err)

	// Verify YAML structure
	expectedYAML := `provisioner: local
topology:
    backend: etcd2
    global-root-path: /test/global
    default-cell-name: test-zone
    default-cell-root-path: /test/zone
`
	assert.YAMLEq(t, expectedYAML, string(configData))
}
