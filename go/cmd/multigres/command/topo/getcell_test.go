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

package topo

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAdminServerFromConfig(t *testing.T) {
	// Create a temporary directory for test configs
	tempDir, err := os.MkdirTemp("", "multigres_test_")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	t.Run("valid local config", func(t *testing.T) {
		// Create a test config file
		configContent := `provisioner: local
provisioner-config:
  multiadmin:
    grpc-port: 12345
    http-port: 12346
`
		configFile := filepath.Join(tempDir, "multigres.yaml")
		err := os.WriteFile(configFile, []byte(configContent), 0o644)
		require.NoError(t, err)

		// Test the function
		address, err := getAdminServerFromConfig([]string{tempDir})
		require.NoError(t, err)
		assert.Equal(t, "localhost:12345", address)
	})

	t.Run("config file not found", func(t *testing.T) {
		// Test with non-existent directory
		_, err := getAdminServerFromConfig([]string{"/nonexistent/path"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "multigres.yaml not found")
	})

	t.Run("invalid YAML", func(t *testing.T) {
		// Create invalid YAML file
		configFile := filepath.Join(tempDir, "multigres.yaml")
		err := os.WriteFile(configFile, []byte("invalid: yaml: content: ["), 0o644)
		require.NoError(t, err)

		_, err = getAdminServerFromConfig([]string{tempDir})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to unmarshal config")
	})

	t.Run("unsupported provisioner", func(t *testing.T) {
		// Create config with unsupported provisioner
		configContent := `provisioner: unsupported
provisioner-config: {}
`
		configFile := filepath.Join(tempDir, "multigres.yaml")
		err := os.WriteFile(configFile, []byte(configContent), 0o644)
		require.NoError(t, err)

		_, err = getAdminServerFromConfig([]string{tempDir})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported provisioner: unsupported")
	})
}

func TestGetAdminServerAddress(t *testing.T) {
	// Create a temporary directory for test configs
	tempDir, err := os.MkdirTemp("", "multigres_test_")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a test config file
	configContent := `provisioner: local
provisioner-config:
  multiadmin:
    grpc-port: 12345
    http-port: 12346
`
	configFile := filepath.Join(tempDir, "multigres.yaml")
	err = os.WriteFile(configFile, []byte(configContent), 0o644)
	require.NoError(t, err)

	t.Run("admin-server flag takes precedence", func(t *testing.T) {
		// Create a mock command with both flags set
		cmd := &cobra.Command{}
		cmd.Flags().String("admin-server", "", "")
		cmd.Flags().StringSlice("config-path", []string{}, "")

		// Set both flags
		require.NoError(t, cmd.Flags().Set("admin-server", "custom:9999"))
		require.NoError(t, cmd.Flags().Set("config-path", tempDir))

		address, err := getAdminServerAddress(cmd)
		require.NoError(t, err)
		assert.Equal(t, "custom:9999", address)
	})

	t.Run("falls back to config path", func(t *testing.T) {
		// Create a mock command with only config-path set
		cmd := &cobra.Command{}
		cmd.Flags().String("admin-server", "", "")
		cmd.Flags().StringSlice("config-path", []string{}, "")

		// Set only config-path
		require.NoError(t, cmd.Flags().Set("config-path", tempDir))

		address, err := getAdminServerAddress(cmd)
		require.NoError(t, err)
		assert.Equal(t, "localhost:12345", address)
	})

	t.Run("error when neither flag is provided", func(t *testing.T) {
		// Create a mock command with no flags set
		cmd := &cobra.Command{}
		cmd.Flags().String("admin-server", "", "")
		cmd.Flags().StringSlice("config-path", []string{}, "")

		_, err := getAdminServerAddress(cmd)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "either --admin-server flag or --config-path must be provided")
	})
}

func TestGetCellCommandFlags(t *testing.T) {
	t.Run("name flag is required", func(t *testing.T) {
		// Check that the name flag is marked as required
		nameFlag := GetCellCommand.Flag("name")
		assert.NotNil(t, nameFlag)

		// Check if the flag is in the required flags list
		annotations := nameFlag.Annotations
		required := false
		if reqAnnotations, exists := annotations[cobra.BashCompOneRequiredFlag]; exists {
			required = len(reqAnnotations) > 0 && reqAnnotations[0] == "true"
		}
		assert.True(t, required, "name flag should be marked as required")
	})

	t.Run("admin-server flag is optional", func(t *testing.T) {
		// Check that the admin-server flag exists but is not required
		adminServerFlag := GetCellCommand.Flag("admin-server")
		assert.NotNil(t, adminServerFlag)
		assert.Equal(t, "", adminServerFlag.DefValue)
	})
}
