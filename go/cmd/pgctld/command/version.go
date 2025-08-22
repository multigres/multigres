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

package command

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// VersionResult contains the result of getting PostgreSQL version information
type VersionResult struct {
	Version string
	Message string
}

func init() {
	Root.AddCommand(versionCmd)

	// Add version-specific flags for connecting to PostgreSQL server
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show PostgreSQL server version information",
	Long: `Show version information from a running PostgreSQL server.

The version command connects to a running PostgreSQL server and retrieves its
version information using SQL. This is useful for verifying server version,
compatibility checking, and debugging.

Examples:
  # Show version from default server
  pgctld version

  # Show version from specific server
  pgctld version --host localhost --port 5433

  # Use in scripts for compatibility checks
  if pgctld version | grep -q "PostgreSQL 15"; then
    echo "Compatible version found"
  fi`,
	RunE: runVersion,
}

// GetVersionWithResult gets PostgreSQL server version information and returns detailed result information
func GetVersionWithResult(config *PostgresConfig) (*VersionResult, error) {
	result := &VersionResult{}

	// Get server version using the same method as the gRPC service
	version := getServerVersionWithConfig(config)
	if version == "" {
		return nil, fmt.Errorf("failed to get server version - ensure PostgreSQL server is running and accessible")
	}

	result.Version = version
	result.Message = "Version retrieved successfully"
	return result, nil
}

func runVersion(cmd *cobra.Command, args []string) error {
	config := NewPostgresConfigFromViper()

	// No local flag overrides needed - all flags are global now

	result, err := GetVersionWithResult(config)
	if err != nil {
		return err
	}

	// Display version information for CLI users
	fmt.Print(strings.TrimSpace(result.Version))
	fmt.Println()

	return nil
}
