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
	"os"
	"testing"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// SetupTestPgCtldCleanup provides cleanup for pgctld tests that use cobra commands directly
// This automatically captures all global flag variables and provides comprehensive cleanup
func SetupTestPgCtldCleanup(t *testing.T) func() {
	t.Helper()

	// Store original command line args and flag state
	originalArgs := os.Args
	originalCommandLine := pflag.CommandLine

	// Store original global flag values
	originalPgDataDir := pgDataDir
	originalPgPort := pgPort
	originalPgHost := pgHost
	originalPgDatabase := pgDatabase
	originalPgUser := pgUser
	originalPgPassword := pgPassword
	originalPgConfigFile := pgConfigFile
	originalPgSocketDir := pgSocketDir
	originalTimeout := timeout

	// Return cleanup function
	return func() {
		os.Args = originalArgs
		pflag.CommandLine = originalCommandLine
		// Reset viper to clean state
		viper.Reset()

		// Reset global flag variables
		pgDataDir = originalPgDataDir
		pgPort = originalPgPort
		pgHost = originalPgHost
		pgDatabase = originalPgDatabase
		pgUser = originalPgUser
		pgPassword = originalPgPassword
		pgConfigFile = originalPgConfigFile
		pgSocketDir = originalPgSocketDir
		timeout = originalTimeout
	}
}
