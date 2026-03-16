// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
)

// TestDatabaseEnvVar verifies that the --database flag reads from POSTGRES_DB
// and that an explicit flag value overrides the env var.
func TestDatabaseEnvVar(t *testing.T) {
	t.Run("defaults to postgres when POSTGRES_DB not set", func(t *testing.T) {
		_, mp := CreateMultiPoolerCommand()
		assert.Equal(t, constants.DefaultPostgresDatabase, mp.Database())
	})

	t.Run("POSTGRES_DB env var is used when flag not set", func(t *testing.T) {
		t.Setenv(constants.PgDatabaseEnvVar, "mydb")
		_, mp := CreateMultiPoolerCommand()
		assert.Equal(t, "mydb", mp.Database())
	})

	t.Run("flag overrides POSTGRES_DB env var", func(t *testing.T) {
		t.Setenv(constants.PgDatabaseEnvVar, "envdb")
		cmd, mp := CreateMultiPoolerCommand()
		cmd.SetArgs([]string{"--database", "flagdb"})
		// Parse flags without executing to avoid topo validation
		require.NoError(t, cmd.ParseFlags([]string{"--database", "flagdb"}))
		assert.Equal(t, "flagdb", mp.Database())
	})
}

// TestInit_TopoMissingAddresses verifies that Init() returns an error when
// topo-global-server-addresses is not configured.
func TestInit_TopoMissingAddresses(t *testing.T) {
	cmd, _ := CreateMultiPoolerCommand()

	cmd.SetArgs([]string{
		"--database", "testdb",
		"--table-group", "default",
		"--config-file-not-found-handling", "ignore",
	})

	err := cmd.Execute()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "topo-global-server-addresses must be configured")
}

// TestInit_TopoMissingRoot verifies that Init() returns an error when
// topo-global-root is not configured.
func TestInit_TopoMissingRoot(t *testing.T) {
	cmd, _ := CreateMultiPoolerCommand()

	cmd.SetArgs([]string{
		"--database", "testdb",
		"--table-group", "default",
		"--topo-global-server-addresses", "localhost:2379",
		"--config-file-not-found-handling", "ignore",
	})

	err := cmd.Execute()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "topo-global-root must be non-empty")
}
