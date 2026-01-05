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

package cluster

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getBackupCommand creates a cluster command and adds backup to it for testing
func getBackupCommand() *cobra.Command {
	clusterCmd := &cobra.Command{Use: "cluster"}
	AddBackupCommand(clusterCmd)
	cmd, _, _ := clusterCmd.Find([]string{"backup"})
	return cmd
}

func TestBackupCommandFlags(t *testing.T) {
	t.Run("database flag exists with default", func(t *testing.T) {
		cmd := getBackupCommand()
		require.NotNil(t, cmd)

		databaseFlag := cmd.Flag("database")
		assert.NotNil(t, databaseFlag)
		assert.Equal(t, "postgres", databaseFlag.DefValue)
	})
}

func TestBackupCommandFlags_Primary(t *testing.T) {
	cmd := getBackupCommand()
	require.NotNil(t, cmd)

	primaryFlag := cmd.Flag("primary")
	assert.NotNil(t, primaryFlag, "primary flag should exist")
	assert.Equal(t, "false", primaryFlag.DefValue, "primary flag should default to false")
}

func TestBackupCommandFlags_AdminServer(t *testing.T) {
	cmd := getBackupCommand()
	require.NotNil(t, cmd)

	adminServerFlag := cmd.Flag("admin-server")
	assert.NotNil(t, adminServerFlag, "admin-server flag should exist")
	assert.Equal(t, "", adminServerFlag.DefValue, "admin-server flag should default to empty string")
}
