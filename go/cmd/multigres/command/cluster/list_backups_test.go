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

// getListBackupsCommand creates a cluster command and adds list-backups to it for testing
func getListBackupsCommand() *cobra.Command {
	clusterCmd := &cobra.Command{Use: "cluster"}
	AddListBackupsCommand(clusterCmd)
	cmd, _, _ := clusterCmd.Find([]string{"list-backups"})
	return cmd
}

func TestListBackupsCommandFlags(t *testing.T) {
	t.Run("database flag exists with default", func(t *testing.T) {
		cmd := getListBackupsCommand()
		require.NotNil(t, cmd)

		databaseFlag := cmd.Flag("database")
		assert.NotNil(t, databaseFlag)
		assert.Equal(t, "postgres", databaseFlag.DefValue, "database should default to postgres")
	})

	t.Run("limit flag exists with default", func(t *testing.T) {
		cmd := getListBackupsCommand()
		require.NotNil(t, cmd)

		limitFlag := cmd.Flag("limit")
		assert.NotNil(t, limitFlag)
		assert.Equal(t, "0", limitFlag.DefValue, "limit should default to 0 (no limit)")
	})
}
