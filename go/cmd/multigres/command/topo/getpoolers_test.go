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

package topo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPoolersCommand(t *testing.T) {
	t.Run("command is initialized", func(t *testing.T) {
		require.NotNil(t, GetPoolersCommand)
		assert.Equal(t, "getpoolers", GetPoolersCommand.Use)
		assert.True(t, GetPoolersCommand.Flags().HasAvailableFlags())
		assert.True(t, GetPoolersCommand.Flags().Lookup("admin-server") != nil)
		assert.True(t, GetPoolersCommand.Flags().Lookup("cells") != nil)
		assert.True(t, GetPoolersCommand.Flags().Lookup("database") != nil)
	})

	t.Run("handles database flag", func(t *testing.T) {
		cmd := GetPoolersCommand
		err := cmd.Flags().Set("database", "testdb")
		assert.NoError(t, err)

		databaseFlag, err := cmd.Flags().GetString("database")
		assert.NoError(t, err)
		assert.Equal(t, "testdb", databaseFlag)
	})

	t.Run("admin-server flag is optional", func(t *testing.T) {
		// Check that the admin-server flag exists but is not required
		adminServerFlag := GetPoolersCommand.Flag("admin-server")
		assert.NotNil(t, adminServerFlag)
		assert.Equal(t, "", adminServerFlag.DefValue)
	})

	t.Run("cells flag is optional", func(t *testing.T) {
		// Check that the cells flag exists but is not required
		cellsFlag := GetPoolersCommand.Flag("cells")
		assert.NotNil(t, cellsFlag)
		assert.Equal(t, "", cellsFlag.DefValue)
	})

	t.Run("database flag is optional", func(t *testing.T) {
		// Check that the database flag exists but is not required
		databaseFlag := GetPoolersCommand.Flag("database")
		assert.NotNil(t, databaseFlag)
		assert.Equal(t, "", databaseFlag.DefValue)
	})
}
