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

func TestGetGatewaysCommand(t *testing.T) {
	t.Run("command is initialized", func(t *testing.T) {
		require.NotNil(t, GetGatewaysCommand)
		assert.Equal(t, "getgateways", GetGatewaysCommand.Use)
		assert.True(t, GetGatewaysCommand.Flags().HasAvailableFlags())
		assert.True(t, GetGatewaysCommand.Flags().Lookup("admin-server") != nil)
		assert.True(t, GetGatewaysCommand.Flags().Lookup("cells") != nil)
	})

	t.Run("handles cells flag parsing", func(t *testing.T) {
		cmd := GetGatewaysCommand
		err := cmd.Flags().Set("cells", "cell1,cell2,cell3")
		assert.NoError(t, err)

		cellsFlag, err := cmd.Flags().GetString("cells")
		assert.NoError(t, err)
		assert.Equal(t, "cell1,cell2,cell3", cellsFlag)
	})

	t.Run("admin-server flag is optional", func(t *testing.T) {
		// Check that the admin-server flag exists but is not required
		adminServerFlag := GetGatewaysCommand.Flag("admin-server")
		assert.NotNil(t, adminServerFlag)
		assert.Equal(t, "", adminServerFlag.DefValue)
	})

	t.Run("cells flag is optional", func(t *testing.T) {
		// Check that the cells flag exists but is not required
		cellsFlag := GetGatewaysCommand.Flag("cells")
		assert.NotNil(t, cellsFlag)
		assert.Equal(t, "", cellsFlag.DefValue)
	})
}
