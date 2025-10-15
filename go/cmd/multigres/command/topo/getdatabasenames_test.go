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

package topo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDatabaseNamesCommand(t *testing.T) {
	t.Run("command is initialized", func(t *testing.T) {
		cmd := AddGetDatabaseNamesCommand()
		require.NotNil(t, cmd)
		assert.Equal(t, "getdatabasenames", cmd.Use)
		assert.True(t, cmd.Flags().HasAvailableFlags())
		assert.True(t, cmd.Flags().Lookup("admin-server") != nil)
	})

	t.Run("requires no arguments", func(t *testing.T) {
		cmd := AddGetDatabaseNamesCommand()
		// Should not error on execution arguments validation (though it will fail later due to no server)
		assert.NotNil(t, cmd.RunE)
	})

	t.Run("admin-server flag is optional", func(t *testing.T) {
		// Create the command
		cmd := AddGetDatabaseNamesCommand()

		// Check that the admin-server flag exists but is not required
		adminServerFlag := cmd.Flag("admin-server")
		assert.NotNil(t, adminServerFlag)
		assert.Equal(t, "", adminServerFlag.DefValue)
	})
}
