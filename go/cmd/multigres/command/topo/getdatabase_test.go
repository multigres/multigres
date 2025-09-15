// Copyright 2025 The Supabase, Inc.
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

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestGetDatabaseCommandFlags(t *testing.T) {
	t.Run("name flag is required", func(t *testing.T) {
		// Check that the name flag is marked as required
		nameFlag := GetDatabaseCommand.Flag("name")
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
		adminServerFlag := GetDatabaseCommand.Flag("admin-server")
		assert.NotNil(t, adminServerFlag)
		assert.Equal(t, "", adminServerFlag.DefValue)
	})

	t.Run("command has correct metadata", func(t *testing.T) {
		assert.Equal(t, "getdatabase", GetDatabaseCommand.Use)
		assert.Equal(t, "Get information about a specific database", GetDatabaseCommand.Short)
		assert.Contains(t, GetDatabaseCommand.Long, "Retrieve detailed information about a database")
	})
}
