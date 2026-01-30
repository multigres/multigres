// Copyright 2026 Supabase, Inc.
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

package command

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/config"
	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
)

func TestPgHbaTemplate(t *testing.T) {
	// Save the original template so we can restore it after tests
	originalTemplate := config.PostgresHbaDefaultTmpl
	defer func() {
		config.PostgresHbaDefaultTmpl = originalTemplate
	}()

	t.Run("custom template replaces default", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_hba_test")
		defer cleanup()

		// Create a custom pg_hba.conf template file
		customTemplate := `# Custom pg_hba.conf template for testing
local   all             all                                     trust
host    all             all             127.0.0.1/32            scram-sha-256
host    all             all             ::1/128                 scram-sha-256
`
		templatePath := filepath.Join(baseDir, "custom_pg_hba.conf")
		err := os.WriteFile(templatePath, []byte(customTemplate), 0o644)
		require.NoError(t, err)

		// Reset template before test
		config.PostgresHbaDefaultTmpl = originalTemplate

		// Create command and test validateGlobalFlags directly
		_, pc := GetRootCommand()
		pc.poolerDir.Set(baseDir)
		pc.pgHbaTemplate.Set(templatePath)

		err = pc.validateGlobalFlags(nil, nil)
		require.NoError(t, err)

		// Verify the template was replaced
		assert.Equal(t, customTemplate, config.PostgresHbaDefaultTmpl)
		assert.NotEqual(t, originalTemplate, config.PostgresHbaDefaultTmpl)
	})

	t.Run("non-existent template file returns error", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_hba_test")
		defer cleanup()

		nonExistentPath := filepath.Join(baseDir, "nonexistent.conf")

		// Reset template before test
		config.PostgresHbaDefaultTmpl = originalTemplate

		// Create command and test validateGlobalFlags directly
		_, pc := GetRootCommand()
		pc.poolerDir.Set(baseDir)
		pc.pgHbaTemplate.Set(nonExistentPath)

		err := pc.validateGlobalFlags(nil, nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read pg-hba-template file")
		assert.Contains(t, err.Error(), nonExistentPath)

		// Verify template was NOT changed
		assert.Equal(t, originalTemplate, config.PostgresHbaDefaultTmpl)
	})

	t.Run("empty template flag does not replace default", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_hba_test")
		defer cleanup()

		// Reset template before test
		config.PostgresHbaDefaultTmpl = originalTemplate

		// Create command and test validateGlobalFlags directly without setting pgHbaTemplate
		_, pc := GetRootCommand()
		pc.poolerDir.Set(baseDir)
		// Don't set pgHbaTemplate - it should remain empty

		err := pc.validateGlobalFlags(nil, nil)
		require.NoError(t, err)

		// Verify template was NOT changed
		assert.Equal(t, originalTemplate, config.PostgresHbaDefaultTmpl)
	})

	t.Run("template with empty content is accepted", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_hba_test")
		defer cleanup()

		// Create an empty template file
		emptyTemplate := ""
		templatePath := filepath.Join(baseDir, "empty_pg_hba.conf")
		err := os.WriteFile(templatePath, []byte(emptyTemplate), 0o644)
		require.NoError(t, err)

		// Reset template before test
		config.PostgresHbaDefaultTmpl = originalTemplate

		// Create command and test validateGlobalFlags directly
		_, pc := GetRootCommand()
		pc.poolerDir.Set(baseDir)
		pc.pgHbaTemplate.Set(templatePath)

		err = pc.validateGlobalFlags(nil, nil)
		require.NoError(t, err)

		// Verify the template was replaced with empty content
		assert.Equal(t, "", config.PostgresHbaDefaultTmpl)
	})

	t.Run("unreadable template file returns error", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_hba_test")
		defer cleanup()

		// Create a template file with no read permissions
		templatePath := filepath.Join(baseDir, "unreadable_pg_hba.conf")
		err := os.WriteFile(templatePath, []byte("test content"), 0o000)
		require.NoError(t, err)
		defer func() {
			_ = os.Chmod(templatePath, 0o644) // Cleanup - ignore error as file may be deleted by cleanup
		}()

		// Reset template before test
		config.PostgresHbaDefaultTmpl = originalTemplate

		// Create command and test validateGlobalFlags directly
		_, pc := GetRootCommand()
		pc.poolerDir.Set(baseDir)
		pc.pgHbaTemplate.Set(templatePath)

		err = pc.validateGlobalFlags(nil, nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read pg-hba-template file")

		// Verify template was NOT changed
		assert.Equal(t, originalTemplate, config.PostgresHbaDefaultTmpl)
	})
}

func TestPostgresConfigTemplate(t *testing.T) {
	// Save the original template so we can restore it after tests
	originalTemplate := config.PostgresConfigDefaultTmpl
	defer func() {
		config.PostgresConfigDefaultTmpl = originalTemplate
	}()

	t.Run("custom template replaces default", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_pg_config_test")
		defer cleanup()

		// Create a custom postgresql.conf template file
		customTemplate := `# Custom postgresql.conf template for testing
max_connections = {{.MaxConnections}}
shared_buffers = {{.SharedBuffers}}
work_mem = {{.WorkMem}}
`
		templatePath := filepath.Join(baseDir, "custom_postgresql.conf")
		err := os.WriteFile(templatePath, []byte(customTemplate), 0o644)
		require.NoError(t, err)

		// Reset template before test
		config.PostgresConfigDefaultTmpl = originalTemplate

		// Create command and test validateGlobalFlags directly
		_, pc := GetRootCommand()
		pc.poolerDir.Set(baseDir)
		pc.postgresConfigTmpl.Set(templatePath)

		err = pc.validateGlobalFlags(nil, nil)
		require.NoError(t, err)

		// Verify the template was replaced
		assert.Equal(t, customTemplate, config.PostgresConfigDefaultTmpl)
		assert.NotEqual(t, originalTemplate, config.PostgresConfigDefaultTmpl)
	})

	t.Run("non-existent template file returns error", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_pg_config_test")
		defer cleanup()

		nonExistentPath := filepath.Join(baseDir, "nonexistent.conf")

		// Reset template before test
		config.PostgresConfigDefaultTmpl = originalTemplate

		// Create command and test validateGlobalFlags directly
		_, pc := GetRootCommand()
		pc.poolerDir.Set(baseDir)
		pc.postgresConfigTmpl.Set(nonExistentPath)

		err := pc.validateGlobalFlags(nil, nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read postgres-config-template file")
		assert.Contains(t, err.Error(), nonExistentPath)

		// Verify template was NOT changed
		assert.Equal(t, originalTemplate, config.PostgresConfigDefaultTmpl)
	})

	t.Run("empty template flag does not replace default", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_pg_config_test")
		defer cleanup()

		// Reset template before test
		config.PostgresConfigDefaultTmpl = originalTemplate

		// Create command and test validateGlobalFlags directly without setting postgresConfigTmpl
		_, pc := GetRootCommand()
		pc.poolerDir.Set(baseDir)
		// Don't set postgresConfigTmpl - it should remain empty

		err := pc.validateGlobalFlags(nil, nil)
		require.NoError(t, err)

		// Verify template was NOT changed
		assert.Equal(t, originalTemplate, config.PostgresConfigDefaultTmpl)
	})
}
