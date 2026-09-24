// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"bytes"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	migratorpb "github.com/multigres/multigres/go/pb/migrator"
)

// runE executes a command's RunE with the given args, capturing output and
// returning the error. Usage and error printing are silenced so the returned
// error is exactly what RunE produced.
func runE(cmd *cobra.Command, args ...string) error {
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs(args)
	return cmd.Execute()
}

func TestMarkersToSelection(t *testing.T) {
	// "*" -> all_tables; "schema.*" -> a schema object; "schema.table" -> a table object.
	all, objs := markersToSelection([]string{"*", "sales.*", "public.orders"})
	require.True(t, all)
	require.Len(t, objs, 2)
	require.Equal(t, "sales", objs[0].GetSchema())
	require.Equal(t, "public.orders", objs[1].GetTable().GetQualifiedName())

	// No "*": all_tables stays false.
	all, objs = markersToSelection([]string{"public.orders"})
	require.False(t, all)
	require.Len(t, objs, 1)
	require.Equal(t, "public.orders", objs[0].GetTable().GetQualifiedName())

	// Empty input.
	all, objs = markersToSelection(nil)
	require.False(t, all)
	require.Empty(t, objs)
}

// TestCommandConstruction checks that each subcommand constructor wires its use
// string, its flags, and (where applicable) the required --id flag.
func TestCommandConstruction(t *testing.T) {
	tests := []struct {
		name     string
		cmd      *cobra.Command
		use      string
		flags    []string
		required []string
	}{
		{
			name:  "create",
			cmd:   AddCreateMigrationCommand(),
			use:   "create-migration",
			flags: []string{"admin-server", "source-dsn", "target-database", "target-shard", "name", "tables", "copy-data", "skip-schema-copy"},
		},
		{
			name:     "start",
			cmd:      AddStartMigrationCommand(),
			use:      "start-migration",
			flags:    []string{"admin-server", "id"},
			required: []string{"id"},
		},
		{
			name:     "update",
			cmd:      AddUpdateMigrationCommand(),
			use:      "update-migration",
			flags:    []string{"admin-server", "id", "source-dsn", "sequence-margin", "tables"},
			required: []string{"id"},
		},
		{
			name:     "activate",
			cmd:      AddActivateMigrationCommand(),
			use:      "activate-migration",
			flags:    []string{"admin-server", "id"},
			required: []string{"id"},
		},
		{
			name:     "deactivate",
			cmd:      AddDeactivateMigrationCommand(),
			use:      "deactivate-migration",
			flags:    []string{"admin-server", "id"},
			required: []string{"id"},
		},
		{
			name:  "list",
			cmd:   AddListMigrationsCommand(),
			use:   "list-migrations",
			flags: []string{"admin-server"},
		},
		{
			name:     "get",
			cmd:      AddGetMigrationCommand(),
			use:      "get-migration",
			flags:    []string{"admin-server", "id"},
			required: []string{"id"},
		},
		{
			name:     "drop",
			cmd:      AddDropMigrationCommand(),
			use:      "drop-migration",
			flags:    []string{"admin-server", "id", "wait", "wait-timeout", "force"},
			required: []string{"id"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.use, tt.cmd.Use)
			assert.NotEmpty(t, tt.cmd.Short)
			assert.NotNil(t, tt.cmd.RunE)
			for _, f := range tt.flags {
				assert.NotNil(t, tt.cmd.Flags().Lookup(f), "flag %q must be defined", f)
			}
			for _, r := range tt.required {
				ann := tt.cmd.Flags().Lookup(r).Annotations[cobra.BashCompOneRequiredFlag]
				assert.NotEmpty(t, ann, "flag %q must be marked required", r)
			}
		})
	}
}

// TestCommandRequiredFlags checks that omitting a required flag fails before RunE.
func TestCommandRequiredFlags(t *testing.T) {
	for _, cmd := range []*cobra.Command{
		AddStartMigrationCommand(),
		AddUpdateMigrationCommand(),
		AddActivateMigrationCommand(),
		AddDeactivateMigrationCommand(),
		AddGetMigrationCommand(),
		AddDropMigrationCommand(),
	} {
		err := runE(cmd)
		require.Error(t, err, "%s must require a flag", cmd.Use)
		assert.Contains(t, err.Error(), "required flag")
	}
}

// TestCommandValidationErrors covers the RunE argument-validation branches that
// run before any admin client is created.
func TestCommandValidationErrors(t *testing.T) {
	t.Run("update with no fields", func(t *testing.T) {
		err := runE(AddUpdateMigrationCommand(), "--id", "m1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no fields to update")
	})
	t.Run("drop wait and force are mutually exclusive", func(t *testing.T) {
		err := runE(AddDropMigrationCommand(), "--id", "m1", "--wait", "--force")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mutually exclusive")
	})
}

// TestCommandClientError drives each command's RunE past flag parsing to the
// admin-client construction, which fails deterministically offline (no
// --admin-server and no --config-path flag), exercising the flag-reading and
// error-return paths without a live cluster.
func TestCommandClientError(t *testing.T) {
	cases := []struct {
		name string
		cmd  *cobra.Command
		args []string
	}{
		{"create", AddCreateMigrationCommand(), []string{"--tables", "*"}},
		{"start", AddStartMigrationCommand(), []string{"--id", "m1"}},
		{"update", AddUpdateMigrationCommand(), []string{"--id", "m1", "--source-dsn", "host=h"}},
		{"activate", AddActivateMigrationCommand(), []string{"--id", "m1"}},
		{"deactivate", AddDeactivateMigrationCommand(), []string{"--id", "m1"}},
		{"list", AddListMigrationsCommand(), nil},
		{"get", AddGetMigrationCommand(), []string{"--id", "m1"}},
		{"drop", AddDropMigrationCommand(), []string{"--id", "m1", "--wait", "--wait-timeout", "5"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := runE(tc.cmd, tc.args...)
			require.Error(t, err)
			// Address resolution fails because neither --admin-server nor a
			// --config-path is available to the standalone command.
			assert.Contains(t, err.Error(), "config-path")
		})
	}
}

// TestPrintJSON checks the protojson rendering helper writes the message to the
// command's output using proto field names.
func TestPrintJSON(t *testing.T) {
	cmd := &cobra.Command{}
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	err := printJSON(cmd, &migratorpb.Migration{Id: "m1", Name: "nightly"})
	require.NoError(t, err)
	assert.Contains(t, out.String(), `"id": "m1"`)
	assert.Contains(t, out.String(), `"name": "nightly"`)
}
