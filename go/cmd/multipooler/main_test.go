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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
)

// TestDatabaseEnvVar verifies that the --database flag reads from POSTGRES_DB
// and that an explicit flag value overrides the env var.
func TestDatabaseEnvVar(t *testing.T) {
	t.Run("defaults to postgres when POSTGRES_DB not set", func(t *testing.T) {
		_, mp := CreateMultipoolerCommand()
		assert.Equal(t, constants.DefaultPostgresDatabase, mp.Database())
	})

	t.Run("POSTGRES_DB env var is used when flag not set", func(t *testing.T) {
		t.Setenv(constants.PgDatabaseEnvVar, "mydb")
		_, mp := CreateMultipoolerCommand()
		assert.Equal(t, "mydb", mp.Database())
	})

	t.Run("flag overrides POSTGRES_DB env var", func(t *testing.T) {
		t.Setenv(constants.PgDatabaseEnvVar, "envdb")
		cmd, mp := CreateMultipoolerCommand()
		cmd.SetArgs([]string{"--database", "flagdb"})
		// Parse flags without executing to avoid topo validation
		require.NoError(t, cmd.ParseFlags([]string{"--database", "flagdb"}))
		assert.Equal(t, "flagdb", mp.Database())
	})
}

// TestInit_TopoMissingAddresses verifies that Init() returns an error when
// topo-global-server-addresses is not configured.
func TestInit_TopoMissingAddresses(t *testing.T) {
	cmd, _ := CreateMultipoolerCommand()

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
	cmd, _ := CreateMultipoolerCommand()

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

// TestConfigFileReachesMultipoolerSettings verifies that a configuration file
// sets the multipooler's own settings, not only the process ones: main loads
// the file into the registry its servenv uses, so the multipooler must
// configure its settings on that same registry.
func TestConfigFileReachesMultipoolerSettings(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "multipooler.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte("database: db-from-file\ncell: cell-from-file\n"), 0o600))

	cmd, mp := CreateMultipoolerCommand()
	require.NoError(t, cmd.ParseFlags([]string{"--config-file", configFile, "--table-group", "tg-from-flag"}))
	require.NoError(t, cmd.PreRunE(cmd, nil))

	id := mp.ServiceIdentity()
	assert.Equal(t, "db-from-file", id.Database)
	assert.Equal(t, "cell-from-file", id.Cell)
	assert.Equal(t, "tg-from-flag", id.TableGroup)
}

// The flags that configured multipooler's own pgBackRest were removed with it.
// A multigres-operator that does not deploy the pgbackrestd sidecar still
// passes them, and such a pod has nothing to archive WAL with, so the process
// must refuse to start and name the flag. It must never accept one and go on.
func TestRemovedPgBackRestFlagsRefuseStartup(t *testing.T) {
	for _, arg := range []string{
		"--pgbackrest-cert-file=/certs/pgbackrest.crt",
		"--pgbackrest-key-file=/certs/pgbackrest.key",
		"--pgbackrest-ca-file=/certs/ca.crt",
		"--pgbackrest-port=8432",
		"--pgbackrest-cipher-key-file=/secrets/pgbackrest-cipher/keys.json",
	} {
		t.Run(arg, func(t *testing.T) {
			cmd, _ := CreateMultipoolerCommand()
			// Everything else a pod passes is valid, so only the removed flag can
			// be what stops the start.
			cmd.SetArgs([]string{
				"--cell", "zone1", "--database", "postgres", "--table-group", "default", "--shard", "0-inf",
				"--topo-global-server-addresses", "localhost:2379", "--topo-global-root", "/multigres/global",
				"--pooler-dir", t.TempDir(), arg,
			})
			cmd.SilenceUsage, cmd.SilenceErrors = true, true
			ran := false
			cmd.RunE = func(*cobra.Command, []string) error { ran = true; return nil }

			err := cmd.Execute()

			name, _, _ := strings.Cut(arg, "=")
			require.EqualError(t, err, "flag "+name+" was removed: pgBackRest runs in the pgbackrestd sidecar, and this multipooler needs a multigres-operator that deploys that sidecar and does not pass the flag")
			assert.False(t, ran, "the multipooler must not start")
		})
	}
}

// The cipher key file could also be named by an environment variable, and all
// five settings by a config file key. Neither may be accepted and ignored: a
// pod configured that way to encrypt its repository would start and leave a
// new repository unencrypted.
func TestRemovedPgBackRestSettingsRefuseStartupFromEnvironmentAndConfigFile(t *testing.T) {
	execute := func(t *testing.T, extra ...string) (error, bool) {
		t.Helper()
		cmd, _ := CreateMultipoolerCommand()
		cmd.SetArgs(append([]string{
			"--cell", "zone1", "--database", "postgres", "--table-group", "default", "--shard", "0-inf",
			"--topo-global-server-addresses", "localhost:2379", "--topo-global-root", "/multigres/global",
			"--pooler-dir", t.TempDir(),
		}, extra...))
		cmd.SilenceUsage, cmd.SilenceErrors = true, true
		ran := false
		cmd.RunE = func(*cobra.Command, []string) error { ran = true; return nil }
		return cmd.Execute(), ran
	}

	for _, value := range []string{"/secrets/pgbackrest-cipher/keys.json", ""} {
		t.Run("environment variable set to "+strconv.Quote(value), func(t *testing.T) {
			t.Setenv("MULTIGRES_PGBACKREST_CIPHER_KEY_FILE", value)
			err, ran := execute(t)
			require.EqualError(t, err, "environment variable MULTIGRES_PGBACKREST_CIPHER_KEY_FILE was removed: pgBackRest runs in the pgbackrestd sidecar, which holds the cipher keys, and this multipooler needs a multigres-operator that deploys that sidecar and does not set the variable")
			assert.False(t, ran, "the multipooler must not start")
		})
	}

	for _, key := range []string{
		"pgbackrest-cert-file", "pgbackrest-key-file", "pgbackrest-ca-file", "pgbackrest-port", "pgbackrest-cipher-key-file",
	} {
		t.Run("config file key "+key, func(t *testing.T) {
			configFile := filepath.Join(t.TempDir(), "multipooler.yaml")
			require.NoError(t, os.WriteFile(configFile, []byte("database: postgres\n"+key+": 8432\n"), 0o600))
			err, ran := execute(t, "--config-file", configFile)
			require.ErrorContains(t, err, "config file setting "+key+" was removed: pgBackRest runs in the pgbackrestd sidecar, and this multipooler needs a multigres-operator that deploys that sidecar and does not set it")
			assert.False(t, ran, "the multipooler must not start")
		})
	}

	t.Run("a config file without them loads", func(t *testing.T) {
		configFile := filepath.Join(t.TempDir(), "multipooler.yaml")
		require.NoError(t, os.WriteFile(configFile, []byte("database: postgres\n"), 0o600))
		err, ran := execute(t, "--config-file", configFile)
		require.NoError(t, err)
		assert.True(t, ran)
	})
}
