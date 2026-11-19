// Copyright 2026 Supabase, Inc.
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

package command

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
)

func TestPgDatabaseEnvVar(t *testing.T) {
	t.Run("defaults to postgres when POSTGRES_DB not set", func(t *testing.T) {
		_, pc := GetRootCommand()
		assert.Equal(t, constants.DefaultPostgresDatabase, pc.pgDatabase.Get())
	})

	t.Run("POSTGRES_DB env var is used when flag not set", func(t *testing.T) {
		t.Setenv(constants.PgDatabaseEnvVar, "mydb")
		_, pc := GetRootCommand()
		assert.Equal(t, "mydb", pc.pgDatabase.Get())
	})

	t.Run("flag overrides POSTGRES_DB env var", func(t *testing.T) {
		t.Setenv(constants.PgDatabaseEnvVar, "envdb")
		_, pc := GetRootCommand()
		pc.pgDatabase.Set("flagdb")
		assert.Equal(t, "flagdb", pc.pgDatabase.Get())
	})
}

func TestPgInitdbArgsEnvVar(t *testing.T) {
	t.Run("defaults to empty when POSTGRES_INITDB_ARGS not set", func(t *testing.T) {
		_, pc := GetRootCommand()
		assert.Equal(t, "", pc.pgInitdbArgs.Get())
	})

	t.Run("POSTGRES_INITDB_ARGS env var is used when flag not set", func(t *testing.T) {
		t.Setenv(constants.PgInitdbArgsEnvVar, "--locale-provider=icu")
		_, pc := GetRootCommand()
		assert.Equal(t, "--locale-provider=icu", pc.pgInitdbArgs.Get())
	})

	t.Run("flag overrides POSTGRES_INITDB_ARGS env var", func(t *testing.T) {
		t.Setenv(constants.PgInitdbArgsEnvVar, "--locale-provider=icu")
		_, pc := GetRootCommand()
		pc.pgInitdbArgs.Set("--encoding=UTF-8")
		assert.Equal(t, "--encoding=UTF-8", pc.pgInitdbArgs.Get())
	})
}

// TestBuildServiceConfigPropagatesInitdbArgs pins the runInit/runServer
// shared path that forwards pgInitdbArgs into PgCtldServiceConfig.InitdbArgs.
// Earlier this assignment was missing from the runServer path, so the
// --pg-initdb-args flag (and POSTGRES_INITDB_ARGS env var) was silently
// dropped when pgctld bootstrapped via the gRPC InitDataDir RPC.
func TestBuildServiceConfigPropagatesInitdbArgs(t *testing.T) {
	t.Setenv(constants.PgPasswordEnvVar, "test-pw")

	t.Run("InitdbArgs from flag flows into config", func(t *testing.T) {
		_, pc := GetRootCommand()
		pc.pgInitdbArgs.Set("--no-locale --encoding=UTF8")
		cfg, err := pc.buildServiceConfig()
		require.NoError(t, err)
		assert.Equal(t, "--no-locale --encoding=UTF8", cfg.InitdbArgs)
	})

	t.Run("InitdbArgs from env flows into config", func(t *testing.T) {
		t.Setenv(constants.PgInitdbArgsEnvVar, "--locale-provider=icu")
		_, pc := GetRootCommand()
		cfg, err := pc.buildServiceConfig()
		require.NoError(t, err)
		assert.Equal(t, "--locale-provider=icu", cfg.InitdbArgs)
	})

	t.Run("empty InitdbArgs produces empty config field", func(t *testing.T) {
		_, pc := GetRootCommand()
		cfg, err := pc.buildServiceConfig()
		require.NoError(t, err)
		assert.Equal(t, "", cfg.InitdbArgs)
	})

	t.Run("password resolution error propagates", func(t *testing.T) {
		t.Setenv(constants.PgPasswordEnvVar, "")
		_, pc := GetRootCommand()
		_, err := pc.buildServiceConfig()
		assert.Error(t, err)
	})
}

func TestPgInitdbSQLFilesFlag(t *testing.T) {
	t.Run("defaults to empty slice", func(t *testing.T) {
		_, pc := GetRootCommand()
		assert.Empty(t, pc.pgInitdbSQLFiles.Get())
	})

	t.Run("accepts repeated flag", func(t *testing.T) {
		root, pc := GetRootCommand()
		require.NoError(t, root.ParseFlags([]string{
			"--pg-initdb-sql-files", "/tmp/a.sql",
			"--pg-initdb-sql-files", "/tmp/b.sql",
		}))
		assert.Equal(t, []string{"/tmp/a.sql", "/tmp/b.sql"}, pc.pgInitdbSQLFiles.Get())
	})

	t.Run("accepts comma-separated values", func(t *testing.T) {
		root, pc := GetRootCommand()
		require.NoError(t, root.ParseFlags([]string{
			"--pg-initdb-sql-files", "/tmp/a.sql,/tmp/b.sql",
		}))
		assert.Equal(t, []string{"/tmp/a.sql", "/tmp/b.sql"}, pc.pgInitdbSQLFiles.Get())
	})

	t.Run("POSTGRES_INITDB_SQL_FILES env var is used when flag not set", func(t *testing.T) {
		t.Setenv(constants.PgInitdbSQLFilesEnvVar, "/tmp/a.sql")
		_, pc := GetRootCommand()
		assert.Equal(t, []string{"/tmp/a.sql"}, pc.pgInitdbSQLFiles.Get())
	})

	t.Run("flag overrides POSTGRES_INITDB_SQL_FILES env var", func(t *testing.T) {
		t.Setenv(constants.PgInitdbSQLFilesEnvVar, "/tmp/env.sql")
		root, pc := GetRootCommand()
		require.NoError(t, root.ParseFlags([]string{"--pg-initdb-sql-files", "/tmp/flag.sql"}))
		assert.Equal(t, []string{"/tmp/flag.sql"}, pc.pgInitdbSQLFiles.Get())
	})

	t.Run("legacy --init-db-sql-file flag aliases to --pg-initdb-sql-files", func(t *testing.T) {
		root, pc := GetRootCommand()
		require.NoError(t, root.ParseFlags([]string{
			"--init-db-sql-file", "/tmp/legacy-a.sql",
			"--init-db-sql-file", "/tmp/legacy-b.sql",
		}))
		assert.Equal(t, []string{"/tmp/legacy-a.sql", "/tmp/legacy-b.sql"}, pc.pgInitdbSQLFiles.Get())
	})
}

func TestPgInitdbExtraConfFlag(t *testing.T) {
	t.Run("defaults to empty slice", func(t *testing.T) {
		_, pc := GetRootCommand()
		assert.Empty(t, pc.pgInitdbExtraConf.Get())
	})

	t.Run("accepts repeated flag", func(t *testing.T) {
		root, pc := GetRootCommand()
		require.NoError(t, root.ParseFlags([]string{
			"--pg-initdb-extra-conf", "/etc/pg/a.conf",
			"--pg-initdb-extra-conf", "/etc/pg/b.conf",
		}))
		assert.Equal(t, []string{"/etc/pg/a.conf", "/etc/pg/b.conf"}, pc.pgInitdbExtraConf.Get())
	})

	t.Run("accepts comma-separated values", func(t *testing.T) {
		root, pc := GetRootCommand()
		require.NoError(t, root.ParseFlags([]string{
			"--pg-initdb-extra-conf", "/etc/pg/a.conf,/etc/pg/b.conf",
		}))
		assert.Equal(t, []string{"/etc/pg/a.conf", "/etc/pg/b.conf"}, pc.pgInitdbExtraConf.Get())
	})

	t.Run("POSTGRES_INITDB_EXTRA_CONF env var is used when flag not set", func(t *testing.T) {
		t.Setenv(constants.PgInitdbExtraConfEnvVar, "/etc/pg/env.conf")
		_, pc := GetRootCommand()
		assert.Equal(t, []string{"/etc/pg/env.conf"}, pc.pgInitdbExtraConf.Get())
	})

	t.Run("flag overrides POSTGRES_INITDB_EXTRA_CONF env var", func(t *testing.T) {
		t.Setenv(constants.PgInitdbExtraConfEnvVar, "/etc/pg/env.conf")
		root, pc := GetRootCommand()
		require.NoError(t, root.ParseFlags([]string{"--pg-initdb-extra-conf", "/etc/pg/flag.conf"}))
		assert.Equal(t, []string{"/etc/pg/flag.conf"}, pc.pgInitdbExtraConf.Get())
	})
}

func TestPgInitdbSQLDirsFlag(t *testing.T) {
	t.Run("defaults to empty slice", func(t *testing.T) {
		_, pc := GetRootCommand()
		assert.Empty(t, pc.pgInitdbSQLDirs.Get())
	})

	t.Run("accepts repeated flag", func(t *testing.T) {
		root, pc := GetRootCommand()
		require.NoError(t, root.ParseFlags([]string{
			"--pg-initdb-sql-dirs", "postgres:/tmp/init-scripts",
			"--pg-initdb-sql-dirs", "multigres:/tmp/migrations",
		}))
		assert.Equal(t, []string{"postgres:/tmp/init-scripts", "multigres:/tmp/migrations"}, pc.pgInitdbSQLDirs.Get())
	})

	t.Run("accepts comma-separated values", func(t *testing.T) {
		root, pc := GetRootCommand()
		require.NoError(t, root.ParseFlags([]string{
			"--pg-initdb-sql-dirs", "postgres:/tmp/init-scripts,multigres:/tmp/migrations",
		}))
		assert.Equal(t, []string{"postgres:/tmp/init-scripts", "multigres:/tmp/migrations"}, pc.pgInitdbSQLDirs.Get())
	})

	t.Run("POSTGRES_INITDB_SQL_DIRS env var is used when flag not set", func(t *testing.T) {
		t.Setenv(constants.PgInitdbSQLDirsEnvVar, "postgres:/tmp/init-scripts")
		_, pc := GetRootCommand()
		assert.Equal(t, []string{"postgres:/tmp/init-scripts"}, pc.pgInitdbSQLDirs.Get())
	})

	t.Run("flag overrides POSTGRES_INITDB_SQL_DIRS env var", func(t *testing.T) {
		t.Setenv(constants.PgInitdbSQLDirsEnvVar, "postgres:/tmp/env-dir")
		root, pc := GetRootCommand()
		require.NoError(t, root.ParseFlags([]string{"--pg-initdb-sql-dirs", "postgres:/tmp/flag-dir"}))
		assert.Equal(t, []string{"postgres:/tmp/flag-dir"}, pc.pgInitdbSQLDirs.Get())
	})
}

// The flags that made pgctld run the pgBackRest TLS server were removed with
// it. A multigres-operator that does not deploy the pgbackrestd sidecar still
// passes them, so `pgctld server` must refuse to start and name the flag, and
// never accept one and go on without a server.
func TestRemovedPgBackRestFlagsRefuseStartup(t *testing.T) {
	for _, arg := range []string{
		"--pgbackrest-port=8432",
		"--pgbackrest-cert-dir=/certs",
	} {
		t.Run(arg, func(t *testing.T) {
			root, _ := GetRootCommand()
			root.SetArgs([]string{"server", "--pooler-dir", t.TempDir(), "--pg-port", "5432", arg})
			root.SilenceErrors = true
			ran := false
			for _, sub := range root.Commands() {
				if sub.Name() == "server" {
					sub.RunE = func(*cobra.Command, []string) error { ran = true; return nil }
				}
			}

			err := root.Execute()

			name, _, _ := strings.Cut(arg, "=")
			require.EqualError(t, err, "flag "+name+" was removed: pgBackRest runs in the pgbackrestd sidecar, and this pgctld needs a multigres-operator that deploys that sidecar and does not pass the flag")
			assert.False(t, ran, "the server must not start")
		})
	}
}

// The two settings were config file keys as well. A config file that sets one
// stops the server the same way the flag does.
func TestRemovedPgBackRestSettingsRefuseStartupFromConfigFile(t *testing.T) {
	for _, key := range []string{"pgbackrest-port", "pgbackrest-cert-dir"} {
		t.Run(key, func(t *testing.T) {
			configFile := filepath.Join(t.TempDir(), "pgctld.yaml")
			require.NoError(t, os.WriteFile(configFile, []byte(key+": 8432\n"), 0o600))
			root, _ := GetRootCommand()
			root.SetArgs([]string{"server", "--pooler-dir", t.TempDir(), "--pg-port", "5432", "--config-file", configFile})
			root.SilenceErrors = true
			ran := false
			for _, sub := range root.Commands() {
				if sub.Name() == "server" {
					sub.RunE = func(*cobra.Command, []string) error { ran = true; return nil }
				}
			}

			err := root.Execute()

			require.ErrorContains(t, err, "config file setting "+key+" was removed: pgBackRest runs in the pgbackrestd sidecar, and this pgctld needs a multigres-operator that deploys that sidecar and does not set it")
			assert.False(t, ran, "the server must not start")
		})
	}
}
