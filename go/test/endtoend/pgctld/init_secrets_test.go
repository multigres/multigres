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

package pgctld

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

// assertNoLogLeak fails if any of the given secret values appears in any *.log
// file under poolerDir (notably the transient server's pg_data/setup.log).
// Setting or modifying a password over psql must never write the plaintext to a
// PostgreSQL server log, even though the generated config sets
// log_statement = 'ddl'. Any test that seeds or changes a password must call it.
func assertNoLogLeak(t *testing.T, poolerDir string, secrets ...string) {
	t.Helper()
	scanned := 0
	err := filepath.WalkDir(poolerDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(path, ".log") {
			return nil
		}
		data, rerr := os.ReadFile(path) //nolint:gosec // path is from WalkDir over a test-owned temp dir
		if rerr != nil {
			return rerr
		}
		scanned++
		for _, s := range secrets {
			assert.NotContainsf(t, string(data), s, "plaintext password leaked into log file %s", path)
		}
		return nil
	})
	require.NoError(t, err)
	require.Positive(t, scanned, "expected at least one .log file under %s to scan for leaks", poolerDir)
}

// runPsqlAs connects to the running instance over its Unix socket as user with
// password and runs a single SQL statement, returning trimmed output. Unlike
// setupTestEnv it sets PGPASSWORD to the given password so a seeded (non-super)
// role can be exercised.
func runPsqlAs(t *testing.T, poolerDir string, port int, user, password, sql string) (string, error) {
	t.Helper()
	socketDir := filepath.Join(poolerDir, "pg_sockets")
	cmd := executil.Command(t.Context(), "psql",
		"-h", socketDir,
		"-p", strconv.Itoa(port),
		"-U", user,
		"-d", "postgres",
		"-Atc", sql,
	)
	env := append(utils.BaseTestEnv(),
		"PGCONNECT_TIMEOUT=5",
		constants.PgDataDirEnvVar+"="+filepath.Join(poolerDir, "pg_data"),
		"PGPASSWORD="+password,
	)
	if runtime.GOOS == "darwin" {
		env = append(env, "LC_ALL=en_US.UTF-8")
	}
	cmd.SetEnv(env)
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// TestInitSecrets_SeedsRolePasswordsAndSettings verifies the happy path: a
// login role created without a password by the init SQL is seeded from
// --pg-init-secrets-file, can then authenticate over SCRAM, and a day-0
// database setting from the same file is applied.
func TestInitSecrets_SeedsRolePasswordsAndSettings(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL binaries not found, skipping real PostgreSQL test")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_init_secrets_test")
	t.Cleanup(cleanup)
	poolerDir := filepath.Join(tempDir, "data")

	// init-scripts create a LOGIN role with no password, mirroring how the image
	// creates service roles (e.g. authenticator) that are seeded separately.
	scriptsDir := filepath.Join(tempDir, "init-scripts")
	require.NoError(t, os.MkdirAll(scriptsDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(scriptsDir, "00-roles.sql"),
		[]byte("CREATE ROLE authenticator LOGIN NOINHERIT;\n"), 0o644))

	// Distinctive token so the log-leak scan is unambiguous.
	const authPassword = "authpw_D0NOTLOG_a1b2c3"
	secretsFile := filepath.Join(tempDir, "init-secrets.json")
	require.NoError(t, os.WriteFile(secretsFile, []byte(
		`{"roles": {"authenticator": "`+authPassword+`"}, `+
			`"database_settings": {"postgres": {"app.settings.jwt_exp": "3600"}}}`), 0o600))

	port := utils.GetFreePort(t)
	initCmd := executil.Command(t.Context(), "pgctld", "init",
		"--pooler-dir", poolerDir,
		"--pg-port", strconv.Itoa(port),
		"--pg-initdb-sql-dirs", "postgres:"+scriptsDir,
		"--pg-init-secrets-file", secretsFile,
	)
	setupTestEnv(initCmd, poolerDir)
	initOut, err := initCmd.CombinedOutput()
	require.NoError(t, err, "pgctld init failed: %s", string(initOut))

	startCmd := executil.Command(t.Context(), "pgctld", "start",
		"--pooler-dir", poolerDir, "--pg-port", strconv.Itoa(port))
	setupTestEnv(startCmd, poolerDir)
	startOut, err := startCmd.CombinedOutput()
	require.NoError(t, err, "pgctld start failed: %s", string(startOut))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		stopCmd := executil.Command(ctx, "pgctld", "stop",
			"--pooler-dir", poolerDir, "--pg-port", strconv.Itoa(port))
		setupTestEnv(stopCmd, poolerDir)
		_ = stopCmd.Run()
	})

	// 1. The seeded role can authenticate over SCRAM with its password.
	out, err := runPsqlAs(t, poolerDir, port, "authenticator", authPassword, "SELECT 1")
	require.NoError(t, err, "authenticator login should succeed: %s", out)
	assert.Equal(t, "1", out)

	// 2. Wrong password is rejected — proves the password is actually enforced.
	_, err = runPsqlAs(t, poolerDir, port, "authenticator", "wrongpw", "SELECT 1")
	require.Error(t, err, "authenticator login with wrong password should fail")

	// 3. pg_authid confirms the role now has a password.
	out, err = runPsqlAs(t, poolerDir, port, "postgres", "test-password",
		"SELECT rolpassword IS NOT NULL FROM pg_authid WHERE rolname = 'authenticator'")
	require.NoError(t, err, "pg_authid query failed: %s", out)
	assert.Equal(t, "t", out)

	// 4. The day-0 database setting from the same file was applied.
	out, err = runPsqlAs(t, poolerDir, port, "postgres", "test-password", `SHOW "app.settings.jwt_exp"`)
	require.NoError(t, err, "SHOW app.settings.jwt_exp failed: %s", out)
	assert.Equal(t, "3600", out)

	// 5. The plaintext password must not have leaked into any server log
	// (the generated config sets log_statement = 'ddl').
	assertNoLogLeak(t, poolerDir, authPassword)
}

// TestInitSecrets_FailsWhenLoginRoleLeftPasswordless verifies the drift-catch:
// if the init SQL creates a LOGIN role that the secrets file does not seed, the
// post-apply pg_authid check fails init rather than letting the cluster come up
// with a passwordless login role.
func TestInitSecrets_FailsWhenLoginRoleLeftPasswordless(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL binaries not found, skipping real PostgreSQL test")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_init_secrets_fail_test")
	t.Cleanup(cleanup)
	poolerDir := filepath.Join(tempDir, "data")

	// Two LOGIN roles created; the secrets file seeds only one.
	scriptsDir := filepath.Join(tempDir, "init-scripts")
	require.NoError(t, os.MkdirAll(scriptsDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(scriptsDir, "00-roles.sql"),
		[]byte("CREATE ROLE role_a LOGIN;\nCREATE ROLE role_b LOGIN;\n"), 0o644))

	// Distinctive tokens so the log-leak scan is unambiguous.
	const pwA = "pw_a_D0NOTLOG_11aa"
	const pwB = "pw_b_D0NOTLOG_22bb"
	secretsFile := filepath.Join(tempDir, "init-secrets.json")
	require.NoError(t, os.WriteFile(secretsFile, []byte(`{"roles": {"role_a": "`+pwA+`"}}`), 0o600))

	port := utils.GetFreePort(t)
	initCmd := executil.Command(t.Context(), "pgctld", "init",
		"--pooler-dir", poolerDir,
		"--pg-port", strconv.Itoa(port),
		"--pg-initdb-sql-dirs", "postgres:"+scriptsDir,
		"--pg-init-secrets-file", secretsFile,
	)
	setupTestEnv(initCmd, poolerDir)
	initOut, err := initCmd.CombinedOutput()
	require.Error(t, err, "init should fail when a login role is left passwordless; output: %s", string(initOut))
	assert.Contains(t, string(initOut), "without a password")
	assert.Contains(t, string(initOut), "role_b")

	// Retry-safety: the failed init must have rolled back the data directory so
	// it does not read as initialized. Otherwise a retry would no-op and start
	// would bring up a cluster with passwordless roles.
	_, statErr := os.Stat(filepath.Join(poolerDir, "pg_data", "PG_VERSION"))
	require.True(t, os.IsNotExist(statErr),
		"data dir should be rolled back after failed init (PG_VERSION should not exist), stat err: %v", statErr)

	// A corrected retry (now seeding both roles) succeeds against the same dir,
	// proving the rollback left a clean slate.
	require.NoError(t, os.WriteFile(secretsFile,
		[]byte(`{"roles": {"role_a": "`+pwA+`", "role_b": "`+pwB+`"}}`), 0o600))
	retryCmd := executil.Command(t.Context(), "pgctld", "init",
		"--pooler-dir", poolerDir,
		"--pg-port", strconv.Itoa(port),
		"--pg-initdb-sql-dirs", "postgres:"+scriptsDir,
		"--pg-init-secrets-file", secretsFile,
	)
	setupTestEnv(retryCmd, poolerDir)
	retryOut, err := retryCmd.CombinedOutput()
	require.NoError(t, err, "corrected retry should succeed after rollback; output: %s", string(retryOut))

	// Neither password may leak into a server log from the ALTER ROLE apply.
	assertNoLogLeak(t, poolerDir, pwA, pwB)
}
