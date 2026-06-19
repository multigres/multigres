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

package connpoolmanager

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/tools/viperutil"
)

func TestNewConfig(t *testing.T) {
	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	require.NotNil(t, config)
}

func TestConfig_DefaultValues(t *testing.T) {
	viper.Reset()
	defer func() { viper.Reset() }()

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	// Register flags with a command to use defaults.
	cmd := &cobra.Command{}
	config.RegisterFlags(cmd.Flags())

	// Verify default values.
	assert.Equal(t, constants.DefaultPostgresUser, config.pgUser.Default())
	assert.Equal(t, "", config.pgPassword.Default())
	assert.Equal(t, int64(5), config.adminCapacity.Default())

	// Regular pool (capacity managed by rebalancer)
	assert.Equal(t, 5*time.Minute, config.userRegularIdleTimeout.Default())
	assert.Equal(t, 1*time.Hour, config.userRegularMaxLifetime.Default())

	// Reserved pool (capacity managed by rebalancer)
	assert.Equal(t, 30*time.Second, config.userReservedInactivityTimeout.Default())
	assert.Equal(t, 5*time.Minute, config.userReservedIdleTimeout.Default())
	assert.Equal(t, 1*time.Hour, config.userReservedMaxLifetime.Default())

	assert.Equal(t, int64(1024), config.settingsCacheSize.Default())

	// Dial timeout
	assert.Equal(t, 5*time.Second, config.dialTimeout.Default())
}

func TestConfig_RegisterFlags(t *testing.T) {
	viper.Reset()
	defer func() { viper.Reset() }()

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	cmd := &cobra.Command{}
	config.RegisterFlags(cmd.Flags())

	// Verify flags are registered.
	adminUserFlag := cmd.Flags().Lookup("connpool-admin-user")
	require.NotNil(t, adminUserFlag)
	assert.Equal(t, constants.DefaultPostgresUser, adminUserFlag.DefValue)

	adminPasswordFlag := cmd.Flags().Lookup("connpool-admin-password")
	require.NotNil(t, adminPasswordFlag)

	adminCapFlag := cmd.Flags().Lookup("connpool-admin-capacity")
	require.NotNil(t, adminCapFlag)
	assert.Equal(t, "5", adminCapFlag.DefValue)

	demandWindowFlag := cmd.Flags().Lookup("connpool-demand-window")
	require.NotNil(t, demandWindowFlag)
	assert.Equal(t, "30s", demandWindowFlag.DefValue)

	dialTimeoutFlag := cmd.Flags().Lookup("connpool-dial-timeout")
	require.NotNil(t, dialTimeoutFlag)
	assert.Equal(t, "5s", dialTimeoutFlag.DefValue)
}

func TestConfig_Getters_ReturnDefaults(t *testing.T) {
	viper.Reset()
	defer func() { viper.Reset() }()

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	cmd := &cobra.Command{}
	config.RegisterFlags(cmd.Flags())

	// Verify getters return the default values (viperutil.Value returns defaults
	// when no flags are parsed and no env vars are set).
	assert.Equal(t, constants.DefaultPostgresUser, config.PgUser())
	// Until ResolvePgPassword runs, PgPassword returns the zero value for
	// both fields.
	pw, source := config.PgPassword()
	assert.Equal(t, "", pw)
	assert.Equal(t, pwSourceNone, source)
	assert.Equal(t, int64(5), config.AdminCapacity())

	// Regular pool (capacity managed by rebalancer)
	assert.Equal(t, 5*time.Minute, config.UserRegularIdleTimeout())
	assert.Equal(t, 1*time.Hour, config.UserRegularMaxLifetime())

	// Reserved pool (capacity managed by rebalancer)
	assert.Equal(t, 30*time.Second, config.UserReservedInactivityTimeout())
	assert.Equal(t, 5*time.Minute, config.UserReservedIdleTimeout())
	assert.Equal(t, 1*time.Hour, config.UserReservedMaxLifetime())

	assert.Equal(t, 1024, config.SettingsCacheSize())
	assert.Equal(t, 5*time.Second, config.DialTimeout())
}

func TestConfig_GlobalCapacityFromEnv(t *testing.T) {
	viper.Reset()
	defer func() { viper.Reset() }()

	// The cluster docker image sets CONNPOOL_GLOBAL_CAPACITY (max_connections
	// minus a reserve) so the pooler sizes itself to the backing PostgreSQL.
	t.Setenv("CONNPOOL_GLOBAL_CAPACITY", "90")

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	cmd := &cobra.Command{}
	config.RegisterFlags(cmd.Flags())

	assert.Equal(t, int64(90), config.GlobalCapacity())
}

func TestConfig_NewManager(t *testing.T) {
	viper.Reset()
	defer func() { viper.Reset() }()

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	manager := config.NewManager(slog.Default())

	require.NotNil(t, manager)
	assert.Equal(t, config, manager.config)
}

func TestConnectionConfig(t *testing.T) {
	connConfig := &ConnectionConfig{
		SocketFile: "/var/run/postgresql/.s.PGSQL.5432",
		Host:       "localhost",
		Port:       5432,
		Database:   "testdb",
	}

	assert.Equal(t, "/var/run/postgresql/.s.PGSQL.5432", connConfig.SocketFile)
	assert.Equal(t, "localhost", connConfig.Host)
	assert.Equal(t, 5432, connConfig.Port)
	assert.Equal(t, "testdb", connConfig.Database)
}

func TestConnectionConfig_TCPOnly(t *testing.T) {
	connConfig := &ConnectionConfig{
		Host:     "db.example.com",
		Port:     5432,
		Database: "production",
	}

	assert.Empty(t, connConfig.SocketFile)
	assert.Equal(t, "db.example.com", connConfig.Host)
	assert.Equal(t, 5432, connConfig.Port)
	assert.Equal(t, "production", connConfig.Database)
}

func TestManagerStats(t *testing.T) {
	stats := ManagerStats{
		UserPools: make(map[string]UserPoolStats),
	}

	// Add some user pool stats.
	stats.UserPools["user1"] = UserPoolStats{Username: "user1"}
	stats.UserPools["user2"] = UserPoolStats{Username: "user2"}

	assert.Len(t, stats.UserPools, 2)
	assert.Contains(t, stats.UserPools, "user1")
	assert.Contains(t, stats.UserPools, "user2")
}

func TestUserPoolStats(t *testing.T) {
	stats := UserPoolStats{
		Username: "testuser",
	}

	assert.Equal(t, "testuser", stats.Username)
}

// --- PgUser / PgPassword env-var tests ---

func TestConfig_ConnpoolAdminUser_EnvVar(t *testing.T) {
	t.Setenv("CONNPOOL_ADMIN_USER", "connpool_user")

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	assert.Equal(t, "connpool_user", config.PgUser())
}

func TestConfig_ConnpoolAdminPassword_EnvVar(t *testing.T) {
	t.Setenv("CONNPOOL_ADMIN_PASSWORD", "connpool_pass")

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	require.NoError(t, config.ResolvePgPassword())
	pw, source := config.PgPassword()
	assert.Equal(t, "connpool_pass", pw)
	assert.Equal(t, pwSourceEnv, source)
}

func TestConfig_ConnpoolAdminUser_TakesPrecedence(t *testing.T) {
	t.Setenv("CONNPOOL_ADMIN_USER", "connpool_user")
	t.Setenv(constants.PgUserEnvVar, "postgres_user")

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	assert.Equal(t, "connpool_user", config.PgUser())
}

func TestConfig_ConnpoolAdminPassword_TakesPrecedence(t *testing.T) {
	t.Setenv("CONNPOOL_ADMIN_PASSWORD", "connpool_pass")
	t.Setenv(constants.PgPasswordEnvVar, "postgres_pass")

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	require.NoError(t, config.ResolvePgPassword())
	pw, source := config.PgPassword()
	assert.Equal(t, "connpool_pass", pw)
	assert.Equal(t, pwSourceEnv, source)
}

func TestConfig_PgPassword_EnvVar(t *testing.T) {
	t.Setenv(constants.PgPasswordEnvVar, "test-password")

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	require.NoError(t, config.ResolvePgPassword())
	pw, source := config.PgPassword()
	assert.Equal(t, "test-password", pw)
	assert.Equal(t, pwSourceEnv, source)
}

// --- ResolvePgPassword edge cases ---

// writePwFile is a small helper that creates a password file inside a fresh
// temp dir and returns its path. Used by the empty-file / non-empty-file
// fallthrough tests below.
func writePwFile(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "postgres-password")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

// Row 2a: file path set with non-empty path but the file's content is empty.
// Resolver must surface a clear error and NOT silently fall through to env
// even if env vars are set to something else.
func TestResolvePgPassword_FileEmpty_Errors(t *testing.T) {
	require.NoError(t, os.Unsetenv("CONNPOOL_ADMIN_PASSWORD_FILE"))
	t.Setenv(constants.PgPasswordFileEnvVar, writePwFile(t, ""))
	// Distinct env value so a regression that falls through surfaces
	// immediately as a wrong source/value.
	t.Setenv(constants.PgPasswordEnvVar, "from-env-should-be-ignored")

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	err := config.ResolvePgPassword()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is empty")
}

// Row 2b: file path env var is explicitly set to the empty string. That's
// "operator wrote POSTGRES_PASSWORD_FILE= in a manifest" — a deliberate
// misconfiguration, not "unset". os.LookupEnv distinguishes the two.
func TestResolvePgPassword_FilePathEnvSetEmpty_Errors(t *testing.T) {
	require.NoError(t, os.Unsetenv("CONNPOOL_ADMIN_PASSWORD_FILE"))
	t.Setenv(constants.PgPasswordFileEnvVar, "")
	// Env value set to something so a regression would silently use it.
	t.Setenv(constants.PgPasswordEnvVar, "from-env-should-be-ignored")

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	err := config.ResolvePgPassword()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "file path is set to the empty string")
}

// Row 3: --connpool-admin-password flag set to a non-empty value, no file
// path configured. Flag wins over any env var; source must be Option.
func TestResolvePgPassword_OptionFlagSet_UsesOption(t *testing.T) {
	require.NoError(t, os.Unsetenv(constants.PgPasswordFileEnvVar))
	require.NoError(t, os.Unsetenv("CONNPOOL_ADMIN_PASSWORD_FILE"))
	// Set env vars to a distinct value to prove the flag dominates.
	t.Setenv("CONNPOOL_ADMIN_PASSWORD", "from-env-should-be-ignored")

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)
	require.NoError(t, fs.Set("connpool-admin-password", "from-flag"))

	require.NoError(t, config.ResolvePgPassword())
	pw, source := config.PgPassword()
	assert.Equal(t, "from-flag", pw)
	assert.Equal(t, pwSourceOption, source)
}

// Row 4: --connpool-admin-password flag explicitly set to the empty string.
// Resolver must error even when env vars are set to a non-empty value.
func TestResolvePgPassword_OptionFlagSetEmpty_Errors(t *testing.T) {
	require.NoError(t, os.Unsetenv(constants.PgPasswordFileEnvVar))
	require.NoError(t, os.Unsetenv("CONNPOOL_ADMIN_PASSWORD_FILE"))
	t.Setenv("CONNPOOL_ADMIN_PASSWORD", "from-env-should-be-ignored")

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)
	require.NoError(t, fs.Set("connpool-admin-password", ""))

	err := config.ResolvePgPassword()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--connpool-admin-password")
	assert.Contains(t, err.Error(), "empty string")
}

// No source configured at all: file path unset, neither env var set. Both
// branches fall through; Resolve must surface a clear "not configured" error
// rather than silently leave the resolver with an empty cached password.
func TestResolvePgPassword_NoSourceConfigured(t *testing.T) {
	require.NoError(t, os.Unsetenv(constants.PgPasswordFileEnvVar))
	require.NoError(t, os.Unsetenv("CONNPOOL_ADMIN_PASSWORD_FILE"))
	require.NoError(t, os.Unsetenv(constants.PgPasswordEnvVar))
	require.NoError(t, os.Unsetenv("CONNPOOL_ADMIN_PASSWORD"))

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	err := config.ResolvePgPassword()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "admin password not configured")
}

// POSTGRES_PASSWORD deliberately set to the empty string is a misconfiguration
// (operator typed POSTGRES_PASSWORD= in a unit file or manifest). Distinct
// from "unset" via os.LookupEnv; resolver must reject it explicitly.
func TestResolvePgPassword_PostgresPasswordSetEmpty(t *testing.T) {
	require.NoError(t, os.Unsetenv("CONNPOOL_ADMIN_PASSWORD"))
	t.Setenv(constants.PgPasswordEnvVar, "")

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	err := config.ResolvePgPassword()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "POSTGRES_PASSWORD")
	assert.Contains(t, err.Error(), "empty string")
}

// CONNPOOL_ADMIN_PASSWORD deliberately set to empty — same misconfiguration,
// different env-var name.
func TestResolvePgPassword_ConnpoolAdminPasswordSetEmpty(t *testing.T) {
	require.NoError(t, os.Unsetenv(constants.PgPasswordEnvVar))
	t.Setenv("CONNPOOL_ADMIN_PASSWORD", "")

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	err := config.ResolvePgPassword()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CONNPOOL_ADMIN_PASSWORD")
	assert.Contains(t, err.Error(), "empty string")
}

func TestConfig_PgUser_EnvVar(t *testing.T) {
	t.Setenv(constants.PgUserEnvVar, "multigres")

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	assert.Equal(t, "multigres", config.PgUser())
}
