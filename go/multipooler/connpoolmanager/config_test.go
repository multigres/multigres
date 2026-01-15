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
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	assert.Equal(t, "postgres", config.adminUser.Default())
	assert.Equal(t, "", config.adminPassword.Default())
	assert.Equal(t, int64(5), config.adminCapacity.Default())

	assert.Equal(t, int64(10), config.userRegularCapacity.Default())
	assert.Equal(t, int64(5), config.userRegularMaxIdle.Default())
	assert.Equal(t, 5*time.Minute, config.userRegularIdleTimeout.Default())
	assert.Equal(t, 1*time.Hour, config.userRegularMaxLifetime.Default())

	assert.Equal(t, int64(5), config.userReservedCapacity.Default())
	assert.Equal(t, int64(2), config.userReservedMaxIdle.Default())
	assert.Equal(t, 30*time.Second, config.userReservedInactivityTimeout.Default())
	assert.Equal(t, 5*time.Minute, config.userReservedIdleTimeout.Default())
	assert.Equal(t, 1*time.Hour, config.userReservedMaxLifetime.Default())

	assert.Equal(t, int64(0), config.maxUsers.Default())
	assert.Equal(t, int64(1024), config.settingsCacheSize.Default())
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
	assert.Equal(t, "postgres", adminUserFlag.DefValue)

	adminCapFlag := cmd.Flags().Lookup("connpool-admin-capacity")
	require.NotNil(t, adminCapFlag)
	assert.Equal(t, "5", adminCapFlag.DefValue)

	regularCapFlag := cmd.Flags().Lookup("connpool-user-regular-capacity")
	require.NotNil(t, regularCapFlag)
	assert.Equal(t, "10", regularCapFlag.DefValue)

	reservedCapFlag := cmd.Flags().Lookup("connpool-user-reserved-capacity")
	require.NotNil(t, reservedCapFlag)
	assert.Equal(t, "5", reservedCapFlag.DefValue)

	maxUsersFlag := cmd.Flags().Lookup("connpool-max-users")
	require.NotNil(t, maxUsersFlag)
	assert.Equal(t, "0", maxUsersFlag.DefValue)
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
	assert.Equal(t, "postgres", config.AdminUser())
	assert.Equal(t, "", config.AdminPassword())
	assert.Equal(t, int64(5), config.AdminCapacity())

	assert.Equal(t, int64(10), config.UserRegularCapacity())
	assert.Equal(t, int64(5), config.UserRegularMaxIdle())
	assert.Equal(t, 5*time.Minute, config.UserRegularIdleTimeout())
	assert.Equal(t, 1*time.Hour, config.UserRegularMaxLifetime())

	assert.Equal(t, int64(5), config.UserReservedCapacity())
	assert.Equal(t, int64(2), config.UserReservedMaxIdle())
	assert.Equal(t, 30*time.Second, config.UserReservedInactivityTimeout())
	assert.Equal(t, 5*time.Minute, config.UserReservedIdleTimeout())
	assert.Equal(t, 1*time.Hour, config.UserReservedMaxLifetime())

	assert.Equal(t, int64(0), config.MaxUsers())
	assert.Equal(t, 1024, config.SettingsCacheSize())
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
