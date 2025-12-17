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
	"time"

	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/tools/viperutil"
)

// ConnectionConfig holds connection settings passed from the parent multipooler.
// These are not viper-backed since the flags already exist in the multipooler init.
type ConnectionConfig struct {
	// SocketFile is the full path to the PostgreSQL Unix socket file.
	// If set, Unix socket connection is used instead of TCP.
	// Example: /var/run/postgresql/.s.PGSQL.5432
	SocketFile string

	// Host is the PostgreSQL host (for TCP connections).
	// Ignored if SocketFile is set.
	Host string

	// Port is the PostgreSQL port (for TCP connections).
	// Ignored if SocketFile is set.
	Port int

	// Database is the database name to connect to.
	Database string
}

// Config holds viper-backed configuration values for the connection pool manager.
// Create with NewConfig(), register flags with RegisterFlags(), then create the
// manager with NewManager() when ready.
//
// With per-user connection pools, each user gets their own RegularPool and ReservedPool.
// Connections authenticate directly as the user via trust/peer authentication.
type Config struct {
	// --- Admin credential settings ---
	// Admin credentials (postgres superuser) - used by shared AdminPool for kill operations
	adminUser     viperutil.Value[string]
	adminPassword viperutil.Value[string]

	// --- Pool sizing configuration ---

	// Admin pool configuration (shared across all users)
	adminCapacity viperutil.Value[int64]

	// Per-user regular pool configuration (for simple queries without transactions)
	userRegularCapacity    viperutil.Value[int64]
	userRegularMaxIdle     viperutil.Value[int64]
	userRegularIdleTimeout viperutil.Value[time.Duration]
	userRegularMaxLifetime viperutil.Value[time.Duration]

	// Per-user reserved pool configuration (for transactions)
	// Each user's reserved pool has its own underlying connection pool.
	userReservedCapacity          viperutil.Value[int64]
	userReservedMaxIdle           viperutil.Value[int64]
	userReservedInactivityTimeout viperutil.Value[time.Duration] // For reserved connections (client inactivity)
	userReservedIdleTimeout       viperutil.Value[time.Duration] // For underlying pool connections
	userReservedMaxLifetime       viperutil.Value[time.Duration]

	// Maximum number of user pools (0 = unlimited)
	maxUsers viperutil.Value[int64]

	// Settings cache size (0 = use default)
	settingsCacheSize viperutil.Value[int64]
}

// NewConfig creates a new Config with all connection pool settings
// registered to the provided registry.
func NewConfig(reg *viperutil.Registry) *Config {
	// Default values for pool configuration.
	var (
		adminCapacity int64 = 5

		// Per-user regular pool defaults (for simple queries without transactions)
		userRegularCapacity    int64 = 10
		userRegularMaxIdle     int64 = 5
		userRegularIdleTimeout       = 5 * time.Minute
		userRegularMaxLifetime       = 1 * time.Hour

		// Per-user reserved pool defaults (for transactions)
		userReservedCapacity          int64 = 5
		userReservedMaxIdle           int64 = 2
		userReservedInactivityTimeout       = 30 * time.Second // Aggressive - kills reserved connections if client inactive
		userReservedIdleTimeout             = 5 * time.Minute  // Less aggressive - for pool size reduction
		userReservedMaxLifetime             = 1 * time.Hour

		// Maximum number of user pools (0 = unlimited)
		maxUsers int64 = 0

		// Settings cache size
		settingsCacheSize int64 = 1024
	)

	return &Config{
		// Admin credential settings
		adminUser: viperutil.Configure(reg, "connpool.admin.user", viperutil.Options[string]{
			Default:  "postgres",
			FlagName: "connpool-admin-user",
			EnvVars:  []string{"CONNPOOL_ADMIN_USER"},
		}),
		adminPassword: viperutil.Configure(reg, "connpool.admin.password", viperutil.Options[string]{
			Default:  "",
			FlagName: "connpool-admin-password",
			EnvVars:  []string{"CONNPOOL_ADMIN_PASSWORD"},
		}),

		// Admin pool (shared across all users)
		adminCapacity: viperutil.Configure(reg, "connpool.admin.capacity", viperutil.Options[int64]{
			Default:  adminCapacity,
			FlagName: "connpool-admin-capacity",
		}),

		// Per-user regular pool (for simple queries)
		userRegularCapacity: viperutil.Configure(reg, "connpool.user.regular.capacity", viperutil.Options[int64]{
			Default:  userRegularCapacity,
			FlagName: "connpool-user-regular-capacity",
		}),
		userRegularMaxIdle: viperutil.Configure(reg, "connpool.user.regular.max-idle", viperutil.Options[int64]{
			Default:  userRegularMaxIdle,
			FlagName: "connpool-user-regular-max-idle",
		}),
		userRegularIdleTimeout: viperutil.Configure(reg, "connpool.user.regular.idle-timeout", viperutil.Options[time.Duration]{
			Default:  userRegularIdleTimeout,
			FlagName: "connpool-user-regular-idle-timeout",
		}),
		userRegularMaxLifetime: viperutil.Configure(reg, "connpool.user.regular.max-lifetime", viperutil.Options[time.Duration]{
			Default:  userRegularMaxLifetime,
			FlagName: "connpool-user-regular-max-lifetime",
		}),

		// Per-user reserved pool (for transactions)
		userReservedCapacity: viperutil.Configure(reg, "connpool.user.reserved.capacity", viperutil.Options[int64]{
			Default:  userReservedCapacity,
			FlagName: "connpool-user-reserved-capacity",
		}),
		userReservedMaxIdle: viperutil.Configure(reg, "connpool.user.reserved.max-idle", viperutil.Options[int64]{
			Default:  userReservedMaxIdle,
			FlagName: "connpool-user-reserved-max-idle",
		}),
		userReservedInactivityTimeout: viperutil.Configure(reg, "connpool.user.reserved.inactivity-timeout", viperutil.Options[time.Duration]{
			Default:  userReservedInactivityTimeout,
			FlagName: "connpool-user-reserved-inactivity-timeout",
		}),
		userReservedIdleTimeout: viperutil.Configure(reg, "connpool.user.reserved.idle-timeout", viperutil.Options[time.Duration]{
			Default:  userReservedIdleTimeout,
			FlagName: "connpool-user-reserved-idle-timeout",
		}),
		userReservedMaxLifetime: viperutil.Configure(reg, "connpool.user.reserved.max-lifetime", viperutil.Options[time.Duration]{
			Default:  userReservedMaxLifetime,
			FlagName: "connpool-user-reserved-max-lifetime",
		}),

		// Maximum number of user pools
		maxUsers: viperutil.Configure(reg, "connpool.max-users", viperutil.Options[int64]{
			Default:  maxUsers,
			FlagName: "connpool-max-users",
		}),

		// Settings cache size
		settingsCacheSize: viperutil.Configure(reg, "connpool.settings-cache-size", viperutil.Options[int64]{
			Default:  settingsCacheSize,
			FlagName: "connpool-settings-cache-size",
		}),
	}
}

// RegisterFlags registers all connection pool flags with the given FlagSet.
func (c *Config) RegisterFlags(fs *pflag.FlagSet) {
	// Admin credential flags
	fs.String("connpool-admin-user", c.adminUser.Default(), "Admin pool user (PostgreSQL superuser for control operations)")
	fs.String("connpool-admin-password", c.adminPassword.Default(), "Admin pool password (can also be set via CONNPOOL_ADMIN_PASSWORD env var)")

	// Admin pool flags (shared across all users)
	fs.Int64("connpool-admin-capacity", c.adminCapacity.Default(), "Maximum number of admin connections for control operations")

	// Per-user regular pool flags (for simple queries)
	fs.Int64("connpool-user-regular-capacity", c.userRegularCapacity.Default(), "Maximum regular connections per user")
	fs.Int64("connpool-user-regular-max-idle", c.userRegularMaxIdle.Default(), "Maximum idle regular connections per user")
	fs.Duration("connpool-user-regular-idle-timeout", c.userRegularIdleTimeout.Default(), "How long a user's regular connection can remain idle before being closed")
	fs.Duration("connpool-user-regular-max-lifetime", c.userRegularMaxLifetime.Default(), "Maximum lifetime of a user's regular connection before recycling")

	// Per-user reserved pool flags (for transactions)
	fs.Int64("connpool-user-reserved-capacity", c.userReservedCapacity.Default(), "Maximum reserved connections per user")
	fs.Int64("connpool-user-reserved-max-idle", c.userReservedMaxIdle.Default(), "Maximum idle reserved connections per user")
	fs.Duration("connpool-user-reserved-inactivity-timeout", c.userReservedInactivityTimeout.Default(), "How long a reserved connection can be inactive (no client activity) before being killed")
	fs.Duration("connpool-user-reserved-idle-timeout", c.userReservedIdleTimeout.Default(), "How long a connection in the reserved pool can remain idle before being closed")
	fs.Duration("connpool-user-reserved-max-lifetime", c.userReservedMaxLifetime.Default(), "Maximum lifetime of a user's reserved connection before recycling")

	// Max users flag
	fs.Int64("connpool-max-users", c.maxUsers.Default(), "Maximum number of user pools (0 = unlimited)")

	// Settings cache size flag
	fs.Int64("connpool-settings-cache-size", c.settingsCacheSize.Default(), "Maximum number of unique settings combinations to cache (0 = use default)")

	viperutil.BindFlags(fs,
		c.adminUser,
		c.adminPassword,
		c.adminCapacity,
		c.userRegularCapacity,
		c.userRegularMaxIdle,
		c.userRegularIdleTimeout,
		c.userRegularMaxLifetime,
		c.userReservedCapacity,
		c.userReservedMaxIdle,
		c.userReservedInactivityTimeout,
		c.userReservedIdleTimeout,
		c.userReservedMaxLifetime,
		c.maxUsers,
		c.settingsCacheSize,
	)
}

// --- Getters for individual values ---

// AdminUser returns the configured admin pool user.
func (c *Config) AdminUser() string {
	return c.adminUser.Get()
}

// AdminPassword returns the configured admin pool password.
func (c *Config) AdminPassword() string {
	return c.adminPassword.Get()
}

// AdminCapacity returns the configured admin pool capacity.
func (c *Config) AdminCapacity() int64 {
	return c.adminCapacity.Get()
}

// UserRegularCapacity returns the per-user regular pool capacity.
func (c *Config) UserRegularCapacity() int64 {
	return c.userRegularCapacity.Get()
}

// UserRegularMaxIdle returns the per-user regular pool max idle count.
func (c *Config) UserRegularMaxIdle() int64 {
	return c.userRegularMaxIdle.Get()
}

// UserRegularIdleTimeout returns the per-user regular pool idle timeout.
func (c *Config) UserRegularIdleTimeout() time.Duration {
	return c.userRegularIdleTimeout.Get()
}

// UserRegularMaxLifetime returns the per-user regular pool max lifetime.
func (c *Config) UserRegularMaxLifetime() time.Duration {
	return c.userRegularMaxLifetime.Get()
}

// UserReservedCapacity returns the per-user reserved pool capacity.
func (c *Config) UserReservedCapacity() int64 {
	return c.userReservedCapacity.Get()
}

// UserReservedMaxIdle returns the per-user reserved pool max idle count.
func (c *Config) UserReservedMaxIdle() int64 {
	return c.userReservedMaxIdle.Get()
}

// UserReservedInactivityTimeout returns the reserved connection inactivity timeout.
// This is how long a reserved connection can be inactive before being killed.
func (c *Config) UserReservedInactivityTimeout() time.Duration {
	return c.userReservedInactivityTimeout.Get()
}

// UserReservedIdleTimeout returns the idle timeout for connections in the reserved pool.
func (c *Config) UserReservedIdleTimeout() time.Duration {
	return c.userReservedIdleTimeout.Get()
}

// UserReservedMaxLifetime returns the per-user reserved pool max lifetime.
func (c *Config) UserReservedMaxLifetime() time.Duration {
	return c.userReservedMaxLifetime.Get()
}

// MaxUsers returns the maximum number of user pools (0 = unlimited).
func (c *Config) MaxUsers() int64 {
	return c.maxUsers.Get()
}

// SettingsCacheSize returns the settings cache size.
func (c *Config) SettingsCacheSize() int {
	return int(c.settingsCacheSize.Get())
}

// NewManager creates a new connection pool manager from this config.
// Call this after flags have been parsed and when you're ready to create the manager.
func (c *Config) NewManager() *Manager {
	return &Manager{
		config: c,
	}
}
