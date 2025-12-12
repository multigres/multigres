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
type Config struct {
	// --- Credential settings ---
	// Admin credentials (postgres superuser) - used by AdminPool
	adminUser     viperutil.Value[string]
	adminPassword viperutil.Value[string]

	// App credentials - used by RegularPool and ReservedPool
	// The app user should have membership in all application roles for SET ROLE to work
	appUser     viperutil.Value[string]
	appPassword viperutil.Value[string]

	// --- Pool sizing configuration ---

	// Admin pool configuration
	adminCapacity viperutil.Value[int64]

	// Regular pool configuration (for simple queries without transactions)
	regularCapacity    viperutil.Value[int64]
	regularMaxIdle     viperutil.Value[int64]
	regularIdleTimeout viperutil.Value[time.Duration]
	regularMaxLifetime viperutil.Value[time.Duration]

	// Reserved pool configuration (for transactions)
	// The reserved pool has its own underlying connection pool separate from regular.
	reservedCapacity    viperutil.Value[int64]
	reservedMaxIdle     viperutil.Value[int64]
	reservedIdleTimeout viperutil.Value[time.Duration]
	reservedMaxLifetime viperutil.Value[time.Duration]
}

// NewConfig creates a new Config with all connection pool settings
// registered to the provided registry.
func NewConfig(reg *viperutil.Registry) *Config {
	// Default values for pool configuration.
	var (
		adminCapacity int64 = 5

		// Regular pool defaults (for simple queries without transactions)
		regularCapacity    int64 = 100
		regularMaxIdle     int64 = 10
		regularIdleTimeout       = 5 * time.Minute
		regularMaxLifetime       = 1 * time.Hour

		// Reserved pool defaults (for transactions)
		reservedCapacity    int64 = 50
		reservedMaxIdle     int64 = 5
		reservedIdleTimeout       = 30 * time.Second
		reservedMaxLifetime       = 1 * time.Hour
	)

	return &Config{
		// Credential settings
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
		appUser: viperutil.Configure(reg, "connpool.app.user", viperutil.Options[string]{
			Default:  "appuser",
			FlagName: "connpool-app-user",
			EnvVars:  []string{"CONNPOOL_APP_USER"},
		}),
		appPassword: viperutil.Configure(reg, "connpool.app.password", viperutil.Options[string]{
			Default:  "",
			FlagName: "connpool-app-password",
			EnvVars:  []string{"CONNPOOL_APP_PASSWORD"},
		}),

		// Admin pool
		adminCapacity: viperutil.Configure(reg, "connpool.admin.capacity", viperutil.Options[int64]{
			Default:  adminCapacity,
			FlagName: "connpool-admin-capacity",
		}),

		// Regular pool (for simple queries)
		regularCapacity: viperutil.Configure(reg, "connpool.regular.capacity", viperutil.Options[int64]{
			Default:  regularCapacity,
			FlagName: "connpool-regular-capacity",
		}),
		regularMaxIdle: viperutil.Configure(reg, "connpool.regular.max-idle", viperutil.Options[int64]{
			Default:  regularMaxIdle,
			FlagName: "connpool-regular-max-idle",
		}),
		regularIdleTimeout: viperutil.Configure(reg, "connpool.regular.idle-timeout", viperutil.Options[time.Duration]{
			Default:  regularIdleTimeout,
			FlagName: "connpool-regular-idle-timeout",
		}),
		regularMaxLifetime: viperutil.Configure(reg, "connpool.regular.max-lifetime", viperutil.Options[time.Duration]{
			Default:  regularMaxLifetime,
			FlagName: "connpool-regular-max-lifetime",
		}),

		// Reserved pool (for transactions) - has its own underlying connection pool
		reservedCapacity: viperutil.Configure(reg, "connpool.reserved.capacity", viperutil.Options[int64]{
			Default:  reservedCapacity,
			FlagName: "connpool-reserved-capacity",
		}),
		reservedMaxIdle: viperutil.Configure(reg, "connpool.reserved.max-idle", viperutil.Options[int64]{
			Default:  reservedMaxIdle,
			FlagName: "connpool-reserved-max-idle",
		}),
		reservedIdleTimeout: viperutil.Configure(reg, "connpool.reserved.idle-timeout", viperutil.Options[time.Duration]{
			Default:  reservedIdleTimeout,
			FlagName: "connpool-reserved-idle-timeout",
		}),
		reservedMaxLifetime: viperutil.Configure(reg, "connpool.reserved.max-lifetime", viperutil.Options[time.Duration]{
			Default:  reservedMaxLifetime,
			FlagName: "connpool-reserved-max-lifetime",
		}),
	}
}

// RegisterFlags registers all connection pool flags with the given FlagSet.
func (c *Config) RegisterFlags(fs *pflag.FlagSet) {
	// Credential flags
	fs.String("connpool-admin-user", c.adminUser.Default(), "Admin pool user (PostgreSQL superuser for control operations)")
	fs.String("connpool-admin-password", c.adminPassword.Default(), "Admin pool password (can also be set via CONNPOOL_ADMIN_PASSWORD env var)")
	fs.String("connpool-app-user", c.appUser.Default(), "App pool user (for regular/reserved pools, should have membership in all application roles)")
	fs.String("connpool-app-password", c.appPassword.Default(), "App pool password (can also be set via CONNPOOL_APP_PASSWORD env var)")

	// Admin pool flags
	fs.Int64("connpool-admin-capacity", c.adminCapacity.Default(), "Maximum number of admin connections for control operations")

	// Regular pool flags (for simple queries)
	fs.Int64("connpool-regular-capacity", c.regularCapacity.Default(), "Maximum number of regular connections for query execution")
	fs.Int64("connpool-regular-max-idle", c.regularMaxIdle.Default(), "Maximum number of idle regular connections to keep in pool")
	fs.Duration("connpool-regular-idle-timeout", c.regularIdleTimeout.Default(), "How long a regular connection can remain idle before being closed")
	fs.Duration("connpool-regular-max-lifetime", c.regularMaxLifetime.Default(), "Maximum lifetime of a regular connection before recycling")

	// Reserved pool flags (for transactions) - has its own separate connection pool
	fs.Int64("connpool-reserved-capacity", c.reservedCapacity.Default(), "Maximum number of reserved connections for transactions")
	fs.Int64("connpool-reserved-max-idle", c.reservedMaxIdle.Default(), "Maximum number of idle reserved connections to keep in pool")
	fs.Duration("connpool-reserved-idle-timeout", c.reservedIdleTimeout.Default(), "How long a reserved connection can remain idle before being killed")
	fs.Duration("connpool-reserved-max-lifetime", c.reservedMaxLifetime.Default(), "Maximum lifetime of a reserved connection before recycling")

	viperutil.BindFlags(fs,
		c.adminUser,
		c.adminPassword,
		c.appUser,
		c.appPassword,
		c.adminCapacity,
		c.regularCapacity,
		c.regularMaxIdle,
		c.regularIdleTimeout,
		c.regularMaxLifetime,
		c.reservedCapacity,
		c.reservedMaxIdle,
		c.reservedIdleTimeout,
		c.reservedMaxLifetime,
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

// AppUser returns the configured app pool user.
func (c *Config) AppUser() string {
	return c.appUser.Get()
}

// AppPassword returns the configured app pool password.
func (c *Config) AppPassword() string {
	return c.appPassword.Get()
}

// AdminCapacity returns the configured admin pool capacity.
func (c *Config) AdminCapacity() int64 {
	return c.adminCapacity.Get()
}

// RegularCapacity returns the configured regular pool capacity.
func (c *Config) RegularCapacity() int64 {
	return c.regularCapacity.Get()
}

// RegularMaxIdle returns the configured regular pool max idle count.
func (c *Config) RegularMaxIdle() int64 {
	return c.regularMaxIdle.Get()
}

// RegularIdleTimeout returns the configured regular pool idle timeout.
func (c *Config) RegularIdleTimeout() time.Duration {
	return c.regularIdleTimeout.Get()
}

// RegularMaxLifetime returns the configured regular pool max lifetime.
func (c *Config) RegularMaxLifetime() time.Duration {
	return c.regularMaxLifetime.Get()
}

// ReservedCapacity returns the configured reserved pool capacity.
func (c *Config) ReservedCapacity() int64 {
	return c.reservedCapacity.Get()
}

// ReservedMaxIdle returns the configured reserved pool max idle count.
func (c *Config) ReservedMaxIdle() int64 {
	return c.reservedMaxIdle.Get()
}

// ReservedIdleTimeout returns the configured reserved pool idle timeout.
func (c *Config) ReservedIdleTimeout() time.Duration {
	return c.reservedIdleTimeout.Get()
}

// ReservedMaxLifetime returns the configured reserved pool max lifetime.
func (c *Config) ReservedMaxLifetime() time.Duration {
	return c.reservedMaxLifetime.Get()
}

// NewManager creates a new connection pool manager from this config.
// Call this after flags have been parsed and when you're ready to create the manager.
func (c *Config) NewManager() *Manager {
	return &Manager{
		config: c,
	}
}
