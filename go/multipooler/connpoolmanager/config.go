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

// Config holds viper-backed configuration values for the connection pool manager.
// It is created internally by NewManager and configured via RegisterFlags.
type Config struct {
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

// newConfig creates a new Config with all connection pool settings
// registered to the provided registry.
func newConfig(reg *viperutil.Registry) *Config {
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

// registerFlags registers all connection pool flags with the given FlagSet.
func (c *Config) registerFlags(fs *pflag.FlagSet) {
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
