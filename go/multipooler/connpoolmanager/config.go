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
	// Note: Capacity is managed by the rebalancer, not configured here.
	// New pools start with initialUserCapacity (10) and the rebalancer adjusts them.
	userRegularIdleTimeout viperutil.Value[time.Duration]
	userRegularMaxLifetime viperutil.Value[time.Duration]

	// Per-user reserved pool configuration (for transactions)
	// Each user's reserved pool has its own underlying connection pool.
	// Note: Capacity is managed by the rebalancer, not configured here.
	userReservedInactivityTimeout viperutil.Value[time.Duration] // For reserved connections (client inactivity)
	userReservedIdleTimeout       viperutil.Value[time.Duration] // For underlying pool connections
	userReservedMaxLifetime       viperutil.Value[time.Duration]

	// Settings cache size (0 = use default)
	settingsCacheSize viperutil.Value[int64]

	// --- Fair share allocation configuration ---

	// Global capacity is the total number of PostgreSQL connections to manage.
	// This is divided between regular and reserved pools based on reservedRatio.
	globalCapacity viperutil.Value[int64]

	// Reserved ratio is the fraction of global capacity allocated to reserved pools (0.0-1.0).
	// Regular pools get (1 - reservedRatio) of the global capacity.
	reservedRatio viperutil.Value[float64]

	// --- Rebalancer configuration ---

	// Rebalance interval is how often the rebalancer runs to adjust pool capacities.
	rebalanceInterval viperutil.Value[time.Duration]

	// Demand window is the sliding window duration for tracking peak demand.
	demandWindow viperutil.Value[time.Duration]

	// Demand sample interval is how often to sample pool demand within the window.
	demandSampleInterval viperutil.Value[time.Duration]

	// Inactive timeout is how long a user pool can be inactive before being garbage collected.
	inactiveTimeout viperutil.Value[time.Duration]

	// Minimum capacity per user ensures light users always have enough connections
	// for burst demand that point-in-time sampling might miss.
	minCapacityPerUser viperutil.Value[int64]
}

// NewConfig creates a new Config with all connection pool settings
// registered to the provided registry.
func NewConfig(reg *viperutil.Registry) *Config {
	// Default values for pool configuration.
	var (
		adminCapacity int64 = 5

		// Per-user regular pool defaults (for simple queries without transactions)
		userRegularIdleTimeout = 5 * time.Minute
		userRegularMaxLifetime = 1 * time.Hour

		// Per-user reserved pool defaults (for transactions)
		userReservedInactivityTimeout = 30 * time.Second // Aggressive - kills reserved connections if client inactive
		userReservedIdleTimeout       = 5 * time.Minute  // Less aggressive - for pool size reduction
		userReservedMaxLifetime       = 1 * time.Hour

		// Settings cache size
		settingsCacheSize int64 = 1024

		// Fair share allocation defaults
		globalCapacity int64 = 100
		reservedRatio        = 0.2

		// Rebalancer defaults
		rebalanceInterval    = 10 * time.Second
		demandWindow         = 30 * time.Second
		demandSampleInterval = 100 * time.Millisecond
		inactiveTimeout      = 5 * time.Minute

		// Fair share allocation - minimum per user
		// This ensures light users always have enough capacity for burst demand.
		// Set equal to initialUserPoolCapacity (10) so capacity isn't reduced
		// below the initial value until there's actual resource pressure.
		minCapacityPerUser int64 = 10
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
		userRegularIdleTimeout: viperutil.Configure(reg, "connpool.user.regular.idle-timeout", viperutil.Options[time.Duration]{
			Default:  userRegularIdleTimeout,
			FlagName: "connpool-user-regular-idle-timeout",
		}),
		userRegularMaxLifetime: viperutil.Configure(reg, "connpool.user.regular.max-lifetime", viperutil.Options[time.Duration]{
			Default:  userRegularMaxLifetime,
			FlagName: "connpool-user-regular-max-lifetime",
		}),

		// Per-user reserved pool (for transactions)
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

		// Settings cache size
		settingsCacheSize: viperutil.Configure(reg, "connpool.settings-cache-size", viperutil.Options[int64]{
			Default:  settingsCacheSize,
			FlagName: "connpool-settings-cache-size",
		}),

		// Fair share allocation
		globalCapacity: viperutil.Configure(reg, "connpool.global-capacity", viperutil.Options[int64]{
			Default:  globalCapacity,
			FlagName: "connpool-global-capacity",
		}),
		reservedRatio: viperutil.Configure(reg, "connpool.reserved-ratio", viperutil.Options[float64]{
			Default:  reservedRatio,
			FlagName: "connpool-reserved-ratio",
		}),

		// Rebalancer
		rebalanceInterval: viperutil.Configure(reg, "connpool.rebalance-interval", viperutil.Options[time.Duration]{
			Default:  rebalanceInterval,
			FlagName: "connpool-rebalance-interval",
		}),
		demandWindow: viperutil.Configure(reg, "connpool.demand-window", viperutil.Options[time.Duration]{
			Default:  demandWindow,
			FlagName: "connpool-demand-window",
		}),
		demandSampleInterval: viperutil.Configure(reg, "connpool.demand-sample-interval", viperutil.Options[time.Duration]{
			Default:  demandSampleInterval,
			FlagName: "connpool-demand-sample-interval",
		}),
		inactiveTimeout: viperutil.Configure(reg, "connpool.inactive-timeout", viperutil.Options[time.Duration]{
			Default:  inactiveTimeout,
			FlagName: "connpool-inactive-timeout",
		}),
		minCapacityPerUser: viperutil.Configure(reg, "connpool.min-capacity-per-user", viperutil.Options[int64]{
			Default:  minCapacityPerUser,
			FlagName: "connpool-min-capacity-per-user",
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
	fs.Duration("connpool-user-regular-idle-timeout", c.userRegularIdleTimeout.Default(), "How long a user's regular connection can remain idle before being closed")
	fs.Duration("connpool-user-regular-max-lifetime", c.userRegularMaxLifetime.Default(), "Maximum lifetime of a user's regular connection before recycling")

	// Per-user reserved pool flags (for transactions)
	fs.Duration("connpool-user-reserved-inactivity-timeout", c.userReservedInactivityTimeout.Default(), "How long a reserved connection can be inactive (no client activity) before being killed")
	fs.Duration("connpool-user-reserved-idle-timeout", c.userReservedIdleTimeout.Default(), "How long a connection in the reserved pool can remain idle before being closed")
	fs.Duration("connpool-user-reserved-max-lifetime", c.userReservedMaxLifetime.Default(), "Maximum lifetime of a user's reserved connection before recycling")

	// Settings cache size flag
	fs.Int64("connpool-settings-cache-size", c.settingsCacheSize.Default(), "Maximum number of unique settings combinations to cache (0 = use default)")

	// Fair share allocation flags
	fs.Int64("connpool-global-capacity", c.globalCapacity.Default(), "Total PostgreSQL connections to manage (divided between regular and reserved pools)")
	fs.Float64("connpool-reserved-ratio", c.reservedRatio.Default(), "Fraction of global capacity allocated to reserved pools (0.0-1.0)")

	// Rebalancer flags
	fs.Duration("connpool-rebalance-interval", c.rebalanceInterval.Default(), "How often to rebalance pool capacities")
	fs.Duration("connpool-demand-window", c.demandWindow.Default(), "Sliding window duration for peak demand tracking")
	fs.Duration("connpool-demand-sample-interval", c.demandSampleInterval.Default(), "How often to sample pool demand within the window")
	fs.Duration("connpool-inactive-timeout", c.inactiveTimeout.Default(), "How long a user pool can be inactive before garbage collection")
	fs.Int64("connpool-min-capacity-per-user", c.minCapacityPerUser.Default(), "Minimum connections per user (protects against aggressive capacity reduction for light users)")

	viperutil.BindFlags(fs,
		c.adminUser,
		c.adminPassword,
		c.adminCapacity,
		c.userRegularIdleTimeout,
		c.userRegularMaxLifetime,
		c.userReservedInactivityTimeout,
		c.userReservedIdleTimeout,
		c.userReservedMaxLifetime,
		c.settingsCacheSize,
		c.globalCapacity,
		c.reservedRatio,
		c.rebalanceInterval,
		c.demandWindow,
		c.demandSampleInterval,
		c.inactiveTimeout,
		c.minCapacityPerUser,
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

// UserRegularIdleTimeout returns the per-user regular pool idle timeout.
func (c *Config) UserRegularIdleTimeout() time.Duration {
	return c.userRegularIdleTimeout.Get()
}

// UserRegularMaxLifetime returns the per-user regular pool max lifetime.
func (c *Config) UserRegularMaxLifetime() time.Duration {
	return c.userRegularMaxLifetime.Get()
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

// SettingsCacheSize returns the settings cache size.
func (c *Config) SettingsCacheSize() int {
	return int(c.settingsCacheSize.Get())
}

// GlobalCapacity returns the total PostgreSQL connections to manage.
// This is divided between regular and reserved pools based on ReservedRatio.
func (c *Config) GlobalCapacity() int64 {
	return c.globalCapacity.Get()
}

// ReservedRatio returns the fraction of global capacity allocated to reserved pools (0.0-1.0).
// Regular pools get (1 - reservedRatio) of the global capacity.
func (c *Config) ReservedRatio() float64 {
	return c.reservedRatio.Get()
}

// RebalanceInterval returns how often the rebalancer runs to adjust pool capacities.
func (c *Config) RebalanceInterval() time.Duration {
	return c.rebalanceInterval.Get()
}

// DemandWindow returns the sliding window duration for peak demand tracking.
func (c *Config) DemandWindow() time.Duration {
	return c.demandWindow.Get()
}

// DemandSampleInterval returns how often to sample pool demand within the window.
func (c *Config) DemandSampleInterval() time.Duration {
	return c.demandSampleInterval.Get()
}

// InactiveTimeout returns how long a user pool can be inactive before garbage collection.
func (c *Config) InactiveTimeout() time.Duration {
	return c.inactiveTimeout.Get()
}

// MinCapacityPerUser returns the minimum connections per user.
// This ensures light users always have enough capacity for burst demand.
func (c *Config) MinCapacityPerUser() int64 {
	return c.minCapacityPerUser.Get()
}

// NewManager creates a new connection pool manager from this config.
// Call this after flags have been parsed and when you're ready to create the manager.
// The manager starts in a closed state; call Open() before using it.
func (c *Config) NewManager(logger *slog.Logger) *Manager {
	metrics, err := NewMetrics()
	if err != nil {
		logger.Warn("failed to initialize some connection pool metrics (using noop fallbacks)", "error", err)
	}

	mgr := &Manager{
		config:  c,
		logger:  logger,
		metrics: metrics,
	}
	mgr.closed.Store(true) // Manager is closed until Open() is called
	return mgr
}
