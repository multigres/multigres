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
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgsecret"
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

	// SSLMode controls libpq-style sslmode for the multipooler → PostgreSQL leg.
	// Only honored on TCP connections (SocketFile == "").
	SSLMode client.SSLMode

	// SSLNegotiation controls how TLS is established (libpq sslnegotiation,
	// PostgreSQL 17+): "postgres" (SSLRequest negotiation, default) or
	// "direct" (TLS-first with mandatory ALPN). Only honored on TCP
	// connections; direct requires a TLS-enforcing SSLMode.
	SSLNegotiation client.SSLNegotiation

	// TLSConfig is the *tls.Config built from SSLMode + sslrootcert. Nil for
	// disable/allow; non-nil otherwise. Only honored on TCP connections.
	TLSConfig *tls.Config

	// ChannelBinding controls SCRAM-SHA-256-PLUS negotiation (libpq channel_binding)
	// on the multipooler to PostgreSQL leg. Only takes effect over TLS.
	ChannelBinding client.ChannelBindingMode
}

type pgPasswordSource int

const (
	pwSourceNone   pgPasswordSource = iota
	pwSourceEnv                     // CONNPOOL_ADMIN_PASSWORD / POSTGRES_PASSWORD env var
	pwSourceOption                  // --connpool-admin-password flag
	pwSourceFile                    // password file path (flag or env var)
)

// Config holds viper-backed configuration values for the connection pool manager.
// Create with NewConfig(), register flags with RegisterFlags(), then create the
// manager with NewManager() when ready.
//
// With per-user connection pools, each user gets their own RegularPool and ReservedPool.
// Connections authenticate directly as the user via trust/peer authentication.
type Config struct {
	// --- PostgreSQL superuser credentials ---
	// Used by the admin pool for kill operations and internal system queries
	// (heartbeat, replication tracking).
	// Configured via POSTGRES_USER / POSTGRES_PASSWORD environment variables.
	pgUser         viperutil.Value[string]
	pgPassword     viperutil.Value[string]
	pgPasswordFile viperutil.Value[string]
	// pgPasswordCached caches the file-resolved password so the hot path
	// (PgPassword()) does not touch disk. Populated by ResolvePgPassword at
	// startup; "" until then.
	pgPasswordCached string
	pgPasswordSource pgPasswordSource // file / option / env / none
	// flagSet is saved during RegisterFlags so ResolvePgPassword can use
	// pflag.Flag.Changed to distinguish "flag explicitly set" from "flag
	// at default value" — viperutil's Get() collapses both into the
	// flag's resolved value. nil when RegisterFlags has not run (e.g. in
	// tests that exercise only the env-var or file paths).
	flagSet *pflag.FlagSet

	// --- PostgreSQL TLS (multipooler → postgres leg) ---
	// libpq-style server verification. Mode + optional CA bundle. Client cert
	// auth (sslcert/sslkey) and CRLs are deferred — see MUL-383.
	pgSSLMode        viperutil.Value[string]
	pgSSLRootCert    viperutil.Value[string]
	pgSSLNegotiation viperutil.Value[string]
	pgChannelBinding viperutil.Value[string]

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
	// The rebalancer considers peak demand over this window when allocating capacity.
	// Number of buckets = DemandWindow / RebalanceInterval.
	// Example: 30s window with 10s rebalance interval = 3 buckets
	demandWindow viperutil.Value[time.Duration]

	// Inactive timeout is how long a user pool can be inactive before being garbage collected.
	inactiveTimeout viperutil.Value[time.Duration]

	// Minimum capacity per user ensures light users always have enough connections
	// for burst demand that point-in-time sampling might miss.
	minCapacityPerUser viperutil.Value[int64]

	// dialTimeout is the timeout for establishing new PostgreSQL connections.
	// Applied to net.Dialer.Timeout for all pool connections (admin, regular, reserved).
	dialTimeout viperutil.Value[time.Duration]

	// drainGracePeriod is how long to wait for in-flight connections to drain
	// during a not-serving transition before force-closing reserved connections.
	drainGracePeriod viperutil.Value[time.Duration]
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
		rebalanceInterval = 10 * time.Second
		demandWindow      = 30 * time.Second // 30s window / 10s rebalance = 3 buckets
		inactiveTimeout   = 5 * time.Minute

		// Fair share allocation - minimum per user
		// This ensures light users always have enough capacity for burst demand.
		// Set equal to initialUserPoolCapacity (10) so capacity isn't reduced
		// below the initial value until there's actual resource pressure.
		minCapacityPerUser int64 = 10

		// Dial timeout for establishing new PostgreSQL connections.
		dialTimeout = 5 * time.Second

		// Drain grace period for not-serving transitions.
		drainGracePeriod = 3 * time.Second
	)

	return &Config{
		// PostgreSQL superuser credentials (also used for internal system queries)
		pgUser: viperutil.Configure(reg, "connpool.pg.user", viperutil.Options[string]{
			Default:  constants.DefaultPostgresUser,
			FlagName: "connpool-admin-user",
			EnvVars:  []string{"CONNPOOL_ADMIN_USER", constants.PgUserEnvVar},
		}),
		pgPassword: viperutil.Configure(reg, "connpool.pg.password", viperutil.Options[string]{
			Default:  "",
			FlagName: "connpool-admin-password",
			EnvVars:  []string{"CONNPOOL_ADMIN_PASSWORD", constants.PgPasswordEnvVar},
		}),
		pgPasswordFile: viperutil.Configure(reg, "connpool.pg.password-file", viperutil.Options[string]{
			Default:  "",
			FlagName: "connpool-admin-password-file",
			EnvVars:  []string{"CONNPOOL_ADMIN_PASSWORD_FILE", constants.PgPasswordFileEnvVar},
		}),

		// PostgreSQL TLS — libpq parity. Default "prefer" mirrors libpq.
		pgSSLMode: viperutil.Configure(reg, "connpool.pg.sslmode", viperutil.Options[string]{
			Default:  string(client.SSLModePrefer),
			FlagName: "pg-client-sslmode",
		}),
		pgSSLRootCert: viperutil.Configure(reg, "connpool.pg.sslrootcert", viperutil.Options[string]{
			Default:  "",
			FlagName: "pg-client-sslrootcert",
		}),
		pgSSLNegotiation: viperutil.Configure(reg, "connpool.pg.sslnegotiation", viperutil.Options[string]{
			Default:  string(client.SSLNegotiationPostgres),
			FlagName: "pg-client-sslnegotiation",
		}),
		// Default "prefer" mirrors libpq,
		pgChannelBinding: viperutil.Configure(reg, "connpool.pg.channel-binding", viperutil.Options[string]{
			Default:  string(client.ChannelBindingPrefer),
			FlagName: "pg-client-channel-binding",
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
			EnvVars:  []string{"CONNPOOL_GLOBAL_CAPACITY"},
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
		inactiveTimeout: viperutil.Configure(reg, "connpool.inactive-timeout", viperutil.Options[time.Duration]{
			Default:  inactiveTimeout,
			FlagName: "connpool-inactive-timeout",
		}),
		minCapacityPerUser: viperutil.Configure(reg, "connpool.min-capacity-per-user", viperutil.Options[int64]{
			Default:  minCapacityPerUser,
			FlagName: "connpool-min-capacity-per-user",
		}),
		dialTimeout: viperutil.Configure(reg, "connpool.dial-timeout", viperutil.Options[time.Duration]{
			Default:  dialTimeout,
			FlagName: "connpool-dial-timeout",
		}),
		drainGracePeriod: viperutil.Configure(reg, "connpool.drain-grace-period", viperutil.Options[time.Duration]{
			Default:  drainGracePeriod,
			FlagName: "connpool-drain-grace-period",
		}),
	}
}

// RegisterFlags registers all connection pool flags with the given FlagSet.
func (c *Config) RegisterFlags(fs *pflag.FlagSet) {
	// Save the FlagSet so ResolvePgPassword can use pflag.Flag.Changed to
	// distinguish "flag explicitly set" from "flag at default value".
	c.flagSet = fs
	// PostgreSQL superuser credentials
	fs.String("connpool-admin-user", c.pgUser.Default(), "PostgreSQL superuser for admin and internal operations (env: CONNPOOL_ADMIN_USER or POSTGRES_USER)")
	fs.String("connpool-admin-password", c.pgPassword.Default(), "PostgreSQL superuser password (env: CONNPOOL_ADMIN_PASSWORD or POSTGRES_PASSWORD). --connpool-admin-password-file takes precedence.")
	fs.String("connpool-admin-password-file", c.pgPasswordFile.Default(), "Path to a file containing the PostgreSQL superuser password (plaintext, docker-library/postgres convention). Takes precedence over --connpool-admin-password (env: CONNPOOL_ADMIN_PASSWORD_FILE or POSTGRES_PASSWORD_FILE).")

	// PostgreSQL TLS (multipooler → postgres). Mirrors libpq sslmode/sslrootcert.
	fs.String("pg-client-sslmode", c.pgSSLMode.Default(), "TLS mode for connections to PostgreSQL: disable|prefer|require|verify-ca|verify-full (libpq parity; sslmode=allow is not supported)")
	fs.String("pg-client-sslrootcert", c.pgSSLRootCert.Default(), "PEM CA bundle used to verify the PostgreSQL server certificate (required for verify-ca and verify-full)")
	fs.String("pg-client-sslnegotiation", c.pgSSLNegotiation.Default(), "TLS negotiation style for connections to PostgreSQL: postgres (SSLRequest handshake) or direct (TLS-first, PostgreSQL 17+, requires sslmode require|verify-ca|verify-full)")
	fs.String("pg-client-channel-binding", c.pgChannelBinding.Default(), "SCRAM-SHA-256-PLUS channel binding for connections to PostgreSQL: disable|prefer|require (libpq parity). Only applies over TLS; require needs a TLS-enforcing sslmode")

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
	fs.Int64("connpool-global-capacity", c.globalCapacity.Default(), "Total PostgreSQL connections to manage (divided between regular and reserved pools) (env: CONNPOOL_GLOBAL_CAPACITY)")
	fs.Float64("connpool-reserved-ratio", c.reservedRatio.Default(), "Fraction of global capacity allocated to reserved pools (0.0-1.0)")

	// Rebalancer flags
	fs.Duration("connpool-rebalance-interval", c.rebalanceInterval.Default(), "How often to rebalance pool capacities")
	fs.Duration("connpool-demand-window", c.demandWindow.Default(), "Sliding window for peak demand tracking (should be multiple of rebalance-interval)")
	fs.Duration("connpool-inactive-timeout", c.inactiveTimeout.Default(), "How long a user pool can be inactive before garbage collection")
	fs.Int64("connpool-min-capacity-per-user", c.minCapacityPerUser.Default(), "Minimum connections per user (protects against aggressive capacity reduction for light users)")
	fs.Duration("connpool-dial-timeout", c.dialTimeout.Default(), "Timeout for establishing new PostgreSQL connections")
	fs.Duration("connpool-drain-grace-period", c.drainGracePeriod.Default(), "How long to wait for in-flight connections to drain during not-serving transitions before force-closing reserved connections")

	viperutil.BindFlags(fs,
		c.pgUser,
		c.pgPassword,
		c.pgPasswordFile,
		c.pgSSLMode,
		c.pgSSLRootCert,
		c.pgSSLNegotiation,
		c.pgChannelBinding,
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
		c.inactiveTimeout,
		c.minCapacityPerUser,
		c.dialTimeout,
		c.drainGracePeriod,
	)
}

// --- Getters for individual values ---

// PgUser returns the configured PostgreSQL superuser name.
// Defaults to POSTGRES_USER environment variable.
func (c *Config) PgUser() string {
	return c.pgUser.Get()
}

// PgPassword returns the resolved PostgreSQL superuser password and the
// source it was loaded from ("file" or "env"). Both are empty until
// ResolvePgPassword has run successfully; the source string is the canonical
// "is the password configured?" check — an empty source means no source
// produced a value (or Resolve was never called). Production startup in
// services/multipooler/init.go calls ResolvePgPassword first and surfaces
// its error, so callers reaching this point can treat an empty source as a
// programmer-error invariant violation.
func (c *Config) PgPassword() (string, pgPasswordSource) {
	return c.pgPasswordCached, c.pgPasswordSource
}

// ResolvePgPassword chooses the password source and caches the result so
// subsequent PgPassword() calls return it. Three independent inputs are
// considered, in strict precedence order — once a higher-precedence input is
// "explicitly set" it is authoritative and lower-precedence inputs are NOT
// consulted, even when the higher-precedence value turns out to be empty
// (those empty cases become errors instead of fallthroughs):
//
//  1. File path: --connpool-admin-password-file flag, or
//     CONNPOOL_ADMIN_PASSWORD_FILE / POSTGRES_PASSWORD_FILE env.
//     - Path explicitly empty                  → error.
//     - Path set, file content empty           → error.
//     - Path set, file content non-empty       → use it, source=File.
//  2. Flag option: --connpool-admin-password.
//     - Flag set to empty                      → error.
//     - Flag set to non-empty                  → use it, source=Option.
//  3. Env var: CONNPOOL_ADMIN_PASSWORD or POSTGRES_PASSWORD.
//     - Env set to empty                       → error.
//     - Env set to non-empty                   → use it, source=Env.
//
// Reached the end with no input explicitly set: error "not configured".
// "Explicitly set" is distinct from "viperutil resolved to empty" — viperutil
// collapses unset and empty into the same "" via os.Getenv, so we use
// os.LookupEnv and pflag.Flag.Changed to detect operator intent.
func (c *Config) ResolvePgPassword() error {
	// Row 1, 2a, 2b: file path explicitly set.
	if path, explicit, isEmpty := c.passwordFileExplicit(); explicit {
		if isEmpty {
			c.pgPasswordSource = pwSourceNone
			return errors.New("password file path is set to the empty string; unset it or provide a path")
		}
		pw, err := pgsecret.ReadPasswordFile(path)
		if err != nil {
			return err
		}
		if pw == "" {
			c.pgPasswordSource = pwSourceNone
			return fmt.Errorf("password file %q is empty", path)
		}
		c.pgPasswordCached = pw
		c.pgPasswordSource = pwSourceFile
		return nil
	}
	// Row 3, 4: --connpool-admin-password flag explicitly set.
	if c.flagSet != nil {
		if flag := c.flagSet.Lookup("connpool-admin-password"); flag != nil && flag.Changed {
			v := flag.Value.String()
			if v == "" {
				c.pgPasswordSource = pwSourceNone
				return errors.New("--connpool-admin-password is set to the empty string; unset it or provide a non-empty password")
			}
			c.pgPasswordCached = v
			c.pgPasswordSource = pwSourceOption
			return nil
		}
	}
	// Row 5, 6: env vars. CONNPOOL_ADMIN_PASSWORD checked before POSTGRES_PASSWORD.
	for _, name := range []string{"CONNPOOL_ADMIN_PASSWORD", constants.PgPasswordEnvVar} {
		if v, ok := os.LookupEnv(name); ok {
			if v == "" {
				c.pgPasswordSource = pwSourceNone
				return fmt.Errorf("env var %s is set to the empty string; unset it or provide a non-empty password", name)
			}
			c.pgPasswordCached = v
			c.pgPasswordSource = pwSourceEnv
			return nil
		}
	}
	// Row 7: no source configured.
	c.pgPasswordSource = pwSourceNone
	return errors.New("admin password not configured: set CONNPOOL_ADMIN_PASSWORD or POSTGRES_PASSWORD env var, --connpool-admin-password flag, or --connpool-admin-password-file / CONNPOOL_ADMIN_PASSWORD_FILE / POSTGRES_PASSWORD_FILE to point at a password file")
}

// passwordFileExplicit reports whether the file-path input was explicitly
// set (via flag or env var) and whether the resulting path is the empty
// string. It does NOT consider viperutil defaults — the flag's
// pflag.Flag.Changed bit and os.LookupEnv are the source of truth.
func (c *Config) passwordFileExplicit() (path string, explicit, isEmpty bool) {
	if c.flagSet != nil {
		if flag := c.flagSet.Lookup("connpool-admin-password-file"); flag != nil && flag.Changed {
			v := flag.Value.String()
			return v, true, v == ""
		}
	}
	for _, name := range []string{"CONNPOOL_ADMIN_PASSWORD_FILE", constants.PgPasswordFileEnvVar} {
		if v, ok := os.LookupEnv(name); ok {
			return v, true, v == ""
		}
	}
	return "", false, false
}

// PgSSLMode parses and returns the configured libpq-style sslmode.
// An invalid value returns the parser error so the caller can fail startup
// rather than silently downgrading to plaintext.
func (c *Config) PgSSLMode() (client.SSLMode, error) {
	return client.ParseSSLMode(c.pgSSLMode.Get())
}

// PgSSLRootCert returns the configured CA bundle path used to verify the
// PostgreSQL server certificate. Empty unless the operator set it.
func (c *Config) PgSSLRootCert() string {
	return c.pgSSLRootCert.Get()
}

// PgSSLNegotiation parses and returns the configured libpq-style
// sslnegotiation value. An invalid value returns the parser error so the
// caller can fail startup rather than silently using the default.
func (c *Config) PgSSLNegotiation() (client.SSLNegotiation, error) {
	return client.ParseSSLNegotiation(c.pgSSLNegotiation.Get())
}

// PgChannelBinding parses and returns the configured libpq-style
// channel_binding value. An invalid value returns the parser error so the
// caller can fail startup rather than silently using the default.
func (c *Config) PgChannelBinding() (client.ChannelBindingMode, error) {
	return client.ParseChannelBinding(c.pgChannelBinding.Get())
}

// ValidatePGSSL checks the libpq-style sslmode + sslrootcert flags at startup
// so a typo or missing CA bundle aborts the multipooler before the connection
// pool manager opens — preventing a silent downgrade to plaintext.
//
// host is the address the multipooler will dial postgres on (only used to
// validate verify-full). Pass an empty host when the multipooler is configured
// for a Unix socket; a socket never runs TLS, so only channel_binding=require
// (which cannot be satisfied without TLS) is validated in that case.
func (c *Config) ValidatePGSSL(host string) error {
	if host == "" {
		cb, err := c.PgChannelBinding()
		if err != nil {
			return fmt.Errorf("--pg-client-channel-binding: %w", err)
		}
		if cb == client.ChannelBindingRequire {
			return errors.New("--pg-client-channel-binding=require cannot be satisfied over a Unix socket (channel binding needs TLS); use a TCP connection with a TLS-enabled --pg-client-sslmode")
		}
		return nil
	}
	mode, err := client.ParseSSLMode(c.pgSSLMode.Get())
	if err != nil {
		return fmt.Errorf("--pg-client-sslmode: %w", err)
	}
	if _, err := client.BuildTLSConfig(mode, c.pgSSLRootCert.Get(), host); err != nil {
		return fmt.Errorf("--pg-client-sslmode=%s: %w", mode, err)
	}
	negotiation, err := client.ParseSSLNegotiation(c.pgSSLNegotiation.Get())
	if err != nil {
		return fmt.Errorf("--pg-client-sslnegotiation: %w", err)
	}
	if err := client.ValidateSSLNegotiation(negotiation, mode); err != nil {
		return fmt.Errorf("--pg-client-sslnegotiation=%s: %w", negotiation, err)
	}
	cb, err := client.ParseChannelBinding(c.pgChannelBinding.Get())
	if err != nil {
		return fmt.Errorf("--pg-client-channel-binding: %w", err)
	}
	// channel_binding=require needs TLS to bind to.
	if cb == client.ChannelBindingRequire && !mode.AttemptsTLS() {
		return fmt.Errorf("--pg-client-channel-binding=require needs a TLS-enabled --pg-client-sslmode (got %q)", mode)
	}
	return nil
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
// The rebalancer considers peak demand over this window when allocating capacity.
func (c *Config) DemandWindow() time.Duration {
	return c.demandWindow.Get()
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

// DialTimeout returns the timeout for establishing new PostgreSQL connections.
func (c *Config) DialTimeout() time.Duration {
	return c.dialTimeout.Get()
}

// DrainGracePeriod returns how long to wait for in-flight connections to drain
// during not-serving transitions before force-closing reserved connections.
func (c *Config) DrainGracePeriod() time.Duration {
	return c.drainGracePeriod.Get()
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
	mgr.setLifecycle(lifecycleClosed) // Manager is closed until Open() is called
	return mgr
}
