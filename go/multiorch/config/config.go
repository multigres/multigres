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

package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/viperutil"
)

// WatchTarget represents a target to watch in the format:
// - "database" - watch entire database
// - "database/tablegroup" - watch specific tablegroup
// - "database/tablegroup/shard" - watch specific shard
type WatchTarget struct {
	Database   string
	TableGroup string // empty if watching entire database
	Shard      string // empty if watching database or tablegroup level
}

// String returns the string representation of the target.
func (t WatchTarget) String() string {
	if t.Shard != "" {
		return fmt.Sprintf("%s/%s/%s", t.Database, t.TableGroup, t.Shard)
	}
	if t.TableGroup != "" {
		return fmt.Sprintf("%s/%s", t.Database, t.TableGroup)
	}
	return t.Database
}

// MatchesDatabase returns true if this target watches the given database.
func (t WatchTarget) MatchesDatabase(db string) bool {
	return t.Database == db
}

// MatchesTableGroup returns true if this target watches the given database/tablegroup.
// Returns true if watching entire database or specific tablegroup.
func (t WatchTarget) MatchesTableGroup(db, tablegroup string) bool {
	if t.Database != db {
		return false
	}
	// Watching entire database matches all tablegroups
	if t.TableGroup == "" {
		return true
	}
	return t.TableGroup == tablegroup
}

// MatchesShard returns true if this target watches the given database/tablegroup/shard.
// Returns true if watching entire database, entire tablegroup, or specific shard.
func (t WatchTarget) MatchesShard(db, tablegroup, shard string) bool {
	if t.Database != db {
		return false
	}
	// Watching entire database matches all shards
	if t.TableGroup == "" {
		return true
	}
	if t.TableGroup != tablegroup {
		return false
	}
	// Watching entire tablegroup matches all shards
	if t.Shard == "" {
		return true
	}
	return t.Shard == shard
}

// ParseShardWatchTarget parses a shard watch target string.
// Format: "database" or "database/tablegroup" or "database/tablegroup/shard"
//
// Validation rules:
// - Database is always required
// - If shard is provided, tablegroup must be provided
// - Empty parts are not allowed
func ParseShardWatchTarget(s string) (WatchTarget, error) {
	if s == "" {
		return WatchTarget{}, fmt.Errorf("empty shard watch target")
	}

	parts := strings.Split(s, "/")
	if len(parts) > 3 {
		return WatchTarget{}, fmt.Errorf("invalid shard watch target format: %s (expected db, db/tablegroup, or db/tablegroup/shard)", s)
	}

	// Database is always required
	if parts[0] == "" {
		return WatchTarget{}, fmt.Errorf("database cannot be empty")
	}

	target := WatchTarget{Database: parts[0]}

	// Validate and set tablegroup if provided
	if len(parts) >= 2 {
		if parts[1] == "" {
			return WatchTarget{}, fmt.Errorf("tablegroup cannot be empty when specified")
		}
		target.TableGroup = parts[1]
	}

	// Validate and set shard if provided
	if len(parts) == 3 {
		if parts[2] == "" {
			return WatchTarget{}, fmt.Errorf("shard cannot be empty when specified")
		}
		if target.TableGroup == "" {
			return WatchTarget{}, fmt.Errorf("tablegroup must be specified when shard is provided")
		}
		target.Shard = parts[2]
	}

	return target, nil
}

// ParseShardWatchTargets parses multiple shard watch target strings.
func ParseShardWatchTargets(targets []string) ([]WatchTarget, error) {
	result := make([]WatchTarget, 0, len(targets))
	for _, t := range targets {
		parsed, err := ParseShardWatchTarget(t)
		if err != nil {
			return nil, err
		}
		result = append(result, parsed)
	}
	return result, nil
}

// Config encapsulates all multiorch configuration.
// This is passed to the recovery engine and other components.
type Config struct {
	cell                           viperutil.Value[string]
	shardWatchTargets              viperutil.Value[[]string]
	bookkeepingInterval            viperutil.Value[time.Duration]
	clusterMetadataRefreshInterval viperutil.Value[time.Duration]
	clusterMetadataRefreshTimeout  viperutil.Value[time.Duration]
	poolerHealthCheckInterval      viperutil.Value[time.Duration]
	healthCheckWorkers             viperutil.Value[int]
}

// Constants
const (
	// HealthCheckQueueCapacity is the maximum number of poolers that can be queued
	// for health checking before blocking.
	HealthCheckQueueCapacity = 100000
)

// NewConfig creates a new Config with all viperutil values configured.
func NewConfig(reg *viperutil.Registry) *Config {
	return &Config{
		cell: viperutil.Configure(reg, "cell", viperutil.Options[string]{
			Default:  "",
			FlagName: "cell",
			Dynamic:  false,
			EnvVars:  []string{"MT_CELL"},
		}),
		shardWatchTargets: viperutil.Configure(reg, "watch-targets", viperutil.Options[[]string]{
			FlagName: "watch-targets",
			Dynamic:  true,
			EnvVars:  []string{"MT_SHARD_WATCH_TARGETS"},
		}),
		bookkeepingInterval: viperutil.Configure(reg, "bookkeeping-interval", viperutil.Options[time.Duration]{
			Default:  1 * time.Minute,
			FlagName: "bookkeeping-interval",
			Dynamic:  false,
			EnvVars:  []string{"MT_BOOKKEEPING_INTERVAL"},
		}),
		clusterMetadataRefreshInterval: viperutil.Configure(reg, "cluster-metadata-refresh-interval", viperutil.Options[time.Duration]{
			Default:  15 * time.Second,
			FlagName: "cluster-metadata-refresh-interval",
			Dynamic:  false,
			EnvVars:  []string{"MT_CLUSTER_METADATA_REFRESH_INTERVAL"},
		}),
		clusterMetadataRefreshTimeout: viperutil.Configure(reg, "cluster-metadata-refresh-timeout", viperutil.Options[time.Duration]{
			Default:  30 * time.Second,
			FlagName: "cluster-metadata-refresh-timeout",
			Dynamic:  false,
			EnvVars:  []string{"MT_CLUSTER_METADATA_REFRESH_TIMEOUT"},
		}),
		poolerHealthCheckInterval: viperutil.Configure(reg, "pooler-health-check-interval", viperutil.Options[time.Duration]{
			Default:  5 * time.Second,
			FlagName: "pooler-health-check-interval",
			Dynamic:  true,
			EnvVars:  []string{"MT_POOLER_HEALTH_CHECK_INTERVAL"},
		}),
		healthCheckWorkers: viperutil.Configure(reg, "health-check-workers", viperutil.Options[int]{
			Default:  300,
			FlagName: "health-check-workers",
			Dynamic:  false,
			EnvVars:  []string{"MT_HEALTH_CHECK_WORKERS"},
		}),
	}
}

// Getter methods

func (c *Config) GetCell() string {
	return c.cell.Get()
}

func (c *Config) GetShardWatchTargets() []string {
	return c.shardWatchTargets.Get()
}

func (c *Config) GetBookkeepingInterval() time.Duration {
	return c.bookkeepingInterval.Get()
}

func (c *Config) GetClusterMetadataRefreshInterval() time.Duration {
	return c.clusterMetadataRefreshInterval.Get()
}

func (c *Config) GetClusterMetadataRefreshTimeout() time.Duration {
	return c.clusterMetadataRefreshTimeout.Get()
}

func (c *Config) GetPoolerHealthCheckInterval() time.Duration {
	return c.poolerHealthCheckInterval.Get()
}

func (c *Config) GetHealthCheckWorkers() int {
	return c.healthCheckWorkers.Get()
}

// Defaults for flags (used in RegisterFlags)

func (c *Config) DefaultCell() string {
	return c.cell.Default()
}

func (c *Config) DefaultShardWatchTargets() []string {
	return c.shardWatchTargets.Default()
}

func (c *Config) DefaultBookkeepingInterval() time.Duration {
	return c.bookkeepingInterval.Default()
}

func (c *Config) DefaultClusterMetadataRefreshInterval() time.Duration {
	return c.clusterMetadataRefreshInterval.Default()
}

func (c *Config) DefaultClusterMetadataRefreshTimeout() time.Duration {
	return c.clusterMetadataRefreshTimeout.Default()
}

func (c *Config) DefaultPoolerHealthCheckInterval() time.Duration {
	return c.poolerHealthCheckInterval.Default()
}

func (c *Config) DefaultHealthCheckWorkers() int {
	return c.healthCheckWorkers.Default()
}

// RegisterFlags registers the config flags with pflag.
func (c *Config) RegisterFlags(fs *pflag.FlagSet) {
	fs.String("cell", c.DefaultCell(), "cell to use")
	fs.StringSlice("watch-targets", c.DefaultShardWatchTargets(), "list of db/tablegroup/shard targets to watch")
	fs.Duration("bookkeeping-interval", c.DefaultBookkeepingInterval(), "interval for bookkeeping tasks")
	fs.Duration("cluster-metadata-refresh-interval", c.DefaultClusterMetadataRefreshInterval(), "interval for refreshing cluster metadata from topology")
	fs.Duration("cluster-metadata-refresh-timeout", c.DefaultClusterMetadataRefreshTimeout(), "timeout for cluster metadata refresh operation")
	fs.Duration("pooler-health-check-interval", c.DefaultPoolerHealthCheckInterval(), "interval between health checks for a single pooler")
	fs.Int("health-check-workers", c.DefaultHealthCheckWorkers(), "number of concurrent workers polling pooler health")
	viperutil.BindFlags(fs,
		c.cell,
		c.shardWatchTargets,
		c.bookkeepingInterval,
		c.clusterMetadataRefreshInterval,
		c.clusterMetadataRefreshTimeout,
		c.poolerHealthCheckInterval,
		c.healthCheckWorkers)
}

// Test helper functions

// NewTestConfig creates a Config for testing with optional custom values.
func NewTestConfig(opts ...func(*Config)) *Config {
	reg := viperutil.NewRegistry()
	cfg := NewConfig(reg)
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// WithCell sets the cell value for testing.
func WithCell(cell string) func(*Config) {
	return func(cfg *Config) {
		cfg.cell.Set(cell)
	}
}

// WithClusterMetadataRefreshTimeout sets the cluster metadata refresh timeout for testing.
func WithClusterMetadataRefreshTimeout(d time.Duration) func(*Config) {
	return func(cfg *Config) {
		cfg.clusterMetadataRefreshTimeout.Set(d)
	}
}

// WithBookkeepingInterval sets the bookkeeping interval for testing.
func WithBookkeepingInterval(d time.Duration) func(*Config) {
	return func(cfg *Config) {
		cfg.bookkeepingInterval.Set(d)
	}
}

// WithClusterMetadataRefreshInterval sets the cluster metadata refresh interval for testing.
func WithClusterMetadataRefreshInterval(d time.Duration) func(*Config) {
	return func(cfg *Config) {
		cfg.clusterMetadataRefreshInterval.Set(d)
	}
}

// WithPoolerHealthCheckInterval sets the pooler health check interval for testing.
func WithPoolerHealthCheckInterval(d time.Duration) func(*Config) {
	return func(cfg *Config) {
		cfg.poolerHealthCheckInterval.Set(d)
	}
}

// WithHealthCheckWorkers sets the number of health check workers for testing.
func WithHealthCheckWorkers(n int) func(*Config) {
	return func(cfg *Config) {
		cfg.healthCheckWorkers.Set(n)
	}
}
