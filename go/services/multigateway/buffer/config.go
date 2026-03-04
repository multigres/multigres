// Copyright 2026 Supabase, Inc.
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

package buffer

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/tools/viperutil"
)

// Config holds all configuration for failover buffering.
type Config struct {
	Enabled                 viperutil.Value[bool]
	Window                  viperutil.Value[time.Duration]
	Size                    viperutil.Value[int]
	MaxFailoverDuration     viperutil.Value[time.Duration]
	MinTimeBetweenFailovers viperutil.Value[time.Duration]
	DrainConcurrency        viperutil.Value[int]
}

// NewConfig creates a Config with all flags registered in the given registry.
func NewConfig(reg *viperutil.Registry) *Config {
	return &Config{
		Enabled: viperutil.Configure(reg, "buffer-enabled", viperutil.Options[bool]{
			Default:  false,
			FlagName: "buffer-enabled",
			Dynamic:  false,
			EnvVars:  []string{"MT_BUFFER_ENABLED"},
		}),
		Window: viperutil.Configure(reg, "buffer-window", viperutil.Options[time.Duration]{
			Default:  10 * time.Second,
			FlagName: "buffer-window",
			Dynamic:  false,
			EnvVars:  []string{"MT_BUFFER_WINDOW"},
		}),
		Size: viperutil.Configure(reg, "buffer-size", viperutil.Options[int]{
			Default:  1000,
			FlagName: "buffer-size",
			Dynamic:  false,
			EnvVars:  []string{"MT_BUFFER_SIZE"},
		}),
		MaxFailoverDuration: viperutil.Configure(reg, "buffer-max-failover-duration", viperutil.Options[time.Duration]{
			Default:  20 * time.Second,
			FlagName: "buffer-max-failover-duration",
			Dynamic:  false,
			EnvVars:  []string{"MT_BUFFER_MAX_FAILOVER_DURATION"},
		}),
		MinTimeBetweenFailovers: viperutil.Configure(reg, "buffer-min-time-between-failovers", viperutil.Options[time.Duration]{
			Default:  1 * time.Minute,
			FlagName: "buffer-min-time-between-failovers",
			Dynamic:  false,
			EnvVars:  []string{"MT_BUFFER_MIN_TIME_BETWEEN_FAILOVERS"},
		}),
		DrainConcurrency: viperutil.Configure(reg, "buffer-drain-concurrency", viperutil.Options[int]{
			Default:  1,
			FlagName: "buffer-drain-concurrency",
			Dynamic:  false,
			EnvVars:  []string{"MT_BUFFER_DRAIN_CONCURRENCY"},
		}),
	}
}

// RegisterFlags registers buffer flags on the given FlagSet.
func (c *Config) RegisterFlags(fs *pflag.FlagSet) {
	fs.Bool("buffer-enabled", c.Enabled.Default(), "Enable failover buffering")
	fs.Duration("buffer-window", c.Window.Default(), "Max time a request stays buffered")
	fs.Int("buffer-size", c.Size.Default(), "Max total buffered requests (global)")
	fs.Duration("buffer-max-failover-duration", c.MaxFailoverDuration.Default(), "Stop buffering if failover exceeds this")
	fs.Duration("buffer-min-time-between-failovers", c.MinTimeBetweenFailovers.Default(), "Min gap between failovers per shard")
	fs.Int("buffer-drain-concurrency", c.DrainConcurrency.Default(), "Parallel requests retried during drain")
	viperutil.BindFlags(fs,
		c.Enabled,
		c.Window,
		c.Size,
		c.MaxFailoverDuration,
		c.MinTimeBetweenFailovers,
		c.DrainConcurrency,
	)
}

// Validate checks that the configuration values are consistent.
func (c *Config) Validate() error {
	if !c.Enabled.Get() {
		return nil
	}
	if c.Size.Get() < 1 {
		return fmt.Errorf("buffer-size must be >= 1, got %d", c.Size.Get())
	}
	if c.Window.Get() <= 0 {
		return fmt.Errorf("buffer-window must be > 0, got %s", c.Window.Get())
	}
	if c.MaxFailoverDuration.Get() <= 0 {
		return fmt.Errorf("buffer-max-failover-duration must be > 0, got %s", c.MaxFailoverDuration.Get())
	}
	if c.MinTimeBetweenFailovers.Get() < 0 {
		return fmt.Errorf("buffer-min-time-between-failovers must be >= 0, got %s", c.MinTimeBetweenFailovers.Get())
	}
	if c.DrainConcurrency.Get() < 1 {
		return fmt.Errorf("buffer-drain-concurrency must be >= 1, got %d", c.DrainConcurrency.Get())
	}
	if c.Window.Get() > c.MaxFailoverDuration.Get() {
		return fmt.Errorf("buffer-window (%s) must be <= buffer-max-failover-duration (%s)",
			c.Window.Get(), c.MaxFailoverDuration.Get())
	}
	return nil
}
