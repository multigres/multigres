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

package command

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/tools/executil"
)

// gucProbeTimeout caps a single `postgres -C` probe. The probe runs under the
// caller's context additionally capped at this deadline (whichever ends
// first), so a hung probe (e.g. a stuck volume) cannot wedge the Status RPC
// even when the caller's context carries no deadline of its own — the
// multipooler's monitor polls Status with a cancel-only context.
const gucProbeTimeout = 5 * time.Second

// gucFailureRetryInterval bounds how long a failed probe is cached before the
// next get retries it. Failures must be cached (Status is polled every ~5s;
// forking a doomed probe per poll would be wasteful) but not until the next
// config mutation: a transiently failed probe after a restart would otherwise
// pin a stale value in downstream consumers — e.g. the multipooler's seed
// budget carrying a pre-restart reserve next to a post-restart
// max_connections — for an unbounded window.
const gucFailureRetryInterval = time.Minute

// gucProbeCache caches effective numeric GUC values probed via
// `postgres -C <name>`, which resolves include directives and
// postgresql.auto.conf and works whether or not the server is running.
// Status sits on the multipooler's ~5s monitoring poll, so results — failures
// included — are cached until invalidated: a persistent failure must not fork
// postgres on every poll.
//
// invalidate is deferred by every pgctld operation that can change the
// effective config (InitDataDir, Start, Restart, ReloadConfig, PgRewind — the
// last copies the source cluster's postgresql.conf), so it runs after the
// operation completes. The generation counter orders probe publication
// against invalidation: a probe captures the generation before forking and
// publishes only if no invalidation happened in between — otherwise a probe
// overlapping e.g. a Restart could store the pre-restart value after the
// restart's invalidation ran, pinning a stale result until the next mutation.
//
// Probed GUCs: max_connections and the reserved-connections pair
// (superuser_reserved_connections, reserved_connections), which let the
// multipooler's capacity-seed fallback use the server's actual reserves
// instead of hardcoded PostgreSQL defaults.
type gucProbeCache struct {
	logger *slog.Logger
	// probe runs one GUC lookup and is replaceable in tests. The default
	// forks `postgres -C`.
	probe func(ctx context.Context, name string) (int32, error)
	// now is replaceable in tests to exercise the failure-retry interval.
	now func() time.Time

	mu  sync.Mutex
	gen uint64
	// values holds successfully probed results (>= 0).
	values map[string]int32
	// failures records when a probe last failed; the failure is honored as
	// "known unknown" (no re-probe) until gucFailureRetryInterval elapses or
	// the next invalidation, whichever comes first.
	failures map[string]time.Time
}

func newGucProbeCache(logger *slog.Logger) *gucProbeCache {
	return &gucProbeCache{
		logger:   logger,
		probe:    probePostgresGuc,
		now:      time.Now,
		values:   make(map[string]int32),
		failures: make(map[string]time.Time),
	}
}

// probePostgresGuc runs `postgres -C <name>` against the data directory and
// parses the value as a non-negative int32 (0 is a legitimate configured
// value for e.g. reserved_connections).
func probePostgresGuc(ctx context.Context, name string) (int32, error) {
	probeCtx, cancel := context.WithTimeout(ctx, gucProbeTimeout)
	defer cancel()
	out, err := executil.Command(probeCtx, "postgres", "-C", name, "-D", pgctld.PostgresDataDir()).Output()
	if err != nil {
		return 0, err
	}
	trimmed := strings.TrimSpace(string(out))
	v, err := strconv.ParseInt(trimmed, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("unexpected postgres -C %s output %q: %w", name, trimmed, err)
	}
	if v < 0 {
		return 0, fmt.Errorf("unexpected negative postgres -C %s output %q", name, trimmed)
	}
	return int32(v), nil
}

// get returns the effective configured value of the named GUC and whether it
// is known. Unknown (false) before the data directory is initialized or when
// the value cannot be determined; 0 with known=true is a legitimate
// configured value (e.g. reserved_connections).
func (c *gucProbeCache) get(ctx context.Context, name string) (int32, bool) {
	v, ok := c.getAll(ctx, name)[name]
	return v, ok
}

// getAll returns the known values among the named GUCs (absent map entry =
// unknown). All probes for cold names run concurrently under one shared
// generation snapshot, and the whole batch — cached values included — is
// discarded when an invalidation lands mid-call: the caller then sees every
// GUC as unknown rather than a response mixing values from two configs (a
// probe that ran across the mutation may have read the new conf while the
// cached entries predate it). Worst-case blocking stays one gucProbeTimeout
// regardless of how many GUCs are cold.
func (c *gucProbeCache) getAll(ctx context.Context, names ...string) map[string]int32 {
	known := make(map[string]int32, len(names))
	var missing []string
	c.mu.Lock()
	gen := c.gen
	now := c.now()
	for _, name := range names {
		if v, ok := c.values[name]; ok {
			known[name] = v
			continue
		}
		if failedAt, ok := c.failures[name]; ok && now.Sub(failedAt) < gucFailureRetryInterval {
			continue // known unknown; retry only after the interval
		}
		missing = append(missing, name)
	}
	c.mu.Unlock()
	if len(missing) == 0 || !pgctld.IsDataDirInitialized() {
		return known
	}

	results := make([]int32, len(missing))
	var wg sync.WaitGroup
	for i, name := range missing {
		wg.Go(func() {
			results[i] = -1
			if v, err := c.probe(ctx, name); err != nil {
				c.logger.WarnContext(ctx, "could not determine effective GUC value; retrying after the next start/restart/reload or a bounded interval",
					"guc", name, "error", err)
			} else {
				results[i] = v
			}
		})
	}
	wg.Wait()

	c.mu.Lock()
	if c.gen != gen {
		// An invalidation raced the probes: the probe results may reflect
		// the new config while the cached values reflect the old one.
		// Publish nothing and report everything unknown; the next poll
		// re-probes under the new generation.
		c.mu.Unlock()
		return map[string]int32{}
	}
	for i, name := range missing {
		if results[i] >= 0 {
			c.values[name] = results[i]
			delete(c.failures, name)
		} else {
			c.failures[name] = now
		}
	}
	c.mu.Unlock()
	for i, name := range missing {
		if results[i] >= 0 {
			known[name] = results[i]
		}
	}
	return known
}

// invalidate drops every cached value so the next get recomputes it, and
// bumps the generation so in-flight probes discard their results. Deferred by
// config-changing operations so it runs after the operation completes — a
// probe racing the operation must not re-cache a mid-flight value.
func (c *gucProbeCache) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gen++
	clear(c.values)
	clear(c.failures)
}
