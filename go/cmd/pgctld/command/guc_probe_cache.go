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
// max_connections is the only probed GUC today; the reserved-connections GUCs
// (superuser_reserved_connections, reserved_connections) are the expected
// next entries, which would let the multipooler's capacity-seed fallback drop
// its hardcoded PostgreSQL defaults.
type gucProbeCache struct {
	logger *slog.Logger
	// probe runs one GUC lookup and is replaceable in tests. The default
	// forks `postgres -C`.
	probe func(ctx context.Context, name string) (int32, error)

	mu  sync.Mutex
	gen uint64
	// values holds probed results; negative marks a cached failure ("known
	// unknown" — do not re-probe until the next invalidation).
	values map[string]int32
}

func newGucProbeCache(logger *slog.Logger) *gucProbeCache {
	return &gucProbeCache{
		logger: logger,
		probe:  probePostgresGuc,
		values: make(map[string]int32),
	}
}

// probePostgresGuc runs `postgres -C <name>` against the data directory and
// parses the value as a positive int32.
func probePostgresGuc(ctx context.Context, name string) (int32, error) {
	probeCtx, cancel := context.WithTimeout(ctx, gucProbeTimeout)
	defer cancel()
	out, err := executil.Command(probeCtx, "postgres", "-C", name, "-D", pgctld.PostgresDataDir()).Output()
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 32)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("unexpected postgres -C %s output %q: %w", name, strings.TrimSpace(string(out)), err)
	}
	return int32(v), nil
}

// get returns the effective configured value of the named GUC, or 0 (unknown)
// before the data directory is initialized or when the value cannot be
// determined.
func (c *gucProbeCache) get(ctx context.Context, name string) int32 {
	c.mu.Lock()
	v, ok := c.values[name]
	gen := c.gen
	c.mu.Unlock()
	if ok {
		return max(v, 0)
	}
	if !pgctld.IsDataDirInitialized() {
		return 0
	}

	result := int32(-1)
	if v, err := c.probe(ctx, name); err != nil {
		c.logger.WarnContext(ctx, "could not determine effective GUC value; reporting unknown until the next start/restart/reload",
			"guc", name, "error", err)
	} else {
		result = v
	}

	// Publish only if no invalidation ran while the probe was in flight; a
	// discarded result leaves the cache empty for the next poll to retry.
	c.mu.Lock()
	if c.gen == gen {
		c.values[name] = result
	}
	c.mu.Unlock()
	return max(result, 0)
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
}
