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

// Package schematracker computes the schema token a primary pooler publishes on
// its health stream. The token is derived from a canonical dump of the user
// schema and changes when the schema does, so a gateway can reload its catalog
// cache when it sees a pooler's token change.
//
// The token is opaque and comparable only with earlier tokens from the same
// pooler. How it is computed is internal to this package and may change between
// releases. Cross-shard schema agreement is the gateway's job, not the token's.
//
// Detection is by polling: there is no event trigger, since the NOTIFY a trigger
// would send makes PREPARE TRANSACTION fail for any transaction containing DDL.
//
// TODO: recompute the token as soon as a transaction that ran a schema-changing
// statement commits, instead of waiting for the next interval. The pooler sees
// every statement's CommandComplete tag (CREATE TABLE, ALTER TABLE, DROP ...,
// GRANT), so it can mark a session's transaction as having changed the schema
// and trigger a recomputation when that transaction ends in COMMIT (or COMMIT
// PREPARED), or right after the statement in autocommit. It must wait for the
// commit, because DDL is transactional: computed earlier, on another
// connection, the token would not yet see the change. The interval stays as the
// backstop for what tags cannot see: DDL run directly against postgres, and DDL
// inside function bodies (SELECT f(), CALL, DO).
package schematracker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
	"github.com/multigres/multigres/go/services/multipooler/internal/switcher"
	"github.com/multigres/multigres/go/tools/timer"
)

// DefaultInterval is how often the token is recomputed when no interval is
// configured.
const DefaultInterval = time.Minute

// maxComputeTimeout bounds one token computation, so a slow catalog query
// cannot hold up the next one indefinitely on a short interval.
const maxComputeTimeout = 30 * time.Second

// Tracker computes the schema token while this pooler is the writable leader
// and publishes it through the callback it was built with. Only the primary
// accepts DDL, so a follower has nothing to track and publishes an empty token.
// It wraps a switcher.RoleSwitcher the same way replicationstats.Tracker does.
type Tracker struct {
	poller *poller
	sw     *switcher.RoleSwitcher
}

// NewTracker returns a Tracker that recomputes the token every interval (zero
// or negative selects DefaultInterval) and calls publish whenever the token
// changes, including to "" when tracking stops. publish must not block.
func NewTracker(
	queryService executor.InternalQueryService,
	logger *slog.Logger,
	interval time.Duration,
	publish func(token string),
) *Tracker {
	if interval <= 0 {
		interval = DefaultInterval
	}
	p := &poller{
		queryService: queryService,
		logger:       logger.With("component", "schema_tracker"),
		interval:     interval,
		publish:      publish,
		metrics:      newMetrics(logger),
		runner:       timer.NewPeriodicRunner(context.TODO(), interval),
	}
	return &Tracker{poller: p, sw: switcher.NewRoleSwitcher(p, switcher.NoOp{})}
}

// OnStateChange tracks the schema while this pooler is the writable leader and
// stops otherwise. Satisfies manager.StateAware structurally.
func (t *Tracker) OnStateChange(ctx context.Context, state servingstate.State) error {
	return t.sw.OnStateChange(ctx, state)
}

// Close stops tracking for good and publishes an empty token.
func (t *Tracker) Close() {
	t.sw.Close()
}

// Token returns the token last published: empty while not tracking, before the
// first computation, or after a computation failed.
func (t *Tracker) Token() string {
	return t.poller.currentToken()
}

// poller is the switcher.Toggleable that runs while the pooler is writable.
type poller struct {
	queryService executor.InternalQueryService
	logger       *slog.Logger
	interval     time.Duration
	publish      func(token string)
	metrics      *metrics
	runner       *timer.PeriodicRunner

	mu    sync.Mutex
	token string
}

var _ switcher.Toggleable = (*poller)(nil)

// Open starts computing the token, the first time without waiting an interval.
func (p *poller) Open() {
	p.runner.StartWithOptions(p.poll, timer.WithFastStart())
}

// Close stops computing the token and publishes an empty one. Stop waits out a
// computation in flight, so it cannot publish over the empty token afterwards.
func (p *poller) Close() {
	p.runner.Stop()
	p.setToken("")
}

// IsOpen reports whether the token is being computed.
func (p *poller) IsOpen() bool {
	return p.runner.Running()
}

func (p *poller) poll(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, min(p.interval, maxComputeTimeout))
	defer cancel()

	token, err := computeToken(ctx, p.queryService)
	if err != nil {
		// An empty token never equals another, so a consumer treats the next
		// successful computation as a change and reloads, whatever happened to
		// the schema in between.
		p.logger.WarnContext(ctx, "schema token computation failed", "error", err)
		p.metrics.recordError(ctx)
		p.setToken("")
		return
	}
	if p.setToken(token) {
		p.logger.InfoContext(ctx, "schema token changed", "token", token)
		p.metrics.recordChange(ctx)
	}
}

// setToken records token and publishes it if it differs from the last one,
// reporting whether it did.
func (p *poller) setToken(token string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if token == p.token {
		return false
	}
	p.token = token
	p.publish(token)
	return true
}

func (p *poller) currentToken() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.token
}
