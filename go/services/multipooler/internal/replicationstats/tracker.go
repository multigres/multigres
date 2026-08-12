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

package replicationstats

import (
	"context"
	"log/slog"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
	"github.com/multigres/multigres/go/services/multipooler/internal/switcher"
)

// Tracker runs a Poller only while this pooler is the writable leader —
// only the primary has active logical-replication walsenders in
// pg_stat_replication. It wraps switcher.RoleSwitcher (poller vs. a no-op
// secondary) internally, the same shape heartbeat.ReplTracker uses for its
// writer/reader pair, so callers depend only on Tracker's Open/Close-style
// lifecycle and never need to know switcher.RoleSwitcher exists.
type Tracker struct {
	poller  *Poller
	metrics *Metrics
	sw      *switcher.RoleSwitcher
}

// NewTracker creates a Tracker wrapping a new Poller.
func NewTracker(queryService executor.InternalQueryService, metrics *Metrics, logger *slog.Logger, intervalMs int) *Tracker {
	poller := NewPoller(queryService, metrics, logger, intervalMs)
	return &Tracker{poller: poller, metrics: metrics, sw: switcher.NewRoleSwitcher(poller, switcher.NoOp{})}
}

// Poller returns the underlying poller, for callers (tests, status
// accessors) that need to inspect poll counts or IsOpen() directly.
func (t *Tracker) Poller() *Poller {
	return t.poller
}

// OnStateChange starts the poller while this pooler is the writable leader
// and stops it otherwise. Satisfies manager.StateAware structurally.
func (t *Tracker) OnStateChange(ctx context.Context, state servingstate.State) error {
	return t.sw.OnStateChange(ctx, state)
}

// Close stops the poller regardless of whether it was currently running,
// and unregisters its OTel callback for good — unlike Poller.Close (which
// only pauses reporting across a leadership change), this Tracker will
// never be reused. Callers on a real shutdown or connection-reopen path
// must call this explicitly — see switcher.RoleSwitcher.Close.
func (t *Tracker) Close() {
	t.sw.Close()
	_ = t.metrics.Close()
}
