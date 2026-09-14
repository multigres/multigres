// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// Coordinator drives table migrations on the shard primary: it persists state in
// the multigres.migration table (Store), runs target-side SQL locally through
// the admin pool (target), and source-side SQL over the operator-supplied DSN
// (source). It is active only when its multipooler is the shard primary; the
// caller (the migration gRPC service and the become-primary reconcile hook) is
// responsible for that gating.
type Coordinator struct {
	store  *Store
	target *target
	logger *slog.Logger

	// targetConnInfo builds a libpq conninfo the external source can use to reach
	// this (target) Postgres, for the reverse subscription in EXPORT direction.
	// May be nil, in which case EXPORT is unavailable.
	targetConnInfo func(database string) (string, error)

	now func() time.Time

	// mu serializes state transitions so concurrent operator calls on the same
	// migration cannot interleave phase writes.
	mu sync.Mutex
}

// NewCoordinator builds a Coordinator over the given admin query service.
// targetConnInfo builds the target-reachable conninfo used for the EXPORT-side
// reverse subscription; pass nil if EXPORT is not supported in this deployment.
func NewCoordinator(qs executor.InternalQueryService, logger *slog.Logger, targetConnInfo func(database string) (string, error)) *Coordinator {
	return &Coordinator{
		store:          NewStore(qs),
		target:         newTarget(qs),
		logger:         logger,
		targetConnInfo: targetConnInfo,
		now:            time.Now,
	}
}

// EnsureSchema creates the migration table if absent (covers shards bootstrapped
// before the table existed). Safe to call on every coordinator start.
func (c *Coordinator) EnsureSchema(ctx context.Context) error {
	return c.store.EnsureSchema(ctx)
}

// CreateParams is the input to CreateMigration.
type CreateParams struct {
	SourceDSN      string
	TargetDatabase string
	TargetShard    string
	Tables         []string
	SequenceMargin int64
}

// CreateMigration records a new migration (phase CREATED). It makes no changes
// to either database, but it does validate the source read-only up front —
// reachability, wal_level, and a usable replica identity per table — so an
// unusable source is rejected at create time rather than at start.
func (c *Coordinator) CreateMigration(ctx context.Context, p CreateParams) (*Projection, error) {
	if p.SourceDSN == "" {
		return nil, errors.New("source DSN is required")
	}
	if p.TargetDatabase == "" {
		return nil, errors.New("target database is required")
	}
	if len(p.Tables) == 0 {
		return nil, errors.New("at least one table is required")
	}

	// Validate the source read-only before recording anything (no DB changes).
	// Validate also resolves "*"/"schema.*" wildcards to the concrete owned tables.
	src, err := newSource(ctx, p.SourceDSN)
	if err != nil {
		return nil, err
	}
	defer src.close()
	info, resolvedTables, warnings, err := src.Validate(p.Tables)
	if err != nil {
		return nil, err
	}
	for _, w := range warnings {
		c.logger.WarnContext(ctx, "migration source validation warning", "warning", w)
	}
	// EXPORT makes the source a subscriber; if it cannot create subscriptions
	// (PG<16 without superuser), warn now so the operator knows fail-back is
	// unavailable. IMPORT is unaffected.
	if !info.CanCreateSubscription {
		c.logger.WarnContext(ctx, "source cannot create subscriptions; EXPORT (fail-back) will be unavailable for this migration",
			"server_version_num", info.ServerVersionNum)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	m := &Migration{
		ID:              fmt.Sprintf("m%d", c.now().UnixNano()),
		Phase:           PhaseCreated,
		ActiveDirection: DirectionImport,
		SourceDSN:       p.SourceDSN,
		TargetDatabase:  p.TargetDatabase,
		TargetShard:     p.TargetShard,
		Tables:          resolvedTables,
		SequenceMargin:  p.SequenceMargin,
	}
	if err := c.store.Insert(ctx, m); err != nil {
		return nil, err
	}
	return c.project(m, nil), nil
}

// StartMigration drives an IMPORT migration from its current phase up to
// COPYING (subscription created) and refreshes status. It resumes from the
// persisted phase, so a call after the subscription already exists (e.g. after a
// failover) only refreshes status rather than recreating anything.
func (c *Coordinator) StartMigration(ctx context.Context, id string) (*Projection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m, err := c.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	// Only the IMPORT-direction start flow is implemented in this step.
	if m.ActiveDirection != DirectionImport {
		return nil, fmt.Errorf("start is only valid in IMPORT direction; migration %s is %s", id, m.ActiveDirection)
	}

	// If the subscription does not yet exist, run the setup phases in order.
	if phaseRank(m.Phase) < phaseRank(PhaseCopying) {
		if err := c.runSetup(ctx, m); err != nil {
			c.fail(ctx, m, err)
			return nil, err
		}
	}

	if err := c.reconcileLocked(ctx, m); err != nil {
		return nil, err
	}
	status, _ := c.target.SubscriptionStatus(ctx, m.SubscriptionName())
	return c.project(m, status), nil
}

// runSetup runs VALIDATING -> SCHEMA_COPY -> CREATE_PUBLICATION -> COPYING.
func (c *Coordinator) runSetup(ctx context.Context, m *Migration) error {
	// One source connection drives the whole setup (validate, publication, DDL
	// capture); DumpSchema still shells out to pg_dump separately.
	src, err := newSource(ctx, m.SourceDSN)
	if err != nil {
		return err
	}
	defer src.close()

	if err := c.setPhase(ctx, m, PhaseValidating); err != nil {
		return err
	}
	_, _, warnings, err := src.Validate(m.Tables)
	if err != nil {
		return err
	}
	for _, w := range warnings {
		c.logger.WarnContext(ctx, "migration source validation warning", "migration", m.ID, "warning", w)
	}

	if err := c.setPhase(ctx, m, PhaseSchemaCopy); err != nil {
		return err
	}
	schemaSQL, err := src.DumpSchema(m.Tables)
	if err != nil {
		return err
	}
	if err := c.target.ApplySchema(ctx, schemaSQL); err != nil {
		return err
	}

	// EXPERIMENTAL: table-scoped DDL replication (see ddlrepl.go). IMPORT: the
	// target is the subscriber (apply) and the source is the publisher (capture).
	// Register this migration's tables for capture and set up apply before the
	// publication; the publication carries a row-filtered multigres.ddl_log so
	// captured DDL rides the same stream; arm the shared source event trigger last
	// (after the log/function exist and just before CreateSubscription) so little
	// DDL accumulates in ddl_log before the subscription's initial snapshot.
	// (CREATE PUBLICATION is never captured regardless — wrong command tag.)
	if err := setupDDLApply(ctx, c.target.ddlConn(), m.ID); err != nil {
		return err
	}
	if err := setupDDLCapture(ctx, src.ddlConn(), m.ID, m.Tables); err != nil {
		return err
	}

	if err := c.setPhase(ctx, m, PhaseCreatePublication); err != nil {
		return err
	}
	if err := src.CreatePublication(m.PublicationName(), m.Tables, m.ID); err != nil {
		return err
	}
	if err := armDDLCapture(ctx, src.ddlConn()); err != nil {
		return err
	}

	// Create the subscription — this starts the initial copy (copy_data=true) —
	// and only then record COPYING, so the migration table reflects COPYING once
	// the copy has actually started (not before, where a failed CreateSubscription
	// would leave the row wrongly claiming COPYING).
	if err := c.target.CreateSubscription(ctx, m.SubscriptionName(), m.SourceDSN, m.PublicationName(), true); err != nil {
		return err
	}
	return c.setPhase(ctx, m, PhaseCopying)
}

// UpdateParams carries field-masked updates; a nil field is left unchanged.
type UpdateParams struct {
	SourceDSN      *string
	SequenceMargin *int64
	Tables         *[]string
}

// UpdateMigration applies field-masked changes. The source connection can be
// changed at any time (row rewrite before the subscription exists; ALTER
// SUBSCRIPTION ... CONNECTION after), but not to a different source database.
// Creation options may change only while CREATED.
func (c *Coordinator) UpdateMigration(ctx context.Context, id string, p UpdateParams) (*Projection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m, err := c.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	if p.Tables != nil && m.Phase != PhaseCreated {
		return nil, fmt.Errorf("tables can only be changed while the migration is CREATED (current phase %s)", m.Phase)
	}

	if p.SourceDSN != nil {
		if err := sameSourceDatabase(m.SourceDSN, *p.SourceDSN); err != nil {
			return nil, err
		}
		// If the subscription already exists on the local target (IMPORT), repoint
		// its CONNECTION. EXPORT-direction connection updates are a follow-up.
		if phaseRank(m.Phase) >= phaseRank(PhaseCopying) {
			if m.ActiveDirection != DirectionImport {
				return nil, errors.New("source connection update is only supported in IMPORT direction once streaming")
			}
			if err := c.target.AlterSubscriptionConnection(ctx, m.SubscriptionName(), *p.SourceDSN); err != nil {
				return nil, err
			}
		}
		m.SourceDSN = *p.SourceDSN
	}
	if p.SequenceMargin != nil {
		m.SequenceMargin = *p.SequenceMargin
	}
	if p.Tables != nil {
		// Re-resolve against the (possibly updated) source, the same as create, so
		// "*"/"schema.*" wildcards expand to concrete owned tables and missing
		// schemas/tables are rejected — never store raw patterns.
		src, err := newSource(ctx, m.SourceDSN)
		if err != nil {
			return nil, err
		}
		_, resolved, warnings, err := src.Validate(*p.Tables)
		src.close()
		if err != nil {
			return nil, err
		}
		for _, w := range warnings {
			c.logger.WarnContext(ctx, "migration source validation warning", "warning", w)
		}
		m.Tables = resolved
	}

	if err := c.store.Update(ctx, m); err != nil {
		return nil, err
	}
	status, _ := c.liveStatus(ctx, m)
	return c.project(m, status), nil
}

// sameSourceDatabase rejects a connection change that would repoint at a
// different source database (the slot and origin are tied to that source).
// Host/port/user/password/TLS may change; dbname may not.
func sameSourceDatabase(oldDSN, newDSN string) error {
	oldCfg, err := pgx.ParseConfig(oldDSN)
	if err != nil {
		return fmt.Errorf("parse current source DSN: %w", err)
	}
	newCfg, err := pgx.ParseConfig(newDSN)
	if err != nil {
		return fmt.Errorf("parse new source DSN: %w", err)
	}
	if oldCfg.Database != newCfg.Database {
		return fmt.Errorf("source database change is not allowed while streaming (%q -> %q)", oldCfg.Database, newCfg.Database)
	}
	return nil
}

// GetMigration returns the projection for one migration.
//
// It takes c.mu even though it only reads: the store hands back the shared cache
// entry (no copy), and the write paths mutate that same *Migration in place
// (setPhase, reconcileLocked) while holding c.mu. Reading its fields here
// (liveStatus, project) without c.mu would race those writers. Modifications are
// rare, so this short read-side lock is cheap.
func (c *Coordinator) GetMigration(ctx context.Context, id string) (*Projection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, err := c.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	status, _ := c.liveStatus(ctx, m)
	return c.project(m, status), nil
}

// ListMigrations returns projections for all migrations. It takes c.mu for the
// same reason as GetMigration: the cached *Migration values it reads are the
// ones the write paths mutate in place under c.mu.
func (c *Coordinator) ListMigrations(ctx context.Context) ([]*Projection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ms, err := c.store.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*Projection, 0, len(ms))
	for _, m := range ms {
		status, _ := c.liveStatus(ctx, m)
		out = append(out, c.project(m, status))
	}
	return out, nil
}

// DropOptions controls DropMigration. Default (neither set) drains to lag zero
// first and requires a caught-up STREAMING state. Wait blocks until it becomes
// completable; Force skips the drain and tears down from any phase.
type DropOptions struct {
	Wait        bool
	WaitTimeout time.Duration
	Force       bool
}

// DropMigration tears down a migration's replication link and removes the row.
// Default: drain (quiesce publisher, wait slot-confirmed), advance the surviving
// writer's sequences, then drop sub/pub/slot. Requires STREAMING unless Wait
// (block until STREAMING) or Force (skip the drain).
func (c *Coordinator) DropMigration(ctx context.Context, id string, opts DropOptions) (*Projection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m, err := c.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	if !opts.Force {
		// Advance the phase from live status first, so a just-caught-up migration
		// (COPYING with caught_up=true, before the reconcile poller ticked) is
		// recognized as completable rather than rejected.
		if err := c.reconcileLocked(ctx, m); err != nil {
			return nil, err
		}
		if m.Phase != PhaseStreaming {
			if phaseRank(m.Phase) < phaseRank(PhaseCopying) {
				return nil, fmt.Errorf("migration %s has not started (phase %s); use --force to remove it", id, m.Phase)
			}
			if !opts.Wait {
				return nil, fmt.Errorf("migration %s is not caught up (phase %s); wait for STREAMING, re-run with --wait, or --force to tear down now", id, m.Phase)
			}
			waitCtx := ctx
			if opts.WaitTimeout > 0 {
				var cancel context.CancelFunc
				waitCtx, cancel = context.WithTimeout(ctx, opts.WaitTimeout)
				defer cancel()
			}
			if err := c.waitStreamingLocked(waitCtx, m); err != nil {
				return nil, err
			}
		}
		m.Phase = PhaseCompleting
		if err := c.store.Update(ctx, m); err != nil {
			return nil, err
		}
		if err := c.drainAndAdvance(ctx, m); err != nil {
			return nil, err
		}
	}

	c.teardown(ctx, m, opts.Force)
	if err := c.store.Delete(ctx, id); err != nil {
		return nil, err
	}
	return c.project(m, nil), nil
}

// waitStreamingLocked polls until the migration reaches STREAMING or ctx ends.
// Caller holds c.mu.
func (c *Coordinator) waitStreamingLocked(ctx context.Context, m *Migration) error {
	ticker := time.NewTicker(slotPollInterval)
	defer ticker.Stop()
	for {
		if err := c.reconcileLocked(ctx, m); err != nil {
			return err
		}
		if m.Phase == PhaseStreaming {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for migration %s to catch up (phase %s): %w", m.ID, m.Phase, ctx.Err())
		case <-ticker.C:
		}
	}
}

// drainCurrent quiesces the current publisher, captures its LSN, and waits until
// the subscriber has consumed past it (lag zero). Returns the captured LSN.
func (c *Coordinator) drainCurrent(ctx context.Context, m *Migration) (string, error) {
	if m.ActiveDirection == DirectionImport {
		src, err := newSource(ctx, m.SourceDSN)
		if err != nil {
			return "", err
		}
		defer src.close()
		if err := src.SetReadOnly(true); err != nil {
			return "", err
		}
		lsn, err := src.CurrentLSN()
		if err != nil {
			return "", err
		}
		if err := src.WaitSlotConfirmed(m.SubscriptionName(), lsn); err != nil {
			return "", err
		}
		return lsn, nil
	}
	// EXPORT: the target is the publisher. Its GUC is not flipped (it would
	// interfere with the pooler); the operator stops application writes.
	lsn, err := c.target.CurrentLSN(ctx)
	if err != nil {
		return "", err
	}
	if err := c.target.WaitSlotConfirmed(ctx, m.SubscriptionName(), lsn); err != nil {
		return "", err
	}
	return lsn, nil
}

// drainAndAdvance runs the drain barrier and advances the surviving writer's
// sequences (the target in IMPORT, the source in EXPORT).
func (c *Coordinator) drainAndAdvance(ctx context.Context, m *Migration) error {
	if _, err := c.drainCurrent(ctx, m); err != nil {
		return err
	}
	if m.ActiveDirection == DirectionImport {
		return c.target.AdvanceSequences(ctx, m.Tables, m.SequenceMargin)
	}
	src, err := newSource(ctx, m.SourceDSN)
	if err != nil {
		return err
	}
	defer src.close()
	return src.AdvanceSequences(m.Tables, m.SequenceMargin)
}

// SetMigrationDirection sets the active direction declaratively. Setting the
// current direction is a no-op; the other performs the symmetric barrier + flip:
// drain the current publisher, advance the new writer's sequences, tear down the
// current link, and establish the reverse link (copy_data=false). It requires a
// caught-up STREAMING state.
func (c *Coordinator) SetMigrationDirection(ctx context.Context, id string, target Direction) (*Projection, error) {
	if target != DirectionImport && target != DirectionExport {
		return nil, fmt.Errorf("invalid direction %q", target)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	m, err := c.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if m.ActiveDirection == target {
		status, _ := c.liveStatus(ctx, m)
		return c.project(m, status), nil // no-op
	}
	if target == DirectionExport {
		if c.targetConnInfo == nil {
			return nil, errors.New("EXPORT direction is not configured (no target conninfo for the reverse subscription)")
		}
		// Fail fast before any destructive drain/switch: building the reverse-
		// subscription conninfo enforces the EXPORT preconditions (a gateway
		// advertise host is configured, slot-based replication is enabled). Without
		// this probe those errors would only surface mid-switch, after the current
		// link has already been torn down.
		if _, err := c.targetConnInfo(m.TargetDatabase); err != nil {
			return nil, err
		}
		// EXPORT makes the source a subscriber; verify it can create subscriptions
		// before touching anything (PG<16 without superuser cannot).
		src, err := newSource(ctx, m.SourceDSN)
		if err != nil {
			return nil, err
		}
		info, err := src.Info()
		src.close()
		if err != nil {
			return nil, err
		}
		if !info.CanCreateSubscription {
			return nil, fmt.Errorf("cannot switch to EXPORT: the source cannot create subscriptions (server_version_num %d) — needs superuser, or PostgreSQL 16+ with pg_create_subscription membership", info.ServerVersionNum)
		}
	}

	if err := c.reconcileLocked(ctx, m); err != nil {
		return nil, err
	}
	if m.Phase != PhaseStreaming {
		return nil, fmt.Errorf("migration %s must be caught up (STREAMING) to switch direction; current phase %s", id, m.Phase)
	}

	m.Phase = PhaseSwitching
	if err := c.store.Update(ctx, m); err != nil {
		return nil, err
	}

	if _, err := c.drainCurrent(ctx, m); err != nil {
		c.fail(ctx, m, err)
		return nil, err
	}
	if err := c.switchTo(ctx, m, target); err != nil {
		c.fail(ctx, m, err)
		return nil, err
	}

	m.ActiveDirection = target
	m.Phase = PhaseStreaming
	if err := c.store.Update(ctx, m); err != nil {
		return nil, err
	}
	status, _ := c.liveStatus(ctx, m)
	return c.project(m, status), nil
}

// switchTo tears down the current-direction link and establishes the reverse
// link (copy_data=false). Caller holds c.mu and has already drained.
func (c *Coordinator) switchTo(ctx context.Context, m *Migration, target Direction) error {
	src, err := newSource(ctx, m.SourceDSN)
	if err != nil {
		return err
	}
	defer src.close()
	sub, pub := m.SubscriptionName(), m.PublicationName()

	if target == DirectionExport {
		// IMPORT -> EXPORT: the target (Multigres) becomes publisher/writer, and
		// the old source becomes a subscriber. Un-quiesce the source first (the
		// drain left it read-only) so its own DDL and replication apply can write.
		if err := src.SetReadOnly(false); err != nil {
			return err
		}
		if err := c.target.AdvanceSequences(ctx, m.Tables, m.SequenceMargin); err != nil {
			return err
		}
		if err := c.target.DropSubscription(ctx, sub); err != nil {
			return err
		}
		if err := src.DropPublication(pub); err != nil {
			return err
		}
		// Flip DDL replication to match: capture moves from the source to the
		// target, apply from the target to the source (see ddlrepl.go). Tear down
		// the old-direction roles, then set up the new ones before recreating the
		// link.
		if err := teardownDDLCapture(ctx, src.ddlConn(), m.ID); err != nil {
			return err
		}
		if err := teardownDDLApply(ctx, c.target.ddlConn(), m.ID); err != nil {
			return err
		}
		if err := setupDDLApply(ctx, src.ddlConn(), m.ID); err != nil {
			return err
		}
		if err := setupDDLCapture(ctx, c.target.ddlConn(), m.ID, m.Tables); err != nil {
			return err
		}
		if err := c.target.CreatePublication(ctx, pub, m.Tables, m.ID); err != nil {
			return err
		}
		if err := armDDLCapture(ctx, c.target.ddlConn()); err != nil {
			return err
		}
		conninfo, err := c.targetConnInfo(m.TargetDatabase)
		if err != nil {
			return err
		}
		return src.CreateSubscription(sub, conninfo, pub, false)
	}

	// EXPORT -> IMPORT: the external source becomes publisher/writer again.
	if err := src.AdvanceSequences(m.Tables, m.SequenceMargin); err != nil {
		return err
	}
	if err := src.DropSubscription(sub); err != nil {
		return err
	}
	if err := c.target.DropPublication(ctx, pub); err != nil {
		return err
	}
	// Flip DDL replication back: capture moves from the target to the source,
	// apply from the source to the target.
	if err := teardownDDLCapture(ctx, c.target.ddlConn(), m.ID); err != nil {
		return err
	}
	if err := teardownDDLApply(ctx, src.ddlConn(), m.ID); err != nil {
		return err
	}
	if err := setupDDLApply(ctx, c.target.ddlConn(), m.ID); err != nil {
		return err
	}
	if err := setupDDLCapture(ctx, src.ddlConn(), m.ID, m.Tables); err != nil {
		return err
	}
	if err := src.CreatePublication(pub, m.Tables, m.ID); err != nil {
		return err
	}
	if err := armDDLCapture(ctx, src.ddlConn()); err != nil {
		return err
	}
	return c.target.CreateSubscription(ctx, sub, m.SourceDSN, pub, false)
}

// teardown drops the subscription and publication (and thus the slot) on both
// sides for the active direction. force makes source-side failures best-effort
// (the source may be unreachable during an abort).
func (c *Coordinator) teardown(ctx context.Context, m *Migration, _ bool) {
	logErr := func(step string, err error) {
		if err != nil {
			c.logger.WarnContext(ctx, "migration teardown step failed", "migration", m.ID, "step", step, "error", err)
		}
	}
	// The source may be unreachable during an abort; still tear down the target
	// side. Source-side drops run only if we could connect.
	src, err := newSource(ctx, m.SourceDSN)
	if err != nil {
		logErr("connect source", err)
	} else {
		defer src.close()
	}
	if m.ActiveDirection == DirectionImport {
		// IMPORT: subscription (apply) on the target, publication (capture) on the source.
		logErr("drop target subscription", c.target.DropSubscription(ctx, m.SubscriptionName()))
		// EXPERIMENTAL: tear down DDL replication (see ddlrepl.go), refcounted so
		// other migrations on this server keep working.
		logErr("drop target DDL apply", teardownDDLApply(ctx, c.target.ddlConn(), m.ID))
		if src != nil {
			logErr("drop source publication", src.DropPublication(m.PublicationName()))
			logErr("drop source DDL capture", teardownDDLCapture(ctx, src.ddlConn(), m.ID))
		}
		return
	}
	// EXPORT: subscription (apply) on the source, publication (capture) on the target.
	if src != nil {
		logErr("drop source subscription", src.DropSubscription(m.SubscriptionName()))
		logErr("drop source DDL apply", teardownDDLApply(ctx, src.ddlConn(), m.ID))
	}
	logErr("drop target publication", c.target.DropPublication(ctx, m.PublicationName()))
	logErr("drop target DDL capture", teardownDDLCapture(ctx, c.target.ddlConn(), m.ID))
}

// Reconcile refreshes every in-flight migration: it advances COPYING to
// STREAMING once the initial copy is caught up. It is the body run on the
// become-primary trigger and the periodic timer.
func (c *Coordinator) Reconcile(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ms, err := c.store.List(ctx)
	if err != nil {
		return err
	}
	for _, m := range ms {
		if err := c.reconcileLocked(ctx, m); err != nil {
			c.logger.WarnContext(ctx, "reconcile migration failed", "migration", m.ID, "error", err)
		}
	}
	return nil
}

// reconcileLocked advances a single migration. Caller holds c.mu.
func (c *Coordinator) reconcileLocked(ctx context.Context, m *Migration) error {
	switch m.Phase {
	case PhaseCopying, PhaseStreaming:
		status, err := c.target.SubscriptionStatus(ctx, m.SubscriptionName())
		if err != nil {
			return err
		}
		if status.CaughtUp && m.Phase != PhaseStreaming {
			now := c.now()
			m.Phase = PhaseStreaming
			m.StreamingSince = &now
			return c.store.Update(ctx, m)
		}
	}
	return nil
}

// liveStatus reads subscription status for phases where it is meaningful.
func (c *Coordinator) liveStatus(ctx context.Context, m *Migration) (*SubscriptionStatus, error) {
	if m.Phase == PhaseCopying || m.Phase == PhaseStreaming {
		return c.target.SubscriptionStatus(ctx, m.SubscriptionName())
	}
	return nil, nil
}

// setPhase persists a phase transition. Caller holds c.mu.
func (c *Coordinator) setPhase(ctx context.Context, m *Migration, phase Phase) error {
	m.Phase = phase
	return c.store.Update(ctx, m)
}

// fail records a terminal error on a migration (best-effort persist). Caller
// holds c.mu.
func (c *Coordinator) fail(ctx context.Context, m *Migration, err error) {
	m.Phase = PhaseFailed
	m.LastError = err.Error()
	if uerr := c.store.Update(ctx, m); uerr != nil {
		c.logger.WarnContext(ctx, "persist failed phase", "migration", m.ID, "error", uerr)
	}
}

// phaseRank orders the linear IMPORT setup phases so the coordinator can resume
// from where it left off. Non-linear phases sort high so they never re-run setup.
func phaseRank(p Phase) int {
	switch p {
	case PhaseCreated:
		return 0
	case PhaseValidating:
		return 1
	case PhaseSchemaCopy:
		return 2
	case PhaseCreatePublication:
		return 3
	case PhaseCopying:
		return 4
	case PhaseStreaming:
		return 5
	default:
		return 100
	}
}

// Projection is the redacted, operator-facing view of a migration: persisted
// intent plus live status. It never carries the source DSN/credentials.
type Projection struct {
	ID               string
	Source           string // redacted host[:port]/db
	Phase            Phase
	ActiveDirection  Direction
	TargetDatabase   string
	TargetShard      string
	Tables           []string
	PublicationName  string
	SubscriptionName string
	TotalRelations   int64
	ReadyRelations   int64
	CaughtUp         bool
	ReceivedLSN      string
	LatestEndLSN     string
	LastError        string
	CreatedAt        time.Time
	StreamingSince   *time.Time
}

// project builds the redacted projection, merging optional live status.
func (c *Coordinator) project(m *Migration, status *SubscriptionStatus) *Projection {
	p := &Projection{
		ID:               m.ID,
		Source:           redactDSN(m.SourceDSN),
		Phase:            m.Phase,
		ActiveDirection:  m.ActiveDirection,
		TargetDatabase:   m.TargetDatabase,
		TargetShard:      m.TargetShard,
		Tables:           m.Tables,
		PublicationName:  m.PublicationName(),
		SubscriptionName: m.SubscriptionName(),
		LastError:        m.LastError,
		CreatedAt:        m.CreatedAt,
		StreamingSince:   m.StreamingSince,
	}
	if status != nil {
		p.TotalRelations = status.TotalRelations
		p.ReadyRelations = status.ReadyRelations
		p.CaughtUp = status.CaughtUp
		p.ReceivedLSN = status.ReceivedLSN
		p.LatestEndLSN = status.LatestEndLSN
	}
	return p
}

// redactDSN returns host[:port]/db from a DSN, dropping user and password. On a
// parse failure it returns empty rather than risk leaking credentials.
func redactDSN(dsn string) string {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return ""
	}
	if cfg.Port != 0 {
		return fmt.Sprintf("%s:%d/%s", cfg.Host, cfg.Port, cfg.Database)
	}
	return cfg.Host + "/" + cfg.Database
}
