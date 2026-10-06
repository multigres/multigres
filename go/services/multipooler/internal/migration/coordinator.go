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
	"strings"
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
	store  migrationStore
	target migrationTarget
	logger *slog.Logger

	// newSource opens the external source side over the operator-supplied DSN. It is
	// a field (defaulted to the real newSource in NewCoordinator) so unit tests can
	// substitute a fake source without a live Postgres — see ports.go.
	newSource func(ctx context.Context, dsn string) (migrationSource, error)

	// targetConnInfo builds a libpq conninfo the external source can use to reach
	// this (target) Postgres, for the reverse subscription in EXPORT direction.
	// May be nil, in which case EXPORT is unavailable.
	targetConnInfo func(database string) (string, error)

	// drainForImport forces this pooler to non-serving and blocks until the
	// graceful drain completes, so the IMPORT setup never drops target tables or
	// starts streaming while clients can still write to the target. It is the
	// synchronous serving barrier the async ~5s postgres-monitor gate cannot
	// guarantee on its own (setup finishes faster than a tick). May be nil (unit
	// tests without a manager), in which case the barrier is skipped.
	drainForImport func(ctx context.Context) error

	// releaseForExport flips this pooler to SERVING synchronously the moment the
	// IMPORT->EXPORT cutover commits the EXPORTING phase, rather than waiting for the
	// async ~5s postgres-monitor tick to observe it. Prompt serving-on is what lets
	// the gateway's failover buffer replay the queries it held during the cutover
	// (the buffer drains when the leader self-attests SERVING) inside its bounded
	// window, instead of them timing out and being refused. May be nil (unit tests
	// without a manager), in which case the flip is left to the monitor tick.
	releaseForExport func(ctx context.Context) error

	now func() time.Time

	// mu serializes state transitions so concurrent operator calls on the same
	// migration cannot interleave phase writes.
	mu sync.Mutex
}

// NewCoordinator builds a Coordinator over the given admin query service.
// targetConnInfo builds the target-reachable conninfo used for the EXPORT-side
// reverse subscription; pass nil if EXPORT is not supported in this deployment.
// drainForImport is the synchronous serving barrier run before IMPORT setup
// touches the target (see the drainForImport field); pass nil to skip it.
// releaseForExport is the synchronous serving flip run when the EXPORT cutover
// commits (see the releaseForExport field); pass nil to leave it to the monitor.
func NewCoordinator(qs executor.InternalQueryService, logger *slog.Logger, targetConnInfo func(database string) (string, error), drainForImport func(ctx context.Context) error, releaseForExport func(ctx context.Context) error) *Coordinator {
	return &Coordinator{
		store:            NewStore(qs),
		target:           newTarget(qs),
		newSource:        func(ctx context.Context, dsn string) (migrationSource, error) { return newSource(ctx, dsn) },
		logger:           logger,
		targetConnInfo:   targetConnInfo,
		drainForImport:   drainForImport,
		releaseForExport: releaseForExport,
		now:              time.Now,
	}
}

// EnsureSchema creates the migration table if absent (covers shards bootstrapped
// before the table existed). Safe to call on every coordinator start.
func (c *Coordinator) EnsureSchema(ctx context.Context) error {
	return c.store.EnsureSchema(ctx)
}

// resolveSourceDSN returns m's current source DSN: a fresh (uncached) lookup of
// the connection m.ConnectionID references, rather than trusting any cached
// value — the live-reference semantics documented on Migration.ConnectionID.
func (c *Coordinator) resolveSourceDSN(ctx context.Context, m *Migration) (string, error) {
	conn, err := c.store.GetConnectionByRef(ctx, Ref{ID: m.ConnectionID})
	if err != nil {
		return "", err
	}
	return conn.DSN, nil
}

// CreateConnection stores a new named connection. conn.ID is assigned here
// (like CreateMigration's ID — see c.now()), not accepted from the caller.
// Connections are immutable once created — there is no in-place update, only
// create/drop. Takes c.mu, the same lock every other coordinator method that
// mutates migration/connection state holds.
func (c *Coordinator) CreateConnection(ctx context.Context, conn *Connection) error {
	if conn.Name == "" {
		return errors.New("connection name is required")
	}
	if conn.DSN == "" {
		return errors.New("connection dsn is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	conn.ID = c.now().UnixNano()
	return c.store.InsertConnection(ctx, conn)
}

// DropConnection removes a connection. Fails with a foreign-key-violation error
// if a migration still references it. Takes c.mu, the same lock every other
// coordinator method that mutates migration/connection state holds.
func (c *Coordinator) DropConnection(ctx context.Context, id int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.store.DeleteConnection(ctx, id)
}

// GetConnection returns one connection, addressed by ref (id or name).
func (c *Coordinator) GetConnection(ctx context.Context, ref Ref) (*Connection, error) {
	return c.store.GetConnectionByRef(ctx, ref)
}

// ListConnections returns every stored connection.
func (c *Coordinator) ListConnections(ctx context.Context) ([]*Connection, error) {
	return c.store.ListConnections(ctx)
}

// CreateParams is the input to CreateMigration.
type CreateParams struct {
	// ConnectionName names a stored Connection (see Connection) this migration
	// reads its source DSN from, live, for as long as it exists: altering the
	// named connection takes effect on this migration's very next action.
	// Required — every migration has a source connection.
	ConnectionName string
	TargetDatabase string
	TargetShard    string
	// TargetTableGroup is the tablegroup the target shard belongs to — needed
	// alongside TargetDatabase/TargetShard to uniquely identify a shard, since
	// a database can have multiple tablegroups each with their own shard
	// namespace. Optional, like TargetShard.
	TargetTableGroup string
	// Name is optional; when set it must be unique per target database.
	Name string
	// Tables is the flat selection ("*", "schema.*", "schema.table"); the RPC
	// handler folds the structured selection (all_tables/schemas/table_specs) into
	// it before calling.
	Tables []string
	// CopyData chooses the initial-copy behavior (default true).
	CopyData bool
	// SkipSchemaCopy skips the pg_dump --schema-only step.
	SkipSchemaCopy bool
	SequenceMargin int64
}

// CreateMigration records a new migration (phase CREATED). It makes no changes
// to either database, but it does validate the source read-only up front —
// reachability, wal_level, and a usable replica identity per table — so an
// unusable source is rejected at create time rather than at start.
func (c *Coordinator) CreateMigration(ctx context.Context, p CreateParams) (*Projection, error) {
	if p.ConnectionName == "" {
		return nil, errors.New("source connection is required")
	}
	if p.TargetDatabase == "" {
		return nil, errors.New("target database is required")
	}
	if len(p.Tables) == 0 {
		return nil, errors.New("at least one table is required")
	}

	// Resolve the named connection to its current DSN, used once, here, to
	// validate reachability — it is not what gets persisted on the row (the row
	// references the connection live by id; see m.ConnectionID below).
	conn, err := c.store.GetConnectionByRef(ctx, Ref{Name: p.ConnectionName})
	if err != nil {
		if errors.Is(err, ErrConnectionNotFound) {
			return nil, fmt.Errorf("connection %q does not exist", p.ConnectionName)
		}
		return nil, err
	}

	// Validate the source read-only before recording anything (no DB changes).
	// Validate also resolves "*"/"schema.*" wildcards to the concrete owned tables.
	src, err := c.newSource(ctx, conn.DSN)
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

	// Only one migration at a time is supported: the serving gate
	// (refreshMigrationHold) counts migration rows by phase, not by migration id,
	// so a second concurrent migration would make that count ambiguous. c.mu
	// serializes creates on the primary, so this check-then-insert cannot race
	// another create in this coordinator. A FAILED migration still counts — its
	// row must be explicitly dropped first.
	if existing, err := c.store.List(ctx); err != nil {
		return nil, err
	} else if len(existing) > 0 {
		return nil, fmt.Errorf("migration %d already exists (phase %s); only one migration is supported at a time, drop it first", existing[0].ID, existing[0].Phase)
	}

	m := &Migration{
		ID:               c.now().UnixNano(),
		Phase:            PhaseCreated,
		Name:             p.Name,
		ConnectionID:     conn.ID,
		TargetDatabase:   p.TargetDatabase,
		TargetShard:      p.TargetShard,
		TargetTableGroup: p.TargetTableGroup,
		Tables:           resolvedTables,
		CopyData:         p.CopyData,
		SkipSchemaCopy:   p.SkipSchemaCopy,
		SequenceMargin:   p.SequenceMargin,
	}
	if err := c.store.Insert(ctx, m); err != nil {
		return nil, err
	}
	c.journal(ctx, m, JournalEventCreate, "")
	return c.project(ctx, m, nil), nil
}

// runSetup runs VALIDATING -> SCHEMA_COPY -> CREATE_PUBLICATION -> COPYING.
// callerRole is ActivateOptions.CallerRole, threaded through to gate DropTables.
func (c *Coordinator) runSetup(ctx context.Context, m *Migration, callerRole string) error {
	dsn, err := c.resolveSourceDSN(ctx, m)
	if err != nil {
		return err
	}
	// One source connection drives the whole setup (validate, publication);
	// DumpSchema still shells out to pg_dump separately.
	src, err := c.newSource(ctx, dsn)
	if err != nil {
		return err
	}
	defer src.close()

	if err := c.advancePhase(ctx, m, PhaseValidating); err != nil {
		return err
	}
	_, _, warnings, err := src.Validate(m.Tables)
	if err != nil {
		return err
	}
	for _, w := range warnings {
		c.logger.WarnContext(ctx, "migration source validation warning", "migration", m.ID, "warning", w)
	}

	// Serving barrier: make this pooler non-serving and drain in-flight writes
	// BEFORE any destructive target change (DropTables) or streaming, so a client
	// cannot read a half-dropped table or land a write that the drop/initial COPY
	// then silently discards. The phase already left CREATED, so the ~5s monitor
	// would eventually hold serving — but setup usually finishes within one tick,
	// so hold it synchronously here. Runs after the read-only Validate, so a source
	// that fails validation never drains the shard needlessly.
	if c.drainForImport != nil {
		if err := c.drainForImport(ctx); err != nil {
			return fmt.Errorf("drain to non-serving before import setup: %w", err)
		}
	}

	if err := c.advancePhase(ctx, m, PhaseSchemaCopy); err != nil {
		return err
	}
	// A caller that merely passed the gateway's CanCreateMigration gate
	// (database CREATE + pg_create_subscription) does not necessarily own
	// these target tables; verify DROP privilege before the admin pool does
	// anything on their behalf. This must run unconditionally, not just under
	// schema copy: even with SkipSchemaCopy, the subscription created below
	// streams source writes into these tables through the superuser admin
	// pool, so a caller who doesn't own a pre-existing target table could
	// otherwise have it written to without authorization. See
	// CheckDropPrivilege and ActivateOptions.CallerRole's doc comments.
	if err := c.target.CheckDropPrivilege(ctx, m.Tables, callerRole); err != nil {
		return err
	}
	// SkipSchemaCopy: the target schema already exists (seeded out-of-band), so
	// bypass the pg_dump --schema-only + apply. The phase still advances so the
	// resume path and progress reporting stay monotonic.
	if !m.SkipSchemaCopy {
		// Drop the migrated tables on the target first, so a pre-existing table (a
		// re-run after a partial migration, or a target that already had them) does
		// not fail the schema apply with "relation already exists".
		if err := c.target.DropTables(ctx, m.Tables); err != nil {
			return err
		}
		schemaSQL, err := src.DumpSchema(m.Tables)
		if err != nil {
			return err
		}
		if err := c.target.ApplySchema(ctx, schemaSQL); err != nil {
			return err
		}
		// Disable every user trigger the dump just carried over before anything
		// can fire it — not just the subscription below (apply worker,
		// session_replication_role=replica) but ordinary application DML once
		// the migration completes, since the trigger function itself is now
		// owned by the target admin regardless of fire mode. See
		// DisableUserTriggers's doc comment for the attack this closes.
		if err := c.target.DisableUserTriggers(ctx, m.Tables); err != nil {
			return err
		}
		// CHECK constraints have no DISABLE equivalent and evaluate
		// unconditionally on every write, including replicated ones — see
		// DropUserCheckConstraints's doc comment for the same SECURITY DEFINER
		// vector this closes for constraints instead of triggers.
		if err := c.target.DropUserCheckConstraints(ctx, m.Tables); err != nil {
			return err
		}
		// A rewrite rule's action is arbitrary DML, same SECURITY DEFINER vector
		// again — see DisableUserRewriteRules's doc comment for why DISABLE RULE
		// (unlike a CHECK constraint) is unconditional, same as a trigger.
		if err := c.target.DisableUserRewriteRules(ctx, m.Tables); err != nil {
			return err
		}
	}

	if err := c.advancePhase(ctx, m, PhaseCreatePublication); err != nil {
		return err
	}
	if err := src.CreatePublication(m.PublicationName(), m.Tables); err != nil {
		return err
	}

	// Create the subscription — with copy_data per the migration's choice (default
	// true starts the initial copy; false subscribes without one) — and only then
	// record COPYING, so the migration table reflects COPYING once the copy has
	// actually started (not before, where a failed CreateSubscription would leave
	// the row wrongly claiming COPYING).
	if err := c.target.CreateSubscription(ctx, m.SubscriptionName(), dsn, m.PublicationName(), m.CopyData); err != nil {
		return err
	}
	return c.advancePhase(ctx, m, PhaseCopying)
}

// GetMigration returns the projection for one migration.
//
// It takes c.mu even though it only reads: the store hands back the shared cache
// entry (no copy), and the write paths mutate that same *Migration in place
// (setPhase, reconcileLocked) while holding c.mu. Reading its fields here
// (liveStatus, project) without c.mu would race those writers. Modifications are
// rare, so this short read-side lock is cheap.
func (c *Coordinator) GetMigration(ctx context.Context, ref Ref) (*Projection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, err := c.store.GetByRef(ctx, ref)
	if err != nil {
		return nil, err
	}
	status, _ := c.liveStatus(ctx, m)
	return c.project(ctx, m, status), nil
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
		out = append(out, c.project(ctx, m, status))
	}
	return out, nil
}

// GetMigrationJournal returns a migration's journal entries, oldest first. It
// reads the journal table directly, so it also returns entries for a migration
// whose row has already been dropped (the journal is retained for audit after a
// drop). ref is an id or name: a live migration is resolved by either, but a
// dropped migration is addressable only by its id (the row that held the name is
// gone). The journal never contains credentials, so entries are returned as-is.
func (c *Coordinator) GetMigrationJournal(ctx context.Context, ref Ref) ([]*JournalEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := ref.ID
	if m, err := c.store.GetByRef(ctx, ref); err == nil {
		id = m.ID
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	// A dropped migration is addressable only by id: with no live row and no id
	// (name-only ref), there is nothing to look up.
	if id == 0 {
		return nil, ErrNotFound
	}
	return c.store.ListJournal(ctx, id)
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
func (c *Coordinator) DropMigration(ctx context.Context, ref Ref, opts DropOptions) (*Projection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m, err := c.store.GetByRef(ctx, ref)
	if err != nil {
		return nil, err
	}

	// A row already in COMPLETING is a drop that committed the phase but did not
	// finish (a crash/restart, or a drain that failed after the phase was
	// persisted). Finish it idempotently from the persisted direction — regardless
	// of force/wait — rather than re-running the drain from a non-streaming phase.
	if m.Phase == PhaseCompleting {
		c.teardown(ctx, m, m.effectiveDirection())
		c.journal(ctx, m, JournalEventDrop, "")
		if err := c.store.Delete(ctx, m.ID); err != nil {
			return nil, err
		}
		return c.project(ctx, m, nil), nil
	}

	// Capture the direction while the phase still carries one — PhaseCompleting
	// below does not, so drain and teardown must be told which side is which.
	dir := directionOf(m.Phase)
	// dropLSN is the drained-to (quiesce) LSN of a graceful drop's barrier, recorded
	// in the DROP journal entry as the final position of the surviving writer; empty
	// for a forced (undrained) drop.
	var dropLSN string

	if !opts.Force {
		// Advance the phase from live status first, so a just-caught-up migration
		// (COPYING with caught_up=true, before the reconcile poller ticked) is
		// recognized as completable rather than rejected.
		if err := c.reconcileLocked(ctx, m); err != nil {
			return nil, err
		}
		if !isStreaming(m.Phase) {
			if phaseRank(m.Phase) < phaseRank(PhaseCopying) {
				return nil, fmt.Errorf("migration %d has not started (phase %s); use --force to remove it", m.ID, m.Phase)
			}
			if !opts.Wait {
				return nil, fmt.Errorf("migration %d is not caught up (phase %s); wait for it to catch up, re-run with --wait, or --force to tear down now", m.ID, m.Phase)
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
		// reconcileLocked/waitStreamingLocked may have advanced the phase (e.g.
		// COPYING -> IMPORTING); re-derive the direction from the settled phase.
		dir = directionOf(m.Phase)
		origPhase := m.Phase
		m.Phase = PhaseCompleting
		m.Direction = dir
		if err := c.store.Update(ctx, m); err != nil {
			return nil, err
		}
		lsn, err := c.drainAndAdvance(ctx, m, dir)
		if err != nil {
			// The drain did not complete (e.g. the subscriber never reached lag
			// zero and the deadline fired). Roll the phase back to its streaming
			// state so the migration is not stranded in COMPLETING — which the
			// serving gate treats as non-serving and the shard never recovers from.
			// The restore runs on a context detached from the request deadline: the
			// most common drain failure IS the deadline firing, and the rollback must
			// still land then (a reconcile-poller heal is the crash-only backstop, not
			// the path for an ordinary slow drain).
			restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			m.Phase = origPhase
			if uerr := c.store.Update(restoreCtx, m); uerr != nil {
				c.logger.WarnContext(restoreCtx, "restore migration phase after failed drain", "migration", m.ID, "error", uerr)
			}
			cancel()
			return nil, fmt.Errorf("drain before dropping migration %d failed (left in %s, serving preserved): %w", m.ID, origPhase, err)
		}
		dropLSN = lsn
	} else {
		// Force skips the drain but must still record the direction so teardown
		// (which no longer inspects the phase) drops the correct side's objects.
		m.Direction = dir
	}

	c.teardown(ctx, m, dir)
	// DROP journal entry, best-effort (audit only). from_lsn carries a graceful
	// drop's drained-to LSN (the final quiesce position); empty for a forced drop.
	if err := c.store.InsertJournal(ctx, &JournalEntry{
		MigrationID:   m.ID,
		MigrationName: m.Name,
		Event:         JournalEventDrop,
		Phase:         m.Phase,
		Direction:     dir,
		FromLSN:       dropLSN,
	}); err != nil {
		c.logger.WarnContext(ctx, "append migration journal entry", "migration", m.ID, "event", string(JournalEventDrop), "error", err)
	}
	if err := c.store.Delete(ctx, m.ID); err != nil {
		return nil, err
	}
	return c.project(ctx, m, nil), nil
}

// waitStreamingLocked polls until the migration reaches a caught-up streaming
// state (IMPORTING/EXPORTING) or ctx ends. Caller holds c.mu.
func (c *Coordinator) waitStreamingLocked(ctx context.Context, m *Migration) error {
	ticker := time.NewTicker(slotPollInterval)
	defer ticker.Stop()
	for {
		if err := c.reconcileLocked(ctx, m); err != nil {
			return err
		}
		if isStreaming(m.Phase) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for migration %d to catch up (phase %s): %w", m.ID, m.Phase, ctx.Err())
		case <-ticker.C:
		}
	}
}

// drainCurrent quiesces the current publisher, captures its LSN, and waits until
// the subscriber has consumed past it (lag zero). Returns the captured LSN. dir is
// the active direction, passed in because a drop may have already moved the phase
// to PhaseCompleting (which carries no direction).
//
// For the IMPORT publisher (the external source), quiescing is read-only-only: it
// flips default_transaction_read_only, blocking new transactions, and relies on
// that plus the later un-quiesce at teardown/rollback. It does not terminate
// existing client backends or fence any role's CONNECT — a client already
// mid-transaction when the barrier flips could still commit that one transaction.
//
// DefaultDrainWaitTimeout bounds the drain regardless of the caller's own
// deadline (context.WithTimeout only ever shortens one): a drain that never
// catches up — e.g. the slot it is waiting on was lost in a failover — must not
// hold c.mu past this ceiling, since c.mu also guards quick reads like
// GetMigration/ListMigrations that would otherwise starve for as long as the
// drain runs. A var, not a const, so tests can shorten it rather than actually
// waiting out the default.
var DefaultDrainWaitTimeout = 30 * time.Second

func (c *Coordinator) drainCurrent(ctx context.Context, m *Migration, dir Direction) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, DefaultDrainWaitTimeout)
	defer cancel()
	if dir == DirectionImport {
		dsn, err := c.resolveSourceDSN(ctx, m)
		if err != nil {
			return "", err
		}
		src, err := c.newSource(ctx, dsn)
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
	//
	// On a DEACTIVATE (EXPORT->IMPORT) this barrier runs AFTER drainForImport has
	// flipped the target pooler to non-serving. It waits for the reverse
	// subscription — whose walsender streams the target's WAL to the source over
	// the gateway replication tunnel — to confirm past the captured LSN, so the
	// source has consumed every target write before the reverse link is torn down
	// and the target re-imports with copy_data=false. That requires the reverse
	// tunnel to survive the serving drain: the pooler exempts logical-replication
	// streaming tunnels from the drain force-close and counter (see
	// reserved.Pool.KillAllForDrain / NewLogicalReplicationConn), because a
	// read-only walsender cannot diverge the subscriber. Without that exemption
	// the drain severed the tunnel and this poll blocked forever.
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
// sequences (the target in IMPORT, the source in EXPORT). dir is the active
// direction (see drainCurrent). It returns the drained-to (quiesce) LSN captured
// by the barrier, for the drop's journal entry.
func (c *Coordinator) drainAndAdvance(ctx context.Context, m *Migration, dir Direction) (string, error) {
	drainedLSN, err := c.drainCurrent(ctx, m, dir)
	if err != nil {
		return "", err
	}
	if dir == DirectionImport {
		return drainedLSN, c.target.AdvanceSequences(ctx, m.Tables, m.SequenceMargin)
	}
	dsn, err := c.resolveSourceDSN(ctx, m)
	if err != nil {
		return "", err
	}
	src, err := c.newSource(ctx, dsn)
	if err != nil {
		return "", err
	}
	defer src.close()
	return drainedLSN, src.AdvanceSequences(m.Tables, m.SequenceMargin)
}

// Cutover readiness tuning. The activation cutover quiesces the source and drains
// the residual lag to zero under a read-only barrier; during that window the gateway
// buffers client queries (bounded by its failover buffer-window, default ~10s, and
// max-failover-duration, ~20s). The readiness gate below bounds how much residual
// there is when the barrier starts, so the drain plus the fixed flip/serving-on
// overhead fits inside that window and buffered queries are replayed, not refused.
const (
	// DefaultActivateMaxLagBytes is the readiness threshold used when the request
	// leaves max_lag_bytes at 0. Chosen well under MaxActivateMaxLagBytes so a caught-
	// up stream (lag ~0 in the IMPORTING steady state) proceeds immediately, while a
	// backlog under write load blocks until it drains.
	DefaultActivateMaxLagBytes uint64 = 8 << 20 // 8 MiB
	// MaxActivateMaxLagBytes is the recommended upper bound on the readiness
	// threshold. NOTE: it is advisory only and is NOT enforced (see the note on
	// ActivateOptions.resolve). Above roughly this much residual, at realistic apply
	// throughput the drain may not finish within the gateway buffer window and the
	// buffer could overflow, so operators should keep max_lag_bytes at or below it,
	// in step with the gateway's buffer-window / max-failover-duration.
	MaxActivateMaxLagBytes uint64 = 16 << 20 // 16 MiB
	// DefaultActivateWaitTimeout bounds the readiness wait when the request leaves
	// wait_timeout_seconds at 0.
	DefaultActivateWaitTimeout = 30 * time.Second
	// activateLagPollInterval is how often the readiness gate re-reads live lag.
	activateLagPollInterval = 500 * time.Millisecond
)

// ErrNotReady is returned by Activate when the migration cannot be cut over yet:
// the live replication lag did not fall to the requested threshold within the wait
// timeout. The migration is left untouched in the IMPORT direction (no cutover, no
// serving change), so the operator can retry (optionally with a larger threshold or
// timeout, or after write load subsides). The gRPC layer maps it to
// FAILED_PRECONDITION.
var ErrNotReady = errors.New("migration not ready to activate")

// ActivateOptions parameterizes the cutover readiness gate. The zero value uses the
// server defaults (DefaultActivateMaxLagBytes, DefaultActivateWaitTimeout).
type ActivateOptions struct {
	// MaxLagBytes is the readiness threshold: activation waits until the live
	// replication lag is at or below this before it quiesces the source and cuts
	// over. 0 uses DefaultActivateMaxLagBytes; a value above MaxActivateMaxLagBytes
	// is refused.
	MaxLagBytes uint64
	// WaitTimeout bounds the readiness wait. 0 uses DefaultActivateWaitTimeout.
	WaitTimeout time.Duration

	// CallerRole is the PostgreSQL role the gateway's SCRAM-authenticated
	// client is connected as, set only on the gateway SQL path (ALTER
	// MIGRATION ... PHASE). It gates the destructive DropTables step a
	// not-yet-started IMPORT runs: the gateway's CanCreateMigration check only
	// confirms the caller could perform logical-replication setup in
	// principle (database CREATE + pg_create_subscription), not that it owns
	// the specific target tables this migration names — without this, that
	// role could name an arbitrary existing table and have it dropped via the
	// admin pool regardless of ownership. Empty on the CLI/multiadmin path,
	// which never touches the gateway and has no SQL-authenticated identity
	// to check (an existing, separately tracked trust boundary — see
	// ActivateOptions.resolve's doc comment on that path's other gaps); the
	// check is skipped in that case, unchanged from today's behavior.
	CallerRole string
}

// resolve fills in the server defaults for any unset option.
//
// NOTE: the up-front "threshold too large" check is intentionally NOT enforced here.
// Whether a given max_lag_bytes is too large depends on the gateway's buffer window,
// which the coordinator cannot observe across services, and the CLI/multiadmin path
// never touches the gateway at all — so a hard refuse here would be guessing.
// MaxActivateMaxLagBytes remains as advisory guidance (and is documented in the
// design doc); choosing a threshold small enough that the drain fits the buffer
// window is the operator's responsibility. Revisit if the gateway buffer window
// becomes visible to the coordinator.
func (o ActivateOptions) resolve() (uint64, time.Duration) {
	lag := o.MaxLagBytes
	if lag == 0 {
		lag = DefaultActivateMaxLagBytes
	}
	wait := o.WaitTimeout
	if wait == 0 {
		wait = DefaultActivateWaitTimeout
	}
	return lag, wait
}

// SetMigrationDirection sets the active direction declaratively. Setting the
// current direction is a no-op; the other performs the symmetric barrier + flip:
// drain the current publisher, advance the new writer's sequences, tear down the
// current link, and establish the reverse link (copy_data=false). It requires a
// caught-up STREAMING state.
// Activate cuts a migration over to serving by switching to the EXPORT direction
// (wait until lag <= threshold, drain to lag zero, flip direction, start serving).
// It requires the migration to be currently importing; activating an already-active
// (EXPORT) migration is an error. The readiness gate (opts) bounds the residual lag
// at the moment the source is quiesced so the cutover fits the gateway buffer
// window; if the lag does not fall to the threshold within opts.WaitTimeout it
// returns ErrNotReady and leaves the migration importing. SetMigrationDirection does
// the barrier + flip.
// waitForCutoverReadiness polls the live source-slot lag until it is at or below
// maxLag, or returns ErrNotReady once wait elapses. It runs before the source is
// quiesced, so it observes the real streaming backlog under concurrent write load.
func (c *Coordinator) waitForCutoverReadiness(ctx context.Context, m *Migration, maxLag uint64, wait time.Duration) error {
	dsn, err := c.resolveSourceDSN(ctx, m)
	if err != nil {
		return err
	}
	src, err := c.newSource(ctx, dsn)
	if err != nil {
		return err
	}
	defer src.close()

	deadline := c.now().Add(wait)
	ticker := time.NewTicker(activateLagPollInterval)
	defer ticker.Stop()
	for {
		lag, _, present, err := src.ReplicationLag(m.SubscriptionName())
		if err != nil {
			return err
		}
		if present && lag <= maxLag {
			c.logger.InfoContext(ctx, "migration ready to activate", "id", m.ID, "lag_bytes", lag, "max_lag_bytes", maxLag)
			return nil
		}
		if !c.now().Before(deadline) {
			if !present {
				return fmt.Errorf("%w: replication slot %q not found on source after %s (nothing consumed yet)", ErrNotReady, m.SubscriptionName(), wait)
			}
			return fmt.Errorf("%w: replication lag %d bytes did not fall to %d within %s (source still under write load?)", ErrNotReady, lag, maxLag, wait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// checkExportPreconditions fails fast, before any destructive drain/switch or
// readiness wait, on the two EXPORT-direction preconditions: a target conninfo
// must be configured for the reverse subscription, and the source must be able
// to create a subscription (PG<16 without superuser cannot). Both are cheap,
// local checks (a config lookup and one source query), so this always runs
// before the potentially long readiness-gate wait in SetMigrationDirection —
// otherwise a misconfigured switch would only surface after wasting
// opts.WaitTimeout polling live lag, or worse, mid-switch after the current
// link has already been torn down.
func (c *Coordinator) checkExportPreconditions(ctx context.Context, m *Migration) error {
	if c.targetConnInfo == nil {
		return errors.New("EXPORT direction is not configured (no target conninfo for the reverse subscription)")
	}
	// Building the reverse-subscription conninfo enforces the EXPORT
	// preconditions (a gateway advertise host is configured, slot-based
	// replication is enabled).
	if _, err := c.targetConnInfo(m.TargetDatabase); err != nil {
		return err
	}
	dsn, err := c.resolveSourceDSN(ctx, m)
	if err != nil {
		return err
	}
	src, err := c.newSource(ctx, dsn)
	if err != nil {
		return err
	}
	info, err := src.Info()
	src.close()
	if err != nil {
		return err
	}
	if !info.CanCreateSubscription {
		return fmt.Errorf("cannot switch to EXPORT: the source cannot create subscriptions (server_version_num %d) — needs superuser, or PostgreSQL 16+ with pg_create_subscription membership", info.ServerVersionNum)
	}
	return nil
}

// SetMigrationDirection sets the migration's active direction declaratively.
// For a migration that has not yet started (still short of COPYING),
// target=IMPORT runs the initial setup pipeline (validate, schema copy,
// publication, subscription, catch-up) — there is no separate start action,
// since there is nothing yet to resume. Otherwise it performs the symmetric
// barrier + flip: drain the current publisher, advance the new writer's
// sequences, tear down the current link, and establish the reverse link
// (copy_data=false). Setting the current direction is a no-op; the switch
// requires a caught-up STREAMING state.
//
// Switching to EXPORT additionally gates on replication readiness (opts):
// before quiescing the source, it waits until the live replication lag falls
// to opts.MaxLagBytes (bounded by opts.WaitTimeout), so the residual drain
// under the read-only barrier completes inside the gateway's failover-buffer
// window; if the lag does not converge in time it returns ErrNotReady and
// leaves the migration importing. opts is ignored for target=IMPORT.
func (c *Coordinator) SetMigrationDirection(ctx context.Context, ref Ref, target Direction, opts ActivateOptions) (*Projection, error) {
	if target != DirectionImport && target != DirectionExport {
		return nil, fmt.Errorf("invalid direction %q", target)
	}

	c.mu.Lock()
	m, err := c.store.GetByRef(ctx, ref)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	// Not-yet-started IMPORT: subsumes the old StartMigration. A CREATED (or
	// otherwise pre-COPYING) migration's directionOf() is already IMPORT, so
	// without this check the no-op path below would short-circuit before ever
	// running setup. (The directionOf guard is a defensive sanity check: every
	// phase ranked below COPYING is, by construction, always IMPORT direction.)
	if target == DirectionImport && phaseRank(m.Phase) < phaseRank(PhaseCopying) {
		defer c.mu.Unlock()
		if directionOf(m.Phase) != DirectionImport {
			return nil, fmt.Errorf("start is only valid in IMPORT direction; migration %d is %s", m.ID, directionOf(m.Phase))
		}
		c.journal(ctx, m, JournalEventStart, "")
		if err := c.runSetup(ctx, m, opts.CallerRole); err != nil {
			c.fail(ctx, m, err)
			return nil, err
		}
		if err := c.reconcileLocked(ctx, m); err != nil {
			return nil, err
		}
		status, _ := c.target.SubscriptionStatus(ctx, m.SubscriptionName())
		return c.project(ctx, m, status), nil
	}
	// A switch already recorded in the row (crash resume, or a duplicate call):
	// roll the same-target one forward idempotently; reject a conflicting one.
	// Checked here (still locked, before ever unlocking for the gate below) so a
	// conflicting/duplicate call fails fast rather than waiting out the gate first.
	if m.Phase == PhaseSwitchingToExport || m.Phase == PhaseSwitchingToImport {
		defer c.mu.Unlock()
		if switchTarget(m.Phase) != target {
			return nil, fmt.Errorf("a switch to %s is already in progress for migration %d", switchTarget(m.Phase), m.ID)
		}
		return c.applySwitch(ctx, m, target)
	}
	if directionOf(m.Phase) == target {
		defer c.mu.Unlock()
		status, _ := c.liveStatus(ctx, m)
		return c.project(ctx, m, status), nil // no-op
	}
	// EXPORT preconditions are cheap, local checks (config lookup, one source
	// query) — fail fast on them before ever spending opts.WaitTimeout polling
	// live lag below, rather than discovering a misconfiguration only after
	// the wait.
	if target == DirectionExport {
		if err := c.checkExportPreconditions(ctx, m); err != nil {
			c.mu.Unlock()
			return nil, err
		}
	}
	c.mu.Unlock() // re-taken below; released so the readiness wait (if any) doesn't hold it

	// Cutover readiness gate: block until the live lag falls to the readiness
	// threshold BEFORE quiescing the source, so the subsequent read-only drain
	// barrier (drainCurrent, inside applySwitch) has only a small residual to
	// flush and completes inside the gateway buffer window. Only relevant
	// switching IMPORT -> EXPORT from a live IMPORTING state; a resume of an
	// already-recorded switch skips the gate (the source is already read-only
	// past that point).
	if target == DirectionExport && m.Phase == PhaseImporting {
		maxLag, wait := opts.resolve()
		if err := c.waitForCutoverReadiness(ctx, m, maxLag, wait); err != nil {
			return nil, err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	m, err = c.store.GetByRef(ctx, ref)
	if err != nil {
		return nil, err
	}
	// Re-validate against the authoritative, freshly re-locked state: the gate
	// above ran unlocked, so another call (a concurrent duplicate, or a drop)
	// may have changed the migration while this one waited.
	if m.Phase == PhaseSwitchingToExport || m.Phase == PhaseSwitchingToImport {
		if switchTarget(m.Phase) != target {
			return nil, fmt.Errorf("a switch to %s is already in progress for migration %d", switchTarget(m.Phase), m.ID)
		}
		return c.applySwitch(ctx, m, target)
	}
	if directionOf(m.Phase) == target {
		status, _ := c.liveStatus(ctx, m)
		return c.project(ctx, m, status), nil // no-op
	}
	if target == DirectionExport {
		if err := c.checkExportPreconditions(ctx, m); err != nil {
			return nil, err
		}
	}

	if err := c.reconcileLocked(ctx, m); err != nil {
		return nil, err
	}
	if !isStreaming(m.Phase) {
		return nil, fmt.Errorf("migration %d must be caught up (IMPORTING/EXPORTING) to switch direction; current phase %s", m.ID, m.Phase)
	}

	// Commit the switch intent to the row before touching either database. A crash
	// after this point leaves a directional SWITCHING_TO_* phase that the resume
	// path (reconcileLocked) rolls forward — the row is the switch's write-ahead log.
	m.Phase = switchingPhase(target)
	if err := c.store.Update(ctx, m); err != nil {
		return nil, err
	}
	return c.applySwitch(ctx, m, target)
}

// applySwitch performs — or, after a crash, resumes — the switch recorded by
// m.Phase toward target. It is idempotent: the drain barrier runs only while the
// current-direction subscription still exists (once the switch has dropped it,
// the barrier already held), and switchTo skips objects it has already created,
// so re-running converges. Caller holds c.mu; m.Phase is switchingPhase(target).
func (c *Coordinator) applySwitch(ctx context.Context, m *Migration, target Direction) (*Projection, error) {
	// Switching to IMPORT makes the target a subscriber, so it must stop serving
	// client writes before it starts applying replicated changes — otherwise a stray
	// client write on the target-as-subscriber would diverge, the symmetric hazard to
	// the source side. Drain the pooler to non-serving synchronously now rather than
	// waiting for the async monitor tick to observe the IMPORTING phase. This is the
	// mirror of the EXPORT cutover's synchronous releaseForExport, and cannot deadlock:
	// the migrator's target-side ops run through the admin InternalQueryService, which
	// bypasses the serving gate. Idempotent, so safe on a crash-resumed switch. A nil
	// hook (unit tests without a manager) skips it, leaving the flip to the monitor.
	if target == DirectionImport && c.drainForImport != nil {
		if err := c.drainForImport(ctx); err != nil {
			c.fail(ctx, m, err)
			return nil, fmt.Errorf("drain to non-serving before deactivate switch: %w", err)
		}
	}
	live, err := c.currentLinkLive(ctx, m)
	if err != nil {
		c.fail(ctx, m, err)
		return nil, err
	}
	// drainedLSN is the quiesce point on the old writer (the "drained-to" LSN),
	// captured for the handoff journal entry. It is only known when the drain
	// actually runs: on a crash-resumed switch the current link is already gone
	// (live == false), so the drain is skipped and drainedLSN stays empty.
	var drainedLSN string
	if live {
		// A switching phase still carries the current (pre-switch) direction, so
		// derive it from the phase: SWITCHING_TO_EXPORT drains the import publisher,
		// SWITCHING_TO_IMPORT drains the export publisher.
		drainedLSN, err = c.drainCurrent(ctx, m, directionOf(m.Phase))
		if err != nil {
			c.fail(ctx, m, err)
			return nil, err
		}
	}
	// newWriterLSN is the start LSN on the side that becomes the writer after the
	// switch (the handoff point past the switch's own catalog WAL).
	newWriterLSN, err := c.switchTo(ctx, m, target)
	if err != nil {
		c.fail(ctx, m, err)
		return nil, err
	}
	// Durable handoff record: append the switch's handoff LSNs BEFORE committing
	// the streaming phase, so a switch is never committed without an auditable
	// handoff entry. The event is the operator verb the switch corresponds to
	// (ACTIVATE for IMPORT->EXPORT, DEACTIVATE for EXPORT->IMPORT). A failed append
	// fails the call while the phase is still the SWITCHING_* intent, so
	// reconcileLocked re-runs applySwitch and retries (on that retry the current
	// link is already gone, so drainedLSN is empty). Deliberately not routed through
	// fail(): switchTo already reconfigured replication, so the switch must roll
	// forward, not be marked FAILED.
	handoffEvent := JournalEventActivate
	if target == DirectionImport {
		handoffEvent = JournalEventDeactivate
	}
	if err := c.store.InsertJournal(ctx, &JournalEntry{
		MigrationID:   m.ID,
		MigrationName: m.Name,
		Event:         handoffEvent,
		Phase:         streamingPhase(target),
		Direction:     target,
		FromLSN:       drainedLSN,
		ToLSN:         newWriterLSN,
	}); err != nil {
		return nil, fmt.Errorf("record migration %d handoff journal entry: %w", m.ID, err)
	}
	m.Phase = streamingPhase(target)
	if err := c.store.Update(ctx, m); err != nil {
		return nil, err
	}
	// EXPORT: flip this pooler to SERVING synchronously now that EXPORTING is
	// committed, instead of waiting for the async ~5s postgres-monitor tick to
	// observe the phase. Prompt serving-on is what lets the gateway's failover
	// buffer replay the queries it held during the cutover inside its bounded
	// window (the buffer drains when the leader self-attests SERVING); a slow flip
	// risks the buffer timing out and refusing those queries. It also unblocks the
	// reverse subscription below, which the source can only attach once the target
	// serves. A nil hook (unit tests) leaves the flip to the monitor.
	if target == DirectionExport && c.releaseForExport != nil {
		if err := c.releaseForExport(ctx); err != nil {
			// Do not fail the migration: EXPORTING is already committed and the monitor
			// tick will reconcile serving on its own; log and continue.
			c.logger.WarnContext(ctx, "synchronous serving flip failed; falling back to monitor tick", "id", m.ID, "error", err)
		}
	}
	// EXPORT: establish the reverse subscription now that the phase is EXPORTING,
	// so the gateway (which serves only at EXPORTING) will accept the source's
	// connection. Retry the transient "temporarily unavailable" window. This runs
	// after the EXPORTING commit on purpose; a failure here does NOT fail the
	// migration (serving is already up) — reconcileLocked keeps re-ensuring the
	// link. IMPORT's reverse subscription dials the source directly and was already
	// created in switchTo.
	if target == DirectionExport {
		if err := c.retryReverseExportLink(ctx, m); err != nil {
			return nil, err
		}
	}
	status, _ := c.liveStatus(ctx, m)
	return c.project(ctx, m, status), nil
}

// Reverse-export retry bounds: the gateway starts serving only once the phase is
// EXPORTING and the monitor propagates that to the pooler's serving status, so
// the source's reverse subscription may see a brief "temporarily unavailable".
const (
	reverseExportRetryFor      = 90 * time.Second
	reverseExportRetryInterval = time.Second
)

// retryReverseExportLink establishes the EXPORT reverse subscription, retrying
// the gateway's transient not-yet-serving signal for a bounded window. Any other
// error, a cancelled context, or the deadline returns immediately.
func (c *Coordinator) retryReverseExportLink(ctx context.Context, m *Migration) error {
	deadline := c.now().Add(reverseExportRetryFor)
	for {
		err := c.ensureReverseExportLink(ctx, m)
		if err == nil {
			return nil
		}
		if !isRetryableUnavailable(err) || ctx.Err() != nil || !c.now().Before(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reverseExportRetryInterval):
		}
	}
}

// ensureReverseExportLink creates the EXPORT reverse subscription — this source
// subscribing back to the target through the gateway — if it is not already
// present, and detects (but does not repair) a degraded link if the slot it
// depends on has become unusable. Idempotent, so both applySwitch and
// reconcileLocked (crash recovery, when a migration is found already EXPORTING
// but the link is missing or degraded) call it.
//
// The target-side slot is not catalog/WAL state like pg_subscription — a
// freshly-created failover slot is PostgreSQL-internally RS_TEMPORARY until it
// is actually persisted, and a target-primary failover landing before that
// drops it outright, even with --enable-slot-based-replication on (see
// docs/migration/migrator_design.md's EXPORT-direction failover note). When
// that happens the subscription row survives (its apply worker just errors
// forever), so checking subscription existence alone — the only thing this
// function used to check — reports healthy on a permanently broken link.
//
// There is no safe way to repair a slot in place: a recreated slot starts
// streaming from "now", so resuming the existing subscription into it would
// silently skip everything in between (see
// docs/ha/logical_slot_failover_design.md §4.4.1 on why reconstruct-and-resume
// is unsafe). The only correctness-preserving recovery is the detect-and-reseed
// pattern designed in that doc's §4.4.2: drop the stale subscription, recreate
// the slot, and resubscribe with copy_data=true for a fresh full copy — but
// that is a large, unbounded operation against the source (potentially a full
// copy of every migrated table), so it must never run as a side effect of a
// routine reconcile tick with no operator in the loop. This function only
// detects and records the condition; Coordinator.ReseedReverseLink performs
// the actual repair, as an explicit, operator-triggered action.
func (c *Coordinator) ensureReverseExportLink(ctx context.Context, m *Migration) error {
	conninfo, err := c.targetConnInfo(m.TargetDatabase)
	if err != nil {
		return err
	}
	dsn, err := c.resolveSourceDSN(ctx, m)
	if err != nil {
		return err
	}
	src, err := c.newSource(ctx, dsn)
	if err != nil {
		return err
	}
	defer src.close()
	sub, pub := m.SubscriptionName(), m.PublicationName()

	subExists, err := src.SubscriptionExists(sub)
	if err != nil {
		return err
	}
	slotReady, err := c.target.SlotReady(ctx, sub)
	if err != nil {
		return err
	}

	if subExists && slotReady {
		return nil // healthy
	}

	if !slotReady {
		return c.markReverseLinkDegraded(ctx, m, subExists)
	}

	// Slot is ready but the subscription doesn't exist yet: the original path —
	// a still-valid slot the subscription hasn't attached to (crash before
	// attach, or first attach after switch). The barrier already made the two
	// sides identical at the slot's LSN, so no copy is needed.
	if err := unquiesceForReverseSubscription(src); err != nil {
		return err
	}
	return src.CreateSubscription(sub, conninfo, pub, false, sub)
}

// markReverseLinkDegraded records that the EXPORT reverse link is unusable and
// returns an error, so callers (applySwitch, reconcileLocked) surface the
// failure instead of silently repairing it. Idempotent on the persisted state:
// it only writes and journals on the transition into degraded, so a periodic
// reconcile tick re-detecting the same condition does not spam the journal or
// re-persist the same row on every tick — it still returns an error every
// time, though, so the caller (Reconcile's per-migration loop) keeps logging
// until ReseedReverseLink repairs it.
func (c *Coordinator) markReverseLinkDegraded(ctx context.Context, m *Migration, subExists bool) error {
	reason := "reverse export slot is missing or unusable"
	if subExists {
		reason = "reverse export subscription exists but its slot is missing or unusable"
	}
	if m.ReverseLinkError != reason {
		m.ReverseLinkError = reason
		if err := c.store.Update(ctx, m); err != nil {
			return fmt.Errorf("persist reverse link degraded state: %w", err)
		}
		c.journal(ctx, m, JournalEventReverseLinkDegraded, reason)
	}
	return fmt.Errorf("%s: reseed required (call ReseedReverseLink)", reason)
}

// ReseedReverseLink repairs a degraded EXPORT reverse link: the explicit,
// operator-triggered counterpart to ensureReverseExportLink's detection, which
// never repairs the link on its own (see that function's doc comment for why).
// Performs the same drop-and-reseed recovery ensureReverseExportLink used to
// perform automatically: drops the broken subscription (if any), recreates the
// slot fresh, and resubscribes with a full copy (copy_data=true) — there is no
// safe way to resume from a dropped slot, so a fresh full copy is the only
// correctness-preserving recovery.
//
// Idempotent: if the link is already healthy by the time this runs (someone
// else already fixed it, or the detection was transient), it is a no-op
// beyond clearing a stale degraded marker.
func (c *Coordinator) ReseedReverseLink(ctx context.Context, ref Ref) (*Projection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, err := c.store.GetByRef(ctx, ref)
	if err != nil {
		return nil, err
	}
	if m.Phase != PhaseExporting {
		return nil, fmt.Errorf("reseed is only valid for an EXPORTING migration; migration %d is %s", m.ID, m.Phase)
	}

	conninfo, err := c.targetConnInfo(m.TargetDatabase)
	if err != nil {
		return nil, err
	}
	dsn, err := c.resolveSourceDSN(ctx, m)
	if err != nil {
		return nil, err
	}
	src, err := c.newSource(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer src.close()
	sub, pub := m.SubscriptionName(), m.PublicationName()

	subExists, err := src.SubscriptionExists(sub)
	if err != nil {
		return nil, err
	}
	slotReady, err := c.target.SlotReady(ctx, sub)
	if err != nil {
		return nil, err
	}

	if subExists && slotReady {
		return c.clearReverseLinkDegraded(ctx, m)
	}

	if subExists {
		// The subscription's apply worker depends on a slot that is now gone,
		// still temporary, or invalidated. Drop it so the reseed below starts
		// clean rather than racing a worker still trying (and failing) to attach.
		if err := src.DropSubscription(sub); err != nil {
			return nil, err
		}
	}
	// (Re)create the slot fresh. DropLogicalSlot no-ops if there is nothing to
	// drop (e.g. the slot never existed because a prior attempt failed between
	// creating it and the subscription ever attaching).
	if err := c.target.DropLogicalSlot(ctx, sub); err != nil {
		return nil, err
	}
	if err := c.target.CreateLogicalSlot(ctx, sub); err != nil {
		return nil, err
	}
	lsn, err := c.target.CurrentLSN(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.target.AdvanceSlot(ctx, sub, lsn); err != nil {
		return nil, err
	}
	// copy_data=true: the old slot (and whatever position it held) is gone, so
	// resuming from it is not an option — a fresh full copy is the only
	// correctness-preserving choice.
	if err := unquiesceForReverseSubscription(src); err != nil {
		return nil, err
	}
	if err := src.CreateSubscription(sub, conninfo, pub, true, sub); err != nil {
		return nil, err
	}
	c.journal(ctx, m, JournalEventReseed, "operator-triggered reseed of the reverse export link")
	return c.clearReverseLinkDegraded(ctx, m)
}

// clearReverseLinkDegraded resets the persisted degraded marker once the
// reverse link is confirmed healthy, and returns a fresh projection. No-op
// (beyond the projection) if the marker was already clear.
func (c *Coordinator) clearReverseLinkDegraded(ctx context.Context, m *Migration) (*Projection, error) {
	if m.ReverseLinkError != "" {
		m.ReverseLinkError = ""
		if err := c.store.Update(ctx, m); err != nil {
			return nil, fmt.Errorf("clear reverse link degraded state: %w", err)
		}
	}
	status, _ := c.liveStatus(ctx, m)
	return c.project(ctx, m, status), nil
}

// unquiesceForReverseSubscription lifts the cluster-wide read-only barrier
// switchTo left in place (see its IMPORT->EXPORT branch) and terminates any
// client backend that reconnected in the window since — called immediately
// before CreateSubscription attaches the reverse subscription, the first
// point the source's own writes become safe (the target-side slot this
// attaches to already exists and is anchored at the correct LSN, so nothing
// written from here on is lost). Terminating here, unconditionally, is what
// closes the residual race: a stale client is free to reconnect the instant
// the barrier lifts and must be cut again before it can commit anything the
// reverse stream would never capture.
func unquiesceForReverseSubscription(src migrationSource) error {
	if err := src.SetReadOnly(false); err != nil {
		return err
	}
	return src.TerminateClientBackends()
}

// isRetryableUnavailable reports whether err is the gateway's transient
// not-yet-serving signal — SQLSTATE 57P03 (cannot_connect_now) or 08006
// (connection_failure) carrying "temporarily unavailable" — which clears once
// the target's serving status catches up to the EXPORTING phase.
func isRetryableUnavailable(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "temporarily unavailable") ||
		strings.Contains(s, "57P03") || strings.Contains(s, "08006")
}

// currentLinkLive reports whether the current-direction subscription still
// exists — i.e. the switch has not yet dropped it, so the drain barrier is still
// required. The current subscriber is the target while importing, the source
// while exporting.
func (c *Coordinator) currentLinkLive(ctx context.Context, m *Migration) (bool, error) {
	if directionOf(m.Phase) == DirectionImport {
		return c.target.SubscriptionExists(ctx, m.SubscriptionName())
	}
	dsn, err := c.resolveSourceDSN(ctx, m)
	if err != nil {
		return false, err
	}
	src, err := c.newSource(ctx, dsn)
	if err != nil {
		return false, err
	}
	defer src.close()
	return src.SubscriptionExists(m.SubscriptionName())
}

// switchTo tears down the current-direction link and establishes the reverse
// link (copy_data=false). Caller holds c.mu and has already drained. It returns
// the start LSN on the side that becomes the writer after the switch (the handoff
// point, past the switch's own catalog WAL) for the handoff journal entry.
func (c *Coordinator) switchTo(ctx context.Context, m *Migration, target Direction) (string, error) {
	dsn, err := c.resolveSourceDSN(ctx, m)
	if err != nil {
		return "", err
	}
	src, err := c.newSource(ctx, dsn)
	if err != nil {
		return "", err
	}
	defer src.close()
	sub, pub := m.SubscriptionName(), m.PublicationName()

	if target == DirectionExport {
		// IMPORT -> EXPORT: the target (Multigres) becomes publisher/writer, and
		// the old source becomes a subscriber. The source must stay cluster-wide
		// read-only (set by drainCurrent) until the reverse subscription
		// actually attaches — ensureReverseExportLink, called once EXPORTING
		// commits and the gateway serves, is what un-quiesces it (see that
		// function). Lifting the barrier here, before that link exists, would let
		// a stale client reconnect and commit a write the reverse stream can never
		// capture, diverging source and target. DropPublication just below is this
		// connection's own catalog change, not a client write, so it only needs a
		// session-local override, not the cluster-wide barrier lifted.
		if err := src.setSessionReadOnly(false); err != nil {
			return "", err
		}
		if err := c.target.AdvanceSequences(ctx, m.Tables, m.SequenceMargin); err != nil {
			return "", err
		}
		if err := c.target.DropSubscription(ctx, sub); err != nil {
			return "", err
		}
		if err := src.DropPublication(pub); err != nil {
			return "", err
		}
		if exists, err := c.target.PublicationExists(ctx, pub); err != nil {
			return "", err
		} else if !exists {
			if err := c.target.CreatePublication(ctx, pub, m.Tables); err != nil {
				return "", err
			}
		}
		// Pre-create the reverse slot on the target *now*, before serving turns on,
		// so it captures every subsequent target write; then advance it to the
		// current LSN — the handoff point, past the switch's own catalog WAL. This
		// is a local target operation (not through the gateway), so it avoids the
		// serving-gate deadlock. The reverse SUBSCRIPTION is created later, once the
		// phase is EXPORTING and the gateway serves (applySwitch ->
		// retryReverseExportLink), and attaches to this slot with create_slot=false —
		// so no write is lost in the window between serving turning on and the
		// subscription attaching. Idempotent on resume: before serving there are no
		// app writes, so re-advancing only skips more switch WAL, never data.
		if exists, err := c.target.SlotExists(ctx, sub); err != nil {
			return "", err
		} else if !exists {
			if err := c.target.CreateLogicalSlot(ctx, sub); err != nil {
				return "", err
			}
		}
		lsn, lerr := c.target.CurrentLSN(ctx)
		if lerr != nil {
			return "", lerr
		}
		if err := c.target.AdvanceSlot(ctx, sub, lsn); err != nil {
			return "", err
		}
		// The target's current LSN is the handoff point: the reverse slot was just
		// advanced to it, and the target is the new writer.
		return lsn, nil
	}

	// EXPORT -> IMPORT: the external source becomes publisher/writer again. The
	// reverse subscription on the source (dropped below) has its publisher on the
	// target, reached over the gateway replication tunnel — which the flip toward
	// IMPORT has already gated. src.DropSubscription detaches the slot before
	// dropping so it never dials that gated tunnel; the target-side reverse slot is
	// dropped explicitly here (DropLogicalSlot), so nothing is orphaned.
	//
	// Advance sequences before any write can land, so the side about to take writes
	// does not collide with copied values (sequences are not replicated).
	if err := src.AdvanceSequences(m.Tables, m.SequenceMargin); err != nil {
		return "", err
	}
	// The source is the new writer; its current LSN is the handoff point for the
	// journal entry (best-effort — a read failure here must not fail the switch).
	// Captured before CONNECT is restored below, so it marks the switch point, not
	// some later moment after app writes may already have landed.
	newWriterLSN, err := src.CurrentLSN()
	if err != nil {
		newWriterLSN = ""
	}
	if err := src.DropSubscription(sub); err != nil {
		return "", err
	}
	if err := c.target.DropPublication(ctx, pub); err != nil {
		return "", err
	}
	// Drop the reverse slot on the target. The reverse subscription attached with
	// create_slot=false, so dropping it (above, on the source) does not drop this
	// slot — do it explicitly or it lingers, pinning WAL and catalog_xmin.
	if err := c.target.DropLogicalSlot(ctx, sub); err != nil {
		return "", err
	}
	if exists, err := src.PublicationExists(pub); err != nil {
		return "", err
	} else if !exists {
		if err := src.CreatePublication(pub, m.Tables); err != nil {
			return "", err
		}
	}
	if exists, err := c.target.SubscriptionExists(ctx, sub); err != nil {
		return "", err
	} else if !exists {
		if err := c.target.CreateSubscription(ctx, sub, dsn, pub, false); err != nil {
			return "", err
		}
	}
	return newWriterLSN, nil
}

// teardown drops the subscription and publication (and thus the slot) on both
// sides for the given direction. dir is passed in rather than derived from m.Phase
// because a drop tears down while the phase is PhaseCompleting, which carries no
// direction. Every drop is IF EXISTS / existence-checked, so teardown is idempotent
// and safe to re-run (the reconcile poller finishes an interrupted drop this way).
// Source-side failures are best-effort — the source may be unreachable during an
// abort — so they are logged, never returned.
func (c *Coordinator) teardown(ctx context.Context, m *Migration, dir Direction) {
	logErr := func(step string, err error) {
		if err != nil {
			c.logger.WarnContext(ctx, "migration teardown step failed", "migration", m.ID, "step", step, "error", err)
		}
	}
	// The source may be unreachable during an abort; still tear down the target
	// side. Source-side drops run only if we could resolve a DSN and connect.
	var src migrationSource
	if dsn, err := c.resolveSourceDSN(ctx, m); err != nil {
		logErr("resolve source connection", err)
	} else if s, err := c.newSource(ctx, dsn); err != nil {
		logErr("connect source", err)
	} else {
		src = s
		defer src.close()
	}
	if dir == DirectionImport {
		// IMPORT: subscription (apply) on the target, publication (capture) on the source.
		logErr("drop target subscription", c.target.DropSubscription(ctx, m.SubscriptionName()))
		if src != nil {
			// A graceful (non-force) drop drained the source with
			// default_transaction_read_only=on; reset it before the source-side
			// DROP PUBLICATION, which would otherwise be rejected under a read-only
			// transaction, orphaning the publication. Also leaves the abandoned old
			// source writable again (the migration is being torn down). No-op on a
			// force drop that never quiesced.
			logErr("un-quiesce source", src.SetReadOnly(false))
			logErr("drop source publication", src.DropPublication(m.PublicationName()))
		}
		return
	}
	// EXPORT: subscription (apply) on the source, publication (capture) on the target.
	if src != nil {
		logErr("drop source subscription", src.DropSubscription(m.SubscriptionName()))
	}
	logErr("drop target publication", c.target.DropPublication(ctx, m.PublicationName()))
	// The reverse slot was pre-created on the target (create_slot=false), so the
	// source-side subscription drop above does not remove it.
	logErr("drop target reverse slot", c.target.DropLogicalSlot(ctx, m.SubscriptionName()))
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

// reconcileLocked advances a single migration. Caller holds c.mu. It moves
// COPYING to IMPORTING once the initial copy is caught up, and rolls a switch
// that was interrupted by a crash/failover forward to completion — the
// directional SWITCHING_TO_* phase is the recorded intent (see the design doc's
// crash-safe-switch section).
func (c *Coordinator) reconcileLocked(ctx context.Context, m *Migration) error {
	switch m.Phase {
	case PhaseCopying:
		status, err := c.target.SubscriptionStatus(ctx, m.SubscriptionName())
		if err != nil {
			return err
		}
		if status.CaughtUp {
			now := c.now()
			m.Phase = PhaseImporting
			m.StreamingSince = &now
			if err := c.store.Update(ctx, m); err != nil {
				return err
			}
			c.journal(ctx, m, JournalEventPhase, string(PhaseCopying)+"->"+string(PhaseImporting))
			return nil
		}
	case PhaseSwitchingToExport:
		_, err := c.applySwitch(ctx, m, DirectionExport)
		return err
	case PhaseSwitchingToImport:
		_, err := c.applySwitch(ctx, m, DirectionImport)
		return err
	case PhaseExporting:
		// Crash recovery: a migration committed to EXPORTING before its reverse
		// subscription was established (applySwitch commits EXPORTING, then creates
		// the link) re-establishes it here. Idempotent — a no-op once the link
		// exists. A degraded link (slot lost) is detected here on every tick, not
		// repaired: ensureReverseExportLink records it and returns an error, which
		// Reconcile just logs and moves on from — the actual repair is
		// Coordinator.ReseedReverseLink, an explicit operator action.
		return c.ensureReverseExportLink(ctx, m)
	case PhaseCompleting:
		// Crash recovery: a drop committed COMPLETING but did not finish (the
		// primary restarted mid-teardown, or a drain failed after the phase was
		// persisted and the phase-restore did not land). Finish the teardown from
		// the persisted direction and remove the row, so the serving gate — which
		// counts any non-EXPORTING migration and would otherwise hold this shard
		// non-serving forever — is released.
		c.teardown(ctx, m, m.effectiveDirection())
		c.journal(ctx, m, JournalEventDrop, "")
		return c.store.Delete(ctx, m.ID)
	}
	return nil
}

// liveStatus reads subscription status for phases where it is meaningful.
func (c *Coordinator) liveStatus(ctx context.Context, m *Migration) (*SubscriptionStatus, error) {
	if m.Phase != PhaseCopying && !isStreaming(m.Phase) {
		return nil, nil
	}
	st, err := c.target.SubscriptionStatus(ctx, m.SubscriptionName())
	if err != nil {
		return nil, err
	}
	// Attach live publisher-side lag (best-effort; a lag-probe failure must not fail
	// status). Source-side in IMPORT, target-side in EXPORT — the same slot the drain
	// barrier waits on.
	st.LagBytes, st.LagSeconds = c.liveLag(ctx, m)
	return st, nil
}

// liveLag returns the current replication lag (bytes, seconds) for the migration,
// measured on whichever side is currently the publisher: the external source in
// IMPORT, the local target in EXPORT. It is best-effort — any error (source
// unreachable, slot absent) yields (0, 0) rather than failing status. The IMPORT
// path opens a short-lived source connection, bounded so a slow source cannot stall
// a status read.
func (c *Coordinator) liveLag(ctx context.Context, m *Migration) (uint64, float64) {
	switch directionOf(m.Phase) {
	case DirectionImport:
		lagCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		dsn, err := c.resolveSourceDSN(lagCtx, m)
		if err != nil {
			return 0, 0
		}
		src, err := c.newSource(lagCtx, dsn)
		if err != nil {
			return 0, 0
		}
		defer src.close()
		b, s, present, err := src.ReplicationLag(m.SubscriptionName())
		if err != nil || !present {
			return 0, 0
		}
		return b, s
	case DirectionExport:
		b, s, present, err := c.target.ReplicationLag(ctx, m.SubscriptionName())
		if err != nil || !present {
			return 0, 0
		}
		return b, s
	default:
		return 0, 0
	}
}

// setPhase persists a phase transition. Caller holds c.mu.
func (c *Coordinator) setPhase(ctx context.Context, m *Migration, phase Phase) error {
	m.Phase = phase
	return c.store.Update(ctx, m)
}

// advancePhase persists a phase transition (setPhase) and appends a PHASE journal
// entry recording "from->to". Caller holds c.mu. The journal append is
// best-effort (see journal); the phase change itself is the durable record.
func (c *Coordinator) advancePhase(ctx context.Context, m *Migration, phase Phase) error {
	from := m.Phase
	if err := c.setPhase(ctx, m, phase); err != nil {
		return err
	}
	c.journal(ctx, m, JournalEventPhase, string(from)+"->"+string(phase))
	return nil
}

// journal appends one entry to the migration journal, best-effort: a failed
// append is logged but never aborts the migration. The journal is an append-only
// audit log, not the crash-safe intent record — that is the migration row — so a
// lost audit entry must not fail an operation. The one exception is the switch
// handoff record, which applySwitch appends durably (fatal on error) so a switch
// is never committed without its handoff LSNs. Caller holds c.mu.
func (c *Coordinator) journal(ctx context.Context, m *Migration, event JournalEvent, detail string) {
	e := &JournalEntry{
		MigrationID:   m.ID,
		MigrationName: m.Name,
		Event:         event,
		Phase:         m.Phase,
		Direction:     m.effectiveDirection(),
		LastError:     m.LastError,
		Detail:        detail,
	}
	if err := c.store.InsertJournal(ctx, e); err != nil {
		c.logger.WarnContext(ctx, "append migration journal entry", "migration", m.ID, "event", string(event), "error", err)
	}
}

// fail records a terminal error on a migration (best-effort persist). Caller
// holds c.mu. err is most often itself a context deadline/cancellation (a stuck
// drain hitting DefaultDrainWaitTimeout), so persisting must run on a context
// detached from ctx — reusing ctx here would hit the already-expired deadline
// immediately (connpool fails fast on a dead context) and the FAILED phase and
// its journal entry would silently never land, exactly like DropMigration's
// failed-drain rollback a few hundred lines above.
func (c *Coordinator) fail(ctx context.Context, m *Migration, err error) {
	m.Phase = PhaseFailed
	m.LastError = err.Error()
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if uerr := c.store.Update(persistCtx, m); uerr != nil {
		c.logger.WarnContext(persistCtx, "persist failed phase", "migration", m.ID, "error", uerr)
	}
	c.journal(ctx, m, JournalEventFailed, "")
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
	case PhaseImporting, PhaseExporting:
		return 5
	default:
		return 100
	}
}

// Projection is the operator-facing view of a migration: persisted intent plus
// live status. Source holds the redacted host[:port]/db summary of the
// migration's current connection, resolved live (not cached) each time, same
// as resolveSourceDSN.
type Projection struct {
	ID     int64
	Name   string
	Source string // redacted host[:port]/db of the current connection
	// ConnectionName is the name of the Connection this migration's source
	// resolves through (see Migration.ConnectionID). Exposed to the migrator
	// gRPC API as Migration.connection_name.
	ConnectionName   string
	Phase            Phase
	ActiveDirection  Direction
	TargetDatabase   string
	TargetShard      string
	TargetTableGroup string
	Tables           []string
	SequenceMargin   int64
	PublicationName  string
	SubscriptionName string
	TotalRelations   int64
	ReadyRelations   int64
	CaughtUp         bool
	ReceivedLSN      string
	LatestEndLSN     string
	// LagBytes / LagSeconds are the live replication lag on the current publisher
	// (source in IMPORT, target in EXPORT); 0 when not streaming or unavailable.
	LagBytes   uint64
	LagSeconds float64
	LastError  string
	// ReverseLinkError is non-empty when the EXPORT reverse link is degraded
	// (see Migration.ReverseLinkError) — the migration is still EXPORTING and
	// serving, but its rollback path needs Coordinator.ReseedReverseLink.
	ReverseLinkError string
	CreatedAt        time.Time
	StreamingSince   *time.Time
}

// project builds the redacted projection, merging optional live status. It
// resolves the migration's current connection fresh (not cached) for Source/
// ConnectionName, same as resolveSourceDSN — a connection lookup failure (e.g.
// a race with a concurrent drop, which the FK normally prevents) degrades
// those two fields to empty rather than failing the whole projection, since a
// status read should not fail outright over a display-only field.
func (c *Coordinator) project(ctx context.Context, m *Migration, status *SubscriptionStatus) *Projection {
	var source, connectionName string
	if conn, err := c.store.GetConnectionByRef(ctx, Ref{ID: m.ConnectionID}); err == nil {
		source = redactDSN(conn.DSN)
		connectionName = conn.Name
	}
	p := &Projection{
		ID:               m.ID,
		Name:             m.Name,
		Source:           source,
		ConnectionName:   connectionName,
		Phase:            m.Phase,
		ActiveDirection:  directionOf(m.Phase),
		TargetDatabase:   m.TargetDatabase,
		TargetShard:      m.TargetShard,
		TargetTableGroup: m.TargetTableGroup,
		Tables:           m.Tables,
		SequenceMargin:   m.SequenceMargin,
		PublicationName:  m.PublicationName(),
		SubscriptionName: m.SubscriptionName(),
		LastError:        m.LastError,
		ReverseLinkError: m.ReverseLinkError,
		CreatedAt:        m.CreatedAt,
		StreamingSince:   m.StreamingSince,
	}
	if status != nil {
		p.TotalRelations = status.TotalRelations
		p.ReadyRelations = status.ReadyRelations
		p.CaughtUp = status.CaughtUp
		p.ReceivedLSN = status.ReceivedLSN
		p.LatestEndLSN = status.LatestEndLSN
		p.LagBytes = status.LagBytes
		p.LagSeconds = status.LagSeconds
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
