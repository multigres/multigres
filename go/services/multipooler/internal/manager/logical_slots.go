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

package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// ============================================================================
// Logical Replication Slots
//
// This file contains the low-level primitives for managing multigres-owned
// logical replication slots used by the slot-failover machinery. They run as
// admin SQL through the same superuser query path (pm.exec / pm.query) used by
// pg_promote() and ALTER SYSTEM.
//
// These primitives are deliberately standalone: nothing in the lifecycle
// (recruit / SetPrimary / promote / demote) calls them yet. Wiring them in is a
// later step.
// ============================================================================

const (
	// logicalSlotNamePrefix namespaces every multigres-managed replication
	// slot. Client-created slots never carry it, so a multigres slot can never
	// collide with one a user made by hand. Mirrors the "multigres"
	// sidecar-schema convention; replication slot names cannot be
	// schema-qualified, so the namespacing has to live in the name itself. We
	// use a short prefix to leave more room for the sanitized pooler name and
	// potentially avoid hitting PostgreSQL's 63-character slot-name limit,
	// which would trigger using a hash. In summary, with a short prefix we will
	// usually get a more readable slot name, and only fall back to a hash when
	// the pooler name is long or contains characters that need sanitizing.
	logicalSlotNamePrefix = "mg_"

	// maxReplicationSlotNameLength is PostgreSQL's limit on a replication slot
	// name (NAMEDATALEN - 1).
	maxReplicationSlotNameLength = 63

	// logicalSlotStateTimeout bounds the pg_replication_slots reads. These are
	// cheap catalog lookups on the local instance.
	logicalSlotStateTimeout = 500 * time.Millisecond

	// logicalSlotWriteTimeout bounds slot creation and drop. Creating a logical
	// slot has to reach a consistent decoding point, which can wait for a
	// running-xacts / standby-snapshot record, so it gets a generous bound
	// rather than the sub-second read timeout.
	logicalSlotWriteTimeout = timeouts.RemoteOperationTimeout
)

// ErrLogicalSlotNotFound is returned by GetSlotState when pg_replication_slots
// has no slot with the requested name.
var ErrLogicalSlotNotFound = errors.New("logical replication slot not found")

// LogicalSlotState is a projection of a single pg_replication_slots row for a
// multigres-managed logical slot.
type LogicalSlotState struct {
	// The slot's name.
	Name string

	// RestartLSN is pg_replication_slots.restart_lsn (the oldest WAL the slot
	// still needs), in pg_lsn text form, or nil when NULL (the slot has not yet
	// reserved WAL). Kept as text — the codebase's LSN convention (e.g. the
	// target_lsn RPC field) — so it round-trips straight back into a pg_lsn
	// context without lossy parsing.
	RestartLSN *string

	// CatalogXmin is pg_replication_slots.catalog_xmin (the transaction id whose
	// catalog rows the slot needs retained), or nil when NULL (e.g. a slot with
	// no catalog hold). An xid is 32-bit, so int64 holds every value losslessly.
	// Note that comparing two xids requires modular (wraparound-aware) logic,
	// not a plain <, per the failover catalog-safety check.
	CatalogXmin *int64

	// InvalidationReason is pg_replication_slots.invalidation_reason, or nil when
	// the slot has not been invalidated (the column is NULL).
	InvalidationReason *string

	// FailoverReady reports whether the slot can safely take over after a
	// failover: it has been synced to this node, is not temporary, and has not
	// been invalidated. Computed in SQL as
	// (synced AND NOT temporary AND invalidation_reason IS NULL).
	FailoverReady bool
}

// LogicalSlotName derives the deterministic, cluster-unique name of a
// multigres-managed logical replication slot from a multipooler component name
// (already cluster-unique). Pass the bare name — clustermetadata ID.Name — not
// the "{cell}_{name}" application_name: the cell is not part of the slot name.
// The result is prefixed with logicalSlotNamePrefix, lower-cased, restricted to
// PostgreSQL's slot-name charset [a-z0-9_], and capped at
// maxReplicationSlotNameLength.
//
// It returns an error if name contains an underscore. '_' is not a legal
// character in a multipooler ID.Name — NewReplicaID rejects it because the
// "{cell}_{name}" application_name uses '_' as its delimiter — so a name
// carrying one is a violated identity invariant, not a value to sanitize.
// Erroring here surfaces that bug instead of masking it behind a munged slot
// name. (It also keeps the transform below collision-free: since '_' cannot
// reach it, the only rewrite in the injective domain, '-'→'_', can never alias
// onto a pre-existing '_'.)
//
// The transform is collision-free. On the expected input domain — a lower-case
// [a-z0-9-] name — lower-casing is a no-op and the only rewrite is '-'→'_', so
// distinct pooler names yield distinct slot names. Characters that are legal in
// a name but not in a slot name (an upper-case letter that folds to lower case,
// or a symbol such as '.') make the sanitizing map many-to-one; so does a
// sanitized name that would exceed the length cap. In those cases a short
// deterministic hash of the original name is appended (and the body truncated)
// to keep the mapping collision-free.
func LogicalSlotName(name string) (string, error) {
	if strings.Contains(name, "_") {
		return "", mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"multipooler name %q contains an underscore, which is not a valid ID.Name character", name)
	}

	lowered := strings.ToLower(name)

	// injective stays true only while every character is in the [a-z0-9-] domain
	// on which the sanitizing map is one-to-one.
	injective := name == lowered
	var b strings.Builder
	b.Grow(len(lowered))
	for _, r := range lowered {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
			// A character outside [a-z0-9-] (e.g. '.'); rewriting it to '_' can
			// alias distinct inputs, so fall back to the hash-disambiguated form
			// below.
			if r != '-' {
				injective = false
			}
		}
	}
	sanitized := b.String()

	candidate := logicalSlotNamePrefix + sanitized
	if injective && len(candidate) <= maxReplicationSlotNameLength {
		return candidate, nil
	}

	// Non-injective transform or over-length: append a short deterministic hash
	// of the original name and truncate the sanitized body so the whole name
	// fits within the length cap.
	sum := sha256.Sum256([]byte(name))
	suffix := "_" + hex.EncodeToString(sum[:4]) // '_' + 8 hex chars
	budget := maxReplicationSlotNameLength - len(logicalSlotNamePrefix) - len(suffix)
	if len(sanitized) > budget {
		sanitized = sanitized[:budget]
	}
	return logicalSlotNamePrefix + sanitized + suffix, nil
}

// EnsureLogicalSlot creates the logical replication slot with the given name
// using the given output plugin, requesting failover-slot behavior when
// failover is true. It is idempotent: if a slot with that name already exists
// it returns nil rather than raising duplicate_object.
//
// The multipooler is the sole manager of its own slots, so there is no
// competing writer to race with between the existence check and the create.
func (pm *MultipoolerManager) EnsureLogicalSlot(ctx context.Context, name, plugin string, failover bool) error {
	exists, err := pm.LogicalSlotExists(ctx, name)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	execCtx, cancel := context.WithTimeout(ctx, logicalSlotWriteTimeout)
	defer cancel()
	// Bind the slot name and plugin as parameters rather than interpolating them
	// into the statement. temporary=false and twophase=false; failover is
	// caller-controlled (the PostgreSQL slot-sync flag that lets a standby
	// maintain a copy of this slot).
	if err := pm.execArgs(execCtx,
		"SELECT pg_create_logical_replication_slot($1, $2, false, false, $3)",
		name, plugin, failover); err != nil {
		return mterrors.Wrap(err, "failed to create logical replication slot")
	}
	return nil
}

// DropLogicalSlot drops the replication slot named name. pg_drop_replication_slot
// drops both logical and physical slots, so this also drops physical slots.
func (pm *MultipoolerManager) DropLogicalSlot(ctx context.Context, name string) error {
	execCtx, cancel := context.WithTimeout(ctx, logicalSlotWriteTimeout)
	defer cancel()
	// Bind the slot name as a parameter rather than interpolating it.
	if err := pm.execArgs(execCtx, "SELECT pg_drop_replication_slot($1)", name); err != nil {
		return mterrors.Wrap(err, "failed to drop replication slot")
	}
	return nil
}

// EnsurePhysicalSlot creates the physical replication slot named name. A standby
// points its primary_slot_name at such a slot so the primary durably retains
// WAL and the standby's hot_standby_feedback (catalog_xmin) across disconnects —
// the durable hold the logical-slot failover machinery relies on. Idempotent:
// returns nil if a slot with that name already exists (LogicalSlotExists is a
// type-agnostic existence check on pg_replication_slots). Drop via
// DropLogicalSlot, which drops physical slots too.
func (pm *MultipoolerManager) EnsurePhysicalSlot(ctx context.Context, name string) error {
	exists, err := pm.LogicalSlotExists(ctx, name)
	if err != nil {
		return err
	}

	if !exists {
		execCtx, cancel := context.WithTimeout(ctx, logicalSlotWriteTimeout)
		defer cancel()
		// immediately_reserve=true retains WAL from slot creation (before the standby
		// attaches); temporary=false so the slot survives reconnects.
		if err := pm.execArgs(execCtx,
			"SELECT pg_create_physical_replication_slot($1, true, false)", name); err != nil {
			return mterrors.Wrap(err, "failed to create physical replication slot")
		}
	}

	return nil
}

// slotBasedReplicationEnabled reports whether the dynamic slot-based physical
// replication gate is on. It reads the getter live (the backing flag is dynamic
// and reloadable at runtime), so callers should invoke it at each decision point
// rather than caching. A nil getter (e.g. tests that don't set it) reads as
// disabled, preserving the slot-less default.
func (pm *MultipoolerManager) slotBasedReplicationEnabled() bool {
	if pm.config == nil || pm.config.SlotBasedReplicationEnabled == nil {
		return false
	}
	return pm.config.SlotBasedReplicationEnabled()
}

// ensureFollowerPhysicalSlots creates a physical replication slot for each
// follower in cohort (cohort minus this pooler) so their primary_slot_name
// resolves and the primary durably retains WAL and catalog_xmin for them. Slot
// names are derived from each follower's component name via LogicalSlotName. It
// is a no-op unless slot-based replication is enabled, and idempotent per slot.
// Intended to run on the node that is (about to be) primary.
func (pm *MultipoolerManager) ensureFollowerPhysicalSlots(ctx context.Context, cohort []*clustermetadatapb.ID) error {
	if !pm.slotBasedReplicationEnabled() {
		return nil
	}
	selfName := pm.serviceID.GetName()
	for _, member := range cohort {
		if member.GetName() != selfName {
			if slot, err := LogicalSlotName(member.GetName()); err != nil {
				return mterrors.Wrapf(err, "compute physical slot name for follower %q", member.GetName())
			} else if err := pm.EnsurePhysicalSlot(ctx, slot); err != nil {
				return mterrors.Wrapf(err, "ensure physical slot %q for follower %q", slot, member.GetName())
			}
		}
	}
	return nil
}

// dropFollowerPhysicalSlots drops the physical replication slot backing each
// member in ids (e.g. followers removed from the cohort) so a departed
// follower's slot stops pinning WAL on the primary. Slot names are derived via
// LogicalSlotName. Idempotent (skips slots that are not present); no-op unless
// slot-based replication is enabled; skips this pooler.
func (pm *MultipoolerManager) dropFollowerPhysicalSlots(ctx context.Context, ids []*clustermetadatapb.ID) error {
	if !pm.slotBasedReplicationEnabled() {
		return nil
	}
	selfName := pm.serviceID.GetName()
	for _, member := range ids {
		if member.GetName() != selfName {
			if slot, err := LogicalSlotName(member.GetName()); err != nil {
				return mterrors.Wrapf(err, "compute physical slot name for member %q", member.GetName())
			} else if exists, err := pm.LogicalSlotExists(ctx, slot); err != nil {
				return mterrors.Wrapf(err, "check physical slot %q for member %q", slot, member.GetName())
			} else if exists {
				if err := pm.DropLogicalSlot(ctx, slot); err != nil {
					return mterrors.Wrapf(err, "drop physical slot %q for member %q", slot, member.GetName())
				}
			}
		}
	}
	return nil
}

// dropManagedPhysicalSlotsSQL drops every physical replication slot whose name
// begins with $1 (the multigres slot-name prefix). The slot_type filter keeps
// it from ever touching logical (failover) slots.
const dropManagedPhysicalSlotsSQL = `
SELECT pg_drop_replication_slot(slot_name)
  FROM pg_replication_slots
 WHERE slot_type = 'physical'
   AND starts_with(slot_name, $1)`

// dropManagedPhysicalSlots drops every multigres-managed physical replication
// slot on this node (physical slots whose name carries the logicalSlotNamePrefix).
// A node that is (becoming) a standby backs no followers, so any such slot it
// still holds — as a demoted primary, or a former primary rejoining after a
// crash — is obsolete and would otherwise pin WAL. It is safe to call once the
// node's walsenders are gone (e.g. after a restart-as-standby), when the slots
// are inactive and therefore droppable. No-op unless slot-based replication is
// enabled; the slot_type filter guarantees logical (failover) slots are never
// touched.
func (pm *MultipoolerManager) dropManagedPhysicalSlots(ctx context.Context) error {
	if !pm.slotBasedReplicationEnabled() {
		return nil
	}

	execCtx, cancel := context.WithTimeout(ctx, logicalSlotWriteTimeout)
	defer cancel()
	if err := pm.execArgs(execCtx, dropManagedPhysicalSlotsSQL, logicalSlotNamePrefix); err != nil {
		return mterrors.Wrap(err, "failed to drop managed physical replication slots")
	}

	return nil
}

// listManagedPhysicalSlotsSQL lists this node's managed physical replication
// slots (name + active flag) so a reconcile can drop the ones no longer wanted.
// Mirrors dropManagedPhysicalSlotsSQL's WHERE clause; the slot_type filter keeps
// it from ever touching logical (failover) slots.
const listManagedPhysicalSlotsSQL = `
SELECT slot_name, active
  FROM pg_replication_slots
 WHERE slot_type = 'physical'
   AND starts_with(slot_name, $1)`

// ReconcileFollowers declaratively reconciles this (primary) node's per-follower
// physical replication slots to exactly the set of followers in followerIDs: it
// creates any slot that is missing and drops managed slots for members no longer
// in the set. It is the level-triggered entry point the orchestrator calls off
// the consensus path (see the ReconcileFollowers RPC) so a follower's slot exists
// before its WAL receiver attaches — breaking the bootstrap ordering deadlock
// where a late-joining standby can never stream (and thus never join the cohort,
// and thus never get its slot). Idempotent; no-op unless slot-based replication
// is enabled.
//
// It touches ONLY the physical slots. It deliberately does NOT update
// synchronized_standby_slots: a follower is added there only once it is streaming
// and caught up (the cohort path), because listing a not-yet-streaming follower
// would stall logical decoding on the primary.
func (pm *MultipoolerManager) ReconcileFollowers(ctx context.Context, followerIDs []*clustermetadatapb.ID) error {
	if !pm.slotBasedReplicationEnabled() {
		return nil
	}

	// Create-missing half: reuse the per-follower ensure.
	if err := pm.ensureFollowerPhysicalSlots(ctx, followerIDs); err != nil {
		return err
	}

	// Drop-departed half: any managed physical slot not in the desired set and
	// not currently active (streaming). The active guard is defensive — a
	// streaming follower momentarily absent from the set must never have its slot
	// yanked out from under it; such a slot is left for a later reconcile or the
	// cohort-remove path.
	desiredNames, err := pm.followerPhysicalSlotNames(followerIDs)
	if err != nil {
		return err
	}
	desired := make(map[string]struct{}, len(desiredNames))
	for _, name := range desiredNames {
		desired[name] = struct{}{}
	}

	queryCtx, cancel := context.WithTimeout(ctx, logicalSlotStateTimeout)
	defer cancel()
	result, err := pm.queryArgs(queryCtx, listManagedPhysicalSlotsSQL, logicalSlotNamePrefix)
	if err != nil {
		return mterrors.Wrap(err, "failed to list managed physical replication slots")
	}
	for _, row := range result.Rows {
		var (
			name   string
			active bool
		)
		if err := executor.ScanRow(row, &name, &active); err != nil {
			return mterrors.Wrap(err, "failed to scan managed physical replication slot")
		}
		if _, keep := desired[name]; keep || active {
			continue
		}
		if err := pm.DropLogicalSlot(ctx, name); err != nil {
			return mterrors.Wrapf(err, "drop departed managed physical slot %q", name)
		}
		pm.logger.InfoContext(ctx, "dropped departed follower physical slot during reconcile", "slot", name)
	}
	return nil
}

// unreadyFailoverSlotsSQL counts, and names, this node's synced logical failover
// slots that are NOT yet failover-ready (not synced, temporary, or invalidated).
const unreadyFailoverSlotsSQL = `
SELECT count(*), coalesce(string_agg(slot_name, ', '), '')
  FROM pg_replication_slots
 WHERE slot_type = 'logical'
   AND failover
   AND NOT (synced AND NOT temporary AND invalidation_reason IS NULL)`

// unreadyFailoverSlots returns how many failover slots on this node are not yet
// failover-ready, plus a comma-separated list of their names for logging.
func (pm *MultipoolerManager) unreadyFailoverSlots(ctx context.Context) (int, string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, logicalSlotStateTimeout)
	defer cancel()
	result, err := pm.query(queryCtx, unreadyFailoverSlotsSQL)
	if err != nil {
		return 0, "", mterrors.Wrap(err, "failed to query failover-slot readiness")
	}
	var count int64
	var names string
	if err := executor.ScanSingleRow(result, &count, &names); err != nil {
		return 0, "", mterrors.Wrap(err, "failed to scan failover-slot readiness")
	}
	return int(count), names, nil
}

// logUnreadyFailoverSlots checks, once, whether any synced failover slot on this
// node is not failover-ready, and logs those that are not. It is advisory and
// never waits: durable slot creation guarantees a failover slot is synced and
// persistent on the required standbys before its creation is acknowledged (see
// docs/ha/decision-log/2026-08-17-failover-slot-readiness-before-promotion.md), so
// the temporary/catching-up transient a wait would ride out cannot occur at
// promotion. A slot that is still not ready here is in a terminal state —
// invalidated, or synced=false because the sync machinery is broken (the
// unreadyFailoverSlots query tests synced too) — that a wait could not recover.
// Promotion must never be blocked, so this only logs. No-op unless slot-based
// replication is enabled.
func (pm *MultipoolerManager) logUnreadyFailoverSlots(ctx context.Context) {
	if !pm.slotBasedReplicationEnabled() {
		return
	}

	if count, names, err := pm.unreadyFailoverSlots(ctx); err != nil {
		// Best-effort: a transient read error must not block promotion.
		pm.logger.WarnContext(ctx, "failover-slot readiness check failed; proceeding with promotion", "error", err)
		return
	} else if count > 0 {
		pm.logger.WarnContext(ctx, "failover slots are not failover-ready at promotion; proceeding anyway",
			"unready_count", count, "unready_slots", names)
	}
}

// failoverSlotReadinessSQL counts this node's logical failover slots: how many
// are failover-ready, and how many exist in total.
const failoverSlotReadinessSQL = `
SELECT
	count(*) FILTER (WHERE synced AND NOT temporary AND invalidation_reason IS NULL),
	count(*)
  FROM pg_replication_slots
 WHERE slot_type = 'logical'
   AND failover`

// failoverSlotReadiness returns how many logical failover slots on this node are
// failover-ready and how many exist in total. It feeds the health snapshot so
// multiorch can prefer a slot-ready promotion candidate; a node with more
// failover-ready slots keeps more subscribers resumable across a failover.
func (pm *MultipoolerManager) failoverSlotReadiness(ctx context.Context) (ready int, total int, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, logicalSlotStateTimeout)
	defer cancel()
	result, err := pm.query(queryCtx, failoverSlotReadinessSQL)
	if err != nil {
		return 0, 0, mterrors.Wrap(err, "failed to query failover-slot readiness")
	}
	var readyCount, totalCount int64
	if err := executor.ScanSingleRow(result, &readyCount, &totalCount); err != nil {
		return 0, 0, mterrors.Wrap(err, "failed to scan failover-slot readiness")
	}
	return int(readyCount), int(totalCount), nil
}

const fetchLogicalSlotStateSQL = `SELECT
	slot_name,
	restart_lsn,
	catalog_xmin,
	invalidation_reason,
	(synced AND NOT temporary AND invalidation_reason IS NULL) AS failover_ready
FROM pg_replication_slots
WHERE slot_name = $1`

// GetSlotState reads the pg_replication_slots row for the slot named name.
// It returns ErrLogicalSlotNotFound (wrapped) when no such slot exists.
func (pm *MultipoolerManager) GetSlotState(ctx context.Context, name string) (*LogicalSlotState, error) {
	queryCtx, cancel := context.WithTimeout(ctx, logicalSlotStateTimeout)
	defer cancel()
	// Bind the slot name as a parameter rather than interpolating it.
	result, err := pm.queryArgs(queryCtx, fetchLogicalSlotStateSQL, name)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read replication slot state")
	}
	if result.RowCount() == 0 {
		return nil, mterrors.Wrapf(ErrLogicalSlotNotFound, "slot %q", name)
	}

	var (
		slotName           string
		restartLSN         *string
		catalogXmin        *int64
		invalidationReason *string
		failoverReady      bool
	)
	if err := executor.ScanSingleRow(result, &slotName, &restartLSN, &catalogXmin, &invalidationReason, &failoverReady); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan replication slot state")
	}

	slotState := &LogicalSlotState{
		Name:               slotName,
		RestartLSN:         restartLSN,
		CatalogXmin:        catalogXmin,
		InvalidationReason: invalidationReason,
		FailoverReady:      failoverReady,
	}
	return slotState, nil
}

// LogicalSlotExists reports whether a replication slot named name is present in
// pg_replication_slots.
func (pm *MultipoolerManager) LogicalSlotExists(ctx context.Context, name string) (bool, error) {
	queryCtx, cancel := context.WithTimeout(ctx, logicalSlotStateTimeout)
	defer cancel()
	// Bind the slot name as a parameter rather than interpolating it. SELECT
	// EXISTS(...) returns exactly one boolean row and lets the planner stop at
	// the first matching row; it's the existence-check idiom used elsewhere in
	// this package (see querySchemaExists).
	result, err := pm.queryArgs(queryCtx, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", name)
	if err != nil {
		return false, mterrors.Wrap(err, "failed to check replication slot existence")
	}
	var exists bool
	if err := executor.ScanSingleRow(result, &exists); err != nil {
		return false, mterrors.Wrap(err, "failed to scan replication slot existence result")
	}
	return exists, nil
}
