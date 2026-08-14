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

// DropLogicalSlot drops the replication slot named name.
func (pm *MultipoolerManager) DropLogicalSlot(ctx context.Context, name string) error {
	execCtx, cancel := context.WithTimeout(ctx, logicalSlotWriteTimeout)
	defer cancel()
	// Bind the slot name as a parameter rather than interpolating it.
	if err := pm.execArgs(execCtx, "SELECT pg_drop_replication_slot($1)", name); err != nil {
		return mterrors.Wrap(err, "failed to drop replication slot")
	}
	return nil
}

const fetchLogicalSlotStateSQL = `SELECT
	slot_name,
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
		catalogXmin        *int64
		invalidationReason *string
		failoverReady      bool
	)
	if err := executor.ScanSingleRow(result, &slotName, &catalogXmin, &invalidationReason, &failoverReady); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan replication slot state")
	}

	slotState := &LogicalSlotState{
		Name:               slotName,
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
