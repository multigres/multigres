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

package handler

// GatewayManagedVariable holds a variable with three priority layers matching
// PostgreSQL's GUC semantics:
//
//  1. defaultValue — the server default, set once at connection init from startup
//     params or the server flag.
//  2. currentValue — a session-level override set via SET var, cleared via RESET.
//     Persists across transactions.
//  3. localValue   — a transaction-scoped override set via SET LOCAL var, cleared
//     when the surrounding transaction COMMITs or ROLLBACKs.
//
// GetEffective resolves with priority local > session > default, matching
// PostgreSQL's behavior.
//
// snapshots is a stack of saved (currentValue, isSet, localValue, isLocalSet)
// tuples driven by SAVEPOINT / RELEASE / ROLLBACK TO and the surrounding
// BEGIN / COMMIT / ROLLBACK. The stack is managed in lockstep with
// MultiGatewayConnectionState.savepoints — each frame on the connection's
// stack corresponds to one snapshot here, by index.
type GatewayManagedVariable[T comparable] struct {
	defaultValue T
	currentValue T
	isSet        bool
	localValue   T
	isLocalSet   bool
	snapshots    []gmvSnapshot[T]
}

// gmvSnapshot captures the full mutable state of a GatewayManagedVariable at
// the moment a savepoint (or BEGIN-level frame) was pushed.
type gmvSnapshot[T comparable] struct {
	currentValue T
	isSet        bool
	localValue   T
	isLocalSet   bool
}

// NewGatewayManagedVariable creates a variable with the given default value.
func NewGatewayManagedVariable[T comparable](defaultValue T) GatewayManagedVariable[T] {
	return GatewayManagedVariable[T]{defaultValue: defaultValue}
}

// Set stores a session-level override and clears any active transaction-local
// override. Matches PostgreSQL: a non-LOCAL SET issued inside a transaction
// that has a prior SET LOCAL supersedes the LOCAL — effective value
// immediately becomes the new session value.
func (g *GatewayManagedVariable[T]) Set(v T) {
	g.currentValue = v
	g.isSet = true
	var zero T
	g.localValue = zero
	g.isLocalSet = false
}

// Reset clears both the session override and any active transaction-local
// override, reverting to the default. Matches PostgreSQL: RESET issued inside
// a transaction that has a prior SET LOCAL supersedes the LOCAL — effective
// value immediately becomes the default.
func (g *GatewayManagedVariable[T]) Reset() {
	var zero T
	g.currentValue = zero
	g.isSet = false
	g.localValue = zero
	g.isLocalSet = false
}

// SetLocal stores a transaction-local override. Cleared by ResetLocal at
// transaction end (COMMIT or ROLLBACK).
func (g *GatewayManagedVariable[T]) SetLocal(v T) {
	g.localValue = v
	g.isLocalSet = true
}

// SetLocalToDefault is the LOCAL form of SET ... TO DEFAULT: a transaction-
// scoped override whose value is the server default. It masks any session-level
// override for the duration of the transaction without destroying it. Matches
// PostgreSQL's behavior for `SET LOCAL var TO DEFAULT` — after the transaction
// ends and ResetLocal fires, the session-level value (if any) is restored.
func (g *GatewayManagedVariable[T]) SetLocalToDefault() {
	g.localValue = g.defaultValue
	g.isLocalSet = true
}

// ResetLocal clears any transaction-local override. Called at COMMIT/ROLLBACK
// to revert the value to the session-level (or default) value.
func (g *GatewayManagedVariable[T]) ResetLocal() {
	var zero T
	g.localValue = zero
	g.isLocalSet = false
}

// GetEffective returns the active value with priority:
// transaction-local > session > default.
func (g *GatewayManagedVariable[T]) GetEffective() T {
	if g.isLocalSet {
		return g.localValue
	}
	if g.isSet {
		return g.currentValue
	}
	return g.defaultValue
}

// IsSet returns whether a session-level override is active.
func (g *GatewayManagedVariable[T]) IsSet() bool {
	return g.isSet
}

// IsLocalSet returns whether a transaction-local override is active.
func (g *GatewayManagedVariable[T]) IsLocalSet() bool {
	return g.isLocalSet
}

// Snapshot pushes the current (currentValue, isSet, localValue, isLocalSet) tuple
// onto the snapshot stack. Called by MultiGatewayConnectionState when a SAVEPOINT
// is opened (or at BEGIN-level frame creation).
func (g *GatewayManagedVariable[T]) Snapshot() {
	g.snapshots = append(g.snapshots, gmvSnapshot[T]{
		currentValue: g.currentValue,
		isSet:        g.isSet,
		localValue:   g.localValue,
		isLocalSet:   g.isLocalSet,
	})
}

// RestoreFromDepth restores from the snapshot at index `depth`, then truncates the
// stack to depth+1 so the frame at `depth` remains. Used for ROLLBACK TO sp:
// PostgreSQL leaves `sp` active after the rollback, so its snapshot must stay so a
// subsequent ROLLBACK TO sp can be issued again. Precondition: depth < len(snapshots).
func (g *GatewayManagedVariable[T]) RestoreFromDepth(depth int) {
	s := g.snapshots[depth]
	g.currentValue = s.currentValue
	g.isSet = s.isSet
	g.localValue = s.localValue
	g.isLocalSet = s.isLocalSet
	g.snapshots = g.snapshots[:depth+1]
}

// PopFrom drops snapshot frames at index `depth` and above, keeping the current
// in-memory values untouched. Used for RELEASE sp: PG merges sub-transaction
// changes into the parent, so we drop sp's frame (and any nested ones) but
// preserve current state.
func (g *GatewayManagedVariable[T]) PopFrom(depth int) {
	g.snapshots = g.snapshots[:depth]
}

// ClearSnapshots drops all snapshot frames. Called at COMMIT (current values
// become persistent session state) and after RollbackTransaction has restored
// from the BEGIN-level frame.
func (g *GatewayManagedVariable[T]) ClearSnapshots() {
	g.snapshots = nil
}

// SnapshotDepth returns the current size of the snapshot stack. Used by tests
// and as an internal sanity check that connection-state and variable stacks
// stay in lockstep.
func (g *GatewayManagedVariable[T]) SnapshotDepth() int {
	return len(g.snapshots)
}
