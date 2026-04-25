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
type GatewayManagedVariable[T comparable] struct {
	defaultValue T
	currentValue T
	isSet        bool
	localValue   T
	isLocalSet   bool
}

// NewGatewayManagedVariable creates a variable with the given default value.
func NewGatewayManagedVariable[T comparable](defaultValue T) GatewayManagedVariable[T] {
	return GatewayManagedVariable[T]{defaultValue: defaultValue}
}

// Set stores a session-level override.
func (g *GatewayManagedVariable[T]) Set(v T) {
	g.currentValue = v
	g.isSet = true
}

// Reset clears the session override, reverting to the default.
// Does not touch any active transaction-local override.
func (g *GatewayManagedVariable[T]) Reset() {
	var zero T
	g.currentValue = zero
	g.isSet = false
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
