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

// GatewayManagedVariable holds a session-overridable variable with a server default.
// The default is set once at connection init (from startup params or the server flag)
// and the current value can be overridden per-session via SET / cleared via RESET.
type GatewayManagedVariable[T comparable] struct {
	defaultValue T
	currentValue T
	isSet        bool
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
func (g *GatewayManagedVariable[T]) Reset() {
	var zero T
	g.currentValue = zero
	g.isSet = false
}

// GetEffective returns the session override if set, otherwise the default.
func (g *GatewayManagedVariable[T]) GetEffective() T {
	if g.isSet {
		return g.currentValue
	}
	return g.defaultValue
}

// IsSet returns whether a session override is active.
func (g *GatewayManagedVariable[T]) IsSet() bool {
	return g.isSet
}
