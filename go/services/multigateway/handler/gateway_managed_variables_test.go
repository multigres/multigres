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

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGatewayManagedVariablesRegistryComplete guards the single source of truth:
// every registered gateway-managed variable must supply every behavior, and keys
// must be lower-case (lookups lower-case the name). newGMVSpec forces
// all behaviors to be *passed* at compile time; this catches the one thing it
// can't — a behavior passed as nil — and the casing invariant.
func TestGatewayManagedVariablesRegistryComplete(t *testing.T) {
	require.NotEmpty(t, gatewayManagedVariables)
	for name, spec := range gatewayManagedVariables {
		assert.Equal(t, strings.ToLower(name), name, "registry key %q must be lower-case", name)
		assert.NotNil(t, spec.canonicalize, "%s: canonicalize", name)
		assert.NotNil(t, spec.applySet, "%s: applySet", name)
		assert.NotNil(t, spec.reset, "%s: reset", name)
		assert.NotNil(t, spec.setLocalToDefault, "%s: setLocalToDefault", name)
		assert.NotNil(t, spec.showEffective, "%s: showEffective", name)

		// Every registered variable must also be reported as gateway-managed, and
		// canonicalizing must not panic on a benign value.
		assert.True(t, IsGatewayManagedVariable(name))
		assert.True(t, IsGatewayManagedVariable(strings.ToUpper(name)), "case-insensitive")
	}
}

// TestGatewayManagedVariablesRoundTrip exercises the full set/show/reset cycle for
// each registered variable through the generic (registry-routed) methods, so a
// broken behavior on any variable is caught, not just a nil one.
func TestGatewayManagedVariablesRoundTrip(t *testing.T) {
	for name := range gatewayManagedVariables {
		t.Run(name, func(t *testing.T) {
			m := NewMultigatewayConnectionState()

			// Both current gateway-managed variables are GUC_UNIT_MS timeouts, so
			// "1000" is a valid value that canonicalizes to "1s". If a future
			// variable rejects it, give it its own case here.
			canonical, err := GatewayManagedCanonicalValue(name, "1000")
			require.NoError(t, err)
			assert.Equal(t, "1s", canonical)

			require.NoError(t, m.SetGatewayManaged(name, "1000", false))
			shown, err := m.ShowGatewayManaged(name)
			require.NoError(t, err)
			assert.Equal(t, "1s", shown, "SET then SHOW must reflect the value")

			require.NoError(t, m.ResetGatewayManaged(name))
			shown, err = m.ShowGatewayManaged(name)
			require.NoError(t, err)
			assert.Equal(t, "0", shown, "RESET must revert to the default")

			// Invalid value must be rejected (validated, not silently applied).
			_, err = GatewayManagedCanonicalValue(name, "not-a-timeout")
			assert.Error(t, err)
			assert.Error(t, m.SetGatewayManaged(name, "not-a-timeout", false))
		})
	}
}
