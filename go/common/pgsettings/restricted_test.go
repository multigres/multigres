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

package pgsettings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestrictedGUCError(t *testing.T) {
	err := RestrictedGUCError("synchronous_commit")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "synchronous_commit")
	// The statement path has a revert form, so the message points at it.
	assert.Contains(t, err.Error(), "RESET synchronous_commit")

	// Case-insensitive, matching PostgreSQL's parameter names.
	require.Error(t, RestrictedGUCError("SYNCHRONOUS_COMMIT"))

	assert.NoError(t, RestrictedGUCError("work_mem"))
	assert.NoError(t, RestrictedGUCError(""))
}

// TestRestrictedGUCStartupError covers the connect-time variant. It must flag
// the same names, but advise differently: there is no revert form at startup,
// so pointing at RESET would be wrong — omitting the parameter is the fix.
func TestRestrictedGUCStartupError(t *testing.T) {
	err := RestrictedGUCStartupError("synchronous_commit")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "synchronous_commit")
	assert.Contains(t, err.Error(), "connection startup")
	assert.NotContains(t, err.Error(), "RESET",
		"there is no revert form at startup; advising RESET would be misleading")

	require.Error(t, RestrictedGUCStartupError("Synchronous_Commit"))

	assert.NoError(t, RestrictedGUCStartupError("work_mem"))
	assert.NoError(t, RestrictedGUCStartupError("application_name"))
	assert.NoError(t, RestrictedGUCStartupError(""))
}

// TestRestrictedGUCSurfacesAgree pins that both surfaces are driven by the same
// map, so adding an entry covers the statement path and the startup path at
// once rather than only the one the author remembered.
func TestRestrictedGUCSurfacesAgree(t *testing.T) {
	for name := range restrictedGUCs {
		assert.Error(t, RestrictedGUCError(name), "statement guard must reject %q", name)
		assert.Error(t, RestrictedGUCStartupError(name), "startup guard must reject %q", name)
	}
}
