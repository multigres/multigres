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
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// slotNameCharset matches a fully-sanitized multigres slot name: the fixed
// prefix followed only by PostgreSQL's legal slot-name characters. Derived from
// logicalSlotNamePrefix so it tracks the prefix rather than hard-coding it.
var slotNameCharset = regexp.MustCompile(`^` + regexp.QuoteMeta(logicalSlotNamePrefix) + `[a-z0-9_]*$`)

func TestLogicalSlotName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string // exact expected output; only set for the injective (hash-free) cases
	}{
		{
			name:     "simple name",
			input:    "replica1",
			expected: logicalSlotNamePrefix + "replica1",
		},
		{
			name:     "hyphens map to underscores",
			input:    "replica-1",
			expected: logicalSlotNamePrefix + "replica_1",
		},
		{
			name:     "multiple hyphens",
			input:    "us-east-1a-pooler",
			expected: logicalSlotNamePrefix + "us_east_1a_pooler",
		},
		{
			name:     "digits only",
			input:    "001",
			expected: logicalSlotNamePrefix + "001",
		},
		{
			name:     "exactly at the length cap stays verbatim",
			input:    strings.Repeat("a", maxReplicationSlotNameLength-len(logicalSlotNamePrefix)),
			expected: logicalSlotNamePrefix + strings.Repeat("a", maxReplicationSlotNameLength-len(logicalSlotNamePrefix)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LogicalSlotName(tt.input)
			require.NoError(t, err)

			assert.Equal(t, tt.expected, got)
			assert.True(t, strings.HasPrefix(got, logicalSlotNamePrefix), "must carry the multigres slot-name prefix")
			assert.Regexp(t, slotNameCharset, got, "must be restricted to [a-z0-9_]")
			assert.LessOrEqual(t, len(got), maxReplicationSlotNameLength, "must respect the length cap")
		})
	}
}

// TestLogicalSlotName_UnderscoreRejected verifies that an underscore in the name
// — which NewReplicaID already forbids in an ID.Name — is reported as an error
// rather than silently sanitized into a slot name.
func TestLogicalSlotName_UnderscoreRejected(t *testing.T) {
	for _, input := range []string{"a_b", "_leading", "trailing_", "us_east"} {
		t.Run(input, func(t *testing.T) {
			_, err := LogicalSlotName(input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "underscore")
		})
	}
}

// TestLogicalSlotName_Sanitization covers inputs outside the expected
// [a-z0-9-] domain: they must still produce a legal, capped name, and because
// the sanitizing map is lossy for them, a disambiguating hash is appended.
func TestLogicalSlotName_Sanitization(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "uppercase is folded", input: "Replica-1"},
		{name: "mixed case", input: "PoolerABC"},
		{name: "dot separator", input: "pooler.zone"},
		{name: "over length", input: strings.Repeat("x", 80)},
		{name: "over length with hyphens", input: strings.Repeat("a-", 40)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LogicalSlotName(tt.input)
			require.NoError(t, err)

			assert.True(t, strings.HasPrefix(got, logicalSlotNamePrefix), "must carry the multigres slot-name prefix")
			assert.Regexp(t, slotNameCharset, got, "must be restricted to [a-z0-9_]")
			assert.LessOrEqual(t, len(got), maxReplicationSlotNameLength, "must respect the length cap")

			// The disambiguating suffix is "_" + 8 hex characters.
			assert.Regexp(t, regexp.MustCompile(`_[0-9a-f]{8}$`), got, "lossy inputs get a hash suffix")

			// Deterministic: same input always yields the same slot name.
			again, err := LogicalSlotName(tt.input)
			require.NoError(t, err)
			assert.Equal(t, got, again)
		})
	}
}

// TestLogicalSlotName_CollisionFree checks that distinct pooler names — both
// within the injective domain and across the lossy boundary — never map to the
// same slot name.
func TestLogicalSlotName_CollisionFree(t *testing.T) {
	// verbatimBodyMax is the longest sanitized body that still fits under the
	// length cap without a hash. Computed from the prefix so these cases stay
	// meaningful regardless of its length.
	verbatimBodyMax := maxReplicationSlotNameLength - len(logicalSlotNamePrefix)

	inputs := []string{
		// Injective domain.
		"replica1",
		"replica-1",
		"replica2",
		"us-east-1a",
		"us-east-1b",
		strings.Repeat("a", verbatimBodyMax),   // exactly fits verbatim
		strings.Repeat("a", verbatimBodyMax+1), // one over -> hashed, body truncated
		strings.Repeat("a", verbatimBodyMax+9), // far over -> hashed, same truncated body, different original
		// Lossy inputs that share a sanitized body and would collide without the hash.
		"abc",
		"aBc",
		"AbC",
		"a-b",
		"pooler.zone",
		"pooler-zone",
	}

	seen := make(map[string]string, len(inputs))
	for _, in := range inputs {
		got, err := LogicalSlotName(in)
		require.NoError(t, err)
		if prev, dup := seen[got]; dup {
			t.Fatalf("collision: %q and %q both map to %q", prev, in, got)
		}
		seen[got] = in
	}
}

func TestEnsureLogicalSlot(t *testing.T) {
	const (
		slotName = "multigres_replica_1"
		plugin   = "pgoutput"
	)

	t.Run("creates slot when absent", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPatternOnce("SELECT EXISTS", mock.MakeQueryResult([]string{"exists"}, [][]any{{false}}))
		m.AddQueryPatternOnce("pg_create_logical_replication_slot",
			mock.MakeQueryResult([]string{"slot_name", "lsn"}, [][]any{{slotName, "0/1500000"}}))

		require.NoError(t, pm.EnsureLogicalSlot(t.Context(), slotName, plugin, true))
		assert.NoError(t, m.ExpectationsWereMet())
	})

	t.Run("idempotent when slot already exists", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		// Only the existence check is registered. If EnsureLogicalSlot tried to
		// create the slot, the mock would return a no-matching-pattern error.
		m.AddQueryPatternOnce("SELECT EXISTS", mock.MakeQueryResult([]string{"exists"}, [][]any{{true}}))

		require.NoError(t, pm.EnsureLogicalSlot(t.Context(), slotName, plugin, true))
		assert.NoError(t, m.ExpectationsWereMet())
	})

	t.Run("propagates existence-check error", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPatternOnceWithError("SELECT EXISTS", errors.New("boom"))

		err := pm.EnsureLogicalSlot(t.Context(), slotName, plugin, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to check replication slot existence")
	})

	t.Run("propagates create error", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPatternOnce("SELECT EXISTS", mock.MakeQueryResult([]string{"exists"}, [][]any{{false}}))
		m.AddQueryPatternOnceWithError("pg_create_logical_replication_slot", errors.New("boom"))

		err := pm.EnsureLogicalSlot(t.Context(), slotName, plugin, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create logical replication slot")
	})
}

func TestDropLogicalSlot(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPatternOnce("pg_drop_replication_slot", mock.MakeQueryResult(nil, nil))

		require.NoError(t, pm.DropLogicalSlot(t.Context(), "multigres_replica_1"))
		assert.NoError(t, m.ExpectationsWereMet())
	})

	t.Run("propagates error", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPatternOnceWithError("pg_drop_replication_slot", errors.New("boom"))

		err := pm.DropLogicalSlot(t.Context(), "multigres_replica_1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to drop replication slot")
	})
}

func TestGetSlotState(t *testing.T) {
	const slotName = "multigres_replica_1"

	stateColumns := []string{"slot_name", "catalog_xmin", "invalidation_reason", "failover_ready"}

	t.Run("failover-ready slot", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPatternOnce("failover_ready",
			mock.MakeQueryResult(stateColumns, [][]any{{slotName, "12345", nil, true}}))

		state, err := pm.GetSlotState(t.Context(), slotName)
		require.NoError(t, err)
		assert.Equal(t, slotName, state.Name)
		require.NotNil(t, state.CatalogXmin)
		assert.Equal(t, int64(12345), *state.CatalogXmin)
		assert.Nil(t, state.InvalidationReason)
		assert.True(t, state.FailoverReady)
	})

	t.Run("invalidated slot is not failover-ready", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPatternOnce("failover_ready",
			mock.MakeQueryResult(stateColumns, [][]any{{slotName, "12345", "wal_removed", false}}))

		state, err := pm.GetSlotState(t.Context(), slotName)
		require.NoError(t, err)
		require.NotNil(t, state.InvalidationReason)
		assert.Equal(t, "wal_removed", *state.InvalidationReason)
		assert.False(t, state.FailoverReady)
	})

	t.Run("null catalog_xmin", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPatternOnce("failover_ready",
			mock.MakeQueryResult(stateColumns, [][]any{{slotName, nil, nil, false}}))

		state, err := pm.GetSlotState(t.Context(), slotName)
		require.NoError(t, err)
		assert.Nil(t, state.CatalogXmin)
	})

	t.Run("not found", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPatternOnce("failover_ready", mock.MakeQueryResult(stateColumns, nil))

		_, err := pm.GetSlotState(t.Context(), slotName)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrLogicalSlotNotFound)
	})

	t.Run("propagates query error", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPatternOnceWithError("failover_ready", errors.New("boom"))

		_, err := pm.GetSlotState(t.Context(), slotName)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read replication slot state")
	})
}

func TestLogicalSlotExists(t *testing.T) {
	const slotName = "mg_replica_1"

	t.Run("slot present", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPatternOnce("SELECT EXISTS", mock.MakeQueryResult([]string{"exists"}, [][]any{{true}}))

		exists, err := pm.LogicalSlotExists(t.Context(), slotName)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("slot absent", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPatternOnce("SELECT EXISTS", mock.MakeQueryResult([]string{"exists"}, [][]any{{false}}))

		exists, err := pm.LogicalSlotExists(t.Context(), slotName)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("propagates query error", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPatternOnceWithError("SELECT EXISTS", errors.New("boom"))

		_, err := pm.LogicalSlotExists(t.Context(), slotName)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to check replication slot existence")
	})
}
