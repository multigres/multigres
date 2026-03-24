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

package preparedstatement

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPoolerConsolidator_BasicDedup(t *testing.T) {
	pc := NewPoolerConsolidator()

	name1 := pc.CanonicalName("SELECT 1", nil)
	name2 := pc.CanonicalName("SELECT 1", nil)
	require.Equal(t, name1, name2, "same query should return the same canonical name")
	require.Equal(t, "ppstmt0", name1)
}

func TestPoolerConsolidator_DifferentQueries(t *testing.T) {
	pc := NewPoolerConsolidator()

	name1 := pc.CanonicalName("SELECT 1", nil)
	name2 := pc.CanonicalName("SELECT 2", nil)
	require.NotEqual(t, name1, name2, "different queries should get different canonical names")
}

// TestPoolerConsolidator_GatewayNameCollision verifies the fix for the pooler
// name collision bug. Two gateways sending the same incoming name for different
// queries should get different canonical names because the PoolerConsolidator
// deduplicates by query text, not by incoming name.
func TestPoolerConsolidator_GatewayNameCollision(t *testing.T) {
	pc := NewPoolerConsolidator()

	// Both gateways call their first statement "stmt0", but for different queries.
	// The PoolerConsolidator doesn't use incoming names at all — it keys by query.
	name1 := pc.CanonicalName("SELECT * FROM users", nil)
	name2 := pc.CanonicalName("INSERT INTO users (name) VALUES ($1)", []uint32{25})

	require.NotEqual(t, name1, name2)
	require.Equal(t, "ppstmt0", name1)
	require.Equal(t, "ppstmt1", name2)
}

func TestPoolerConsolidator_SameQueryDifferentParamTypes(t *testing.T) {
	pc := NewPoolerConsolidator()

	int4OID := []uint32{23}
	textOID := []uint32{25}

	name1 := pc.CanonicalName("SELECT $1", int4OID)
	name2 := pc.CanonicalName("SELECT $1", textOID)

	require.NotEqual(t, name1, name2,
		"same query with different paramTypes should get different canonical names")
}

func TestPoolerConsolidator_SameQuerySameParamTypes(t *testing.T) {
	pc := NewPoolerConsolidator()

	name1 := pc.CanonicalName("SELECT $1, $2", []uint32{23, 25})
	name2 := pc.CanonicalName("SELECT $1, $2", []uint32{23, 25})

	require.Equal(t, name1, name2,
		"same query with same paramTypes should get the same canonical name")
}

func TestPoolerConsolidator_NilVsEmptyParamTypes(t *testing.T) {
	pc := NewPoolerConsolidator()

	name1 := pc.CanonicalName("SELECT 1", nil)
	name2 := pc.CanonicalName("SELECT 1", []uint32{})

	require.Equal(t, name1, name2,
		"nil and empty paramTypes should be treated the same")
}

func TestPoolerConsolidator_ConcurrentAccess(t *testing.T) {
	pc := NewPoolerConsolidator()
	var wg sync.WaitGroup

	queries := []string{
		"SELECT 1",
		"SELECT 2",
		"INSERT INTO foo VALUES ($1)",
	}

	results := make([]string, len(queries))
	for i, q := range queries {
		wg.Add(1)
		go func(idx int, query string) {
			defer wg.Done()
			results[idx] = pc.CanonicalName(query, nil)
		}(i, q)
	}
	wg.Wait()

	// Each unique query should get a unique canonical name.
	seen := make(map[string]bool)
	for _, name := range results {
		require.False(t, seen[name], "duplicate canonical name: %s", name)
		seen[name] = true
	}

	// Calling again should return the same names.
	for i, q := range queries {
		require.Equal(t, results[i], pc.CanonicalName(q, nil))
	}
}
