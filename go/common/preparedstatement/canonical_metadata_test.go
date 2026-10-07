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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	querypb "github.com/multigres/multigres/go/pb/query"
)

type countingCanonicalStmt struct {
	ast.Stmt
	calls atomic.Int32
}

func (s *countingCanonicalStmt) SqlString() string {
	s.calls.Add(1)
	return s.Stmt.SqlString()
}

func TestCanonicalSQLAndFingerprintConcurrent(t *testing.T) {
	const query = "select id from users where id = $1 and name = 'literal'"
	c := NewConsolidator()
	psi, err := c.AddPreparedStatement(1, "first", query, []uint32{23})
	require.NoError(t, err)
	shared, err := c.AddPreparedStatement(2, "second", query, []uint32{23})
	require.NoError(t, err)
	require.Same(t, psi, shared)
	expectedSQL := psi.AstStmt().SqlString()
	expectedFingerprint := ast.FingerprintSQL(expectedSQL)
	counted := &countingCanonicalStmt{Stmt: psi.astStruct}
	psi.astStruct = counted
	require.Zero(t, counted.calls.Load(), "preparation should not canonicalize eagerly")

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			<-start
			statement := psi
			if i%2 == 0 {
				statement = shared
			}
			for range 100 {
				sql, fingerprint := statement.CanonicalSQLAndFingerprint()
				if sql != expectedSQL || fingerprint != expectedFingerprint {
					t.Errorf("inconsistent shared metadata: %q, %q", sql, fingerprint)
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
	require.Equal(t, int32(1), counted.calls.Load(), "all concurrent and warm uses must share one reconstruction")
	require.Contains(t, expectedSQL, "$1")
	require.Contains(t, expectedSQL, "'literal'", "canonicalization must not replace literals")
	require.Equal(t, query, psi.Query, "the backend must still receive the original SQL")
}

func TestCanonicalSQLAndFingerprintStatementLifetime(t *testing.T) {
	c := NewConsolidator()
	first, err := c.AddPreparedStatement(1, "stmt", "select $1", []uint32{23})
	require.NoError(t, err)
	firstSQL, firstFP := first.CanonicalSQLAndFingerprint()
	differentTypes, err := c.AddPreparedStatement(2, "stmt", "select $1", []uint32{25})
	require.NoError(t, err)
	require.NotSame(t, first, differentTypes, "parameter types retain distinct statement identities")
	sql, fp := differentTypes.CanonicalSQLAndFingerprint()
	require.Equal(t, firstSQL, sql)
	require.Equal(t, firstFP, fp, "fingerprints describe SQL, not parameter types")

	replacement, err := c.AddPreparedStatement(1, "stmt", "select $1 + 1", []uint32{23})
	require.NoError(t, err)
	require.NotSame(t, first, replacement)
	sql, fp = replacement.CanonicalSQLAndFingerprint()
	require.Equal(t, replacement.AstStmt().SqlString(), sql)
	require.Equal(t, ast.FingerprintSQL(sql), fp)
	require.NotEqual(t, firstSQL, sql)
	require.NotEqual(t, firstFP, fp)
	sql, fp = first.CanonicalSQLAndFingerprint()
	require.Equal(t, firstSQL, sql, "a retained old reference keeps its own metadata")
	require.Equal(t, firstFP, fp)

	c.RemoveConnection(2)
	fresh, err := c.AddPreparedStatement(3, "stmt", "select $1", []uint32{25})
	require.NoError(t, err)
	require.NotSame(t, differentTypes, fresh)
	sql, fp = fresh.CanonicalSQLAndFingerprint()
	require.Equal(t, firstSQL, sql)
	require.Equal(t, firstFP, fp)
}

func TestCanonicalSQLAndFingerprintEmpty(t *testing.T) {
	for _, query := range []string{"", "-- no statement", "/* no statement */"} {
		psi, err := NewPreparedStatementInfo(&querypb.PreparedStatement{Query: query})
		require.NoError(t, err)
		require.True(t, psi.IsEmpty())
		for range 2 {
			sql, fp := psi.CanonicalSQLAndFingerprint()
			require.Empty(t, sql)
			require.Empty(t, fp)
		}
	}
}
