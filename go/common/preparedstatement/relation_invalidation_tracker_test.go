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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRelationInvalidationTracker_UnaffectedStatementStaysAtZero(t *testing.T) {
	tr := NewRelationInvalidationTracker()
	tr.RecordDependencies("ppstmt0", []string{"orders"})

	assert.Equal(t, uint64(0), tr.StatementGeneration("ppstmt0"))

	// A DDL on an unrelated table doesn't move this statement's generation.
	tr.InvalidateRelations([]string{"customers"})
	assert.Equal(t, uint64(0), tr.StatementGeneration("ppstmt0"))
}

func TestRelationInvalidationTracker_InvalidateBumpsDependentStatement(t *testing.T) {
	tr := NewRelationInvalidationTracker()
	tr.RecordDependencies("ppstmt0", []string{"orders"})

	before := tr.StatementGeneration("ppstmt0")
	tr.InvalidateRelations([]string{"orders"})
	after := tr.StatementGeneration("ppstmt0")

	assert.Greater(t, after, before)
}

func TestRelationInvalidationTracker_MultiTableStatementTracksEither(t *testing.T) {
	tr := NewRelationInvalidationTracker()
	tr.RecordDependencies("ppstmt0", []string{"orders", "customers"})

	gen0 := tr.StatementGeneration("ppstmt0")
	tr.InvalidateRelations([]string{"customers"})
	gen1 := tr.StatementGeneration("ppstmt0")
	assert.Greater(t, gen1, gen0)

	tr.InvalidateRelations([]string{"orders"})
	gen2 := tr.StatementGeneration("ppstmt0")
	assert.Greater(t, gen2, gen1)
}

func TestRelationInvalidationTracker_RecordDependenciesUnionsAcrossCalls(t *testing.T) {
	tr := NewRelationInvalidationTracker()

	// A caller that doesn't know the statement's dependencies yet (e.g. a
	// forced eager Parse) registers it with no relations — a no-op.
	tr.RecordDependencies("ppstmt0", nil)
	assert.Equal(t, uint64(0), tr.StatementGeneration("ppstmt0"))

	// A later caller that does know the dependencies still registers them
	// correctly; the earlier no-op call didn't erase anything to erase.
	tr.RecordDependencies("ppstmt0", []string{"orders"})
	tr.InvalidateRelations([]string{"orders"})
	assert.Equal(t, uint64(1), tr.StatementGeneration("ppstmt0"))
}

func TestRelationInvalidationTracker_UnknownStatementHasZeroGeneration(t *testing.T) {
	tr := NewRelationInvalidationTracker()
	assert.Equal(t, uint64(0), tr.StatementGeneration("never-seen"))
}
