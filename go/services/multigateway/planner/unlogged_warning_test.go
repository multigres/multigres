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

package planner

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// TestUnloggedTableCreationWarns asserts that every CREATE form producing an
// UNLOGGED table is planned with a leading UnloggedTableWarning notice, while
// the permanent and temp variants are not.
func TestUnloggedTableCreationWarns(t *testing.T) {
	s := newTestSetup(t)

	tests := []struct {
		sql      string
		wantWarn bool
	}{
		{"CREATE UNLOGGED TABLE ut (i int)", true},
		{"CREATE UNLOGGED TABLE ut AS SELECT 1", true},
		{"SELECT 1 INTO UNLOGGED ut", true},
		{"CREATE UNLOGGED SEQUENCE us", true},
		{"CREATE TABLE pt (i int)", false},
		{"CREATE TABLE pt AS SELECT 1", false},
		{"CREATE TEMP TABLE tt (i int)", false},
		{"CREATE SEQUENCE ps", false},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			asts, err := parser.ParseSQL(tc.sql)
			require.NoError(t, err)
			require.Len(t, asts, 1)
			plan, err := s.p.Plan(tc.sql, asts[0], s.conn.Conn)
			require.NoError(t, err)

			warns := leadingUnloggedWarning(plan.Primitive) != nil
			require.Equal(t, tc.wantWarn, warns, "plan primitive = %s", plan.Primitive.String())
		})
	}

	// The extended query protocol must warn for the same statements: an unlogged
	// table created via Parse/Bind/Execute would otherwise plain-portal-execute on
	// the backend with no warning attached.
	for _, tc := range tests {
		t.Run("portal/"+tc.sql, func(t *testing.T) {
			plan, err := s.p.PlanPortal(newPortalInfoFor(t, tc.sql), s.conn.Conn)
			require.NoError(t, err)
			if !tc.wantWarn {
				// Non-unlogged creates either plan locally (temp) or fall through to
				// a plain portal execute (nil plan); neither carries the warning.
				if plan != nil {
					require.Nil(t, leadingUnloggedWarning(plan.Primitive),
						"plan primitive = %s", plan.Primitive.String())
				}
				return
			}
			require.NotNil(t, plan, "unlogged creation must plan locally to attach the warning")
			require.NotNil(t, leadingUnloggedWarning(plan.Primitive),
				"plan primitive = %s", plan.Primitive.String())
		})
	}
}

// leadingUnloggedWarning returns the UnloggedWarning if p is a Sequence whose
// first primitive is one, else nil.
func leadingUnloggedWarning(p engine.Primitive) *engine.UnloggedWarning {
	seq, ok := p.(*engine.Sequence)
	if !ok || len(seq.Primitives) == 0 {
		return nil
	}
	w, _ := seq.Primitives[0].(*engine.UnloggedWarning)
	return w
}
