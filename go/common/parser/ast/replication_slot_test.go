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

package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// selectReplicationSlotFuncCall builds the AST for
// SELECT <funcname>(<args...>), qualifying with pg_catalog when qualified is
// true — used to verify FindNonTemporaryReplicationSlotCall independent of
// the SQL parser (this package can't import go/common/parser, which imports
// ast).
func selectReplicationSlotFuncCall(funcname string, qualified bool, args ...Node) Stmt {
	nameItems := []Node{NewString(funcname)}
	if qualified {
		nameItems = []Node{NewString("pg_catalog"), NewString(funcname)}
	}
	fc := NewFuncCall(&NodeList{Items: nameItems}, &NodeList{Items: args}, 0)
	return &SelectStmt{
		TargetList: &NodeList{Items: []Node{NewResTarget("", fc)}},
		Op:         SETOP_NONE,
	}
}

func TestFindNonTemporaryReplicationSlotCall(t *testing.T) {
	lit := func(v Value) Node { return NewA_Const(v, 0) }

	tests := []struct {
		name       string
		stmt       Stmt
		wantFound  bool
		wantFnName string
	}{
		{
			name: "physical: temporary=true literal accepted",
			stmt: selectReplicationSlotFuncCall("pg_create_physical_replication_slot", false,
				lit(NewString("s1")), lit(NewBoolean(false)), lit(NewBoolean(true))),
		},
		{
			name: "physical: temporary=false literal rejected",
			stmt: selectReplicationSlotFuncCall("pg_create_physical_replication_slot", false,
				lit(NewString("s1")), lit(NewBoolean(false)), lit(NewBoolean(false))),
			wantFound:  true,
			wantFnName: "pg_create_physical_replication_slot",
		},
		{
			name: "physical: temporary omitted rejected",
			stmt: selectReplicationSlotFuncCall("pg_create_physical_replication_slot", false,
				lit(NewString("s1"))),
			wantFound:  true,
			wantFnName: "pg_create_physical_replication_slot",
		},
		{
			name: "physical: non-literal temporary (bound param) rejected",
			stmt: selectReplicationSlotFuncCall("pg_create_physical_replication_slot", false,
				lit(NewString("s1")), lit(NewBoolean(false)), NewParamRef(1, 0)),
			wantFound:  true,
			wantFnName: "pg_create_physical_replication_slot",
		},
		{
			name: "physical: pg_catalog-qualified form accepted with temporary=true",
			stmt: selectReplicationSlotFuncCall("pg_create_physical_replication_slot", true,
				lit(NewString("s1")), lit(NewBoolean(false)), lit(NewBoolean(true))),
		},
		{
			name: "logical: temporary=true literal accepted",
			stmt: selectReplicationSlotFuncCall("pg_create_logical_replication_slot", false,
				lit(NewString("s1")), lit(NewString("pgoutput")), lit(NewBoolean(true))),
		},
		{
			name: "logical: temporary=false literal rejected",
			stmt: selectReplicationSlotFuncCall("pg_create_logical_replication_slot", false,
				lit(NewString("s1")), lit(NewString("pgoutput")), lit(NewBoolean(false))),
			wantFound:  true,
			wantFnName: "pg_create_logical_replication_slot",
		},
		{
			name: "unrelated function is unaffected",
			stmt: selectReplicationSlotFuncCall("pg_drop_replication_slot", false, lit(NewString("s1"))),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, found := FindNonTemporaryReplicationSlotCall(tt.stmt)
			assert.Equal(t, tt.wantFound, found)
			if tt.wantFound {
				assert.Equal(t, tt.wantFnName, name)
			}
		})
	}

	t.Run("nil statement", func(t *testing.T) {
		name, found := FindNonTemporaryReplicationSlotCall(nil)
		assert.False(t, found)
		assert.Empty(t, name)
	})
}

// TestFindNonTemporaryNonFailoverReplicationSlotCall covers the
// failover-aware variant used by the replication preamble once
// slot-based-replication is on, including named-argument (failover =>
// true) calls — the shape that must resolve identically here and in the
// planner's own check, since both delegate to FuncCallArg.
func TestFindNonTemporaryNonFailoverReplicationSlotCall(t *testing.T) {
	lit := func(v Value) Expression { return NewA_Const(v, 0) }
	named := func(name string, v Value) Node { return NewNamedArgExpr(lit(v), name, -1, 0) }

	tests := []struct {
		name      string
		stmt      Stmt
		wantFound bool
	}{
		{
			name: "logical: positional failover=true admitted",
			stmt: selectReplicationSlotFuncCall("pg_create_logical_replication_slot", false,
				lit(NewString("s1")), lit(NewString("pgoutput")), lit(NewBoolean(false)), lit(NewBoolean(false)), lit(NewBoolean(true))),
		},
		{
			name: "logical: named failover => true admitted, temporary/twophase omitted",
			stmt: selectReplicationSlotFuncCall("pg_create_logical_replication_slot", false,
				lit(NewString("s1")), lit(NewString("pgoutput")), named("failover", NewBoolean(true))),
		},
		{
			name:      "logical: named failover => false still rejected",
			wantFound: true,
			stmt: selectReplicationSlotFuncCall("pg_create_logical_replication_slot", false,
				lit(NewString("s1")), lit(NewString("pgoutput")), named("failover", NewBoolean(false))),
		},
		{
			name: "logical: named temporary => true admitted",
			stmt: selectReplicationSlotFuncCall("pg_create_logical_replication_slot", false,
				lit(NewString("s1")), lit(NewString("pgoutput")), named("temporary", NewBoolean(true))),
		},
		{
			name:      "physical: failover has no meaning, still rejected without temporary=true",
			wantFound: true,
			stmt: selectReplicationSlotFuncCall("pg_create_physical_replication_slot", false,
				lit(NewString("s1")), lit(NewBoolean(false)), lit(NewBoolean(false))),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, found := FindNonTemporaryNonFailoverReplicationSlotCall(tt.stmt)
			assert.Equal(t, tt.wantFound, found)
		})
	}
}

// TestFuncCallArg covers positional and named-argument resolution directly,
// including the mixed and omitted-entirely shapes.
func TestFuncCallArg(t *testing.T) {
	lit := func(v Value) Expression { return NewA_Const(v, 0) }
	named := func(name string, v Value) Node { return NewNamedArgExpr(lit(v), name, -1, 0) }
	fc := func(args ...Node) *FuncCall {
		return NewFuncCall(&NodeList{Items: []Node{NewString("f")}}, &NodeList{Items: args}, 0)
	}

	t.Run("positional hit", func(t *testing.T) {
		arg, given := FuncCallArg(fc(lit(NewString("a")), lit(NewBoolean(true))), 1, "x")
		require.True(t, given)
		isTrue, ok := literalBoolArg(arg)
		assert.True(t, ok)
		assert.True(t, isTrue)
	})

	t.Run("named hit regardless of position", func(t *testing.T) {
		arg, given := FuncCallArg(fc(lit(NewString("a")), named("x", NewBoolean(true))), 5, "x")
		require.True(t, given)
		isTrue, ok := literalBoolArg(arg)
		assert.True(t, ok)
		assert.True(t, isTrue)
	})

	t.Run("named arg for a different name doesn't consume a positional slot", func(t *testing.T) {
		// index 1 ("x") is never reached positionally because the only other
		// item is named "y" — a real bug this test guards against is treating
		// named items as occupying a positional slot.
		_, given := FuncCallArg(fc(lit(NewString("a")), named("y", NewBoolean(true))), 1, "x")
		assert.False(t, given)
	})

	t.Run("omitted entirely", func(t *testing.T) {
		_, given := FuncCallArg(fc(lit(NewString("a"))), 1, "x")
		assert.False(t, given)
	})

	t.Run("nil Args", func(t *testing.T) {
		_, given := FuncCallArg(&FuncCall{}, 0, "x")
		assert.False(t, given)
	})
}
