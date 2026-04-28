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
)

// selectSetConfig builds the AST for SELECT set_config(<name>, <value>, <isLocal>)
// — used to verify the normalizer's gating on the literal is_local arg.
func selectSetConfig(name, value string, isLocal bool) Stmt {
	args := &NodeList{Items: []Node{
		NewA_Const(NewString(name), 0),
		NewA_Const(NewString(value), 0),
		NewA_Const(NewBoolean(isLocal), 0),
	}}
	fc := NewFuncCall(&NodeList{Items: []Node{NewString("set_config")}}, args, 0)
	return &SelectStmt{
		TargetList: &NodeList{Items: []Node{NewResTarget("", fc)}},
		Op:         SETOP_NONE,
	}
}

// Helper to build a simple SELECT * FROM users WHERE id = <int> AST.
func selectWhereInt(val int) Stmt {
	return &SelectStmt{
		TargetList: &NodeList{Items: []Node{NewResTarget("", &ColumnRef{Fields: &NodeList{Items: []Node{&A_Star{}}}})}},
		FromClause: &NodeList{Items: []Node{&RangeVar{RelName: "users", Inh: true}}},
		WhereClause: &A_Expr{
			Kind:  AEXPR_OP,
			Name:  &NodeList{Items: []Node{&String{SVal: "="}}},
			Lexpr: &ColumnRef{Fields: &NodeList{Items: []Node{&String{SVal: "id"}}}},
			Rexpr: NewA_Const(NewInteger(val), 0),
		},
		Op: SETOP_NONE,
	}
}

func TestNormalize(t *testing.T) {
	tests := []struct {
		name              string
		stmt              Stmt
		wantNormSQL       string
		wantWasNormalized bool
		wantBindCount     int
	}{
		{
			name:              "SELECT with WHERE integer",
			stmt:              selectWhereInt(42),
			wantNormSQL:       "SELECT * FROM users WHERE id = $1",
			wantWasNormalized: true,
			wantBindCount:     1,
		},
		{
			name: "SELECT with no literals",
			stmt: &SelectStmt{
				TargetList: &NodeList{Items: []Node{NewResTarget("", &ColumnRef{Fields: &NodeList{Items: []Node{&A_Star{}}}})}},
				FromClause: &NodeList{Items: []Node{&RangeVar{RelName: "users", Inh: true}}},
				Op:         SETOP_NONE,
			},
			wantNormSQL:       "SELECT * FROM users",
			wantWasNormalized: false,
			wantBindCount:     0,
		},
		{
			name: "NULL is not normalized",
			stmt: &SelectStmt{
				TargetList: &NodeList{Items: []Node{NewResTarget("", &ColumnRef{Fields: &NodeList{Items: []Node{&A_Star{}}}})}},
				FromClause: &NodeList{Items: []Node{&RangeVar{RelName: "users", Inh: true}}},
				WhereClause: &NullTest{
					Arg:          &ColumnRef{Fields: &NodeList{Items: []Node{&String{SVal: "name"}}}},
					Nulltesttype: IS_NULL,
				},
				Op: SETOP_NONE,
			},
			wantNormSQL:       "SELECT * FROM users WHERE name IS NULL",
			wantWasNormalized: false,
			wantBindCount:     0,
		},
		{
			name: "multiple literals",
			stmt: &SelectStmt{
				TargetList: &NodeList{Items: []Node{NewResTarget("", &ColumnRef{Fields: &NodeList{Items: []Node{&A_Star{}}}})}},
				FromClause: &NodeList{Items: []Node{&RangeVar{RelName: "users", Inh: true}}},
				WhereClause: &BoolExpr{
					Boolop: AND_EXPR,
					Args: &NodeList{Items: []Node{
						&A_Expr{
							Kind:  AEXPR_OP,
							Name:  &NodeList{Items: []Node{&String{SVal: "="}}},
							Lexpr: &ColumnRef{Fields: &NodeList{Items: []Node{&String{SVal: "id"}}}},
							Rexpr: NewA_Const(NewInteger(1), 0),
						},
						&A_Expr{
							Kind:  AEXPR_OP,
							Name:  &NodeList{Items: []Node{&String{SVal: "="}}},
							Lexpr: &ColumnRef{Fields: &NodeList{Items: []Node{&String{SVal: "name"}}}},
							Rexpr: NewA_Const(NewString("alice"), 0),
						},
					}},
				},
				Op: SETOP_NONE,
			},
			wantNormSQL:       "SELECT * FROM users WHERE id = $1 AND name = $2",
			wantWasNormalized: true,
			wantBindCount:     2,
		},
		{
			name: "LIMIT and OFFSET",
			stmt: &SelectStmt{
				TargetList:  &NodeList{Items: []Node{NewResTarget("", &ColumnRef{Fields: &NodeList{Items: []Node{&A_Star{}}}})}},
				FromClause:  &NodeList{Items: []Node{&RangeVar{RelName: "users", Inh: true}}},
				LimitCount:  NewA_Const(NewInteger(10), 0),
				LimitOffset: NewA_Const(NewInteger(5), 0),
				Op:          SETOP_NONE,
			},
			wantNormSQL:       "SELECT * FROM users LIMIT $2 OFFSET $1",
			wantWasNormalized: true,
			wantBindCount:     2,
		},
		{
			name: "boolean constant",
			stmt: &SelectStmt{
				TargetList: &NodeList{Items: []Node{NewResTarget("", &ColumnRef{Fields: &NodeList{Items: []Node{&A_Star{}}}})}},
				FromClause: &NodeList{Items: []Node{&RangeVar{RelName: "users", Inh: true}}},
				WhereClause: &A_Expr{
					Kind:  AEXPR_OP,
					Name:  &NodeList{Items: []Node{&String{SVal: "="}}},
					Lexpr: &ColumnRef{Fields: &NodeList{Items: []Node{&String{SVal: "active"}}}},
					Rexpr: NewA_Const(NewBoolean(true), 0),
				},
				Op: SETOP_NONE,
			},
			wantNormSQL:       "SELECT * FROM users WHERE active = $1",
			wantWasNormalized: true,
			wantBindCount:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Normalize(tt.stmt)
			assert.Equal(t, tt.wantWasNormalized, result.WasNormalized())
			assert.Equal(t, tt.wantBindCount, len(result.BindValues))
			assert.Equal(t, tt.wantNormSQL, result.NormalizedSQL)
		})
	}
}

// TestNormalize_SetConfigIsLocalGating verifies the planner-literal skip
// fires for set_config(..., false) — preserving literals so the planner can
// validate them — but lets set_config(..., true) be parameterized so PG
// REST-style "set_config('request.jwt.claims', '<dynamic JSON>', true)"
// per-request calls collapse into a single plan-cache fingerprint.
func TestNormalize_SetConfigIsLocalGating(t *testing.T) {
	tests := []struct {
		name              string
		stmt              Stmt
		wantWasNormalized bool
	}{
		{
			name:              "is_local=false preserves literals (skips subtree)",
			stmt:              selectSetConfig("work_mem", "256MB", false),
			wantWasNormalized: false,
		},
		{
			name:              "is_local=true allows literals to be parameterized",
			stmt:              selectSetConfig("request.jwt.claims", `{"sub":"alice"}`, true),
			wantWasNormalized: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Normalize(tt.stmt)
			assert.Equal(t, tt.wantWasNormalized, result.WasNormalized(),
				"normalized SQL: %s", result.NormalizedSQL)
		})
	}

	t.Run("is_local=true produces stable fingerprint across distinct values", func(t *testing.T) {
		fp1 := Normalize(selectSetConfig("request.jwt.claims", `{"sub":"alice"}`, true)).Fingerprint()
		fp2 := Normalize(selectSetConfig("request.jwt.claims", `{"sub":"bob"}`, true)).Fingerprint()
		assert.Equal(t, fp1, fp2, "is_local=true set_config calls must share a fingerprint")
	})
}

func TestNormalizeDoesNotMutateOriginal(t *testing.T) {
	stmt := selectWhereInt(42)
	originalSQL := stmt.SqlString()
	_ = Normalize(stmt)
	assert.Equal(t, originalSQL, stmt.SqlString())
}

func TestReconstructSQL(t *testing.T) {
	stmts := []Stmt{
		selectWhereInt(42),
		// Multiple literals
		&SelectStmt{
			TargetList: &NodeList{Items: []Node{NewResTarget("", &ColumnRef{Fields: &NodeList{Items: []Node{&A_Star{}}}})}},
			FromClause: &NodeList{Items: []Node{&RangeVar{RelName: "users", Inh: true}}},
			WhereClause: &BoolExpr{
				Boolop: AND_EXPR,
				Args: &NodeList{Items: []Node{
					&A_Expr{
						Kind:  AEXPR_OP,
						Name:  &NodeList{Items: []Node{&String{SVal: "="}}},
						Lexpr: &ColumnRef{Fields: &NodeList{Items: []Node{&String{SVal: "id"}}}},
						Rexpr: NewA_Const(NewInteger(1), 0),
					},
					&A_Expr{
						Kind:  AEXPR_OP,
						Name:  &NodeList{Items: []Node{&String{SVal: "="}}},
						Lexpr: &ColumnRef{Fields: &NodeList{Items: []Node{&String{SVal: "name"}}}},
						Rexpr: NewA_Const(NewString("alice"), 0),
					},
				}},
			},
			Op: SETOP_NONE,
		},
	}

	for _, stmt := range stmts {
		originalSQL := stmt.SqlString()
		result := Normalize(stmt)
		assert.True(t, result.WasNormalized())

		// ReconstructSQL should produce the same SQL as the original AST
		reconstructed := ReconstructSQL(result.NormalizedAST, result.BindValues)
		assert.Equal(t, originalSQL, reconstructed)
	}
}

func TestReconstructSQLDoesNotMutateNormalizedAST(t *testing.T) {
	stmt := selectWhereInt(42)
	result := Normalize(stmt)
	normalizedSQL := result.NormalizedAST.SqlString()

	_ = ReconstructSQL(result.NormalizedAST, result.BindValues)
	assert.Equal(t, normalizedSQL, result.NormalizedAST.SqlString())
}

func TestNormalizeSameShapeDifferentValues(t *testing.T) {
	result1 := Normalize(selectWhereInt(42))
	result2 := Normalize(selectWhereInt(99))

	// Same query shape should produce the same normalized SQL
	assert.Equal(t, result1.NormalizedSQL, result2.NormalizedSQL)

	// But different bind values
	assert.Equal(t, "42", result1.BindValues[0].SqlString())
	assert.Equal(t, "99", result2.BindValues[0].SqlString())
}

func TestFingerprint(t *testing.T) {
	// Same shape, different values → same fingerprint.
	result1 := Normalize(selectWhereInt(42))
	result2 := Normalize(selectWhereInt(99))
	assert.Equal(t, result1.Fingerprint(), result2.Fingerprint())

	// Fingerprint is 16 hex chars.
	assert.Len(t, result1.Fingerprint(), 16)

	// Different shape → different fingerprint.
	differentShape := &SelectStmt{
		TargetList: &NodeList{Items: []Node{NewResTarget("", &ColumnRef{Fields: &NodeList{Items: []Node{&A_Star{}}}})}},
		FromClause: &NodeList{Items: []Node{&RangeVar{RelName: "orders", Inh: true}}},
		Op:         SETOP_NONE,
	}
	result3 := Normalize(differentShape)
	assert.NotEqual(t, result1.Fingerprint(), result3.Fingerprint())
}

func TestFingerprintStable(t *testing.T) {
	// Fingerprint must be deterministic across invocations.
	result := Normalize(selectWhereInt(42))
	fp1 := result.Fingerprint()
	fp2 := result.Fingerprint()
	assert.Equal(t, fp1, fp2)
}
