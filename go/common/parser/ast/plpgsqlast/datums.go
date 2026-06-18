// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plpgsqlast

import "strings"

// Datum is implemented by every PL/pgSQL datum (a declared variable, etc.) — the
// Go analogue of PG's PLpgSQL_datum supertype (plpgsql.h). Only PLpgSQL_var
// implements it for now; the row/rec/recfield variants arrive with variable
// resolution.
type Datum interface {
	Node
	isDatum()
}

// PLpgSQL_type is a declared type. Ported from plpgsql.h (PLpgSQL_type), reduced
// to the parse-level text: we capture the type as written and do not resolve it
// (no OID, no %TYPE/%ROWTYPE resolution — that is variable resolution).
type PLpgSQL_type struct {
	BaseNode
	// TypeName is the type as written, e.g. "int", "varchar(10)", "foo%TYPE".
	TypeName string `json:"type_name,omitempty"`
}

func (t *PLpgSQL_type) String() string { return "PLpgSQL_type" }

func (t *PLpgSQL_type) SqlString() string { return t.TypeName }

func NewPLpgSQL_type(name string) *PLpgSQL_type {
	return &PLpgSQL_type{
		BaseNode: BaseNode{Tag: T_PLpgSQL_type, Loc: -1},
		TypeName: name,
	}
}

// PLpgSQL_var is a scalar variable declaration. Ported from plpgsql.h
// (PLpgSQL_var), parse-level subset: the execution/resolution fields (dno, the
// resolved datatype OID, cursor/promise state) are dropped.
type PLpgSQL_var struct {
	BaseNode
	Refname    string        `json:"refname,omitempty"`
	IsConst    bool          `json:"is_const,omitempty"`
	NotNull    bool          `json:"not_null,omitempty"`
	DataType   *PLpgSQL_type `json:"datatype,omitempty"`
	DefaultVal *PLpgSQL_expr `json:"default_val,omitempty"` // initializer expression, or nil
}

func (v *PLpgSQL_var) isDatum() {}

func (v *PLpgSQL_var) String() string { return "PLpgSQL_var" }

func (v *PLpgSQL_var) SqlString() string {
	var sb strings.Builder
	sb.WriteString(v.Refname)
	if v.IsConst {
		sb.WriteString(" CONSTANT")
	}
	if v.DataType != nil {
		sb.WriteString(" ")
		sb.WriteString(v.DataType.SqlString())
	}
	if v.NotNull {
		sb.WriteString(" NOT NULL")
	}
	if v.DefaultVal != nil {
		sb.WriteString(" := ")
		sb.WriteString(v.DefaultVal.SqlString())
	}
	sb.WriteString(";")
	return sb.String()
}

func NewPLpgSQL_var(refname string) *PLpgSQL_var {
	return &PLpgSQL_var{
		BaseNode: BaseNode{Tag: T_PLpgSQL_var, Loc: -1},
		Refname:  refname,
	}
}
