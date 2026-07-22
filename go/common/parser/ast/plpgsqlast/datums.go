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
// resolved datatype OID, promise state) are dropped. A CURSOR declaration is
// also a PLpgSQL_var — a refcursor variable with a bound query — matching PG; the
// Cursor* fields carry it and CursorExplicitExpr being non-nil marks it.
type PLpgSQL_var struct {
	BaseNode
	Refname    string        `json:"refname,omitempty"`
	IsConst    bool          `json:"is_const,omitempty"`
	NotNull    bool          `json:"not_null,omitempty"`
	DataType   *PLpgSQL_type `json:"datatype,omitempty"`
	DefaultVal *PLpgSQL_expr `json:"default_val,omitempty"` // initializer expression, or nil
	// Cursor declaration fields (PG's PLpgSQL_var cursor properties).
	CursorExplicitExpr *PLpgSQL_expr  `json:"cursor_explicit_expr,omitempty"` // bound query; non-nil ⇒ cursor
	CursorOptions      int            `json:"cursor_options,omitempty"`       // FAST_PLAN | scroll flags
	CursorArgs         []*PLpgSQL_var `json:"cursor_args,omitempty"`          // declared cursor args (name + type)
}

func (v *PLpgSQL_var) isDatum() {}

func (v *PLpgSQL_var) String() string { return "PLpgSQL_var" }

func (v *PLpgSQL_var) SqlString() string {
	if v.CursorExplicitExpr != nil {
		return v.cursorSqlString()
	}
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

// cursorSqlString deparses a CURSOR declaration:
// name [SCROLL|NO SCROLL] CURSOR [(arg type, …)] FOR <query>;
func (v *PLpgSQL_var) cursorSqlString() string {
	var sb strings.Builder
	sb.WriteString(v.Refname)
	if v.CursorOptions&CURSOR_OPT_NO_SCROLL != 0 {
		sb.WriteString(" NO SCROLL")
	} else if v.CursorOptions&CURSOR_OPT_SCROLL != 0 {
		sb.WriteString(" SCROLL")
	}
	sb.WriteString(" CURSOR")
	if len(v.CursorArgs) > 0 {
		sb.WriteString(" (")
		for i, arg := range v.CursorArgs {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(arg.Refname)
			sb.WriteString(" ")
			sb.WriteString(arg.DataType.SqlString())
		}
		sb.WriteString(")")
	}
	sb.WriteString(" FOR ")
	sb.WriteString(v.CursorExplicitExpr.SqlString())
	sb.WriteString(";")
	return sb.String()
}

func NewPLpgSQL_var(refname string) *PLpgSQL_var {
	return &PLpgSQL_var{
		BaseNode: BaseNode{Tag: T_PLpgSQL_var, Loc: -1},
		Refname:  refname,
	}
}

// PLpgSQL_alias represents an `name ALIAS FOR target` declaration. PG has no
// declaration node for this — ALIAS is a pure namespace side-effect
// (plpgsql_ns_additem) — so this is our parse-level carrier so the alias appears
// in the DECLARE-section datum list and round-trips. Target is the aliased name
// as written (PG resolves it to an existing variable).
type PLpgSQL_alias struct {
	BaseNode
	Refname string `json:"refname,omitempty"` // the alias name
	Target  string `json:"target,omitempty"`  // the aliased (existing) name
}

func (a *PLpgSQL_alias) isDatum() {}

func (a *PLpgSQL_alias) String() string { return "PLpgSQL_alias" }

func (a *PLpgSQL_alias) SqlString() string {
	return a.Refname + " ALIAS FOR " + a.Target + ";"
}

func NewPLpgSQL_alias(refname string) *PLpgSQL_alias {
	return &PLpgSQL_alias{
		BaseNode: BaseNode{Tag: T_PLpgSQL_alias, Loc: -1},
		Refname:  refname,
	}
}
