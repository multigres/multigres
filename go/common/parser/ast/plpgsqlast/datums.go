// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//  Portions Copyright (c) 2025, Supabase, Inc
//
//  Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//  Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//

package plpgsqlast

import (
	"strings"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// Datum is implemented by every PL/pgSQL datum — the Go analogue of PG's
// PLpgSQL_datum supertype (postgres/src/pl/plpgsql/src/plpgsql.h). A datum carries
// a dno: its index in the function's flat datum list. The scanner resolves an
// identifier to a datum and emits T_DATUM carrying it, and the namespace points
// back at the datum by that dno.
//
// The parser currently produces only scalar variables (PLpgSQL_var) and ALIAS
// carriers (PLpgSQL_alias); a resolved T_DATUM is therefore always effectively a
// scalar. PG additionally classifies datums as ROW (a scalar list), REC (a record
// variable), or RECFIELD (a rec.field reference), tagged by a dtype discriminator
// — but that classification is not built here yet: it needs record typing (and,
// for a named composite type, the system catalog) this static, parse-only port
// does not have, so a record reference resolves as text (T_CWORD), not a RECFIELD.
// The dtype discriminator and those datum types can be added when that typing is
// implemented; until then they would be dead code, so they are omitted.
type Datum interface {
	Node
	isDatum()
	// DatumNo returns the datum's dno (index in the function's datum list).
	DatumNo() int
	// SetDatumNo records the datum's dno when it is appended to the datum list.
	SetDatumNo(dno int)
}

// PLpgSQL_type is a declared type, reduced to the parse-level text: we capture
// the type as written and do not resolve it (no OID, no %TYPE/%ROWTYPE
// resolution — that is variable resolution).
// Ported from postgres/src/pl/plpgsql/src/plpgsql.h:198-213
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

// PLpgSQL_var is a scalar variable declaration, a parse-level subset: PG's
// execution/resolution fields (dno, the resolved datatype OID, promise state)
// are dropped. A CURSOR declaration is also a PLpgSQL_var — a refcursor variable
// with a bound query — matching PG; the Cursor* fields carry it and
// CursorExplicitExpr being non-nil marks it.
// Ported from postgres/src/pl/plpgsql/src/plpgsql.h:309-343
type PLpgSQL_var struct {
	BaseNode
	Dno        int           `json:"dno,omitempty"`
	Refname    string        `json:"refname,omitempty"`
	IsConst    bool          `json:"is_const,omitempty"`
	NotNull    bool          `json:"not_null,omitempty"`
	DataType   *PLpgSQL_type `json:"datatype,omitempty"`
	Collate    string        `json:"collate,omitempty"`     // COLLATE name (as written), or "" — PG resolves to an OID
	DefaultVal *PLpgSQL_expr `json:"default_val,omitempty"` // initializer expression, or nil
	// Cursor declaration fields (PG's PLpgSQL_var cursor properties).
	CursorExplicitExpr *PLpgSQL_expr  `json:"cursor_explicit_expr,omitempty"` // bound query; non-nil ⇒ cursor
	CursorOptions      int            `json:"cursor_options,omitempty"`       // FAST_PLAN | scroll flags
	CursorArgs         []*PLpgSQL_var `json:"cursor_args,omitempty"`          // declared cursor args (name + type)
}

func (v *PLpgSQL_var) isDatum() {}

func (v *PLpgSQL_var) DatumNo() int { return v.Dno }

func (v *PLpgSQL_var) SetDatumNo(dno int) { v.Dno = dno }

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
	if v.Collate != "" {
		// The scanner captures the collation name de-quoted; re-quote each part so
		// a case-sensitive name (e.g. "C", "en_US") round-trips.
		sb.WriteString(" COLLATE ")
		sb.WriteString(ast.FormatQualifiedName(strings.Split(v.Collate, ".")...))
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
	Dno     int    `json:"dno,omitempty"`
	Refname string `json:"refname,omitempty"` // the alias name
	Target  string `json:"target,omitempty"`  // the aliased (existing) name
}

func (a *PLpgSQL_alias) isDatum() {}

// An ALIAS has no PG datum of its own (it is a namespace side effect that points
// at the aliased variable). Our parser keeps it in the DECLARE-section datum list
// so it round-trips and so an aliased name resolves like any other; it carries a
// real dno for that resolution.
func (a *PLpgSQL_alias) DatumNo() int { return a.Dno }

func (a *PLpgSQL_alias) SetDatumNo(dno int) { a.Dno = dno }

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

// PG's remaining datum kinds — PLpgSQL_row (a scalar list), PLpgSQL_rec (a record
// variable), and PLpgSQL_recfield (a rec.field reference) — are intentionally not
// defined here: the parser never constructs them yet (see the Datum comment
// above), so they would be dead code. Add them alongside the resolution that
// produces them.
