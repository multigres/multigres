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

import "strings"

// PLpgSQL_resolve_option mirrors PG's #variable_conflict setting
// (plpgsql.h:183-188). PLPGSQL_RESOLVE_UNSPECIFIED is our own addition (not in
// PG): it marks the absence of a #variable_conflict directive so deparse omits
// it, where PG instead defaults the field from the plpgsql.variable_conflict
// GUC. The remaining values match PG's enum meanings.
type PLpgSQL_resolve_option int8

const (
	PLPGSQL_RESOLVE_UNSPECIFIED PLpgSQL_resolve_option = iota
	PLPGSQL_RESOLVE_ERROR                              // #variable_conflict error
	PLPGSQL_RESOLVE_VARIABLE                           // #variable_conflict use_variable
	PLPGSQL_RESOLVE_COLUMN                             // #variable_conflict use_column
)

// PLpgSQL_function is the root of a parsed PL/pgSQL function body. As in PG, the
// body is a single top-level block (PG's function->action). The many
// execution-engine fields of PG's struct (datum array, statement ids, etc.) are
// intentionally omitted — this is a parse tree for static analysis.
//
// The comp_options preamble fields (ResolveOption, PrintStrictParams,
// DumpExecTree) capture PG's #-directives so a body round-trips. They are
// semantically inert here: we neither resolve names nor execute, so the
// directives only affect deparse.
// Ported from postgres/src/pl/plpgsql/src/plpgsql.h:966-1016
type PLpgSQL_function struct {
	BaseNode
	Action *PLpgSQL_stmt_block `json:"action,omitempty"` // the function body block

	ResolveOption     PLpgSQL_resolve_option `json:"resolve_option,omitempty"`      // #variable_conflict
	PrintStrictParams string                 `json:"print_strict_params,omitempty"` // #print_strict_params on|off ("" = unset)
	DumpExecTree      bool                   `json:"dump_exec_tree,omitempty"`      // #option dump
}

func (n *PLpgSQL_function) String() string {
	return "PLpgSQL_function"
}

func (n *PLpgSQL_function) SqlString() string {
	if n.Action == nil {
		return ""
	}
	var b strings.Builder
	switch n.ResolveOption {
	case PLPGSQL_RESOLVE_ERROR:
		b.WriteString("#variable_conflict error\n")
	case PLPGSQL_RESOLVE_VARIABLE:
		b.WriteString("#variable_conflict use_variable\n")
	case PLPGSQL_RESOLVE_COLUMN:
		b.WriteString("#variable_conflict use_column\n")
	}
	if n.PrintStrictParams != "" {
		b.WriteString("#print_strict_params ")
		b.WriteString(n.PrintStrictParams)
		b.WriteString("\n")
	}
	if n.DumpExecTree {
		b.WriteString("#option dump\n")
	}
	b.WriteString(n.Action.SqlString())
	return b.String()
}

func NewPLpgSQL_function() *PLpgSQL_function {
	return &PLpgSQL_function{
		BaseNode: BaseNode{Tag: T_PLpgSQL_function, Loc: -1},
	}
}
