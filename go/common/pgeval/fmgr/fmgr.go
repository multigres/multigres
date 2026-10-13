// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2026, Supabase, Inc
//
//	Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//	Portions Copyright (c) 1994, The Regents of the University of California
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

// Package fmgr ports PostgreSQL's function manager (postgres
// src/backend/utils/fmgr, REL_17_6): the calling convention and registry
// through which the evaluation engine invokes builtin functions.
//
// Every builtin has the uniform signature [PGFunction] — it takes a
// [FunctionCallInfo] and returns a [datum.Datum], with nullness carried
// out-of-band. Function metadata (strictness, arg count) lives in an
// [FmgrInfo] beside the function, not inside it; a strict function's NULL-input
// short-circuit is applied by the caller ([CallFunction]), so ported function
// bodies never check their arguments for NULL. Errors are raised with
// pgerror.Ereport (a panic) and recovered at the evaluation boundary.
package fmgr

import (
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// Oid is PostgreSQL's object identifier, aliased from the leaf pgoid package.
type Oid = pgoid.Oid

// PGFunction is the uniform fmgr calling convention: a function takes a
// FunctionCallInfo and returns a Datum, signaling a NULL result via
// fcinfo.IsNull rather than in the return value - fmgr.h:39
// ("typedef Datum (*PGFunction)(FunctionCallInfo)").
type PGFunction func(fcinfo FunctionCallInfo) datum.Datum

// FmgrInfo is the looked-up descriptor of a callable function - fmgr.h:56
// (struct FmgrInfo). It is built by [FmgrInfoFor] and is read-only once
// created, except for FnExtra which the callee may use as scratch across calls.
// Fields PostgreSQL keeps but this port omits: fn_stats (function-call
// statistics tracking) and fn_mcxt (the memory context for fn_extra — Go's GC
// makes it unnecessary).
type FmgrInfo struct {
	FnAddr   PGFunction // fn_addr: the function to invoke - fmgr.h:58
	FnOid    Oid        // fn_oid: OID of the function - fmgr.h:59
	FnNargs  int16      // fn_nargs: number of declared input args - fmgr.h:60
	FnStrict bool       // fn_strict: NULL in => NULL out - fmgr.h:61
	FnRetset bool       // fn_retset: function returns a set - fmgr.h:62
	FnExtra  any        // fn_extra: callee-owned scratch across calls - fmgr.h:64
	// FnExpr is the parse tree of the call (a FuncExpr/OpExpr node), or nil -
	// fmgr.h:67. PostgreSQL types it as fmNodePtr, a deliberately opaque
	// "struct Node *" (fmgr.h:33) that keeps fmgr.h from depending on the node
	// headers; any is the Go analogue of that opaqueness. It is retyped to the
	// universal Node type once that exists (see the note on Context below).
	FnExpr any
}

// FunctionCallInfoBaseData is the per-call frame - fmgr.h:85. PostgreSQL almost
// always refers to it through the [FunctionCallInfo] pointer typedef; so do the
// call helpers and function bodies here. PostgreSQL stores Args as a flexible
// array inline in the struct; a Go slice is semantically identical.
type FunctionCallInfoBaseData struct {
	Flinfo *FmgrInfo // flinfo: lookup info for this call - fmgr.h:87
	// Context and Resultinfo are PostgreSQL's fmNodePtr fields (fmgr.h:88-89) —
	// an opaque "struct Node *" (fmgr.h:33) so fmgr.h need not know the concrete
	// types. Context carries call-context state (AggState, WindowAggState,
	// TriggerData) for aggregates/window/trigger calls; Resultinfo carries
	// ReturnSetInfo for set-returning functions. Both are nil for an ordinary
	// scalar call — which is all the current engine makes.
	//
	// These are any (the Go analogue of the opaque pointer) rather than a Node
	// type because PostgreSQL's Node is universal — the base of parse, plan, and
	// executor nodes — and our tree so far has only the parse subset (ast.Node);
	// the executor-state types these hold (AggState, ReturnSetInfo) are not
	// ported yet. When the planner/executor port introduces a universal Node
	// equivalent, all three node-pointer fields here (with FnExpr) are retyped to
	// it in one move; any avoids prejudging whether that Node is an expanded
	// ast.Node or a higher interface ast.Node implements.
	Context     any
	Resultinfo  any
	Fncollation Oid                   // fncollation: collation for the function to use - fmgr.h:90
	IsNull      bool                  // isnull: the callee sets this true for a NULL result - fmgr.h:92
	Nargs       int16                 // nargs: number of arguments actually passed - fmgr.h:93
	Args        []datum.NullableDatum // args: the inputs, each with its own nullness - fmgr.h:95
}

// FunctionCallInfo is a pointer to a call frame - fmgr.h:38
// ("typedef struct FunctionCallInfoBaseData *FunctionCallInfo"). This is the
// type function bodies and the call helpers use; PostgreSQL's PG_FUNCTION_ARGS
// expands to "FunctionCallInfo fcinfo".
type FunctionCallInfo = *FunctionCallInfoBaseData

// NewFunctionCallInfo allocates and initializes a call frame with space for nargs
// arguments — the combination of PostgreSQL's LOCAL_FCINFO stack allocation
// (fmgr.h:107) and InitFunctionCallInfoData (fmgr.h:150). The caller fills in
// Args before invoking. IsNull starts false, matching upstream.
func NewFunctionCallInfo(flinfo *FmgrInfo, nargs int, collation Oid) FunctionCallInfo {
	return &FunctionCallInfoBaseData{
		Flinfo:      flinfo,
		Fncollation: collation,
		Nargs:       int16(nargs),
		Args:        make([]datum.NullableDatum, nargs),
	}
}

// CallFunction invokes fcinfo.Flinfo's function with the arguments already
// placed in fcinfo.Args, applying strict-NULL short-circuiting. It mirrors the
// executor's EEOP_FUNCEXPR_STRICT / EEOP_FUNCEXPR paths
// (execExprInterp.c:747): for a strict function, if any argument is NULL the
// result is NULL and the function body is not called; otherwise IsNull is reset
// and the body is invoked, with its IsNull left for the caller to read. This is
// the general entry point once arguments (with their own nullness) are known.
func CallFunction(fcinfo FunctionCallInfo) datum.Datum {
	flinfo := fcinfo.Flinfo
	if flinfo.FnStrict {
		for i := range fcinfo.Args {
			if fcinfo.Args[i].IsNull {
				fcinfo.IsNull = true
				return datum.Datum{}
			}
		}
	}
	fcinfo.IsNull = false
	return flinfo.FnAddr(fcinfo)
}

// FunctionCall1Coll calls a one-argument function through its FmgrInfo with a
// known non-NULL argument - fmgr.c:1128 (FunctionCall1Coll). Unlike
// [CallFunction] it does not short-circuit on strictness (the argument is
// non-NULL by contract) and it treats a NULL result as an internal error,
// since the caller is not expecting one.
func FunctionCall1Coll(flinfo *FmgrInfo, collation Oid, arg1 datum.Datum) datum.Datum {
	fcinfo := NewFunctionCallInfo(flinfo, 1, collation)
	fcinfo.Args[0] = datum.NullableDatum{Value: arg1}
	result := flinfo.FnAddr(fcinfo)
	if fcinfo.IsNull {
		pgerror.Elogf("function %d returned NULL", flinfo.FnOid)
	}
	return result
}

// FunctionCall2Coll calls a two-argument function through its FmgrInfo with
// known non-NULL arguments - fmgr.c:1148 (FunctionCall2Coll).
func FunctionCall2Coll(flinfo *FmgrInfo, collation Oid, arg1, arg2 datum.Datum) datum.Datum {
	fcinfo := NewFunctionCallInfo(flinfo, 2, collation)
	fcinfo.Args[0] = datum.NullableDatum{Value: arg1}
	fcinfo.Args[1] = datum.NullableDatum{Value: arg2}
	result := flinfo.FnAddr(fcinfo)
	if fcinfo.IsNull {
		pgerror.Elogf("function %d returned NULL", flinfo.FnOid)
	}
	return result
}

// DirectFunctionCall1Coll calls a function pointer directly, without an
// FmgrInfo - fmgr.c:791 (DirectFunctionCall1Coll). Neither the argument nor
// the result may be NULL; a NULL result is an internal error. Only usable for
// functions that do not consult flinfo.
func DirectFunctionCall1Coll(fn PGFunction, collation Oid, arg1 datum.Datum) datum.Datum {
	fcinfo := &FunctionCallInfoBaseData{
		Fncollation: collation,
		Nargs:       1,
		Args:        []datum.NullableDatum{{Value: arg1}},
	}
	result := fn(fcinfo)
	if fcinfo.IsNull {
		pgerror.Elogf("function returned NULL")
	}
	return result
}

// DirectFunctionCall2Coll calls a two-argument function pointer directly,
// without an FmgrInfo - fmgr.c:813 (DirectFunctionCall2Coll).
func DirectFunctionCall2Coll(fn PGFunction, collation Oid, arg1, arg2 datum.Datum) datum.Datum {
	fcinfo := &FunctionCallInfoBaseData{
		Fncollation: collation,
		Nargs:       2,
		Args:        []datum.NullableDatum{{Value: arg1}, {Value: arg2}},
	}
	result := fn(fcinfo)
	if fcinfo.IsNull {
		pgerror.Elogf("function returned NULL")
	}
	return result
}
