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

// Package pgcatalog provides PostgreSQL's builtin catalog data (types,
// functions, operators, casts) as compiled-in Go tables, generated from the
// pg_*.dat bootstrap data files vendored under data/ (PostgreSQL REL_17_6).
//
// This is the analogue of PostgreSQL's fmgr builtins table plus the builtin
// rows of pg_type, pg_proc, pg_operator, and pg_cast: the parser, planner,
// and expression evaluation engine resolve builtin names and OIDs through
// this package without any runtime catalog access. User-defined objects are
// not covered here; they arrive later via schema tracking.
//
// Regenerate with:
//
//	go run ./go/tools/pgcataloggen/main
//
// or "make pgcatalog". CI fails if the generated files are out of date.
package pgcatalog

import (
	"github.com/multigres/multigres/go/common/parser/ast"
)

// Oid is PostgreSQL's object identifier type, shared with the parser AST.
type Oid = ast.Oid

// RegProc mirrors PostgreSQL's regproc: an Oid that references a pg_proc
// row - postgres/src/include/c.h:649 ("typedef Oid regproc"). Like the C
// typedef it is a transparent alias, not a distinct type: the alias-ness is
// declaration-site documentation only, and the name-flavored I/O behavior of
// the SQL-level regproc type lives in its pg_type row's input/output
// functions, not here. Used exactly where the upstream headers declare
// regproc (castfunc, for example, is declared plain Oid upstream and stays
// Oid here).
type RegProc = Oid

// Proc volatility classes (pg_proc.provolatile) -
// postgres/src/include/catalog/pg_proc.h:164-166.
const (
	VolatilityImmutable byte = 'i' // PROVOLATILE_IMMUTABLE
	VolatilityStable    byte = 's' // PROVOLATILE_STABLE
	VolatilityVolatile  byte = 'v' // PROVOLATILE_VOLATILE
)

// Proc kinds (pg_proc.prokind) -
// postgres/src/include/catalog/pg_proc.h:151-154.
const (
	ProcKindFunction  byte = 'f' // PROKIND_FUNCTION
	ProcKindProcedure byte = 'p' // PROKIND_PROCEDURE
	ProcKindAggregate byte = 'a' // PROKIND_AGGREGATE
	ProcKindWindow    byte = 'w' // PROKIND_WINDOW
)

// Cast contexts (pg_cast.castcontext) -
// postgres/src/include/catalog/pg_cast.h:77-79.
const (
	CastContextImplicit   byte = 'i' // COERCION_CODE_IMPLICIT
	CastContextAssignment byte = 'a' // COERCION_CODE_ASSIGNMENT
	CastContextExplicit   byte = 'e' // COERCION_CODE_EXPLICIT
)

// Cast methods (pg_cast.castmethod) -
// postgres/src/include/catalog/pg_cast.h:89-91.
const (
	CastMethodFunction byte = 'f' // COERCION_METHOD_FUNCTION
	CastMethodBinary   byte = 'b' // COERCION_METHOD_BINARY
	CastMethodInOut    byte = 'i' // COERCION_METHOD_INOUT
)

// Operator kinds (pg_operator.oprkind) -
// postgres/src/include/catalog/pg_operator.h:45.
const (
	OperatorKindInfix  byte = 'b' // 'b' = infix (binary)
	OperatorKindPrefix byte = 'l' // 'l' = prefix
)

// Type is a builtin type: the subset of pg_type columns multigres carries -
// postgres/src/include/catalog/pg_type.h:36. Storage-only columns (typalign,
// typstorage, ...) are intentionally absent.
type Type struct {
	Oid       Oid     // oid - pg_type.h:38
	Name      string  // typname - pg_type.h:41
	Len       int16   // typlen: -1 = varlena, -2 = C string - pg_type.h:56
	ByVal     bool    // typbyval - pg_type.h:66
	Category  byte    // typcategory - pg_type.h:85 (values pg_type.h:284-300)
	Preferred bool    // typispreferred - pg_type.h:88
	Input     RegProc // typinput: the text input function - pg_type.h:133
	Output    RegProc // typoutput - pg_type.h:134
	Receive   RegProc // typreceive: binary input function, 0 if none - pg_type.h:137
	Send      RegProc // typsend: binary output function, 0 if none - pg_type.h:138
	Elem      Oid     // typelem: element type for array types, else 0 - pg_type.h:120
	Array     Oid     // typarray: the corresponding array type, else 0 - pg_type.h:126
	Collation Oid     // typcollation: 0 if not collatable - pg_type.h:228
}

// Proc is a builtin function: the subset of pg_proc columns multigres
// carries - postgres/src/include/catalog/pg_proc.h:30. Src is the upstream C
// symbol name (prosrc) that the fmgr registry binds Go implementations to;
// for aggregates it is the placeholder "aggregate_dummy", and for
// SQL-language builtins the sentinel "see system_functions.sql".
type Proc struct {
	Oid      Oid    // oid - pg_proc.h:32
	Name     string // proname - pg_proc.h:35
	ArgTypes []Oid  // proargtypes; pronargs == len(ArgTypes) - pg_proc.h:95
	RetType  Oid    // prorettype - pg_proc.h:87
	Strict   bool   // proisstrict - pg_proc.h:68
	Volatile byte   // provolatile - pg_proc.h:74
	RetSet   bool   // proretset - pg_proc.h:71
	Kind     byte   // prokind - pg_proc.h:59
	Src      string // prosrc - pg_proc.h:115
}

// Operator is a builtin operator (full pg_operator row minus the
// selectivity-estimator columns) -
// postgres/src/include/catalog/pg_operator.h:31.
type Operator struct {
	Oid        Oid     // oid - pg_operator.h:33
	Name       string  // oprname - pg_operator.h:36
	Kind       byte    // oprkind: infix or prefix - pg_operator.h:45
	Left       Oid     // oprleft: 0 for prefix operators - pg_operator.h:54
	Right      Oid     // oprright - pg_operator.h:57
	Result     Oid     // oprresult - pg_operator.h:60
	Commutator Oid     // oprcom, 0 if none - pg_operator.h:63
	Negator    Oid     // oprnegate, 0 if none - pg_operator.h:66
	Code       RegProc // oprcode: the implementing pg_proc - pg_operator.h:69
	CanMerge   bool    // oprcanmerge - pg_operator.h:48
	CanHash    bool    // oprcanhash - pg_operator.h:51
}

// Cast is a builtin cast - postgres/src/include/catalog/pg_cast.h:32.
// pg_cast rows carry no hand-assigned OIDs upstream, so casts are identified
// by (Source, Target).
type Cast struct {
	Source  Oid  // castsource - pg_cast.h:37
	Target  Oid  // casttarget - pg_cast.h:40
	Func    Oid  // castfunc: 0 unless Method is CastMethodFunction - pg_cast.h:43
	Context byte // castcontext - pg_cast.h:46
	Method  byte // castmethod - pg_cast.h:49
}

var (
	typesByOid      map[Oid]*Type
	typesByName     map[string]*Type
	procsByOid      map[Oid]*Proc
	procsByName     map[string][]*Proc
	operatorsByOid  map[Oid]*Operator
	operatorsByName map[string][]*Operator
	castsBySrcTgt   map[[2]Oid]*Cast
)

func init() {
	typesByOid = make(map[Oid]*Type, len(Types))
	typesByName = make(map[string]*Type, len(Types))
	for i := range Types {
		t := &Types[i]
		typesByOid[t.Oid] = t
		typesByName[t.Name] = t
	}
	procsByOid = make(map[Oid]*Proc, len(Procs))
	procsByName = make(map[string][]*Proc, len(Procs))
	for i := range Procs {
		p := &Procs[i]
		procsByOid[p.Oid] = p
		procsByName[p.Name] = append(procsByName[p.Name], p)
	}
	operatorsByOid = make(map[Oid]*Operator, len(Operators))
	operatorsByName = make(map[string][]*Operator)
	for i := range Operators {
		o := &Operators[i]
		operatorsByOid[o.Oid] = o
		operatorsByName[o.Name] = append(operatorsByName[o.Name], o)
	}
	castsBySrcTgt = make(map[[2]Oid]*Cast, len(Casts))
	for i := range Casts {
		c := &Casts[i]
		castsBySrcTgt[[2]Oid{c.Source, c.Target}] = c
	}
}

// The lookup functions below return pointers and slices that alias the
// package-global builtin tables (Types, Procs, Operators, Casts) and the
// internal by-name overload slices. These are immutable builtin catalog data:
// callers must treat every result as read-only. Mutating a returned
// *Type/*Proc/*Operator/*Cast, or a returned overload slice, corrupts the
// shared tables and every subsequent lookup in the process.

// TypeByOid returns the builtin type with the given OID, or nil.
func TypeByOid(oid Oid) *Type {
	return typesByOid[oid]
}

// TypeByName returns the builtin type with the given pg_catalog name, or nil.
func TypeByName(name string) *Type {
	return typesByName[name]
}

// ProcByOid returns the builtin function with the given OID, or nil.
func ProcByOid(oid Oid) *Proc {
	return procsByOid[oid]
}

// ProcsByName returns all builtin functions with the given name (overloads
// share a name), or nil.
func ProcsByName(name string) []*Proc {
	return procsByName[name]
}

// OperatorByOid returns the builtin operator with the given OID, or nil.
func OperatorByOid(oid Oid) *Operator {
	return operatorsByOid[oid]
}

// OperatorsByName returns all builtin operators with the given name, or nil.
func OperatorsByName(name string) []*Operator {
	return operatorsByName[name]
}

// LookupCast returns the builtin cast from source to target, or nil.
func LookupCast(source, target Oid) *Cast {
	return castsBySrcTgt[[2]Oid{source, target}]
}
