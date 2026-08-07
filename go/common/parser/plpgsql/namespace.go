// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//  Portions Copyright (c) 2026, Supabase, Inc
//
//  Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
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

package plpgsql

import (
	"fmt"
	"strings"

	"github.com/multigres/multigres/go/common/parser/ast/plpgsqlast"
)

// This is the Go port of PG's PL/pgSQL compile-time namespace and datum list —
// the machinery the scanner consults to resolve an identifier to a declared
// variable (T_DATUM). It ports:
//
//   - the namespace stack (postgres/src/pl/plpgsql/src/pl_funcs.c plpgsql_ns_*)
//   - the flat datum list and plpgsql_adddatum (pl_comp.c)
//
// Everything execution- and catalog-related is dropped: no type resolution, no
// runtime datum storage. What remains is exactly enough to (a) tell an
// assignment target apart from a SQL statement, (b) bind loop/cursor variables,
// and (c) validate labels and duplicate declarations.

// nsItemType is the kind of a namespace entry, ported from PG's
// PLpgSQL_nsitem_type (plpgsql.h:41-46).
type nsItemType int

const (
	nsTypeLabel nsItemType = iota // block label / boundary marker
	nsTypeVar                     // scalar variable
	nsTypeRec                     // composite (record) variable
)

// labelType classifies a LABEL namespace entry, ported from PG's
// PLpgSQL_label_type (plpgsql.h:51-56). For a LABEL nsItem, itemNo holds one of
// these values (rather than a datum dno).
type labelType int

const (
	labelBlock labelType = iota // DECLARE/BEGIN block
	labelLoop                   // looping construct
	labelOther                  // anything else (e.g. an exception block)
)

// nsItem is one entry in the compiler's namespace chain, ported from PG's
// PLpgSQL_nsitem (plpgsql.h:437-448). Entries are singly linked youngest-first
// via prev; LABEL entries mark block boundaries. For a LABEL, itemNo is a
// labelType; otherwise itemNo is the associated datum's dno.
type nsItem struct {
	itemType nsItemType
	itemNo   int
	name     string
	prev     *nsItem
}

// namespace is the compile-time namespace stack (PG's file-static ns_top in
// pl_funcs.c, made explicit here). top points to the youngest entry reachable
// from the block currently being parsed.
type namespace struct {
	top *nsItem
}

// init resets the namespace for a new function (plpgsql_ns_init).
func (ns *namespace) init() {
	ns.top = nil
}

// push opens a new namespace level with a LABEL boundary marker
// (plpgsql_ns_push). A nil/empty label is stored as "".
func (ns *namespace) push(label string, lt labelType) {
	ns.additem(nsTypeLabel, int(lt), label)
}

// pop discards entries back to and including the most recent LABEL
// (plpgsql_ns_pop).
func (ns *namespace) pop() {
	for ns.top.itemType != nsTypeLabel {
		ns.top = ns.top.prev
	}
	ns.top = ns.top.prev
}

// topItem returns the current end of the namespace chain (plpgsql_ns_top).
func (ns *namespace) topItem() *nsItem {
	return ns.top
}

// additem pushes a new entry onto the chain (plpgsql_ns_additem). The first
// entry added to a function must be a LABEL, matching PG's assertion.
func (ns *namespace) additem(it nsItemType, itemNo int, name string) {
	ns.top = &nsItem{itemType: it, itemNo: itemNo, name: name, prev: ns.top}
}

// lookup resolves an identifier against the namespace chain rooted at nsCur,
// ported from plpgsql_ns_lookup. It searches only for variables, not labels.
//
// name1 must be non-empty; pass "" for name2 and/or name3 when the name has
// fewer than three components. If localmode is true, only the topmost block
// level is searched. The second return value ("names used") is 0 on no match,
// 1 when name1 matched an unqualified variable, or 2 when name1+name2 matched a
// block label + variable name.
//
// As in PG, name3 is never matched to anything, but when it is non-empty a
// qualified match to a *scalar* variable is disregarded; likewise a non-empty
// name2 disregards an unqualified match to a scalar variable. ("" is the NULL
// sentinel — a real identifier is never empty.)
func (ns *namespace) lookup(nsCur *nsItem, localmode bool, name1, name2, name3 string) (*nsItem, int) {
	// Outer loop iterates once per block level in the namespace chain.
	for nsCur != nil {
		var item *nsItem

		// Check this level for an unqualified match to a variable name.
		for item = nsCur; item.itemType != nsTypeLabel; item = item.prev {
			if item.name == name1 {
				if name2 == "" || item.itemType != nsTypeVar {
					return item, 1
				}
			}
		}

		// Check this level for a qualified (label.var) match. item is now the
		// LABEL sentinel for this level.
		if name2 != "" && item.name == name1 {
			for item = nsCur; item.itemType != nsTypeLabel; item = item.prev {
				if item.name == name2 {
					if name3 == "" || item.itemType != nsTypeVar {
						return item, 2
					}
				}
			}
		}

		if localmode {
			break // do not look into upper levels
		}

		nsCur = item.prev
	}

	return nil, 0 // no match found
}

// lookupLabel finds a LABEL entry by name (plpgsql_ns_lookup_label).
func (ns *namespace) lookupLabel(nsCur *nsItem, name string) *nsItem {
	for nsCur != nil {
		if nsCur.itemType == nsTypeLabel && nsCur.name == name {
			return nsCur
		}
		nsCur = nsCur.prev
	}
	return nil
}

// findNearestLoop finds the innermost enclosing loop LABEL
// (plpgsql_ns_find_nearest_loop), used to validate unlabeled EXIT/CONTINUE.
func (ns *namespace) findNearestLoop(nsCur *nsItem) *nsItem {
	for nsCur != nil {
		if nsCur.itemType == nsTypeLabel && nsCur.itemNo == int(labelLoop) {
			return nsCur
		}
		nsCur = nsCur.prev
	}
	return nil
}

// addDatum appends a datum to the function's flat datum list, assigning it its
// dno (its index), the Go port of plpgsql_adddatum. The datum list is the array
// that nsItem.itemNo indexes into.
func (l *lexer) addDatum(d plpgsqlast.Datum) {
	d.SetDatumNo(len(l.datums))
	l.datums = append(l.datums, d)
}

// declareVar registers a freshly declared datum in the current block's
// namespace and the function datum list — the Go port of PG's decl_varname
// duplicate check plus plpgsql_build_variable's add-to-namespace step. A name
// already declared in the same block (localmode lookup) is rejected with
// "duplicate declaration", matching PG. A record (PLpgSQL_rec) registers as a
// REC namespace entry so that rec.field references resolve; everything else
// registers as a scalar.
func (l *lexer) declareVar(name string, d plpgsqlast.Datum) {
	if item, _ := l.ns.lookup(l.ns.topItem(), true, name, "", ""); item != nil {
		l.Error("duplicate declaration")
	}
	l.addDatum(d)
	itemType := nsTypeVar
	if _, isRec := d.(*plpgsqlast.PLpgSQL_rec); isRec {
		itemType = nsTypeRec
	}
	l.ns.additem(itemType, d.DatumNo(), name)
}

// checkExit validates an EXIT/CONTINUE against the namespace, the Go port of the
// namespace checks in PG's stmt_exit action. A labelled EXIT/CONTINUE requires
// the label to exist in an enclosing block or loop, and CONTINUE may target only
// a loop label. An unlabelled EXIT/CONTINUE must sit inside some loop. Errors are
// reported (not fatal) via l.Error, so parsing continues.
func (l *lexer) checkExit(isExit bool, label string) {
	if label != "" {
		item := l.ns.lookupLabel(l.ns.topItem(), label)
		if item == nil {
			l.Error(fmt.Sprintf("there is no label %q attached to any block or loop enclosing this statement", label))
			return
		}
		// CONTINUE only allows loop labels.
		if item.itemNo != int(labelLoop) && !isExit {
			l.Error(fmt.Sprintf("block label %q cannot be used in CONTINUE", label))
		}
		return
	}
	// No label: there must be some enclosing loop (the same test for EXIT and
	// CONTINUE — an unlabelled EXIT does not match a block).
	if l.ns.findNearestLoop(l.ns.topItem()) == nil {
		if isExit {
			l.Error("EXIT cannot be used outside a loop, unless it has a label")
		} else {
			l.Error("CONTINUE cannot be used outside a loop")
		}
	}
}

// isUnboundCursorVar reports whether a datum is a refcursor variable declared
// without a bound query (no CURSOR FOR <query>). PG treats a refcursor-typed FOR
// target as a cursor FOR loop and rejects it unless the cursor is bound. A
// variable declared `c CURSOR FOR …` carries CursorExplicitExpr and is bound; a
// plain `c refcursor` is not. Type is matched on the captured text (we have no
// catalog), so only a literal `refcursor` declaration is recognized.
func isUnboundCursorVar(d plpgsqlast.Datum) bool {
	v, ok := d.(*plpgsqlast.PLpgSQL_var)
	if !ok {
		return false
	}
	return v.CursorExplicitExpr == nil &&
		v.DataType != nil &&
		strings.EqualFold(strings.TrimSpace(v.DataType.TypeName), "refcursor")
}

// isCursorVar reports whether a datum is a cursor variable — a bound cursor
// (declared CURSOR FOR <query>, carrying CursorExplicitExpr) or a refcursor-typed
// scalar. It distinguishes a cursor FOR loop from a query FOR: the former builds
// its own record loop variable, so its loop variable need not be a known variable.
func isCursorVar(d plpgsqlast.Datum) bool {
	v, ok := d.(*plpgsqlast.PLpgSQL_var)
	if !ok {
		return false
	}
	return v.CursorExplicitExpr != nil ||
		(v.DataType != nil && strings.EqualFold(strings.TrimSpace(v.DataType.TypeName), "refcursor"))
}

// checkAssignable rejects an assignment to a datum that cannot be written — the
// Go port of PG's check_assignable. A CONSTANT scalar or record is rejected; a ROW
// is always assignable (its members were checked when built); a RECFIELD is
// assignable exactly when its parent record is. An alias carries no CONSTANT flag
// and is treated as assignable. Reported (not fatal) via l.Error.
func (l *lexer) checkAssignable(d plpgsqlast.Datum) {
	switch v := d.(type) {
	case *plpgsqlast.PLpgSQL_var:
		if v.IsConst {
			l.Error(fmt.Sprintf("variable %q is declared CONSTANT", v.Refname))
		}
	case *plpgsqlast.PLpgSQL_rec:
		if v.IsConst {
			l.Error(fmt.Sprintf("variable %q is declared CONSTANT", v.Refname))
		}
	case *plpgsqlast.PLpgSQL_recfield:
		if v.RecParentNo >= 0 && v.RecParentNo < len(l.datums) {
			l.checkAssignable(l.datums[v.RecParentNo])
		}
	}
}

// buildRecfield returns the RECFIELD datum for rec.fldname, creating and
// registering it on first reference — the Go port of plpgsql_build_recfield. PG
// chains a record's fields for O(fields) reuse; with no runtime linkage we scan
// the datum list (cheap at parse time). The datum is built whether or not the
// field exists — a bad-field error is a runtime/catalog matter we do not check.
func (l *lexer) buildRecfield(rec *plpgsqlast.PLpgSQL_rec, fldname string) *plpgsqlast.PLpgSQL_recfield {
	for _, d := range l.datums {
		if rf, ok := d.(*plpgsqlast.PLpgSQL_recfield); ok &&
			rf.RecParentNo == rec.Dno && rf.FieldName == fldname {
			return rf
		}
	}
	rf := plpgsqlast.NewPLpgSQL_recfield(fldname)
	rf.RecParentNo = rec.Dno
	l.addDatum(rf)
	return rf
}
