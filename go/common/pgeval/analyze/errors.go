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

package analyze

import (
	"fmt"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// The parser raises errors through ereport(ERROR, (errcode(...), errmsg(...),
// parser_errposition(...))). These helpers are the Go form of that shape; the
// non-local exit is pgerror's panic, recovered by [ParseState.TransformExpr].
//
// No error carries a position. parser_errposition needs the raw node's
// source offset, and the Go grammar does not record offsets for expression
// nodes (every Location() is 0), so a position would be wrong rather than
// merely absent.

// ereport raises an ERROR with the given SQLSTATE and formatted message.
func ereport(sqlstate, format string, args ...any) {
	pgerror.Ereportf(sqlstate, format, args...)
}

// ereportHint raises an ERROR carrying an errhint.
func ereportHint(sqlstate, hint, format string, args ...any) {
	d := mterrors.NewPgError("ERROR", sqlstate, fmt.Sprintf(format, args...), "")
	d.Hint = hint
	pgerror.Ereport(d)
}

// ereportDetail raises an ERROR carrying an errdetail.
func ereportDetail(sqlstate, detail, format string, args ...any) {
	pgerror.Ereport(mterrors.NewPgError("ERROR", sqlstate, fmt.Sprintf(format, args...), detail))
}

// unsupported raises feature_not_supported for a construct the gateway
// cannot analyze yet. It is the analysis-time counterpart of fmgr's stub for
// an unported function body: the SQL is valid, the engine is incomplete, and
// a caller can route the statement to PostgreSQL instead.
func unsupported(format string, args ...any) {
	pgerror.Ereportf(mterrors.PgSSFeatureNotSupported, format, args...)
}
