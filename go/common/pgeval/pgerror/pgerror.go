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

// Package pgerror ports PostgreSQL's ereport/elog error-raising mechanism to
// Go for the evaluation engine.
//
// PostgreSQL raises an error with ereport(ERROR, ...) — a non-local exit that
// longjmps to the nearest PG_TRY/PG_CATCH boundary (postgres
// src/include/utils/elog.h:141). Go has no longjmp, so the closest analogue is
// panic/recover: [Ereportf] (and [Ereport] for the rarer case needing detail or
// hint) panics with a *mterrors.PgDiagnostic carrying the SQLSTATE and message,
// so it matches what a shard would send byte-for-byte, and [Recover] converts
// that panic back into a Go error at the evaluation boundary while re-panicking
// any genuine Go panic.
//
// Raising errors by panic (rather than threading (Datum, error) through every
// call) is a deliberate decision: it keeps ported utils/adt function bodies a
// mechanical, line-for-line translation of the C, where an ereport(ERROR, ...)
// becomes an Ereport(...) call at the exact site the C exits non-locally, with
// no added error plumbing at each step.
package pgerror

import (
	"fmt"

	"github.com/multigres/multigres/go/common/mterrors"
)

// Ereport raises a PG-style ERROR by panicking with a pre-built
// *mterrors.PgDiagnostic, modeling ereport(ERROR, (...)) (elog.h:141) — a
// non-local exit recovered by [Recover] at the evaluation boundary. It never
// returns.
//
// Use this when the error needs fields beyond a SQLSTATE and message (detail,
// hint, position): build the diagnostic, set those fields, then raise it. For
// the common errcode+errmsg case use [Ereportf].
func Ereport(diag *mterrors.PgDiagnostic) {
	panic(diag)
}

// Ereportf raises an ERROR with the given SQLSTATE and a formatted message —
// the Go form of the near-universal ereport(ERROR, (errcode(sqlstate),
// errmsg(format, ...))) shape. Use the mterrors.PgSS* SQLSTATE constants. It
// never returns.
func Ereportf(sqlstate, format string, args ...any) {
	Ereport(mterrors.NewPgError("ERROR", sqlstate, sprintf(format, args), ""))
}

// Elogf raises an internal ERROR (SQLSTATE XX000), modeling elog(ERROR, ...)
// (elog.h:239) — used for "can't happen" conditions such as a strict function
// returning NULL. It never returns.
func Elogf(format string, args ...any) {
	Ereportf(mterrors.PgSSInternalError, format, args...)
}

func sprintf(format string, args []any) string {
	if len(args) == 0 {
		return format
	}
	return fmt.Sprintf(format, args...)
}

// Recover runs fn and converts an ereport-shaped panic (a *mterrors.PgDiagnostic
// raised by [Ereport]) into a returned error, re-panicking anything else so
// genuine Go bugs (nil dereference, out-of-range index) surface as crashes
// rather than being disguised as SQL errors. It is the analogue of the
// PG_TRY/PG_CATCH boundary that PG's executor wraps around expression
// evaluation.
func Recover(fn func()) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if d, ok := r.(*mterrors.PgDiagnostic); ok {
			err = d
			return
		}
		panic(r)
	}()
	fn()
	return nil
}
