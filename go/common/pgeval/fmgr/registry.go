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

package fmgr

import (
	"sort"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// builtins maps a pg_proc.prosrc C-symbol name to its Go implementation. This
// is the analogue of the compiled-in fmgr_builtins table
// (postgres src/backend/utils/fmgrtab.c), except entries are registered at
// startup as function bodies are ported rather than emitted by a build step.
// The funcs package populates it from its init(); the OID-keyed lookup in
// [FmgrInfoFor] joins it against the pgcatalog.Proc rows.
var builtins = map[string]PGFunction{}

// RegisterBuiltin binds a builtin implementation to its pg_proc.prosrc name
// (e.g. "int4pl"). It is meant to be called from package init() functions as
// bodies are ported. It panics on a duplicate registration or an empty name,
// since both indicate a programming error at startup.
func RegisterBuiltin(prosrc string, fn PGFunction) {
	if prosrc == "" {
		panic("fmgr: RegisterBuiltin called with empty prosrc")
	}
	if _, dup := builtins[prosrc]; dup {
		panic("fmgr: duplicate builtin registration for " + prosrc)
	}
	builtins[prosrc] = fn
}

// IsBuiltinRegistered reports whether a Go implementation has been registered
// for the given pg_proc.prosrc name. Used by coverage tests.
func IsBuiltinRegistered(prosrc string) bool {
	_, ok := builtins[prosrc]
	return ok
}

// RegisteredBuiltins returns the sorted prosrc names of all registered
// implementations. Used by coverage tests to report implemented/total.
func RegisteredBuiltins() []string {
	names := make([]string, 0, len(builtins))
	for name := range builtins {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ProcResolver resolves function metadata for OIDs not in the builtin catalog —
// the seam through which schema tracking will later supply user-defined
// pg_proc rows. It provides metadata only (signature, strictness) so that
// parse analysis can type-check calls to user functions; those functions are
// not executed in the gateway, so [FmgrInfoFor] still yields a stub body for
// them.
type ProcResolver interface {
	ProcByOid(oid Oid) *pgcatalog.Proc
}

var procResolver ProcResolver

// SetProcResolver installs the resolver for user-defined function metadata.
// Passing nil disables it (the default). It is not safe for concurrent use with
// FmgrInfoFor and is intended to be called once at startup.
func SetProcResolver(r ProcResolver) { procResolver = r }

// lookupProc finds a pg_proc row: builtin catalog first (the fast path,
// analogous to fmgr_isbuiltin — fmgr.c:75), then the user-defined resolver.
func lookupProc(oid Oid) *pgcatalog.Proc {
	if p := pgcatalog.ProcByOid(oid); p != nil {
		return p
	}
	if procResolver != nil {
		return procResolver.ProcByOid(oid)
	}
	return nil
}

// FmgrInfoFor builds an FmgrInfo for a function OID, mirroring fmgr_info's
// builtin fast path (fmgr.c:146): the metadata (nargs, strict, retset) comes
// straight from the pg_proc row. If a Go implementation is registered for the
// row's prosrc it becomes FnAddr; otherwise FnAddr is a stub that raises
// feature_not_supported when called, so an unimplemented function is resolvable
// (parse analysis can type-check it) but fails loudly and specifically if the
// gateway actually has to evaluate it. An unknown OID is a Go error, matching
// PostgreSQL's "cache lookup failed for function" at resolution time.
func FmgrInfoFor(oid Oid) (*FmgrInfo, error) {
	proc := lookupProc(oid)
	if proc == nil {
		return nil, mterrors.NewPgError("ERROR", mterrors.PgSSUndefinedFunction,
			"cache lookup failed for function", "no builtin or tracked function has this OID")
	}
	fn := builtins[proc.Src]
	if fn == nil {
		fn = stubFor(proc)
	}
	return &FmgrInfo{
		FnAddr:   fn,
		FnOid:    oid,
		FnNargs:  int16(len(proc.ArgTypes)),
		FnStrict: proc.Strict,
		FnRetset: proc.RetSet,
	}, nil
}

// stubFor returns an FnAddr for a function with no ported implementation. An
// aggregate's pg_proc row is only a placeholder (its real transition/final
// functions live in pg_aggregate and its prosrc is "aggregate_dummy"), so
// calling one as a scalar is a distinct, permanent error — matching upstream's
// aggregate_dummy, which errors rather than reporting "not implemented".
// Everything else is a genuinely not-yet-ported builtin.
func stubFor(proc *pgcatalog.Proc) PGFunction {
	if proc.Kind == pgcatalog.ProcKindAggregate {
		return func(FunctionCallInfo) datum.Datum {
			pgerror.Ereportf(mterrors.PgSSFeatureNotSupported,
				"aggregate function %s (OID %d) called as a scalar function", proc.Name, proc.Oid)
			return datum.Datum{} // unreachable: Ereportf never returns
		}
	}
	return func(FunctionCallInfo) datum.Datum {
		pgerror.Ereportf(mterrors.PgSSFeatureNotSupported,
			"function %s (OID %d, prosrc %q) is not yet implemented in the gateway evaluation engine",
			proc.Name, proc.Oid, proc.Src)
		return datum.Datum{} // unreachable: Ereportf never returns
	}
}
