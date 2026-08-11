// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgsettings

import (
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
)

// RejectTempSchemaSearchPath returns a feature_not_supported error when a
// search_path value names the temporary namespace (pg_temp, or a concrete
// pg_temp_N) in ANY position, and nil otherwise. This is the strict guard for
// client-reachable runtime surfaces: session SET, set_config in every
// disposition, and startup parameters.
//
// With pg_temp as the effective creation target, PostgreSQL creates
// unqualified objects as genuine temporary objects (no TEMP keyword involved)
// and current_schema() instantiates the temp namespace as a side effect —
// both invisible to the gateway's keyword-based temp detection, so they would
// silently taint a shared pooled backend. The creation target is the first
// EXISTING schema in the list (verified empirically: SET search_path =
// no_such_schema, pg_temp makes pg_temp the creation target and
// current_schema() returns pg_temp_N), so a position-aware check would be
// trivially bypassed by a client prefixing a nonexistent schema — any
// explicit mention is rejected on these surfaces. That loses nothing real for
// lookups: pg_temp is always searched implicitly.
//
// Admin-authored persisted configuration (ALTER ROLE/DATABASE ... SET,
// CREATE/ALTER FUNCTION proconfig) uses RejectLeadingTempSchemaSearchPath
// instead, preserving PostgreSQL's own trailing-pg_temp hardening guidance.
//
// The value is split naively on commas: a quoted schema name that itself
// contains a comma and a pg_temp-prefixed fragment could false-positive, but
// user schemas cannot start with pg_ (reserved prefix), so no legitimate
// search_path is rejected.
func RejectTempSchemaSearchPath(value string) error {
	for elem := range strings.SplitSeq(value, ",") {
		if isTempSchemaElem(elem) {
			return mterrors.NewFeatureNotSupported(
				"pg_temp in search_path is not supported under connection pooling: " +
					"the temporary namespace belongs to a pooled backend, not to the client session; " +
					"use CREATE TEMP/TEMPORARY to create temporary objects")
		}
	}
	return nil
}

// RejectLeadingTempSchemaSearchPath rejects a search_path value only when its
// FIRST element names the temporary namespace. It is the lenient guard for
// admin-authored persisted configuration — ALTER ROLE/DATABASE ... SET and
// CREATE/ALTER FUNCTION|PROCEDURE SET clauses (proconfig) — where PostgreSQL's
// own hardening guidance appends a trailing pg_temp to demote temp-object
// lookup priority (the standard SECURITY DEFINER pattern, emitted by pg_dump),
// and rejecting it would break restores of correctly hardened databases.
//
// The trailing position is safe only while some schema listed before pg_temp
// exists — the creation target is the first EXISTING entry — and these
// surfaces are trusted to uphold that: the values are written by roles with
// DDL privileges as part of deliberate configuration, not by arbitrary
// clients at runtime (which get the strict RejectTempSchemaSearchPath). The
// residual risk — an admin listing only nonexistent schemas ahead of
// pg_temp, or dropping them all later — is a misconfiguration within the
// same trust boundary as any other admin DDL.
func RejectLeadingTempSchemaSearchPath(value string) error {
	first, _, _ := strings.Cut(value, ",")
	if isTempSchemaElem(first) {
		return mterrors.NewFeatureNotSupported(
			"pg_temp leading search_path is not supported under connection pooling: " +
				"it would make the temporary namespace of a shared pooled backend the creation target; " +
				"list pg_temp after at least one real schema, or use CREATE TEMP/TEMPORARY")
	}
	return nil
}

func isTempSchemaElem(elem string) bool {
	elem = strings.ToLower(strings.Trim(strings.TrimSpace(elem), `"`))
	return strings.HasPrefix(elem, "pg_temp")
}
