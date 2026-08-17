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
// pg_temp_N) in ANY position, and nil otherwise. It is the single guard for
// every surface that can assign search_path: session SET, set_config in every
// disposition, startup parameters, ALTER ROLE/DATABASE ... SET, and
// CREATE/ALTER FUNCTION proconfig.
//
// With pg_temp as the effective creation target, PostgreSQL creates
// unqualified objects as genuine temporary objects (no TEMP keyword involved)
// and current_schema() instantiates the temp namespace as a side effect —
// both invisible to the gateway's keyword-based temp detection, so they would
// silently taint a shared pooled backend.
//
// The check is deliberately position-INSENSITIVE. The effective creation
// target is the first EXISTING schema in the list, which the gateway cannot
// determine, so a trailing pg_temp is only conditionally safe: verified
// empirically that SET search_path = no_such_schema, pg_temp makes pg_temp the
// creation target and current_schema() returns pg_temp_N. Any position-aware
// rule would therefore depend on schema existence the gateway cannot see, and
// would be bypassed by prefixing a nonexistent schema. One uniform rule
// instead: no surface may put pg_temp in search_path.
//
// The known cost is PostgreSQL's trailing-pg_temp hardening idiom (used to
// demote temp-object lookup priority, e.g. SET search_path = admin, pg_temp on
// a SECURITY DEFINER function). It is rejected too, including in pg_dump output
// that carries it, and must be rewritten without the pg_temp element.
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

func isTempSchemaElem(elem string) bool {
	elem = strings.ToLower(strings.Trim(strings.TrimSpace(elem), `"`))
	return strings.HasPrefix(elem, "pg_temp")
}
