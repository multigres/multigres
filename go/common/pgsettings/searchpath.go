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
// pg_temp_N), and nil otherwise.
//
// With pg_temp first in search_path, PostgreSQL creates unqualified objects as
// genuine temporary objects (no TEMP keyword involved) and current_schema()
// instantiates the temp namespace as a side effect — both invisible to the
// gateway's keyword-based temp detection, so they would silently taint a shared
// pooled backend. Whether pg_temp is "first" further depends on which schemas
// exist in the target database, which the gateway cannot know, so any explicit
// mention is rejected. That is fail-closed but loses nothing real: pg_temp is
// always searched implicitly for lookups, so listing it only matters for the
// unsupported creation-target case.
//
// The value is split naively on commas: a quoted schema name that itself
// contains a comma and a pg_temp-prefixed fragment could false-positive, but
// user schemas cannot start with pg_ (reserved prefix), so no legitimate
// search_path is rejected.
func RejectTempSchemaSearchPath(value string) error {
	for elem := range strings.SplitSeq(value, ",") {
		elem = strings.ToLower(strings.Trim(strings.TrimSpace(elem), `"`))
		if strings.HasPrefix(elem, "pg_temp") {
			return mterrors.NewFeatureNotSupported(
				"pg_temp in search_path is not supported under connection pooling: " +
					"the temporary namespace belongs to a pooled backend, not to the client session; " +
					"use CREATE TEMP/TEMPORARY to create temporary objects")
		}
	}
	return nil
}
