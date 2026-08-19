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
	"fmt"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
)

// restrictedGUCs lists session parameters whose value the cluster manages on
// the client's behalf and that users therefore may not assign through the
// pooler. Each entry maps the (lowercased) GUC name to a short reason, which is
// woven into the rejection message. Reverting to the managed value is always
// allowed (RESET / SET ... TO DEFAULT / RESET ALL), so the guard only blocks
// value assignments.
//
// Keys are lowercased; lookups are case-insensitive, matching PostgreSQL's
// case-insensitive parameter names. To restrict another GUC, add a line here —
// the SET / ALTER ROLE / ALTER DATABASE / set_config paths (plan-time literal
// names in the planner, execute-time bound names in engine.resolveSetConfig)
// all consult this map.
//
//   - synchronous_commit: durability is owned by the multipooler rule store /
//     SyncStandbyManager (the sole writer of synchronous_commit via ALTER
//     SYSTEM), and the HA contract requires synchronous_commit = on so that an
//     acknowledged commit is durably flushed on the synchronous standby
//     (docs/ha/decision-log/2026-02-12-synchronous-commit-on.md). Letting a
//     session lower it silently weakens that guarantee for its writes — a
//     footgun rather than a load-bearing API contract
//     (docs/ha/decision-log/2026-05-29-block-synchronous-commit-changes.md).
var restrictedGUCs = map[string]string{
	"synchronous_commit": "replication durability is managed by the cluster",
}

// RestrictedGUCError returns a feature_not_supported rejection if name is a
// cluster-managed GUC, or nil otherwise. The message names the GUC, gives the
// reason, and points the user at RESET, which can only restore the managed
// value. Shared by the planner's statement/expression guards and the engine's
// execute-time bound-name resolution so all surfaces give the same message.
func RestrictedGUCError(name string) error {
	canonical, reason, ok := restrictedGUC(name)
	if !ok {
		return nil
	}
	return mterrors.NewFeatureNotSupported(fmt.Sprintf(
		"setting %s is not supported: %s; use RESET %s (or SET %s TO DEFAULT) to restore the managed value",
		canonical, reason, canonical, canonical))
}

// RestrictedGUCStartupError is RestrictedGUCError for the connection startup
// path, where a client can supply a GUC as a startup parameter or via
// options=-c name=value. Those values flow into the session settings replayed
// onto pooled backends, so the statement-level guard alone would leave a
// connect-time bypass.
//
// The message differs because the advice does: at startup there is no revert
// form to point at — supplying the parameter at all is the assignment — so the
// fix is to omit it and let the cluster-managed value apply.
func RestrictedGUCStartupError(name string) error {
	canonical, reason, ok := restrictedGUC(name)
	if !ok {
		return nil
	}
	return mterrors.NewFeatureNotSupported(fmt.Sprintf(
		"setting %s at connection startup is not supported: %s; omit it and the cluster-managed value applies",
		canonical, reason))
}

// restrictedGUC looks name up in restrictedGUCs, returning its canonical
// (lowercased) spelling and reason. Keeping the lookup in one place means a new
// entry in the map is honored by every surface at once.
func restrictedGUC(name string) (canonical, reason string, ok bool) {
	canonical = strings.ToLower(name)
	reason, ok = restrictedGUCs[canonical]
	return canonical, reason, ok
}
