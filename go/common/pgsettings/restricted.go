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
	canonical := strings.ToLower(name)
	reason, ok := restrictedGUCs[canonical]
	if !ok {
		return nil
	}
	return mterrors.NewFeatureNotSupported(fmt.Sprintf(
		"setting %s is not supported: %s; use RESET %s (or SET %s TO DEFAULT) to restore the managed value",
		canonical, reason, canonical, canonical))
}
