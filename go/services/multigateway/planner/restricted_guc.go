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

package planner

import (
	"fmt"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
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
// the SET / ALTER ROLE / ALTER DATABASE / set_config paths all consult this map.
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

// restrictedGUCError returns a feature_not_supported rejection if name is a
// cluster-managed GUC, or nil otherwise. The message names the GUC, gives the
// reason, and points the user at RESET, which can only restore the managed
// value. Shared by the statement guard and the set_config expression path so
// both surfaces give the same message.
func restrictedGUCError(name string) error {
	canonical := strings.ToLower(name)
	reason, ok := restrictedGUCs[canonical]
	if !ok {
		return nil
	}
	return mterrors.NewFeatureNotSupported(fmt.Sprintf(
		"setting %s is not supported: %s; use RESET %s (or SET %s TO DEFAULT) to restore the managed value",
		canonical, reason, canonical, canonical))
}

// checkRestrictedGUCChange rejects statements that assign a value to a
// cluster-managed GUC (see restrictedGUCs) at any gateway-reachable level:
// session/transaction SET (including SET LOCAL and SET ... FROM CURRENT),
// ALTER DATABASE ... SET, and ALTER ROLE ... SET. (ALTER SYSTEM is already
// blocked wholesale as a Tier 2 statement; set_config(...) is handled in the
// expression walker.)
//
// Reverts are allowed because they can only restore the cluster-managed value:
// RESET, RESET ALL, and SET ... TO DEFAULT.
//
// Runs pre-dispatch via planUnsupportedConstructs, so it covers both the simple
// and extended query protocols and is short-circuited by the plan cache.
func checkRestrictedGUCChange(stmt ast.Stmt) error {
	var setstmt *ast.VariableSetStmt
	switch s := stmt.(type) {
	case *ast.VariableSetStmt:
		setstmt = s
	case *ast.AlterRoleSetStmt:
		setstmt = s.Setstmt
	case *ast.AlterDatabaseSetStmt:
		setstmt = s.Setstmt
	default:
		return nil
	}
	if setstmt == nil {
		return nil
	}

	// VAR_SET_DEFAULT (SET ... TO DEFAULT), VAR_RESET (RESET), and
	// VAR_RESET_ALL (RESET ALL) revert to the managed global value and are
	// allowed. Everything else — VAR_SET_VALUE (SET ... = x / SET LOCAL ... = x)
	// and VAR_SET_CURRENT (SET ... FROM CURRENT) — pins an explicit value.
	switch setstmt.Kind {
	case ast.VAR_SET_DEFAULT, ast.VAR_RESET, ast.VAR_RESET_ALL:
		return nil
	}
	return restrictedGUCError(setstmt.Name)
}
