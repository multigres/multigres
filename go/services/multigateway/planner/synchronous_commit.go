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
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
)

// synchronousCommitGUC is the GUC whose value the cluster manages on the
// client's behalf. Compared case-insensitively, matching PostgreSQL's
// case-insensitive parameter names.
const synchronousCommitGUC = "synchronous_commit"

// errSynchronousCommitChange is the user-facing rejection for any attempt to
// assign a value to synchronous_commit through the pooler. It is shared with
// the set_config expression path so both surfaces give the same message.
//
// Replication durability is owned by the multipooler rule store /
// SyncStandbyManager (the sole writer of synchronous_commit via ALTER SYSTEM),
// and the HA contract requires synchronous_commit = on so that an acknowledged
// commit is durably flushed on the synchronous standby
// (docs/ha/decision-log/2026-02-12-synchronous-commit-on.md). Letting a session
// override the value silently weakens that guarantee for its writes, which is a
// footgun rather than a load-bearing API contract — so we block it here and
// point users at RESET, which can only restore the managed value.
const errSynchronousCommitChange = "setting synchronous_commit is not supported: " +
	"replication durability is managed by the cluster; " +
	"use RESET synchronous_commit (or SET synchronous_commit TO DEFAULT) to restore the managed value"

// checkSynchronousCommitChange rejects statements that assign a value to
// synchronous_commit at any gateway-reachable level: session/transaction SET
// (including SET LOCAL and SET ... FROM CURRENT), ALTER DATABASE ... SET, and
// ALTER ROLE ... SET. (ALTER SYSTEM is already blocked wholesale as a Tier 2
// statement; set_config(...) is handled in the expression walker.)
//
// Reverts are allowed because they can only restore the cluster-managed value:
// RESET, RESET ALL, and SET ... TO DEFAULT.
//
// Runs pre-dispatch via planUnsupportedConstructs, so it covers both the simple
// and extended query protocols and is short-circuited by the plan cache.
func checkSynchronousCommitChange(stmt ast.Stmt) error {
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

	if isSynchronousCommitValueSet(setstmt) {
		return mterrors.NewFeatureNotSupported(errSynchronousCommitChange)
	}
	return nil
}

// isSynchronousCommitValueSet reports whether vs assigns a value to
// synchronous_commit (as opposed to reverting it to the managed default).
//
// VAR_SET_DEFAULT (SET ... TO DEFAULT), VAR_RESET (RESET), and VAR_RESET_ALL
// (RESET ALL) revert to the global value and are allowed. Everything else —
// VAR_SET_VALUE (SET ... = x / SET LOCAL ... = x) and VAR_SET_CURRENT
// (SET ... FROM CURRENT) — pins an explicit value and is rejected.
func isSynchronousCommitValueSet(vs *ast.VariableSetStmt) bool {
	if vs == nil {
		return false
	}
	if !strings.EqualFold(vs.Name, synchronousCommitGUC) {
		return false
	}
	switch vs.Kind {
	case ast.VAR_SET_DEFAULT, ast.VAR_RESET, ast.VAR_RESET_ALL:
		return false
	default:
		return true
	}
}
