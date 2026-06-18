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

	"github.com/multigres/multigres/go/common/parser/ast"
)

// extensionsAlteringConnectionDefaults lists extensions whose installation runs
// an ALTER DATABASE/ROLE ... SET internally, so an already-open pooled backend
// would not observe the new per-database/role GUC defaults until it reconnects.
// CREATE EXTENSION of one of these is treated as changing connection defaults
// (see statementChangesConnectionDefaults).
//
// This is a deliberate, narrow allowlist rather than general detection: the
// ALTER runs server-side inside the extension's install script, invisible to
// the gateway, so the only signal is the extension name. Keys are lowercased;
// CREATE EXTENSION resolves names against lowercase control-file names.
//
//   - postgis_topology: its install script calls topology.AddToSearchPath(...),
//     which runs `ALTER DATABASE <db> SET search_path = ..., topology`.
var extensionsAlteringConnectionDefaults = map[string]struct{}{
	"postgis_topology": {},
}

// statementChangesConnectionDefaults reports whether stmt changes per-database
// or per-role session GUC defaults (the pg_db_role_setting catalog) in a way
// that an already-open pooled backend will not observe until it reconnects.
// PostgreSQL applies those defaults only at session start, so the multigateway
// flags such statements (ExecuteOptions.InvalidatesConnectionDefaults) and the
// multipooler refreshes its pooled connections once the change is durable.
//
//   - ALTER DATABASE ... SET/RESET and ALTER ROLE ... SET/RESET: every form
//     mutates pg_db_role_setting (a value pin, or a RESET that drops an entry),
//     so all are flagged. Over-flagging only costs an extra pool refresh.
//   - CREATE EXTENSION <name>: flagged only for the allowlist above.
func statementChangesConnectionDefaults(stmt ast.Stmt) bool {
	switch s := stmt.(type) {
	case *ast.AlterDatabaseSetStmt:
		return true
	case *ast.AlterRoleSetStmt:
		return true
	case *ast.CreateExtensionStmt:
		_, ok := extensionsAlteringConnectionDefaults[strings.ToLower(s.Extname)]
		return ok
	default:
		return false
	}
}
