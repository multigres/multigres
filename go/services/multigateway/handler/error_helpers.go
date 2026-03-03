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

package handler

import (
	"errors"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
)

// ExtractSQLSTATE unwraps the error chain looking for a *mterrors.PgDiagnostic
// and returns its SQLSTATE Code. Returns "" for nil errors and "XX000"
// (internal_error) for non-PgDiagnostic errors.
func ExtractSQLSTATE(err error) string {
	if err == nil {
		return ""
	}
	var diag *mterrors.PgDiagnostic
	if errors.As(err, &diag) {
		return diag.Code
	}
	return mterrors.PgSSInternalError // "XX000"
}

// ClassifyErrorSource categorises the origin of an error for metric attribution.
// Returns one of:
//   - "backend"  — real PostgreSQL SQLSTATE (not MT-prefixed)
//   - "internal" — MT-prefixed error codes (e.g. MTD01)
//   - "routing"  — connection-level failures (EOF, ECONNRESET, Class 08, …)
//   - "client"   — fallback for unclassified errors
func ClassifyErrorSource(err error) string {
	if err == nil {
		return ""
	}

	// Connection errors (I/O failures, Class 08, shutdown codes) → routing.
	if mterrors.IsConnectionError(err) {
		return "routing"
	}

	var diag *mterrors.PgDiagnostic
	if errors.As(err, &diag) {
		// MT-prefixed codes (e.g. "MTD01", "MTE01") → internal.
		if strings.HasPrefix(diag.Code, "MT") {
			return "internal"
		}
		// Real PostgreSQL SQLSTATE → backend.
		return "backend"
	}

	return "client"
}

// ExtractOperationName returns the SQL statement type (e.g. "SELECT", "INSERT")
// from an AST node. Returns "UNKNOWN" if stmt is nil.
func ExtractOperationName(stmt ast.Stmt) string {
	if stmt == nil {
		return "UNKNOWN"
	}
	return stmt.StatementType()
}
