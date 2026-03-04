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

package mterrors

import (
	"errors"
	"strings"
)

// ExtractSQLSTATE unwraps the error chain looking for a *PgDiagnostic
// and returns its SQLSTATE Code. Returns "" for nil errors and "XX000"
// (internal_error) for non-PgDiagnostic errors.
func ExtractSQLSTATE(err error) string {
	if err == nil {
		return ""
	}
	var diag *PgDiagnostic
	if errors.As(err, &diag) {
		return diag.Code
	}
	return PgSSInternalError // "XX000"
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
	if IsConnectionError(err) {
		return "routing"
	}

	var diag *PgDiagnostic
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
