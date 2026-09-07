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

// IsCachedPlanError reports whether err is PostgreSQL's SQLSTATE 0A000
// "cached plan must not change result type" — raised when a cached prepared
// statement's result columns changed (e.g. after DDL) and the statement must be
// re-prepared. Matched on both the SQLSTATE and the (stable) message text so that
// other 0A000 (feature_not_supported) errors are not treated as re-preparable.
func IsCachedPlanError(err error) bool {
	var diag *PgDiagnostic
	if !errors.As(err, &diag) {
		return false
	}
	return diag.Code == PgSSFeatureNotSupported &&
		strings.Contains(diag.Message, "cached plan must not change result type")
}

// IsStalePreparedStatementError reports whether err is one a stale, shared
// backend prepared statement can raise after a schema change, and which
// re-Parsing the statement can recover:
//
//   - 0A000 "cached plan must not change result type" (result columns changed)
//   - 22P02 invalid_text_representation (a bound value no longer decodes against
//     the parameter type the stale plan inferred, e.g. "1" against a dropped-and-
//     recreated uuid→bigint column)
//   - 42883 undefined_function (a stale parameter type makes the operator/function
//     resolution in the cached plan no longer exist, e.g. "uuid = integer")
//
// The reactive heal (cachedPlanRetry) uses this to close+re-Parse+retry once.
// Unlike 0A000, the 22P02/42883 codes also arise from genuine input errors; a
// re-Parse then reproduces the same statement and the retry returns the same
// error, so a genuine error is never masked — only one extra backend round trip
// is spent. This cannot recover inside an already-aborted transaction (the
// retry would run in the failed block); the gateway's force_reparse handles the
// in-transaction path proactively instead.
func IsStalePreparedStatementError(err error) bool {
	if IsCachedPlanError(err) {
		return true
	}
	var diag *PgDiagnostic
	if !errors.As(err, &diag) {
		return false
	}
	return diag.Code == PgSSInvalidTextRepresentation || diag.Code == PgSSUndefinedFunction
}

// IsGatewayRejection reports whether err is a GatewayRejection — a
// feature_not_supported policy rejection that multigres raised itself, before
// the statement reached the backend, rather than a diagnostic forwarded from a
// PostgreSQL backend. Because the backend never saw the statement, its
// transaction is left intact; callers use this to avoid marking the session
// transaction aborted for a rejection the backend never saw, which would desync
// the gateway's transaction status from the still-open backend transaction.
func IsGatewayRejection(err error) bool {
	var g *GatewayRejection
	return errors.As(err, &g)
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
