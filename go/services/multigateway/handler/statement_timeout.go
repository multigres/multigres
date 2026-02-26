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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
)

// ResolveStatementTimeout returns the per-query directive if present, otherwise
// the effective timeout from the connection state (session override or default).
// A nil directive means no directive was found; a non-nil directive (including 0,
// which disables timeouts) takes priority over everything else.
func ResolveStatementTimeout(directive *time.Duration, effective time.Duration) time.Duration {
	if directive != nil {
		return *directive
	}
	return effective
}

// ParseStatementTimeoutDirective is a placeholder for per-query directive parsing.
// Per-query directives (e.g., /*mg+ STATEMENT_TIMEOUT_MS=500 */) will be supported
// in the future by parsing them in the SQL grammar (similar to Vitess).
// For now, this always returns nil (no directive found).
func ParseStatementTimeoutDirective(query ast.Stmt) *time.Duration {
	return nil
}

// ParsePostgresInterval parses a PostgreSQL-style interval value for statement_timeout
// into a time.Duration. Returns PgDiagnostic errors matching PostgreSQL's error format.
// Supports:
//   - Plain integers as milliseconds (e.g., "5000" -> 5s) — PostgreSQL's default unit
//   - Go-compatible duration strings (e.g., "30s", "200ms", "1m")
func ParsePostgresInterval(paramName, value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, invalidParamError(paramName, value, "")
	}

	// Try parsing as plain integer (milliseconds) first — this is the common PG case.
	if ms, err := strconv.ParseInt(value, 10, 64); err == nil {
		if ms < 0 {
			return 0, outOfRangeParamError(paramName, value)
		}
		return time.Duration(ms) * time.Millisecond, nil
	}

	// Try Go duration format (e.g., "30s", "200ms", "1m").
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, invalidParamError(paramName, value,
			`Valid units for this parameter are "us", "ms", "s", "m", "h".`)
	}
	if d < 0 {
		return 0, outOfRangeParamError(paramName, value)
	}
	return d, nil
}

// invalidParamError returns a PgDiagnostic for an invalid parameter value (SQLSTATE 22023).
func invalidParamError(paramName, value, hint string) *mterrors.PgDiagnostic {
	diag := mterrors.NewPgError("ERROR", mterrors.PgSSInvalidParameterValue,
		fmt.Sprintf("invalid value for parameter %q: %q", paramName, value), "")
	diag.Hint = hint
	return diag
}

// outOfRangeParamError returns a PgDiagnostic for an out-of-range parameter value (SQLSTATE 22023).
func outOfRangeParamError(paramName, value string) *mterrors.PgDiagnostic {
	return mterrors.NewPgError("ERROR", mterrors.PgSSInvalidParameterValue,
		fmt.Sprintf("%s is outside the valid range for parameter %q (0 .. 2147483647)", value, paramName), "")
}

// formatDurationAsMs formats a time.Duration as a PostgreSQL-compatible milliseconds string.
// This matches PostgreSQL's display format for statement_timeout.
func formatDurationAsMs(d time.Duration) string {
	return strconv.FormatInt(d.Milliseconds(), 10)
}
