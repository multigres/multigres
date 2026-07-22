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
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/constants"
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
// Supports PostgreSQL's time-valued GUC syntax:
//   - Plain integers as milliseconds (e.g., "5000" -> 5s) — PostgreSQL's default unit
//   - The documented units "us", "ms", "s", "min", "h", and "d", with optional
//     whitespace between number and unit (e.g., "30s", "1 min", "2d")
func ParsePostgresInterval(paramName, value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, invalidParamError(paramName, value, "")
	}

	// Try parsing as plain integer (milliseconds) first — this is the common PG case.
	if ms, err := strconv.ParseInt(value, 10, 64); err == nil {
		if ms < 0 || ms > constants.MaxStatementTimeoutMS {
			return 0, outOfRangeParamError(paramName, ms)
		}
		return time.Duration(ms) * time.Millisecond, nil
	}

	d, ok := parsePostgresIntervalWithUnit(value)
	if !ok {
		return 0, invalidParamError(paramName, value,
			`Valid units for this parameter are "us", "ms", "s", "min", "h", and "d".`)
	}
	// PostgreSQL stores statement_timeout as whole milliseconds, so round to the
	// base unit (rather than truncate) before range-checking and reporting. This
	// keeps the reported value honest for sub-millisecond inputs: e.g. "-600us"
	// reports "-1 ms ..." instead of a misleading "0 ms ...", and "-400us" rounds
	// to 0 (no timeout), matching PostgreSQL.
	ms := int64(math.Round(float64(d) / float64(time.Millisecond)))
	if ms < 0 || ms > constants.MaxStatementTimeoutMS {
		return 0, outOfRangeParamError(paramName, ms)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func parsePostgresIntervalWithUnit(value string) (time.Duration, bool) {
	fields := strings.Fields(value)
	var number, unit string
	switch len(fields) {
	case 1:
		idx := -1
		for i, r := range fields[0] {
			if (r < '0' || r > '9') && r != '.' && r != '+' && r != '-' {
				idx = i
				break
			}
		}
		if idx <= 0 {
			return 0, false
		}
		number = fields[0][:idx]
		unit = fields[0][idx:]
	case 2:
		number = fields[0]
		unit = fields[1]
	default:
		return 0, false
	}

	amount, err := strconv.ParseFloat(number, 64)
	if err != nil {
		return 0, false
	}

	var multiplier time.Duration
	switch unit {
	case "us":
		multiplier = time.Microsecond
	case "ms":
		multiplier = time.Millisecond
	case "s":
		multiplier = time.Second
	case "min":
		multiplier = time.Minute
	case "h":
		multiplier = time.Hour
	case "d":
		multiplier = 24 * time.Hour
	default:
		return 0, false
	}

	return time.Duration(amount * float64(multiplier)), true
}

// invalidParamError returns a PgDiagnostic for an invalid parameter value (SQLSTATE 22023).
func invalidParamError(paramName, value, hint string) *mterrors.PgDiagnostic {
	diag := mterrors.NewPgError("ERROR", mterrors.PgSSInvalidParameterValue,
		fmt.Sprintf("invalid value for parameter %q: %q", paramName, value), "")
	diag.Hint = hint
	return diag
}

// outOfRangeParamError returns a PgDiagnostic for an out-of-range statement_timeout
// value (SQLSTATE 22023). ms is the value in the base unit (milliseconds). The
// message matches PostgreSQL: "ms" is appended to the value, but range bounds
// are bare integers, e.g.
//
//	-1 ms is outside the valid range for parameter "statement_timeout" (0 .. 2147483647)
func outOfRangeParamError(paramName string, ms int64) *mterrors.PgDiagnostic {
	return mterrors.NewPgError("ERROR", mterrors.PgSSInvalidParameterValue,
		fmt.Sprintf("%d ms is outside the valid range for parameter %q (0 .. %d)",
			ms, paramName, constants.MaxStatementTimeoutMS), "")
}

// formatDurationPg formats a time.Duration using PostgreSQL's GUC_UNIT_MS display
// convention. PostgreSQL picks the largest unit that divides evenly into the value:
//
//	0        → "0"
//	500ms    → "500ms"
//	5s       → "5s"
//	90s      → "1min 30s"  (but we use "90s" — PG only splits at clean boundaries)
//	60s      → "1min"
//	3600s    → "1h"
//
// Values that don't divide evenly into the next-larger unit stay in the smaller unit
// (e.g., 1500ms → "1500ms", not "1.5s").
func formatDurationPg(d time.Duration) string {
	ms := d.Milliseconds()
	if ms == 0 {
		return "0"
	}

	switch {
	case ms%(3600*1000) == 0:
		return strconv.FormatInt(ms/(3600*1000), 10) + "h"
	case ms%(60*1000) == 0:
		return strconv.FormatInt(ms/(60*1000), 10) + "min"
	case ms%1000 == 0:
		return strconv.FormatInt(ms/1000, 10) + "s"
	default:
		return strconv.FormatInt(ms, 10) + "ms"
	}
}
