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

	"github.com/multigres/multigres/go/common/parser/ast"
)

// ResolveStatementTimeout determines the effective statement timeout using strict priority ordering.
// The highest-priority source that is set wins entirely (no minimum across sources).
//
// Priority (highest to lowest):
//  1. Per-query directive — placeholder, always nil for now (will be parsed in grammar)
//  2. Session variable (SET statement_timeout = ...)
//  3. --statement-timeout flag
//
// A value of 0 from any source means "no timeout" (disabled).
func ResolveStatementTimeout(directive *time.Duration, sessionVar *time.Duration, flag time.Duration) time.Duration {
	if directive != nil {
		return *directive
	}
	if sessionVar != nil {
		return *sessionVar
	}
	return flag
}

// ParseStatementTimeoutDirective is a placeholder for per-query directive parsing.
// Per-query directives (e.g., /*mg+ STATEMENT_TIMEOUT_MS=500 */) will be supported
// in the future by parsing them in the SQL grammar (similar to Vitess).
// For now, this always returns nil (no directive found).
func ParseStatementTimeoutDirective(query ast.Stmt) *time.Duration {
	return nil
}

// ParsePostgresInterval parses a PostgreSQL-style interval value into a time.Duration.
// Supports:
//   - Plain integers as milliseconds (e.g., "5000" → 5s) — PostgreSQL's default unit for statement_timeout
//   - Go-compatible duration strings (e.g., "30s", "200ms", "1m")
func ParsePostgresInterval(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty interval value")
	}

	// Try parsing as plain integer (milliseconds) first — this is the common PG case.
	if ms, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.Duration(ms) * time.Millisecond, nil
	}

	// Try Go duration format (e.g., "30s", "200ms", "1m").
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid interval %q: must be an integer (ms) or Go duration string", value)
	}
	return d, nil
}
