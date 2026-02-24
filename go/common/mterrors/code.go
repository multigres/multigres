// Copyright 2022 The Vitess Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Modifications Copyright 2025 Supabase, Inc.

package mterrors

import (
	"errors"
	"fmt"
)

// MTError defines a Multigres error code. Each instance is a template that
// produces a *PgDiagnostic via its New method. The ID (e.g. "MT10001") is
// used as the SQLSTATE code in the PgDiagnostic, making MT errors directly
// identifiable by clients through the standard SQLSTATE field.
type MTError struct {
	ID          string // e.g. "MT10001" — used as the SQLSTATE code
	Description string // long description for docs
	Severity    string
	Format      string // fmt format string for message
}

// New builds a *PgDiagnostic from this error definition.
// The MT ID is placed in the SQLSTATE Code field.
// If args are provided, the Format string is passed through fmt.Sprintf.
func (e *MTError) New(args ...any) *PgDiagnostic {
	msg := e.Format
	if len(args) != 0 {
		msg = fmt.Sprintf(e.Format, args...)
	}
	return &PgDiagnostic{
		MessageType: 'E',
		Severity:    e.Severity,
		Code:        e.ID,
		Message:     msg,
	}
}

var (
	MT09001 = &MTError{
		ID: "MT09001", Severity: "ERROR",
		Format:      "SET TRANSACTION ISOLATION LEVEL must be called before any query",
		Description: "Transaction isolation cannot be changed after queries executed.",
	}

	MT10001 = &MTError{
		ID: "MT10001", Severity: "ERROR",
		Format:      "current transaction is aborted, commands ignored until end of transaction block",
		Description: "Issue ROLLBACK to start a new transaction.",
	}

	MT13001 = &MTError{
		ID: "MT13001", Severity: "ERROR",
		Format:      "[BUG] %s",
		Description: "This error should not happen and is a bug. Please file an issue on GitHub: https://github.com/multigres/multigres/issues/new/choose.",
	}

	MT13002 = &MTError{
		ID: "MT13002", Severity: "ERROR",
		Format:      "pooler type mismatch: topology says %s but PostgreSQL is %s",
		Description: "The pooler type in the topology does not match the actual PostgreSQL role. This indicates the pooler is in an inconsistent state and requires intervention.",
	}

	MT13003 = &MTError{
		ID: "MT13003", Severity: "ERROR",
		Format:      "%s",
		Description: "Internal proxy error.",
	}

	MT13004 = &MTError{
		ID: "MT13004", Severity: "ERROR",
		Format:      "parse failed: %s",
		Description: "Extended query Parse could not be processed.",
	}

	MT13005 = &MTError{
		ID: "MT13005", Severity: "ERROR",
		Format:      "bind failed: %s",
		Description: "Extended query Bind could not be processed.",
	}

	MT13006 = &MTError{
		ID: "MT13006", Severity: "ERROR",
		Format:      "describe failed: %s",
		Description: "Extended query Describe could not be processed.",
	}

	MT13007 = &MTError{
		ID: "MT13007", Severity: "ERROR",
		Format:      "close failed: %s",
		Description: "Extended query Close could not be processed.",
	}

	MT13008 = &MTError{
		ID: "MT13008", Severity: "ERROR",
		Format:      "sync failed: %s",
		Description: "Extended query Sync could not be processed.",
	}

	MT14001 = &MTError{
		ID: "MT14001", Severity: "FATAL",
		Format:      "connection startup failed: %s",
		Description: "Invalid startup message.",
	}
)

// IsError checks whether err (or a wrapped cause) is an MT error matching code.
// For *PgDiagnostic errors it compares the SQLSTATE Code field directly;
// otherwise it falls back to substring matching on the error string.
func IsError(err error, code string) bool {
	if err == nil {
		return false
	}
	var diag *PgDiagnostic
	if errors.As(err, &diag) {
		return diag.Code == code
	}
	return false
}
