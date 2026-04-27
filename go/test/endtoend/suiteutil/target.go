// Copyright 2026 Supabase, Inc.
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

// Package suiteutil holds helpers shared across the PostgreSQL compatibility
// and benchmarking end-to-end suites (pgregresstest, sqllogictest, pgparity,
// benchmarking). It contains no suite logic — only the connection target type,
// a schema-reset helper, and report-writing utilities that were previously
// duplicated across suites.
package suiteutil

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Target identifies a PostgreSQL-compatible endpoint a test runs against.
// The same struct represents a direct PostgreSQL instance, a Multigres
// multigateway, or an intermediary like pgbouncer — all three show up
// interchangeably across the compatibility suites.
type Target struct {
	Name string // human label used in logs/reports ("postgres", "multigateway", "pgbouncer")
	Host string
	Port int
	User string
	Pass string
	DB   string
}

// DSN returns a libpq-style connection string for this target with
// sslmode=disable. Any extra key=value fragments are appended verbatim.
func (t Target) DSN(extra ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		t.Host, t.Port, t.User, t.Pass, t.DB)
	for _, e := range extra {
		if e == "" {
			continue
		}
		b.WriteByte(' ')
		b.WriteString(e)
	}
	return b.String()
}

// ResetPublicSchema drops and recreates the public schema, restoring the
// default grants. Suites call this between corpus files so each file starts
// from an empty database.
//
// The reset is always applied through the supplied target connection; in
// parity/divergence suites, callers pass the direct-PostgreSQL target here
// (not the proxy) so the cleanup itself cannot be affected by the bug under
// test.
func ResetPublicSchema(ctx context.Context, t Target) error {
	conn, err := pgx.Connect(ctx, t.DSN())
	if err != nil {
		return fmt.Errorf("connect to %s for schema reset: %w", t.Name, err)
	}
	defer conn.Close(ctx)

	stmts := []string{
		`DROP SCHEMA IF EXISTS public CASCADE`,
		`CREATE SCHEMA public`,
		`GRANT ALL ON SCHEMA public TO public`,
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("exec %q: %w", s, err)
		}
	}
	return nil
}
