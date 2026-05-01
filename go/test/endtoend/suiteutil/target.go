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
	"errors"
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
//
// Prefer NewSchemaResetter for batch use-cases (e.g. sqllogictest's per-file
// resets). Each fresh connection that opens, runs `DROP SCHEMA … CASCADE`,
// and disconnects emits a wave of pg_class / pg_namespace invalidation
// messages onto every other live backend. When those messages pile up
// against backends that the proxy then routes a query to, the first query
// on that backend pays for the entire backlog as a catalog-cache rebuild
// — observed as multi-second hangs ("slt_good_125 simple-MG = 30s,
// extended-MG = 32ms") in the sqllogictest suite. SchemaResetter holds one
// pgx connection open for the whole batch so the connection that emits the
// invals is also the one that processes them, keeping the queue bounded.
func ResetPublicSchema(ctx context.Context, t Target) error {
	conn, err := pgx.Connect(ctx, t.DSN())
	if err != nil {
		return fmt.Errorf("connect to %s for schema reset: %w", t.Name, err)
	}
	defer conn.Close(ctx)
	return resetPublicSchemaOnConn(ctx, conn)
}

// resetPublicSchemaOnConn runs the three DROP/CREATE/GRANT statements on an
// already-open connection. Shared between ResetPublicSchema and
// SchemaResetter so the SQL stays in one place.
func resetPublicSchemaOnConn(ctx context.Context, conn *pgx.Conn) error {
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

// SchemaResetter holds a long-lived pgx connection to a target and exposes
// Reset to drop+recreate the public schema on it.
//
// Two design points are load-bearing for the sqllogictest harness:
//
//  1. Connect directly to the underlying primary PostgreSQL when running
//     against Multigres (not through the multigateway). The gateway is
//     itself a connection pooler, so a single gateway connection does not
//     pin resets to one backend — every `DROP SCHEMA … CASCADE` still
//     emits pg_namespace/pg_class invalidation messages to every other
//     pooled backend. Bypassing the pool guarantees same-backend handling
//     of those invals.
//
//  2. Use ResetIfDirty in tight per-file loops and call MarkDirty only
//     after runs that actually executed SQL. Long stretches of the
//     sqllogictest corpus (190+ files in `test/random/select/`) fail in
//     sqllogictest's parser before opening any connection, so the schema
//     is unchanged and a reset between them is wasted DDL. Without this
//     skip, hundreds of redundant DROP/CREATE cycles between the
//     `test/index/**` workload and `slt_good_125` build a multi-second
//     SI backlog on multipooler's pooled backends — large enough in CI to
//     tip the first parseable simple-protocol file past
//     `statement_timeout`.
//
// Not safe for concurrent use; the caller (sqllogictest's outer loop) is
// already serial. On a transient connection error Reset transparently
// reconnects, so a long suite run survives a single dropped backend.
type SchemaResetter struct {
	target Target
	conn   *pgx.Conn
	// dirty is true when the public schema may contain artefacts from a
	// prior run. ResetIfDirty no-ops when false; the caller flips it via
	// MarkDirty after any run that actually executed SQL.
	dirty bool
}

// NewSchemaResetter opens a connection to target and returns a
// SchemaResetter that Close's caller is responsible for. Returns an error
// if the initial connect fails — partial cluster startup races should not
// be silently swallowed here. The resetter is created in a clean state
// (dirty=false): callers wanting an upfront reset can MarkDirty before
// the first ResetIfDirty.
func NewSchemaResetter(ctx context.Context, target Target) (*SchemaResetter, error) {
	conn, err := pgx.Connect(ctx, target.DSN())
	if err != nil {
		return nil, fmt.Errorf("connect to %s for schema resetter: %w", target.Name, err)
	}
	return &SchemaResetter{target: target, conn: conn}, nil
}

// MarkDirty records that the schema may have been mutated since the last
// reset. Idempotent. Callers flip this on after any run that actually
// reached the database — that's the signal ResetIfDirty consults.
func (r *SchemaResetter) MarkDirty() {
	r.dirty = true
}

// ResetIfDirty runs Reset only when MarkDirty was called since the last
// reset. Use this in tight per-file loops to avoid the
// catalog-invalidation pileup described on SchemaResetter.
func (r *SchemaResetter) ResetIfDirty(ctx context.Context) error {
	if !r.dirty {
		return nil
	}
	if err := r.Reset(ctx); err != nil {
		return err
	}
	r.dirty = false
	return nil
}

// Reset runs the DROP/CREATE/GRANT cycle on the held connection. If the
// connection has died, Reset reopens it once before retrying.
func (r *SchemaResetter) Reset(ctx context.Context) error {
	if r.conn == nil || r.conn.IsClosed() {
		if err := r.reconnect(ctx); err != nil {
			return err
		}
	}
	if err := resetPublicSchemaOnConn(ctx, r.conn); err != nil {
		// Distinguish a dead-connection error from a SQL error. pgx returns
		// errors via the underlying conn.Conn().IsClosed() flag after a
		// network-level failure; reconnect once and retry.
		if r.conn.IsClosed() {
			if rerr := r.reconnect(ctx); rerr != nil {
				return errors.Join(fmt.Errorf("reset on stale connection: %w", err), rerr)
			}
			return resetPublicSchemaOnConn(ctx, r.conn)
		}
		return err
	}
	return nil
}

// Close closes the held connection. Safe to call on a nil receiver and
// idempotent. Always defer this from the caller.
func (r *SchemaResetter) Close(ctx context.Context) {
	if r == nil || r.conn == nil {
		return
	}
	_ = r.conn.Close(ctx)
	r.conn = nil
}

func (r *SchemaResetter) reconnect(ctx context.Context) error {
	if r.conn != nil {
		_ = r.conn.Close(ctx)
		r.conn = nil
	}
	conn, err := pgx.Connect(ctx, r.target.DSN())
	if err != nil {
		return fmt.Errorf("reopen %s connection for schema resetter: %w", r.target.Name, err)
	}
	r.conn = conn
	return nil
}
