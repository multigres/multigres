// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"context"
	"fmt"
	"strings"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// EXPERIMENTAL: table-scoped DDL replication over the same logical-replication
// stream as the data.
//
// The mechanism (see the design note in docs/migration for the full rationale):
//
//   - Publisher side: a shared multigres.ddl_log table plus a single
//     ddl_command_end event trigger (multigres.capture_ddl) that appends the
//     executing statement to ddl_log. Because the INSERT commits in the SAME
//     transaction as the DDL, the log row and the DDL are atomic. capture_ddl
//     fans out by TABLE MEMBERSHIP: it joins each command's target table against
//     multigres.ddl_capture_tables (schema_name, table_name -> migration_id) and
//     inserts one ddl_log row per matching migration, tagged with migration_id.
//     A table shared by two migrations rides both; a table in neither rides
//     none. The single event trigger is REFCOUNTED via ddl_capture_tables:
//     tearing down one migration only drops the trigger once no migration's
//     tables remain registered.
//   - Each migration's publication adds multigres.ddl_log with a ROW FILTER
//     (WHERE migration_id = '<id>', a PG15+ publication row filter), so only that
//     migration's DDL rows flow on its own subscription — the streams stay
//     separate even though the log is shared.
//   - Subscriber side: a matching multigres.ddl_log plus an AFTER INSERT trigger
//     (multigres.apply_ddl) that EXECUTEs each arriving row's statement. The
//     trigger is ENABLE ALWAYS because triggers do NOT fire for
//     replication-applied changes by default. It runs inside the apply
//     transaction, so the replayed DDL lands at the right position in the stream.
//     apply_ddl is GUARDED by multigres.ddl_apply (a per-migration refcount of
//     the migrations this server subscribes to): it replays a row only when the
//     row's migration is registered there. This is what makes a single server
//     that is BOTH a publisher (EXPORT) and a subscriber (IMPORT) correct — the
//     ENABLE ALWAYS apply trigger also fires on the publisher's own locally
//     captured rows, and the guard skips them so they are not double-executed
//     locally (they only exist to be replicated OUT).
//
// Direction symmetry (IMPORT vs EXPORT): capture runs on whichever side is the
// current PUBLISHER (the external source in IMPORT; the Multigres target in
// EXPORT) and apply on the current SUBSCRIBER. The setup/teardown helpers here
// take a ddlConn, implemented by both source (external, pgx) and target (local
// admin pool), so the coordinator installs capture/apply on the correct side for
// the active direction and reconfigures them across a set-migration-direction
// switch.
//
// The machinery lives in the multigres sidecar schema (already present on the
// target, created on the source if absent). Teardown removes a migration's own
// rows and, when it is the last one, the shared objects; it never drops the
// schema itself — on the target it also holds multigres.migration and the other
// shard metadata.
//
// Limitations (this is a proof of concept):
//   - Creating the event trigger requires a SUPERUSER DSN on the publisher side.
//   - Only CREATE TABLE and ALTER TABLE are captured (see capture_ddl). Index
//     DDL (CREATE INDEX, incl. CONCURRENTLY) and DROP statements are not
//     replicated, so those changes drift on the subscriber. DROP is not captured
//     because it reports nothing via pg_event_trigger_ddl_commands() on
//     ddl_command_end.
//   - CREATE TABLE of a brand-new table not in any migration's table set is NOT
//     replicated: it has no ddl_capture_tables membership, so capture_ddl skips
//     it. Opting a new table into a migration is out of scope (it would need an
//     explicit membership insert plus adding the table to the publication).
//   - current_query() stores the whole submitted statement, so a multi-statement
//     batch that touches a migrated table replays the entire batch on the
//     subscriber (potentially over-applying the other statements in the batch).
//     Keep source DDL during a migration simple and scoped to migrated tables.
//   - Captured statements are replayed under the captured schema's search_path
//     and the replay is exception-guarded: a statement that still cannot be
//     applied is skipped with a WARNING rather than stalling the subscription.

// ddlLogTable is the fully-qualified name added to each migration's publication.
const ddlLogTable = "multigres.ddl_log"

// The shared DDL-replication objects. All are idempotent (IF NOT EXISTS /
// CREATE OR REPLACE) so setup for the second and later migrations on a server is
// a no-op on the shared pieces and only adds that migration's own rows.
const (
	// ddlSchemaSQL is only needed on an external source: it has no multigres
	// sidecar schema. On the target the schema already exists (createSidecarSchema
	// at bootstrap), so setup there only creates tables.
	ddlSchemaSQL = `CREATE SCHEMA IF NOT EXISTS multigres`

	// ddlLogTableSQL is the shared, append-only wire table, identical on both
	// sides (logical replication maps by name, so the shape must match). Each row
	// is tagged with migration_id; the PK is (migration_id, log_id) so log_ids
	// generated independently by two different publishers cannot collide when they
	// replicate into one subscriber's shared ddl_log. log_id (rather than a generic
	// id) is a distinctive name so it can be joined with USING. It is BY DEFAULT so
	// the apply worker inserts the publisher's value without an identity conflict.
	ddlLogTableSQL = `CREATE TABLE IF NOT EXISTS multigres.ddl_log (
	migration_id text NOT NULL,
	log_id bigint GENERATED BY DEFAULT AS IDENTITY,
	schema_name text NOT NULL,
	table_name text NOT NULL,
	ddl_command text NOT NULL,
	PRIMARY KEY (migration_id, log_id)
)`

	// ddlCaptureTablesSQL is the publisher-side membership: which tables belong
	// to which currently-publishing migration. capture_ddl joins against it to
	// fan a DDL out to every owning migration. It doubles as the refcount for
	// the shared event trigger (rows remain => keep capturing).
	//
	// A publisher-local membership table is used rather than reading
	// multigres.migration_tables directly because
	//
	// a. an external source has no migration_tables sidecar at all
	//
	// b. migration_tables on the Multigres side lists ALL migrations, including
	//    ones where this side is the SUBSCRIBER (IMPORT), whereas capture must
	//    fire only for migrations currently publishing FROM this side.
	//
	// It is populated from the migration's resolved table set — the same data
	// as migration_tables — at capture setup.
	ddlCaptureTablesSQL = `
CREATE TABLE IF NOT EXISTS multigres.ddl_capture_tables (
	migration_id text NOT NULL,
	schema_name text NOT NULL,
	table_name text NOT NULL,
	PRIMARY KEY (migration_id, schema_name, table_name)
)`

	// ddlApplyRefsSQL is the subscriber-side refcount: the migrations for which
	// THIS server is the subscriber. apply_ddl consults it (see applyFunctionSQL)
	// so it replays only rows belonging to a migration this server subscribes to,
	// never a locally captured row destined for outbound replication.
	ddlApplyRefsSQL = `
CREATE TABLE IF NOT EXISTS multigres.ddl_apply (
	migration_id text PRIMARY KEY
)`

	// captureFunctionSQL captures top-level table-modification DDL, scoped to the
	// migrated tables via the ddl_capture_tables join. The allowlist is
	// deliberately narrow — CREATE TABLE and ALTER TABLE — which excludes
	// CREATE INDEX (including CONCURRENTLY, same tag, not replayable inside the
	// apply transaction) and every other DDL kind. A CREATE TABLE with an inline
	// PRIMARY KEY still matches. DROP statements report nothing via
	// pg_event_trigger_ddl_commands() on ddl_command_end, so they are not
	// captured. The one non-transactional statement carrying an allowlisted tag,
	// ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY, is excluded explicitly
	// (matched on the query text, since the event-trigger metadata does not
	// distinguish it) — it cannot run inside the apply worker's transaction. It
	// must be LANGUAGE plpgsql: a SQL function cannot return the event_trigger
	// pseudo-type.
	captureFunctionSQL = `
CREATE OR REPLACE FUNCTION multigres.capture_ddl()
	RETURNS event_trigger LANGUAGE plpgsql AS $fn$
BEGIN
	INSERT INTO multigres.ddl_log (migration_id, schema_name, table_name, ddl_command)
	SELECT m.migration_id, c.schema_name, cl.relname, current_query()
	FROM pg_event_trigger_ddl_commands() c
	JOIN pg_class cl ON cl.oid = c.objid
	JOIN multigres.ddl_capture_tables m
	  ON m.schema_name = c.schema_name AND m.table_name = cl.relname
	WHERE c.command_tag IN ('CREATE TABLE', 'ALTER TABLE')
	  AND NOT (c.command_tag = 'ALTER TABLE'
	           AND current_query() ~* '\ydetach\s+partition\y'
	           AND current_query() ~* '\yconcurrently\y');
END;
$fn$`

	dropCaptureTriggerSQL   = `DROP EVENT TRIGGER IF EXISTS mg_capture_ddl`
	createCaptureTriggerSQL = `CREATE EVENT TRIGGER mg_capture_ddl
	ON ddl_command_end EXECUTE FUNCTION multigres.capture_ddl()`
	// captureTriggerExistsSQL is the refcount-independent existence check used to
	// arm the shared trigger only once (CREATE EVENT TRIGGER has no IF NOT EXISTS,
	// and blindly DROP+CREATE would momentarily disarm capture for other active
	// migrations, losing any DDL in that window).
	captureTriggerExistsSQL = `SELECT count(*) FROM pg_event_trigger WHERE evtname = 'mg_capture_ddl'`

	// applyFunctionSQL replays each applied row's statement. Like capture_ddl it
	// must be LANGUAGE plpgsql. Three safeguards:
	//   1. guard — replay only rows whose migration is registered in
	//      multigres.ddl_apply (this server is that migration's subscriber). The
	//      ENABLE ALWAYS trigger also fires for a publisher's own locally captured
	//      rows on a dual-role server (EXPORT publisher + IMPORT subscriber); those
	//      migrations are NOT in ddl_apply here, so they are skipped and never
	//      double-executed locally.
	//   2. search_path — the apply worker runs with an empty search_path, so a
	//      captured statement with unqualified names would fail to resolve. We SET
	//      LOCAL search_path to the captured schema before replaying.
	//   3. exception guard — a statement that still cannot be replayed must NOT
	//      wedge the apply worker (logical replication retries forever, stalling
	//      ALL replication). We skip it with a WARNING so the stream keeps
	//      advancing; that one change then drifts on the subscriber.
	applyFunctionSQL = `
CREATE OR REPLACE FUNCTION multigres.apply_ddl()
RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
	IF EXISTS (SELECT 1 FROM multigres.ddl_apply a WHERE a.migration_id = NEW.migration_id) THEN
	    PERFORM set_config('search_path', quote_ident(NEW.schema_name) || ', pg_catalog, pg_temp', true);
	    BEGIN
		    EXECUTE NEW.ddl_command;
	    EXCEPTION WHEN OTHERS THEN
		    RAISE WARNING 'multigres.apply_ddl skipped ddl_log migration=% log_id=% (%): %', NEW.migration_id, NEW.log_id, NEW.ddl_command, SQLERRM;
	    END;
	END IF;
	RETURN NEW;
END;
$fn$`

	dropApplyTriggerSQL   = `DROP TRIGGER IF EXISTS mg_apply_ddl ON multigres.ddl_log`
	createApplyTriggerSQL = `
CREATE TRIGGER mg_apply_ddl
AFTER INSERT ON multigres.ddl_log
FOR EACH ROW EXECUTE FUNCTION multigres.apply_ddl()
`
	// ENABLE ALWAYS so the trigger fires for replication-applied INSERTs.
	enableAlwaysApplyTriggerSQL = `ALTER TABLE multigres.ddl_log ENABLE ALWAYS TRIGGER mg_apply_ddl`
	// applyTriggerExistsSQL checks whether the shared apply trigger is already
	// installed, so setting up a second concurrent migration does not drop and
	// recreate it (which would briefly disarm apply on the shared ddl_log for a
	// migration that is already streaming). ddl_log always exists here — setup
	// creates it (IF NOT EXISTS) before this check.
	applyTriggerExistsSQL = `SELECT count(*) FROM pg_trigger WHERE tgname = 'mg_apply_ddl' AND tgrelid = 'multigres.ddl_log'::regclass`

	// Teardown of the shared objects (only when the last migration on a side goes).
	dropCaptureFunctionSQL = `DROP FUNCTION IF EXISTS multigres.capture_ddl()`
	dropApplyFunctionSQL   = `DROP FUNCTION IF EXISTS multigres.apply_ddl()`
	dropDDLLogTableSQL     = `DROP TABLE IF EXISTS multigres.ddl_log`
	dropCaptureTablesSQL   = `DROP TABLE IF EXISTS multigres.ddl_capture_tables`
	dropApplyRefsSQL       = `DROP TABLE IF EXISTS multigres.ddl_apply`
)

// ddlConn is the minimal exec/query surface the DDL-replication setup needs. It
// is implemented by both the external source (pgx) and the local Multigres
// target (admin pool), so capture and apply can be installed on whichever side
// is the publisher/subscriber for a migration's active direction.
type ddlConn interface {
	exec(ctx context.Context, sql string) error
	execArgs(ctx context.Context, sql string, args ...any) error
	queryCount(ctx context.Context, sql string, args ...any) (int64, error)
}

// sourceDDL adapts a source's pgx connection to ddlConn.
type sourceDDL struct{ s *source }

func (a sourceDDL) exec(ctx context.Context, sql string) error {
	_, err := a.s.conn.Exec(ctx, sql)
	return err
}

func (a sourceDDL) execArgs(ctx context.Context, sql string, args ...any) error {
	_, err := a.s.conn.Exec(ctx, sql, args...)
	return err
}

func (a sourceDDL) queryCount(ctx context.Context, sql string, args ...any) (int64, error) {
	var n int64
	if err := a.s.conn.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// targetDDL adapts the local admin query service to ddlConn.
type targetDDL struct{ qs executor.InternalQueryService }

func (a targetDDL) exec(ctx context.Context, sql string) error {
	_, err := a.qs.QueryAdmin(ctx, sql)
	return err
}

func (a targetDDL) execArgs(ctx context.Context, sql string, args ...any) error {
	_, err := a.qs.QueryAdminArgs(ctx, sql, args...)
	return err
}

func (a targetDDL) queryCount(ctx context.Context, sql string, args ...any) (int64, error) {
	res, err := a.qs.QueryAdminArgs(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	var n int64
	if err := executor.ScanSingleRow(res, &n); err != nil {
		return 0, err
	}
	return n, nil
}

// ddlCapture builds the ddlConn for the current publisher/subscriber side.
func (s *source) ddlConn() ddlConn { return sourceDDL{s} }
func (t *target) ddlConn() ddlConn { return targetDDL{t.qs} }

// setupDDLCapture creates the shared publisher-side objects (idempotent) and
// registers this migration's tables in ddl_capture_tables so capture_ddl fans
// its DDL out to it. It does NOT arm the event trigger — the caller arms it with
// armDDLCapture last (just before CreateSubscription), so little DDL accumulates
// in ddl_log before the subscription's initial snapshot. (CREATE PUBLICATION is
// never captured regardless of arming order — its command tag is not in the
// allowlist.) tables are the migration's resolved "schema.table" names.
func setupDDLCapture(ctx context.Context, c ddlConn, migrationID string, tables []string) error {
	for _, stmt := range []string{ddlSchemaSQL, ddlLogTableSQL, ddlCaptureTablesSQL, captureFunctionSQL} {
		if err := c.exec(ctx, stmt); err != nil {
			return fmt.Errorf("set up ddl capture: %w", err)
		}
	}
	for _, qualified := range tables {
		schema, table, err := splitQualified(qualified)
		if err != nil {
			return err
		}
		if err := c.execArgs(ctx,
			`INSERT INTO multigres.ddl_capture_tables (migration_id, schema_name, table_name)
			 VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
			migrationID, schema, table); err != nil {
			return fmt.Errorf("register ddl capture table %q: %w", qualified, err)
		}
	}
	return nil
}

// armDDLCapture creates the shared ddl_command_end event trigger if it does not
// already exist. It is refcount-safe: for the second and later concurrent
// migrations the trigger already exists, so this is a no-op and capture is never
// disarmed for the migrations already streaming.
func armDDLCapture(ctx context.Context, c ddlConn) error {
	n, err := c.queryCount(ctx, captureTriggerExistsSQL)
	if err != nil {
		return fmt.Errorf("check ddl capture trigger: %w", err)
	}
	if n > 0 {
		return nil
	}
	if err := c.exec(ctx, createCaptureTriggerSQL); err != nil {
		return fmt.Errorf("arm ddl capture trigger: %w", err)
	}
	return nil
}

// teardownDDLCapture removes this migration's capture registration and its
// ddl_log rows. The shared event trigger is refcounted through
// ddl_capture_tables: it (and the rest of the shared publisher objects) is
// dropped only once no migration's tables remain registered. Idempotent;
// best-effort at teardown.
func teardownDDLCapture(ctx context.Context, c ddlConn, migrationID string) error {
	if err := c.execArgs(ctx, `DELETE FROM multigres.ddl_capture_tables WHERE migration_id = $1`, migrationID); err != nil {
		return fmt.Errorf("deregister ddl capture tables: %w", err)
	}
	if err := c.execArgs(ctx, `DELETE FROM multigres.ddl_log WHERE migration_id = $1`, migrationID); err != nil {
		return fmt.Errorf("clear ddl_log rows: %w", err)
	}
	remaining, err := c.queryCount(ctx, `SELECT count(*) FROM multigres.ddl_capture_tables`)
	if err != nil {
		return fmt.Errorf("count ddl capture tables: %w", err)
	}
	if remaining == 0 {
		// Last publishing migration on this side: drop the shared capture objects.
		// ddl_log itself is left in place — a dual-role server may still use it for
		// its subscriber (apply) role; teardownDDLApply drops it when unused.
		for _, stmt := range []string{dropCaptureTriggerSQL, dropCaptureFunctionSQL, dropCaptureTablesSQL} {
			if err := c.exec(ctx, stmt); err != nil {
				return fmt.Errorf("drop shared ddl capture objects: %w", err)
			}
		}
	}
	return nil
}

// setupDDLApply creates the shared subscriber-side objects (idempotent) and
// registers this migration in ddl_apply so apply_ddl replays its rows. Must run
// before the subscription is created so the subscription can sync ddl_log.
func setupDDLApply(ctx context.Context, c ddlConn, migrationID string) error {
	// The shared objects (ddl_log, ddl_apply, apply function) are idempotent. The
	// apply function is CREATE OR REPLACE (atomic, so it never gaps trigger firing
	// for a migration already applying on the shared ddl_log).
	for _, stmt := range []string{ddlSchemaSQL, ddlLogTableSQL, ddlApplyRefsSQL, applyFunctionSQL} {
		if err := c.exec(ctx, stmt); err != nil {
			return fmt.Errorf("set up ddl apply: %w", err)
		}
	}
	// Create the apply trigger only if absent (refcount-safe): recreating it for a
	// second concurrent migration would briefly disarm apply on the shared ddl_log
	// for the migration already streaming.
	n, err := c.queryCount(ctx, applyTriggerExistsSQL)
	if err != nil {
		return fmt.Errorf("check ddl apply trigger: %w", err)
	}
	if n == 0 {
		for _, stmt := range []string{createApplyTriggerSQL, enableAlwaysApplyTriggerSQL} {
			if err := c.exec(ctx, stmt); err != nil {
				return fmt.Errorf("set up ddl apply trigger: %w", err)
			}
		}
	}
	if err := c.execArgs(ctx,
		`INSERT INTO multigres.ddl_apply (migration_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		migrationID); err != nil {
		return fmt.Errorf("register ddl apply: %w", err)
	}
	return nil
}

// teardownDDLApply removes this migration's apply registration. The shared apply
// objects (and ddl_log) are dropped only once no migration subscribes here AND
// no migration publishes here (ddl_log is shared across both roles on a
// dual-role server). Idempotent; best-effort at teardown.
func teardownDDLApply(ctx context.Context, c ddlConn, migrationID string) error {
	if err := c.execArgs(ctx, `DELETE FROM multigres.ddl_apply WHERE migration_id = $1`, migrationID); err != nil {
		return fmt.Errorf("deregister ddl apply: %w", err)
	}
	if err := c.execArgs(ctx, `DELETE FROM multigres.ddl_log WHERE migration_id = $1`, migrationID); err != nil {
		return fmt.Errorf("clear ddl_log rows: %w", err)
	}
	applyRefs, err := c.queryCount(ctx, `SELECT count(*) FROM multigres.ddl_apply`)
	if err != nil {
		return fmt.Errorf("count ddl apply refs: %w", err)
	}
	if applyRefs > 0 {
		return nil
	}
	// No subscriber migrations left here. Drop the apply refcount table and
	// function. ddl_log is only safe to drop if this server is not also publishing
	// (ddl_capture_tables absent/empty); otherwise keep ddl_log and just remove
	// the apply trigger from it.
	if err := c.exec(ctx, dropApplyRefsSQL); err != nil {
		return fmt.Errorf("drop ddl apply refs: %w", err)
	}
	// A subscribe-only server never created ddl_capture_tables; check existence
	// before counting so a missing table reads as "not publishing" without
	// swallowing a real error.
	var capturing int64
	hasCaptureTable, err := c.queryCount(ctx,
		`SELECT count(*) FROM pg_class WHERE relname = 'ddl_capture_tables' AND relnamespace = 'multigres'::regnamespace`)
	if err != nil {
		return fmt.Errorf("check ddl_capture_tables: %w", err)
	}
	if hasCaptureTable > 0 {
		if capturing, err = c.queryCount(ctx, `SELECT count(*) FROM multigres.ddl_capture_tables`); err != nil {
			return fmt.Errorf("count ddl capture tables: %w", err)
		}
	}
	if capturing == 0 {
		if err := c.exec(ctx, dropDDLLogTableSQL); err != nil {
			return fmt.Errorf("drop ddl_log: %w", err)
		}
	} else if err := c.exec(ctx, dropApplyTriggerSQL); err != nil {
		return fmt.Errorf("drop ddl apply trigger: %w", err)
	}
	if err := c.exec(ctx, dropApplyFunctionSQL); err != nil {
		return fmt.Errorf("drop ddl apply function: %w", err)
	}
	return nil
}

// createPublicationWithDDLLogSQL builds a CREATE PUBLICATION ... FOR TABLE
// statement that publishes the migrated tables (all rows) plus multigres.ddl_log
// with a row filter restricting it to this migration's captured DDL. The row
// filter is a PG15+ publication feature; Multigres targets PG17+.
func createPublicationWithDDLLogSQL(name string, tables []string, migrationID string) string {
	quoted := make([]string, 0, len(tables)+1)
	for _, tbl := range tables {
		quoted = append(quoted, quoteQualifiedName(tbl))
	}
	quoted = append(quoted, ddlLogTable+" WHERE (migration_id = "+ast.QuoteStringLiteral(migrationID)+")")
	return fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
		ast.QuoteIdentifier(name), strings.Join(quoted, ", "))
}

// splitQualified splits a canonical "schema.table" on its single dot.
func splitQualified(qualified string) (schema, table string, err error) {
	schema, table, ok := strings.Cut(qualified, ".")
	if !ok || schema == "" || table == "" {
		return "", "", fmt.Errorf("invalid table name %q: expected schema.table", qualified)
	}
	return schema, table, nil
}
