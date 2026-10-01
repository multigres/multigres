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
// Only one migration is supported per server at a time (Coordinator.CreateMigration
// rejects a second one), so none of this is namespaced by migration: there is one
// shared ddl_log, one ddl_capture_tables, and no per-migration refcounting.
//
// The mechanism:
//
//   - Publisher side: a shared multigres.ddl_log table plus THREE event
//     triggers — multigres.capture_ddl on ddl_command_end (ALTER TABLE) and
//     multigres.capture_drop on sql_drop (DROP TABLE), which append the
//     executing statement to ddl_log, and multigres.reject_create_table on
//     ddl_command_start, which rejects CREATE TABLE outright before it runs
//     (see rejectCreateTableFunctionSQL). Because the capture INSERT commits
//     in the SAME transaction as the DDL, the log row and the DDL are atomic.
//     capture_ddl is scoped by TABLE MEMBERSHIP: it joins each command's
//     target table against multigres.ddl_capture_tables (schema_name,
//     table_name), so a DDL row is only ever logged for a table actually in
//     scope for the migration.
//   - The migration's publication adds multigres.ddl_log alongside the data
//     tables, so its rows are decoded and applied in the same stream, in
//     commit order.
//   - Subscriber side: a matching multigres.ddl_log plus an AFTER INSERT trigger
//     (multigres.apply_ddl) that EXECUTEs each arriving row's statement. The
//     trigger is ENABLE ALWAYS because triggers do NOT fire for
//     replication-applied changes by default. It runs inside the apply
//     transaction, so the replayed DDL lands at the right position in the stream.
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
// target, created on the source if absent). Teardown drops it unconditionally —
// there is no other migration whose objects it might still be serving — but never
// drops the schema itself, since on the target it also holds multigres.migration
// and the other shard metadata.
//
// Limitations (this is a proof of concept):
//   - Creating the event triggers requires a SUPERUSER DSN on the publisher side.
//   - ALTER TABLE is captured on ddl_command_end (capture_ddl), and DROP TABLE on
//     sql_drop (capture_drop) — the sql_drop event reports dropped objects that
//     pg_event_trigger_ddl_commands() on ddl_command_end does not. Other DDL —
//     CREATE INDEX (incl. CONCURRENTLY), DROP INDEX, and non-table DDL — is not
//     replicated, so those changes drift on the subscriber.
//   - CREATE TABLE is REJECTED outright while a migration is capturing on a
//     server, rather than captured. The migration's table set is fixed when the
//     migration starts, so a brand-new table is never in scope, and the listed
//     tables already exist (copied by the initial schema copy); opting a new
//     table in is out of scope (it would need a membership insert plus adding
//     the table to the publication). Silently letting it succeed unreplicated
//     would let the source and target permanently diverge with no operator
//     signal, so reject_create_table raises instead — the CREATE TABLE fails on the
//     side issuing it.
//   - current_query() stores the whole submitted statement, so a multi-statement
//     batch that touches a migrated table replays the entire batch on the
//     subscriber (potentially over-applying the other statements in the batch).
//     Keep source DDL during a migration simple and scoped to migrated tables.
//   - Captured statements are replayed under the captured schema's search_path
//     and the replay is exception-guarded: a statement that still cannot be
//     applied is skipped with a WARNING rather than stalling the subscription.

// ddlLogTable is the fully-qualified name added to the migration's publication.
const ddlLogTable = "multigres.ddl_log"

// The DDL-replication objects. All are idempotent (IF NOT EXISTS / CREATE OR
// REPLACE) so a resumed or retried setup is a no-op.
const (
	// ddlSchemaSQL is only needed on an external source: it has no multigres
	// sidecar schema. On the target the schema already exists (createSidecarSchema
	// at bootstrap), so setup there only creates tables.
	ddlSchemaSQL = `CREATE SCHEMA IF NOT EXISTS multigres`

	// ddlLogTableSQL is the shared, append-only wire table, identical on both
	// sides (logical replication maps by name, so the shape must match). log_id
	// (rather than a generic id) is a distinctive name so it can be joined with
	// USING. It is BY DEFAULT so the apply worker inserts the publisher's value
	// without an identity conflict. captured_at records when the publisher
	// captured the statement (DEFAULT clock_timestamp(), not now(), so it
	// reflects the actual capture instant rather than the enclosing transaction's
	// start): it is not read by any capture/apply logic, purely an
	// audit/debugging aid for correlating a replayed statement with when it
	// happened on the source. Neither capture_ddl nor capture_drop lists it in
	// their INSERT's column list, so it is populated by the DEFAULT on the
	// publisher's row and carried across to the subscriber like any other
	// replicated column.
	ddlLogTableSQL = `CREATE TABLE IF NOT EXISTS multigres.ddl_log (
	log_id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
	schema_name text NOT NULL,
	table_name text NOT NULL,
	ddl_command text NOT NULL,
	captured_at timestamptz NOT NULL DEFAULT clock_timestamp()
)`

	// ddlCaptureTablesSQL is the publisher-side membership: which tables belong
	// to the migration currently publishing from this side. capture_ddl joins
	// against it so a DDL row is only ever logged for a table actually in scope.
	//
	// A publisher-local membership table is used rather than reading
	// multigres.migration_tables directly because
	//
	// a. an external source has no migration_tables sidecar at all
	//
	// b. migration_tables on the Multigres side lists this migration regardless
	//    of which side is publishing, whereas capture must fire only when this
	//    side is currently the publisher.
	//
	// It is populated from the migration's resolved table set — the same data
	// as migration_tables — at capture setup.
	ddlCaptureTablesSQL = `
CREATE TABLE IF NOT EXISTS multigres.ddl_capture_tables (
	schema_name text NOT NULL,
	table_name text NOT NULL,
	PRIMARY KEY (schema_name, table_name)
)`

	// rejectCreateTableFunctionSQL fires on ddl_command_start — BEFORE the
	// command does any work — and rejects CREATE TABLE outright rather than
	// letting capture_ddl silently not capture it: the migration's table set is
	// fixed when the migration starts, so a brand-new table is never in scope,
	// and the listed tables already exist (copied by the initial schema copy).
	// Letting it succeed unreplicated would let the source and target
	// permanently diverge with no operator signal, so it is rejected loudly and
	// immediately on the side issuing it instead.
	//
	// This runs at ddl_command_start rather than alongside the ALTER TABLE
	// capture at ddl_command_end for two reasons: pg_event_trigger_ddl_commands()
	// — which capture_ddl uses to read the target schema/table — can only be
	// called from a ddl_command_end trigger, so it is not available here; and,
	// more importantly, rejecting before the command executes means a
	// `CREATE TABLE ... AS SELECT ...` never runs its (potentially expensive or
	// side-effecting) query in the first place, rather than running it only to
	// roll it back at ddl_command_end.
	//
	// Without pg_event_trigger_ddl_commands(), the target schema is not
	// available here, so the multigres sidecar schema is excluded by matching
	// current_query() against the two literal, Go-controlled "CREATE TABLE IF
	// NOT EXISTS multigres...." bootstrap statements (ddl_log,
	// ddl_capture_tables — see ensureDDLCaptureObjects/ensureDDLApplyObjects)
	// rather than by schema_name. Must be LANGUAGE plpgsql: a SQL function
	// cannot return the event_trigger pseudo-type.
	rejectCreateTableFunctionSQL = `
CREATE OR REPLACE FUNCTION multigres.reject_create_table()
	RETURNS event_trigger LANGUAGE plpgsql AS $fn$
BEGIN
	IF current_query() ~* '^\s*create\s+table\s+if\s+not\s+exists\s+multigres\.' THEN
		RETURN;
	END IF;
	RAISE EXCEPTION 'multigres: CREATE TABLE is not allowed while a table migration is active on this database; the migration''s table set is fixed when it starts. Statement: %', current_query();
END;
$fn$`

	createRejectCreateTableTriggerSQL = `CREATE EVENT TRIGGER mg_reject_create_table
	ON ddl_command_start WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION multigres.reject_create_table()`
	// rejectCreateTableTriggerExistsSQL is the existence check used to arm this
	// event trigger only once, alongside mg_capture_ddl and mg_capture_drop.
	rejectCreateTableTriggerExistsSQL = `SELECT count(*) FROM pg_event_trigger WHERE evtname = 'mg_reject_create_table'`

	// captureFunctionSQL captures ALTER TABLE, scoped to the migrated tables via
	// the ddl_capture_tables join. CREATE TABLE never reaches here — it is
	// rejected earlier, at ddl_command_start (see rejectCreateTableFunctionSQL).
	// DROP TABLE is captured separately on the sql_drop event (capture_drop).
	// CREATE INDEX and every other DDL kind are excluded (neither rejected nor
	// captured). The one non-transactional statement carrying the ALTER TABLE
	// tag — ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY — is excluded
	// explicitly (matched on the query text, since the event-trigger metadata
	// does not distinguish it): it cannot run inside the apply worker's
	// transaction. Must be LANGUAGE plpgsql: a SQL function cannot return the
	// event_trigger pseudo-type.
	captureFunctionSQL = `
CREATE OR REPLACE FUNCTION multigres.capture_ddl()
	RETURNS event_trigger LANGUAGE plpgsql AS $fn$
BEGIN
	INSERT INTO multigres.ddl_log (schema_name, table_name, ddl_command)
	SELECT c.schema_name, cl.relname, current_query()
	FROM pg_event_trigger_ddl_commands() c
	JOIN pg_class cl ON cl.oid = c.objid
	JOIN multigres.ddl_capture_tables m
	  ON m.schema_name = c.schema_name AND m.table_name = cl.relname
	WHERE c.command_tag = 'ALTER TABLE'
	  AND NOT (current_query() ~* '\ydetach\s+partition\y'
	           AND current_query() ~* '\yconcurrently\y');
END;
$fn$`

	createCaptureTriggerSQL = `CREATE EVENT TRIGGER mg_capture_ddl
	ON ddl_command_end EXECUTE FUNCTION multigres.capture_ddl()`
	// captureTriggerExistsSQL is the existence check used to arm this event
	// trigger only once (CREATE EVENT TRIGGER has no IF NOT EXISTS, and blindly
	// DROP+CREATE would momentarily disarm capture, losing any DDL in that
	// window).
	captureTriggerExistsSQL = `SELECT count(*) FROM pg_event_trigger WHERE evtname = 'mg_capture_ddl'`

	// captureDropFunctionSQL captures DROP TABLE. A DROP is invisible to
	// pg_event_trigger_ddl_commands() on ddl_command_end (so capture_ddl never
	// sees it); dropped objects are reported instead by
	// pg_event_trigger_dropped_objects() on the sql_drop event. Like capture_ddl it
	// is scoped to the migrated tables via the ddl_capture_tables join, storing
	// current_query() (the DROP statement) for replay. Only tables the user
	// explicitly dropped are captured (d.original) — cascade/internal drops
	// (indexes, sequences, a partition's children) are skipped; the replayed
	// DROP handles its own cascades. Must be LANGUAGE plpgsql (event_trigger
	// return type).
	//
	// After logging, it also deletes the dropped table's own row from
	// ddl_capture_tables. Without this, a table's membership row outlives the
	// table itself for the rest of the migration: since CREATE TABLE is not
	// captured, an operator recreating a table of the same name outside the
	// migration would never get it schema-copied or published, yet a later
	// ALTER on it would still match the stale row by name and be captured and
	// replayed against the target's now-nonexistent table — a silent,
	// needless divergence. Both statements commit atomically with the DROP.
	captureDropFunctionSQL = `
CREATE OR REPLACE FUNCTION multigres.capture_drop()
	RETURNS event_trigger LANGUAGE plpgsql AS $fn$
BEGIN
	INSERT INTO multigres.ddl_log (schema_name, table_name, ddl_command)
	SELECT d.schema_name, d.object_name, current_query()
	FROM pg_event_trigger_dropped_objects() d
	JOIN multigres.ddl_capture_tables m
	  ON m.schema_name = d.schema_name AND m.table_name = d.object_name
	WHERE d.object_type = 'table' AND d.original;

	DELETE FROM multigres.ddl_capture_tables m
	USING pg_event_trigger_dropped_objects() d
	WHERE m.schema_name = d.schema_name AND m.table_name = d.object_name
	  AND d.object_type = 'table' AND d.original;
END;
$fn$`

	createDropCaptureTriggerSQL = `CREATE EVENT TRIGGER mg_capture_drop
	ON sql_drop WHEN TAG IN ('DROP TABLE') EXECUTE FUNCTION multigres.capture_drop()`
	// dropCaptureTriggerExistsSQL is the existence check for this event trigger,
	// armed once alongside mg_capture_ddl.
	dropCaptureTriggerExistsSQL = `SELECT count(*) FROM pg_event_trigger WHERE evtname = 'mg_capture_drop'`

	// applyFunctionSQL replays each applied row's statement. Like capture_ddl it
	// must be LANGUAGE plpgsql. Two safeguards:
	//   1. search_path — the apply worker runs with an empty search_path, so a
	//      captured statement with unqualified names would fail to resolve. We SET
	//      LOCAL search_path to the captured schema before replaying.
	//   2. exception guard — a statement that still cannot be replayed must NOT
	//      wedge the apply worker (logical replication retries forever, stalling
	//      ALL replication). We skip it with a WARNING so the stream keeps
	//      advancing; that one change then drifts on the subscriber.
	applyFunctionSQL = `
CREATE OR REPLACE FUNCTION multigres.apply_ddl()
RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
	PERFORM set_config('search_path', quote_ident(NEW.schema_name) || ', pg_catalog, pg_temp', true);
	BEGIN
		EXECUTE NEW.ddl_command;
	EXCEPTION WHEN OTHERS THEN
		RAISE WARNING 'multigres.apply_ddl skipped ddl_log log_id=% (%): %', NEW.log_id, NEW.ddl_command, SQLERRM;
	END;
	RETURN NEW;
END;
$fn$`

	createApplyTriggerSQL = `
CREATE TRIGGER mg_apply_ddl
AFTER INSERT ON multigres.ddl_log
FOR EACH ROW EXECUTE FUNCTION multigres.apply_ddl()
`
	// ENABLE ALWAYS so the trigger fires for replication-applied INSERTs.
	enableAlwaysApplyTriggerSQL = `ALTER TABLE multigres.ddl_log ENABLE ALWAYS TRIGGER mg_apply_ddl`
	// applyTriggerExistsSQL checks whether the apply trigger is already
	// installed, so a resumed/retried setup does not drop and recreate it
	// (which would briefly disarm apply on ddl_log for a migration that is
	// already streaming). ddl_log always exists here — setup creates it (IF
	// NOT EXISTS) before this check.
	applyTriggerExistsSQL = `SELECT count(*) FROM pg_trigger WHERE tgname = 'mg_apply_ddl' AND tgrelid = 'multigres.ddl_log'::regclass`
)

// teardownDDLCaptureStmts drops the publisher-side capture objects
// unconditionally: only one migration is supported at a time, so there is
// never another migration's registration to preserve. Every drop is IF
// EXISTS, so teardown is idempotent and a no-op when capture was never set up
// on this side (e.g. tearing down a migration that failed before reaching
// this step, or the non-publisher side of the current direction). Event
// triggers are dropped before their functions (a function cannot be dropped
// while an event trigger still depends on it).
var teardownDDLCaptureStmts = []string{
	`DROP EVENT TRIGGER IF EXISTS mg_capture_ddl`,
	`DROP EVENT TRIGGER IF EXISTS mg_capture_drop`,
	`DROP EVENT TRIGGER IF EXISTS mg_reject_create_table`,
	`DROP FUNCTION IF EXISTS multigres.capture_ddl()`,
	`DROP FUNCTION IF EXISTS multigres.capture_drop()`,
	`DROP FUNCTION IF EXISTS multigres.reject_create_table()`,
	`DROP TABLE IF EXISTS multigres.ddl_capture_tables`,
	`DROP TABLE IF EXISTS multigres.ddl_log`,
}

// teardownDDLApplyStmts drops the subscriber-side apply objects
// unconditionally, for the same reason as teardownDDLCaptureStmts: with only
// one migration supported at a time, apply and capture never coexist on the
// same side, so there is nothing else on this side's ddl_log to preserve.
// Dropping the table first takes mg_apply_ddl with it (a trigger cannot be
// dropped standalone by name without referencing its table, which would error
// if ddl_log does not exist), leaving the function droppable without a
// dependent-object error.
var teardownDDLApplyStmts = []string{
	`DROP TABLE IF EXISTS multigres.ddl_log`,
	`DROP FUNCTION IF EXISTS multigres.apply_ddl()`,
}

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

// isAlreadyExists reports whether err is Postgres's "already exists" error —
// the signature of a resumed/retried setup re-creating an object that has no
// CREATE ... IF NOT EXISTS form (event triggers, regular triggers). Matched on
// the error text and SQLSTATE (42710, duplicate_object), the same style as
// coordinator.go's isRetryableUnavailable, since ddlConn is implemented by two
// different client stacks (pgx and the admin query service) with different
// concrete error types.
func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "already exists") || strings.Contains(s, "42710")
}

// ensureDDLCaptureObjects creates the publisher-side objects (idempotent: IF
// NOT EXISTS / CREATE OR REPLACE, so safe to re-run any number of times).
func ensureDDLCaptureObjects(ctx context.Context, c ddlConn) error {
	for _, stmt := range []string{ddlSchemaSQL, ddlLogTableSQL, ddlCaptureTablesSQL, captureFunctionSQL, captureDropFunctionSQL, rejectCreateTableFunctionSQL} {
		if err := c.exec(ctx, stmt); err != nil {
			return fmt.Errorf("set up ddl capture: %w", err)
		}
	}
	return nil
}

// registerDDLCaptureTable inserts one table membership row.
func registerDDLCaptureTable(ctx context.Context, c ddlConn, schema, table string) error {
	return c.execArgs(ctx,
		`INSERT INTO multigres.ddl_capture_tables (schema_name, table_name)
		 VALUES ($1,$2) ON CONFLICT DO NOTHING`,
		schema, table)
}

// setupDDLCapture creates the publisher-side objects (idempotent) and
// registers the migration's tables in ddl_capture_tables so capture_ddl fans
// its DDL out to them. It does NOT arm the event trigger — the caller arms it
// with armDDLCapture last (just before CreateSubscription), so little DDL
// accumulates in ddl_log before the subscription's initial snapshot. (CREATE
// PUBLICATION is never captured regardless of arming order — its command tag
// is not in the allowlist.) tables are the migration's resolved
// "schema.table" names.
func setupDDLCapture(ctx context.Context, c ddlConn, tables []string) error {
	if err := ensureDDLCaptureObjects(ctx, c); err != nil {
		return err
	}
	for _, qualified := range tables {
		schema, table, err := splitQualified(qualified)
		if err != nil {
			return err
		}
		if err := registerDDLCaptureTable(ctx, c, schema, table); err != nil {
			return fmt.Errorf("register ddl capture table %q: %w", qualified, err)
		}
	}
	return nil
}

// armDDLCapture creates the three event triggers if they do not already
// exist: mg_capture_ddl (ddl_command_end, ALTER TABLE), mg_capture_drop
// (sql_drop, DROP TABLE), and mg_reject_create_table (ddl_command_start, rejects
// CREATE TABLE). Checking existence first makes a resumed/retried setup a
// no-op rather than briefly disarming capture. CREATE EVENT TRIGGER has no IF
// NOT EXISTS, so the exists-check-then-create below is not atomic; a resulting
// "already exists" error (e.g. from a retry after a response was lost) is
// tolerated rather than failing setup — the trigger is armed either way.
func armDDLCapture(ctx context.Context, c ddlConn) error {
	for _, trig := range []struct{ existsSQL, createSQL string }{
		{captureTriggerExistsSQL, createCaptureTriggerSQL},
		{dropCaptureTriggerExistsSQL, createDropCaptureTriggerSQL},
		{rejectCreateTableTriggerExistsSQL, createRejectCreateTableTriggerSQL},
	} {
		n, err := c.queryCount(ctx, trig.existsSQL)
		if err != nil {
			return fmt.Errorf("check ddl capture trigger: %w", err)
		}
		if n > 0 {
			continue
		}
		if err := c.exec(ctx, trig.createSQL); err != nil && !isAlreadyExists(err) {
			return fmt.Errorf("arm ddl capture trigger: %w", err)
		}
	}
	return nil
}

// teardownDDLCapture drops the publisher-side capture objects unconditionally
// (see teardownDDLCaptureStmts). Idempotent; best-effort at teardown.
func teardownDDLCapture(ctx context.Context, c ddlConn) error {
	for _, stmt := range teardownDDLCaptureStmts {
		if err := c.exec(ctx, stmt); err != nil {
			return fmt.Errorf("tear down ddl capture: %w", err)
		}
	}
	return nil
}

// setupDDLApply creates the subscriber-side objects (idempotent), including
// the apply trigger if it is not already installed. Must run before the
// subscription is created so the subscription can sync ddl_log. Checking
// trigger existence first makes a resumed/retried setup a no-op rather than
// briefly disarming apply. CREATE TRIGGER has no IF NOT EXISTS either, so —
// like armDDLCapture — a resulting "already exists" error is tolerated rather
// than failing setup.
func setupDDLApply(ctx context.Context, c ddlConn) error {
	// The apply function is CREATE OR REPLACE (atomic, so it never gaps trigger
	// firing for a migration already applying on ddl_log).
	for _, stmt := range []string{ddlSchemaSQL, ddlLogTableSQL, applyFunctionSQL} {
		if err := c.exec(ctx, stmt); err != nil {
			return fmt.Errorf("set up ddl apply: %w", err)
		}
	}
	n, err := c.queryCount(ctx, applyTriggerExistsSQL)
	if err != nil {
		return fmt.Errorf("check ddl apply trigger: %w", err)
	}
	if n == 0 {
		for _, stmt := range []string{createApplyTriggerSQL, enableAlwaysApplyTriggerSQL} {
			if err := c.exec(ctx, stmt); err != nil && !isAlreadyExists(err) {
				return fmt.Errorf("set up ddl apply trigger: %w", err)
			}
		}
	}
	return nil
}

// teardownDDLApply drops the subscriber-side apply objects unconditionally
// (see teardownDDLApplyStmts). Idempotent; best-effort at teardown.
func teardownDDLApply(ctx context.Context, c ddlConn) error {
	for _, stmt := range teardownDDLApplyStmts {
		if err := c.exec(ctx, stmt); err != nil {
			return fmt.Errorf("tear down ddl apply: %w", err)
		}
	}
	return nil
}

// createPublicationWithDDLLogSQL builds a CREATE PUBLICATION ... FOR TABLE
// statement that publishes the migrated tables (all rows) plus
// multigres.ddl_log, so captured DDL rides the same stream.
func createPublicationWithDDLLogSQL(name string, tables []string) string {
	quoted := make([]string, 0, len(tables)+1)
	for _, tbl := range tables {
		quoted = append(quoted, quoteQualifiedName(tbl))
	}
	quoted = append(quoted, ddlLogTable)
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
