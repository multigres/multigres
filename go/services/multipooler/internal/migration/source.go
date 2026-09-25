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
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/tools/executil"
)

// source runs the external (old-database) side of a migration over the
// operator-supplied DSN. It uses pgx (a standard libpq-compatible driver) rather
// than the multigres pgprotocol client, because on-prem/v2/v3 sources commonly
// use MD5 auth, which the pgprotocol client rejects. newSource opens one
// connection and caches it (with the operation's context) on the struct; every
// method reuses it, so one source drives all of a phase's source-side calls
// (e.g. the whole of runSetup) over one connection. The caller releases it with
// close.
type source struct {
	dsn  string
	ctx  context.Context
	conn *pgx.Conn
}

// migratorAppName is the application_name stamped on every migrator source
// connection. TerminateClientBackends excludes it so the hard-quiesce write-cut
// never kills the migrator's own (or a concurrent migrator) connection, and it
// makes migrator sessions identifiable in pg_stat_activity.
const migratorAppName = "multigres_migrator"

// newSource opens the source connection immediately and caches it (with ctx) on
// the returned source; every method reuses it, so one source drives all of a
// phase's source-side calls (e.g. the whole of runSetup) over one connection.
// The caller releases it with close.
func newSource(ctx context.Context, dsn string) (*source, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to source: %w", err)
	}
	// Stamp a distinctive application_name so the hard-quiesce write-cut
	// (TerminateClientBackends) can exclude migrator connections, overriding any
	// application_name the operator set in the DSN. migratorAppName is a compile-time
	// constant, so the inlined literal carries no injection risk.
	if _, err := conn.Exec(ctx, "SET application_name = '"+migratorAppName+"'"); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("set migrator application_name on source: %w", err)
	}
	return &source{dsn: dsn, ctx: ctx, conn: conn}, nil
}

// close releases the cached connection if one is open. Safe to call more than
// once and on a source that never connected.
func (s *source) close() {
	if s.conn != nil {
		_ = s.conn.Close(s.ctx)
		s.conn = nil
		s.ctx = nil
	}
}

// SourceInfo captures source-server capabilities read at validation time.
type SourceInfo struct {
	// ServerVersionNum is PostgreSQL's numeric version (e.g. 160004).
	ServerVersionNum int
	// CanCreateSubscription is whether the DSN role can CREATE SUBSCRIPTION here
	// (superuser, or PG16+ with pg_create_subscription membership). This gates
	// EXPORT, where the source becomes the subscriber.
	CanCreateSubscription bool
}

const rolesQuery = `
SELECT current_setting('server_version_num')::int,
       r.rolsuper,
	   r.rolreplication,
	   pg_has_role(current_user, 'pg_create_subscription', 'MEMBER')
  FROM pg_roles r WHERE r.rolname = current_user`

// capabilities reads the source's version and the current role's attributes in a
// single pg_roles round-trip, then derives what the DSN role can do: whether it
// can create subscriptions (canCreateSubscription — superuser, or PG16+ with
// pg_create_subscription membership; gates EXPORT) and whether it can stream
// (canReplicate — rolsuper OR rolreplication). Both checks previously issued
// their own "FROM pg_roles WHERE rolname = current_user" query.
func (s *source) capabilities() (info *SourceInfo, canReplicate bool, err error) {
	info = &SourceInfo{}
	var isSuper, isReplication bool
	if err = s.conn.QueryRow(s.ctx, rolesQuery).Scan(&info.ServerVersionNum, &isSuper, &isReplication, &info.CanCreateSubscription); err != nil {
		return nil, false, fmt.Errorf("read role attributes: %w", err)
	}
	return info, isSuper || isReplication, nil
}

// Info opens a connection and reads the source capabilities (version,
// subscribe-capability). Used to gate EXPORT before switching direction.
func (s *source) Info() (*SourceInfo, error) {
	info, _, err := s.capabilities()
	return info, err
}

// Validate checks that the source is usable for logical replication: wal_level
// must be logical, every table must exist and have a usable replica identity for
// UPDATE/DELETE (reject missing), and it returns a warning for any table using
// REPLICA IDENTITY FULL (works, but expensive). It also returns the source's
// capabilities (version, subscribe-capability) read on the same connection.
func (s *source) Validate(patterns []string) (info *SourceInfo, resolved []string, warnings []string, err error) {
	var walLevel string
	if err := s.conn.QueryRow(s.ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		return nil, nil, nil, fmt.Errorf("read wal_level: %w", err)
	}
	if walLevel != "logical" {
		return nil, nil, nil, fmt.Errorf("source wal_level is %q, must be logical", walLevel)
	}

	var canReplicate bool
	info, canReplicate, err = s.capabilities()
	if err != nil {
		return nil, nil, nil, err
	}

	// Role-level IMPORT permissions the migration will need later, checked now so
	// an under-privileged DSN fails at create rather than at start. The role must
	// be able to stream (REPLICATION) and create the publication (CREATE on the
	// database); table ownership and SELECT are checked per table below.
	if !canReplicate {
		return nil, nil, nil, errors.New("source role lacks the REPLICATION attribute (needed to stream); grant REPLICATION or use a replication-capable role")
	}
	var canCreateDB bool
	if err := s.conn.QueryRow(
		s.ctx,
		"SELECT has_database_privilege(current_user, current_database(), 'CREATE')",
	).Scan(&canCreateDB); err != nil {
		return nil, nil, nil, fmt.Errorf("read database CREATE privilege: %w", err)
	}
	if !canCreateDB {
		return nil, nil, nil, errors.New("source role lacks CREATE on the database (needed to create the publication)")
	}

	// Split the requested patterns into the "*" (all owned tables) marker, the
	// "schema.*" schema names, and the plain table names. readTableMetadata
	// resolves the wildcards and the named tables in a single query, returning the
	// concrete table list (de-duplicated, unspecified order) and per-table metadata.
	var tables, schemas []string
	allTables := false
	for _, e := range patterns {
		switch {
		case e == "*":
			allTables = true
		case strings.HasSuffix(e, ".*"):
			schemas = append(schemas, strings.TrimSuffix(e, ".*"))
		default:
			tables = append(tables, e)
		}
	}

	// The resolution query returns only rows that exist, so a typo'd schema or
	// table would silently vanish. Reject non-existent schemas ("schema.*") and
	// explicitly-named tables up front so they fail loudly at create.
	if err := s.checkSchemasExist(schemas); err != nil {
		return nil, nil, nil, err
	}
	if err := s.checkTablesExist(tables); err != nil {
		return nil, nil, nil, err
	}

	resolved, meta, err := s.readTableMetadata(allTables, schemas, tables)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(resolved) == 0 {
		return nil, nil, nil, errors.New("no tables to migrate (a wildcard matched no tables owned by the source role)")
	}
	// resolved order is unspecified; every entry exists (checked above).
	for _, tbl := range resolved {
		m := meta[tbl]
		if !m.isOwner {
			return nil, nil, nil, fmt.Errorf("source role does not own table %q", tbl)
		}
		if !m.canSelect {
			return nil, nil, nil, fmt.Errorf("source role lacks SELECT on table %q needed for initial copy", tbl)
		}
		// A bare `FOR TABLE <t>` (without ONLY) sets include_descendants, which is
		// a no-op for an ordinary table but means partition fan-out for a
		// partitioned one — not yet supported, so reject it here where the catalog
		// relkind is known (rather than in the parser, which cannot tell them apart).
		if m.relkind == relkindPartitioned {
			return nil, nil, nil, mterrors.NewFeatureNotSupported(
				fmt.Sprintf("table %q is a partitioned table; partitioned-table migration is not yet supported", tbl),
			)
		}

		switch m.relreplident {
		case replicaIdentityNothing:
			return nil, nil, nil, fmt.Errorf("table %q has no replica identity", tbl)
		case replicaIdentityDefault:
			if !m.hasPK {
				return nil, nil, nil, fmt.Errorf("table %q has no primary key and default replica identity; add a primary key, a unique index (REPLICA IDENTITY USING INDEX), or FULL", tbl)
			}
		case replicaIdentityFull:
			warnings = append(warnings, fmt.Sprintf("table %q uses REPLICA IDENTITY FULL (works, but expensive)", tbl))
		}
	}
	return info, resolved, warnings, nil
}

// tableMeta is the per-table validation data read from pg_class.
type tableMeta struct {
	relreplident string // pg_class.relreplident (n/d/f/i)
	relkind      string // pg_class.relkind (r = ordinary, p = partitioned)
	hasPK        bool
	isOwner      bool
	canSelect    bool
}

// relkindPartitioned is pg_class.relkind for a partitioned table (the parent).
const relkindPartitioned = "p"

const (
	replicaIdentityDefault = "d"
	replicaIdentityFull    = "f"
	replicaIdentityIndex   = "i"
	replicaIdentityNothing = "n"
)

// tableMetadataSQL resolves the requested tables and reads their validation
// metadata in one round-trip. It takes three parameters: $1 the "schema.*"
// schema names, $2 the explicitly-named tables, and $3 whether "*" (all
// search_path tables) was requested.
//
// Two CTEs name the inputs the main query matches against:
//   - tables: the OIDs of the explicitly-named tables ($2), each resolved with
//     to_regclass through the connection's search_path (so a bare "orders"
//     becomes public.orders). A name that doesn't resolve yields NULL, but the
//     caller rejects those via checkTablesExist before this query runs.
//   - schemas: the set of schema names to sweep whole. It is the union of the
//     connection's search_path schemas (current_schemas(true), included only
//     when $3 is true) and the explicit "schema.*" schemas ($1); both drop the
//     system schemas (pg_catalog, information_schema, pg_*).
//
// A table is included when its schema is in `schemas` (the wildcard path) or its
// oid is in `tables` (the explicit path). The wildcard path is restricted to
// ordinary tables (relkind r/p) the role owns, since v1 publishes them via
// CREATE PUBLICATION FOR TABLE (which needs ownership). The explicit path is
// unfiltered, so a named-but-unowned or non-ordinary table still surfaces (with
// isOwner false / a replica-identity error) rather than silently vanishing.
// Because it is a single SELECT over pg_class, a table matched by both paths
// yields exactly one row — dedup is automatic. Names are the canonical
// "schema.table"; row order is unspecified (callers treat it as a set).
const tableMetadataSQL = `
WITH
  tables  AS (SELECT to_regclass(t) AS table_oid FROM unnest($2::text[]) AS t),
  schemas AS (
  	SELECT schema_name
	  FROM unnest(current_schemas(true)) AS schema_name
	 WHERE schema_name NOT IN ('pg_catalog','information_schema')
	   AND schema_name NOT LIKE 'pg\_%'
	   AND $3::bool
	UNION ALL
	SELECT schema_name
	  FROM unnest($1::text[]) AS schema_name
	 WHERE schema_name NOT IN ('pg_catalog','information_schema')
	   AND schema_name NOT LIKE 'pg\_%'
  )
SELECT n.nspname::text,
	   c.relname::text,
       c.relreplident::text,
       c.relkind::text,
       EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary),
       pg_has_role(current_user, c.relowner, 'USAGE'),
       has_table_privilege(current_user, c.oid, 'SELECT')
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relkind IN ('r','p')
   AND pg_has_role(current_user, c.relowner, 'USAGE')
   AND n.nspname IN (SELECT schema_name FROM schemas)
    OR c.oid IN (SELECT table_oid FROM tables)
`

// checkTablesExist errors if any explicitly-named table cannot be resolved on the
// source (to_regclass returns NULL), listing every missing one. Symmetric with
// checkSchemasExist and needed because the resolution query returns only rows
// that exist, so a missing name would otherwise vanish silently.
func (s *source) checkTablesExist(tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	rows, err := s.conn.Query(s.ctx,
		`SELECT t FROM unnest($1::text[]) AS t WHERE to_regclass(t) IS NULL ORDER BY t`, tables)
	if err != nil {
		return fmt.Errorf("check tables exist: %w", err)
	}
	defer rows.Close()
	var missing []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return fmt.Errorf("scan table: %w", err)
		}
		missing = append(missing, t)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("check tables exist: %w", err)
	}
	if len(missing) > 0 {
		return fmt.Errorf("source table(s) do not exist: %s", strings.Join(missing, ", "))
	}
	return nil
}

// checkSchemasExist errors if any requested "schema.*" schema is absent from the
// source, listing every missing one. This turns a typo'd schema into a create
// error instead of a wildcard that silently matches nothing.
func (s *source) checkSchemasExist(schemas []string) error {
	if len(schemas) == 0 {
		return nil
	}
	rows, err := s.conn.Query(s.ctx,
		`SELECT s FROM unnest($1::text[]) AS s
		  WHERE NOT EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspname = s)
		  ORDER BY s`, schemas)
	if err != nil {
		return fmt.Errorf("check schemas exist: %w", err)
	}
	defer rows.Close()
	var missing []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return fmt.Errorf("scan schema: %w", err)
		}
		missing = append(missing, s)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("check schemas exist: %w", err)
	}
	if len(missing) > 0 {
		return fmt.Errorf("source schema(s) do not exist: %s", strings.Join(missing, ", "))
	}
	return nil
}

// readTableMetadata resolves the "*" (allTables) and "schema.*" (schemas)
// wildcards together with the explicit table names in a single query and reads
// each matched table's validation metadata. It returns the concrete table list
// (canonical "schema.table", unspecified order, already unique) and the metadata
// keyed by that name.
func (s *source) readTableMetadata(allTables bool, schemas, explicit []string) ([]string, map[string]tableMeta, error) {
	rows, err := s.conn.Query(s.ctx, tableMetadataSQL, schemas, explicit, allTables)
	if err != nil {
		return nil, nil, fmt.Errorf("read table metadata: %w", err)
	}
	defer rows.Close()
	meta := make(map[string]tableMeta)
	var resolved []string
	for rows.Next() {
		var nspname, relname string
		var m tableMeta
		if err := rows.Scan(&nspname, &relname, &m.relreplident, &m.relkind, &m.hasPK, &m.isOwner, &m.canSelect); err != nil {
			return nil, nil, fmt.Errorf("scan table metadata: %w", err)
		}
		name := nspname + "." + relname
		meta[name] = m
		resolved = append(resolved, name) // single SELECT over pg_class => already unique
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read table metadata: %w", err)
	}
	return resolved, meta, nil
}

// DumpSchema runs pg_dump --schema-only against the source for the named tables
// and strips psql backslash meta-commands (pg_dump emits a few that only psql
// understands). pg_dump accepts a libpq conninfo as its dbname argument.
func (s *source) DumpSchema(tables []string) (string, error) {
	args := []string{"--schema-only", "--no-owner", "--no-privileges", "--no-publications", "--no-subscriptions"}
	for _, tbl := range tables {
		args = append(args, "--table="+tbl)
	}
	args = append(args, s.dsn)
	out, err := executil.Command(s.ctx, "pg_dump", args...).Output()
	if err != nil {
		return "", fmt.Errorf("pg_dump: %w", err)
	}
	var b strings.Builder
	for line := range strings.SplitSeq(string(out), "\n") {
		if strings.HasPrefix(line, "\\") {
			continue // psql-only meta-command
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String(), nil
}

// CreatePublication creates a publication on the source FOR TABLE the given
// tables plus multigres.ddl_log (so captured DDL rides the same stream). Used
// when the source is the publisher (IMPORT direction).
func (s *source) CreatePublication(name string, tables []string) error {
	if _, err := s.conn.Exec(s.ctx, createPublicationWithDDLLogSQL(name, tables)); err != nil {
		return fmt.Errorf("create publication on source: %w", err)
	}
	return nil
}

// DropPublication drops a publication on the source. Idempotent.
func (s *source) DropPublication(name string) error {
	if _, err := s.conn.Exec(s.ctx, "DROP PUBLICATION IF EXISTS "+ast.QuoteIdentifier(name)); err != nil {
		return fmt.Errorf("drop publication on source: %w", err)
	}
	return nil
}

// CreateSubscription creates a subscription on the source (EXPORT direction: the
// source is the subscriber, replicating from the Multigres target). Requires a
// superuser DSN and runs in autocommit (pgx.Exec is not in a transaction).
// CreateSubscription creates a subscription on the source. slotName is non-empty
// for the IMPORT->EXPORT reverse subscription, which attaches (create_slot=false)
// to a slot pre-created on the target during the switch, so no target write is
// missed between serving turning on and this subscription attaching.
func (s *source) CreateSubscription(name, conninfo, publication string, copyData bool, slotName string) error {
	if _, err := s.conn.Exec(s.ctx, createSubscriptionSQL(name, conninfo, publication, copyData, slotName)); err != nil {
		return fmt.Errorf("create subscription on source: %w", err)
	}
	return nil
}

// DropSubscription drops the reverse subscription on the source. Idempotent.
//
// It detaches the slot before dropping so DROP SUBSCRIPTION never dials the
// publisher. The reverse subscription's publisher is the target, reached over the
// gateway's replication tunnel, which is gated the moment a switch/teardown flips
// the shard toward IMPORT (drainForImport turns serving off). A plain DROP
// SUBSCRIPTION would then try to drop the remote slot over that gated tunnel and
// fail with "database is temporarily unavailable" (SQLSTATE 08006), leaving the
// migration FAILED. The coordinator drops the target-side reverse slot itself
// (target.DropLogicalSlot), so detaching here does not orphan it. ALTER
// SUBSCRIPTION has no IF EXISTS, so guard on existence to stay idempotent; DISABLE
// first because the slot can only be detached while the subscription is disabled.
func (s *source) DropSubscription(name string) error {
	q := ast.QuoteIdentifier(name)
	var exists bool
	if err := s.conn.QueryRow(s.ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname = $1)", name).Scan(&exists); err != nil {
		return fmt.Errorf("check subscription on source: %w", err)
	}
	if exists {
		if _, err := s.conn.Exec(s.ctx, "ALTER SUBSCRIPTION "+q+" DISABLE"); err != nil {
			return fmt.Errorf("disable subscription on source: %w", err)
		}
		if _, err := s.conn.Exec(s.ctx, "ALTER SUBSCRIPTION "+q+" SET (slot_name = NONE)"); err != nil {
			return fmt.Errorf("detach slot from subscription on source: %w", err)
		}
	}
	if _, err := s.conn.Exec(s.ctx, "DROP SUBSCRIPTION IF EXISTS "+q); err != nil {
		return fmt.Errorf("drop subscription on source: %w", err)
	}
	return nil
}

// SetReadOnly quiesces (or un-quiesces) the source by flipping
// default_transaction_read_only cluster-wide and reloading. New transactions are
// affected; the operator is responsible for stopping in-flight application
// writes during a cutover.
func (s *source) SetReadOnly(ro bool) error {
	val := "off"
	if ro {
		val = "on"
	}
	if _, err := s.conn.Exec(s.ctx, "ALTER SYSTEM SET default_transaction_read_only = "+val); err != nil {
		return fmt.Errorf("set source read_only: %w", err)
	}
	if _, err := s.conn.Exec(s.ctx, "SELECT pg_reload_conf()"); err != nil {
		return fmt.Errorf("reload source conf: %w", err)
	}
	// Also set it on THIS session. ALTER SYSTEM + pg_reload_conf() propagates
	// asynchronously (the reload is processed between statements, and only by other
	// backends at their next transaction), so a caller that writes on this same
	// connection right after — e.g. teardown's DROP PUBLICATION after un-quiescing
	// on a graceful IMPORT drop — would otherwise still run read-only and fail. The
	// session SET takes effect immediately for this connection.
	if _, err := s.conn.Exec(s.ctx, "SET default_transaction_read_only = "+val); err != nil {
		return fmt.Errorf("set source read_only (session): %w", err)
	}
	return nil
}

// TerminateClientBackends disconnects every ordinary client backend on the
// source database except the migrator's own connections, so a hard quiesce
// leaves no in-flight writer able to commit after the drain barrier. It targets
// only backend_type = 'client backend' — never walsenders, background workers, or
// the logical-replication apply/tablesync workers — so replication is untouched,
// and excludes application_name = migratorAppName so it never kills the migrator
// (this or a concurrent one). pg_terminate_backend requires no more than the
// pg_signal_backend role for same-database peers; the migrator connects with an
// admin/superuser DSN, which has it. Call under default_transaction_read_only=on
// (SetReadOnly(true)) so terminated clients cannot reconnect and write before the
// LSN is captured. Idempotent: with nothing left to terminate it is a no-op.
func (s *source) TerminateClientBackends() error {
	if _, err := s.conn.Exec(s.ctx, terminateClientBackendsSQL, migratorAppName); err != nil {
		return fmt.Errorf("terminate source client backends: %w", err)
	}
	return nil
}

// terminateClientBackendsSQL disconnects every ordinary client backend on the
// current database except this migrator's own connections ($1 = migratorAppName).
// It matches only backend_type = 'client backend' so walsenders/background
// workers/apply workers are never signalled, and skips pg_backend_pid() so the
// caller does not terminate itself.
const terminateClientBackendsSQL = `SELECT pg_terminate_backend(pid)
	FROM pg_stat_activity
	WHERE datname = current_database()
	  AND pid <> pg_backend_pid()
	  AND backend_type = 'client backend'
	  AND application_name IS DISTINCT FROM $1`

// RevokeConnect revokes the CONNECT privilege on the source database from each of
// the given roles (and from PUBLIC, so a role that only had PUBLIC's grant is
// still fenced), so those application roles cannot reconnect and write once the
// source becomes a subscriber. It is the airtight complement to
// TerminateClientBackends + default_transaction_read_only: terminate cuts the
// live backends and read-only defaults new transactions, but a client can
// override the GUC (BEGIN READ WRITE); losing CONNECT cannot be overridden.
// Superusers bypass the CONNECT check, so the migrator's own admin DSN is
// unaffected. GrantConnect reverses it on rollback/teardown. Idempotent; roles
// are validated to exist (and to exclude the DSN role) at create time. A no-op
// when roles is empty (opt-in).
func (s *source) RevokeConnect(roles []string) error {
	if len(roles) == 0 {
		return nil
	}
	db, err := s.currentDatabase()
	if err != nil {
		return err
	}
	for _, stmt := range connectGrantSQL(false, db, roles) {
		if _, err := s.conn.Exec(s.ctx, stmt); err != nil {
			return fmt.Errorf("revoke connect on source: %w", err)
		}
	}
	return nil
}

// GrantConnect restores the CONNECT privilege revoked by RevokeConnect: it grants
// CONNECT back to PUBLIC (the default state of a freshly-created database) and to
// each named role, so applications can reach the source again once it is a
// publisher/primary (a deactivate rollback, or a torn-down migration). Idempotent
// and a no-op when roles is empty. Best-effort at teardown; a failure is logged,
// not returned, so it never blocks a drop.
func (s *source) GrantConnect(roles []string) error {
	if len(roles) == 0 {
		return nil
	}
	db, err := s.currentDatabase()
	if err != nil {
		return err
	}
	for _, stmt := range connectGrantSQL(true, db, roles) {
		if _, err := s.conn.Exec(s.ctx, stmt); err != nil {
			return fmt.Errorf("grant connect on source: %w", err)
		}
	}
	return nil
}

// connectGrantSQL builds the GRANT (grant=true) or REVOKE (grant=false) CONNECT
// statements for the database and roles: one for PUBLIC (so a role that only held
// PUBLIC's grant is still fenced on revoke, and PUBLIC's default access is restored
// on grant) plus one per named role. Every identifier is quoted. Returns nil for an
// empty role list (the callers already short-circuit).
func connectGrantSQL(grant bool, db string, roles []string) []string {
	if len(roles) == 0 {
		return nil
	}
	verb, dir := "REVOKE", "FROM"
	if grant {
		verb, dir = "GRANT", "TO"
	}
	prefix := verb + " CONNECT ON DATABASE " + ast.QuoteIdentifier(db) + " " + dir + " "
	stmts := []string{prefix + "PUBLIC"}
	for _, r := range roles {
		stmts = append(stmts, prefix+ast.QuoteIdentifier(r))
	}
	return stmts
}

// checkQuiesceRoles validates operator-supplied quiesce roles at create time: each
// must exist on the source, and none may be the DSN's own role (current_user) —
// revoking CONNECT from it would lock the migrator out if that role is not a
// superuser (a pg_create_subscription member, say). Returns a clear create-time
// error rather than failing mid-cutover. A no-op when roles is empty.
func (s *source) checkQuiesceRoles(roles []string) error {
	if len(roles) == 0 {
		return nil
	}
	var currentUser string
	if err := s.conn.QueryRow(s.ctx, "SELECT current_user").Scan(&currentUser); err != nil {
		return fmt.Errorf("read source current_user: %w", err)
	}
	for _, r := range roles {
		if r == currentUser {
			return fmt.Errorf("quiesce role %q is the source connection's own role; revoking its CONNECT would lock out the migrator", r)
		}
	}
	rows, err := s.conn.Query(s.ctx,
		`SELECT r FROM unnest($1::text[]) AS r WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) ORDER BY r`, roles)
	if err != nil {
		return fmt.Errorf("check quiesce roles exist: %w", err)
	}
	defer rows.Close()
	var missing []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return fmt.Errorf("scan quiesce role: %w", err)
		}
		missing = append(missing, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("check quiesce roles exist: %w", err)
	}
	if len(missing) > 0 {
		return fmt.Errorf("source quiesce role(s) do not exist: %s", strings.Join(missing, ", "))
	}
	return nil
}

// currentDatabase reads the source connection's database name, needed to build
// GRANT/REVOKE ... ON DATABASE statements (which require the literal name; they do
// not accept current_database()).
func (s *source) currentDatabase() (string, error) {
	var db string
	if err := s.conn.QueryRow(s.ctx, "SELECT current_database()").Scan(&db); err != nil {
		return "", fmt.Errorf("read source current_database: %w", err)
	}
	return db, nil
}

// CurrentLSN returns the source's current WAL LSN (call on a publisher/primary).
func (s *source) CurrentLSN() (string, error) {
	var lsn string
	if err := s.conn.QueryRow(s.ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn); err != nil {
		return "", fmt.Errorf("read source LSN: %w", err)
	}
	return lsn, nil
}

// ReplicationLag returns the current replication lag for the named slot on this
// publisher: byte lag = pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
// (the WAL the subscriber has not yet confirmed-consumed), and time lag = the
// walsender's replay_lag in seconds (0 when no walsender is attached). present is
// false when the slot does not exist yet (nothing has been consumed; the readiness
// gate treats that as not-ready rather than lag zero). Call on the publisher side
// (source in IMPORT). It is both the live readiness metric the cutover polls and the
// lag surfaced in migration status.
func (s *source) ReplicationLag(slot string) (lagBytes uint64, lagSeconds float64, present bool, err error) {
	const lagSQL = `SELECT
		GREATEST(pg_wal_lsn_diff(pg_current_wal_lsn(), sl.confirmed_flush_lsn), 0)::bigint,
		COALESCE(EXTRACT(EPOCH FROM sr.replay_lag), 0)::float8
	FROM pg_replication_slots sl
	LEFT JOIN pg_stat_replication sr ON sr.pid = sl.active_pid
	WHERE sl.slot_name = $1`
	var b int64
	var secs float64
	if err := s.conn.QueryRow(s.ctx, lagSQL, slot).Scan(&b, &secs); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, 0, false, nil
		}
		return 0, 0, false, fmt.Errorf("read source replication lag for slot %q: %w", slot, err)
	}
	return uint64(b), secs, true, nil
}

// SubscriptionExists reports whether a subscription with the given name exists on
// the source. Used to make the direction switch idempotent on resume.
func (s *source) SubscriptionExists(name string) (bool, error) {
	var n int64
	if err := s.conn.QueryRow(s.ctx, "SELECT count(*) FROM pg_subscription WHERE subname = $1", name).Scan(&n); err != nil {
		return false, fmt.Errorf("check source subscription: %w", err)
	}
	return n > 0, nil
}

// PublicationExists reports whether a publication with the given name exists on
// the source.
func (s *source) PublicationExists(name string) (bool, error) {
	var n int64
	if err := s.conn.QueryRow(s.ctx, "SELECT count(*) FROM pg_publication WHERE pubname = $1", name).Scan(&n); err != nil {
		return false, fmt.Errorf("check source publication: %w", err)
	}
	return n > 0, nil
}

// WaitSlotConfirmed blocks until the named replication slot on the source has
// confirmed_flush_lsn >= targetLSN (the subscriber has consumed past the barrier
// point), or the context is done. Call on the publisher side (source in IMPORT).
func (s *source) WaitSlotConfirmed(slot, targetLSN string) error {
	const waitSlotConfirmedSQL = "SELECT confirmed_flush_lsn >= $1::pg_lsn FROM pg_replication_slots WHERE slot_name = $2"
	return pollSlotConfirmed(s.ctx, func(c context.Context) (bool, error) {
		var ok bool
		err := s.conn.QueryRow(c, waitSlotConfirmedSQL, targetLSN, slot).Scan(&ok)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return ok, err
	})
}

// AdvanceSequences sets each migrated table's owned sequences past their current
// max plus margin, so the side about to take writes does not collide with copied
// values (sequences are not replicated).
func (s *source) AdvanceSequences(tables []string, margin int64) error {
	for _, tbl := range tables {
		rows, err := s.conn.Query(s.ctx, ownedSequencesSQL, tbl)
		if err != nil {
			return fmt.Errorf("list sequences for %q: %w", tbl, err)
		}
		var seqs [][2]string // seq, col
		for rows.Next() {
			var col string
			var seq *string
			if err := rows.Scan(&col, &seq); err != nil {
				rows.Close()
				return fmt.Errorf("scan sequence for %q: %w", tbl, err)
			}
			if seq != nil && *seq != "" {
				seqs = append(seqs, [2]string{*seq, col})
			}
		}
		rows.Close()
		for _, sc := range seqs {
			if _, err := s.conn.Exec(s.ctx, setvalSQL(sc[0], sc[1], tbl, margin)); err != nil {
				return fmt.Errorf("advance sequence %s: %w", sc[0], err)
			}
		}
	}
	return nil
}
