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

package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/tools/humansize"
)

// MigrationBackend gives the migration DDL primitive what it needs: a migrator
// gRPC client (resolved lazily so it follows the shard primary). Named
// connections are no longer gateway-local — CREATE/ALTER/DROP/SHOW CONNECTION
// all go through the same client, so they persist in the shard's Postgres (see
// migration.Connection) and are visible from any gateway replica.
// TargetDatabase/TargetShard are the target for new migrations (the
// database/cluster the gateway fronts).
type MigrationBackend struct {
	// Client returns a migrator client aimed at the current shard primary, or
	// nil if none is reachable.
	Client func() migratorpb.MigratorClient

	TargetDatabase string
	TargetShard    string
}

func (b *MigrationBackend) client() (migratorpb.MigratorClient, error) {
	if b == nil || b.Client == nil {
		return nil, errors.New("migration interface is not configured on this gateway")
	}
	c := b.Client()
	if c == nil {
		return nil, errors.New("no migrator is currently reachable (shard primary unavailable)")
	}
	return c, nil
}

// MigrationDDL is the gateway-local primitive for the migration/connection DDL
// statements. It never touches PostgreSQL; it manipulates the connection store
// and calls migrator RPCs, synthesizing the result set in-gateway.
type MigrationDDL struct {
	sql     string
	stmt    ast.Stmt
	backend *MigrationBackend
}

// NewMigrationDDL builds the primitive for one intercepted statement.
func NewMigrationDDL(sql string, stmt ast.Stmt, backend *MigrationBackend) *MigrationDDL {
	return &MigrationDDL{sql: sql, stmt: stmt, backend: backend}
}

func (m *MigrationDDL) GetTableGroup() string { return "" }
func (m *MigrationDDL) GetQuery() string      { return m.sql }
func (m *MigrationDDL) String() string        { return "MigrationDDL(" + m.sql + ")" }

func (m *MigrationDDL) StreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	_ *handler.MultigatewayConnectionState,
	_ []*ast.A_Const,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	result, err := m.execute(ctx, true)
	if err != nil {
		return err
	}
	return callback(ctx, result)
}

func (m *MigrationDDL) PortalStreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	_ *handler.MultigatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	includeDescribe bool,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	result, err := m.execute(ctx, includeDescribe)
	if err != nil {
		return err
	}
	return callback(ctx, result)
}

// execute dispatches on the statement type. includeFields controls whether a
// result set attaches column metadata (see GatewayShowVersion for the rationale).
func (m *MigrationDDL) execute(ctx context.Context, includeFields bool) (*sqltypes.Result, error) {
	if m.backend == nil {
		return nil, errors.New("the migration interface is not configured on this gateway")
	}
	switch s := m.stmt.(type) {
	case *ast.CreateConnectionStmt:
		client, err := m.backend.client()
		if err != nil {
			return nil, err
		}
		if _, err := client.GetConnection(ctx, &migratorpb.GetConnectionRequest{Ref: connByName(s.Name)}); err == nil {
			if s.IfNotExists {
				return commandTag("CREATE CONNECTION"), nil
			}
			return nil, fmt.Errorf("connection %q already exists", s.Name)
		}
		dsn, err := connectionDSN(s.Options)
		if err != nil {
			return nil, err
		}
		if _, err := client.CreateConnection(ctx, &migratorpb.CreateConnectionRequest{Connection: &migratorpb.Connection{
			Name: s.Name,
			Dsn:  dsn,
		}}); err != nil {
			return nil, err
		}
		return commandTag("CREATE CONNECTION"), nil

	case *ast.AlterConnectionStmt:
		client, err := m.backend.client()
		if err != nil {
			return nil, err
		}
		existing, err := client.GetConnection(ctx, &migratorpb.GetConnectionRequest{Ref: connByName(s.Name)})
		if err != nil {
			return nil, fmt.Errorf("connection %q does not exist", s.Name)
		}
		// Merge onto the current option set: ADD/SET/DROP touch only the named
		// option, leaving the rest as they were (see applyConnectionOptions).
		current := parseConnInfo(existing.GetConnection().GetDsn())
		if err := applyConnectionOptions(current, s.Options); err != nil {
			return nil, err
		}
		if _, ok := current["host"]; !ok {
			return nil, errors.New("CONNECTION requires a host option")
		}
		if _, err := client.UpdateConnection(ctx, &migratorpb.UpdateConnectionRequest{
			Connection: &migratorpb.Connection{Id: existing.GetConnection().GetId(), Dsn: connInfoString(current)},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"dsn"}},
		}); err != nil {
			return nil, err
		}
		return commandTag("ALTER CONNECTION"), nil

	case *ast.DropConnectionStmt:
		client, err := m.backend.client()
		if err != nil {
			return nil, err
		}
		for _, name := range nameList(s.Names) {
			if _, err := client.DropConnection(ctx, &migratorpb.DropConnectionRequest{Ref: connByName(name), IfExists: s.IfExists}); err != nil {
				return nil, fmt.Errorf("connection %q: %w", name, err)
			}
		}
		return commandTag("DROP CONNECTION"), nil

	case *ast.ShowConnectionsStmt:
		return m.showConnections(ctx, s, includeFields)

	case *ast.CreateMigrationStmt:
		return m.createMigration(ctx, s)

	case *ast.AlterMigrationStmt:
		return m.alterMigration(ctx, s)

	case *ast.DropMigrationStmt:
		return m.dropMigration(ctx, s)

	case *ast.ShowMigrationsStmt:
		return m.showMigrations(ctx, s, includeFields)

	default:
		return nil, fmt.Errorf("unsupported migration statement %T", m.stmt)
	}
}

func (m *MigrationDDL) createMigration(ctx context.Context, s *ast.CreateMigrationStmt) (*sqltypes.Result, error) {
	client, err := m.backend.client()
	if err != nil {
		return nil, err
	}
	req := &migratorpb.CreateMigrationRequest{
		Migration: &migratorpb.Migration{
			Name:           s.Name,
			ConnectionName: s.Connection,
			TargetDatabase: m.backend.TargetDatabase,
			TargetShard:    m.backend.TargetShard,
		},
	}
	if s.ForAllTables {
		req.Migration.Objects = &migratorpb.SelectionObject{Object: &migratorpb.SelectionObject_All{All: true}}
	} else {
		obj, err := selectionObjects(s.Objects)
		if err != nil {
			return nil, err
		}
		req.Migration.Objects = obj
	}
	if err := applyMigrationOptions(req, s.Options); err != nil {
		return nil, err
	}
	if _, err := client.CreateMigration(ctx, req); err != nil {
		return nil, err
	}
	return commandTag("CREATE MIGRATION"), nil
}

// refByName addresses a migration by its unique name, for the DDL primitive's
// ALTER/DROP/SHOW statements (which only ever carry a name, not an id).
func refByName(name string) *migratorpb.MigrationRef {
	return &migratorpb.MigrationRef{Ref: &migratorpb.MigrationRef_Name{Name: name}}
}

func refByID(id int64) *migratorpb.MigrationRef {
	return &migratorpb.MigrationRef{Ref: &migratorpb.MigrationRef_Id{Id: id}}
}

// connByName addresses a connection by its unique name, for the DDL
// primitive's CREATE/ALTER/DROP/SHOW CONNECTION statements.
func connByName(name string) *migratorpb.ConnectionRef {
	return &migratorpb.ConnectionRef{Ref: &migratorpb.ConnectionRef_Name{Name: name}}
}

func (m *MigrationDDL) alterMigration(ctx context.Context, s *ast.AlterMigrationStmt) (*sqltypes.Result, error) {
	client, err := m.backend.client()
	if err != nil {
		return nil, err
	}
	switch s.Action {
	case ast.MigrationActionStart:
		_, err = client.StartMigration(ctx, &migratorpb.StartMigrationRequest{Ref: refByName(s.Name)})
	case ast.MigrationActionActivate:
		req := &migratorpb.ActivateMigrationRequest{Ref: refByName(s.Name)}
		if err = applyActivateOptions(req, s.Options); err != nil {
			return nil, err
		}
		_, err = client.ActivateMigration(ctx, req)
	case ast.MigrationActionDeactivate:
		_, err = client.DeactivateMigration(ctx, &migratorpb.DeactivateMigrationRequest{Ref: refByName(s.Name)})
	case ast.MigrationActionSetConnection:
		_, err = client.UpdateMigration(ctx, &migratorpb.UpdateMigrationRequest{
			Migration:  &migratorpb.Migration{Name: s.Name, ConnectionName: s.Connection},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"connection_name"}},
		})
	case ast.MigrationActionSetOptions:
		req := &migratorpb.UpdateMigrationRequest{Migration: &migratorpb.Migration{Name: s.Name}}
		var paths []string
		paths, err = applyUpdateOptions(req, s.Options)
		if err != nil {
			return nil, err
		}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: paths}
		_, err = client.UpdateMigration(ctx, req)
	default:
		return nil, errors.New("unsupported ALTER MIGRATION action")
	}
	if err != nil {
		return nil, err
	}
	return commandTag("ALTER MIGRATION"), nil
}

func (m *MigrationDDL) dropMigration(ctx context.Context, s *ast.DropMigrationStmt) (*sqltypes.Result, error) {
	client, err := m.backend.client()
	if err != nil {
		return nil, err
	}
	for _, name := range nameList(s.Names) {
		req := &migratorpb.DropMigrationRequest{
			Ref:   refByName(name),
			Force: s.Force,
			Wait:  s.Wait,
		}
		if s.HasTimeout {
			req.WaitTimeoutSeconds = int64(s.WaitTimeout)
		}
		if _, err := client.DropMigration(ctx, req); err != nil {
			// IF EXISTS swallows "not found"; other errors propagate.
			if s.IfExists {
				continue
			}
			return nil, err
		}
	}
	return commandTag("DROP MIGRATION"), nil
}

// fetchMigrations returns one migration by name, or every migration when name
// is empty: it lists ids via ListMigrations and fetches each one's full info
// via GetMigration (GetMigrationsResponse no longer carries a "list all"
// shape now that GetMigration returns exactly one migration).
func fetchMigrations(ctx context.Context, client migratorpb.MigratorClient, name string) ([]*migratorpb.GetMigrationResponse, error) {
	if name != "" {
		resp, err := client.GetMigration(ctx, &migratorpb.GetMigrationRequest{Ref: refByName(name)})
		if err != nil {
			return nil, err
		}
		return []*migratorpb.GetMigrationResponse{resp}, nil
	}
	list, err := client.ListMigrations(ctx, &migratorpb.ListMigrationsRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]*migratorpb.GetMigrationResponse, 0, len(list.GetIds()))
	for _, id := range list.GetIds() {
		resp, err := client.GetMigration(ctx, &migratorpb.GetMigrationRequest{Ref: refByID(id)})
		if err != nil {
			return nil, err
		}
		out = append(out, resp)
	}
	return out, nil
}

func (m *MigrationDDL) showMigrations(ctx context.Context, s *ast.ShowMigrationsStmt, includeFields bool) (*sqltypes.Result, error) {
	client, err := m.backend.client()
	if err != nil {
		return nil, err
	}
	migrations, err := fetchMigrations(ctx, client, s.Name)
	if err != nil {
		return nil, err
	}
	cols := []string{
		"name", "id", "connection_name", "target_database", "target_shard",
		"phase", "active_direction", "total_relations", "ready_relations",
		"caught_up", "lag_bytes", "lag_seconds", "last_error",
	}
	result := &sqltypes.Result{CommandTag: fmt.Sprintf("SELECT %d", len(migrations))}
	if includeFields {
		result.Fields = textFields(cols)
	}
	for _, mig := range migrations {
		cfg, st := mig.GetMigration(), mig.GetStatus()
		result.Rows = append(result.Rows, sqltypes.MakeRow([][]byte{
			[]byte(cfg.GetName()),
			[]byte(strconv.FormatInt(cfg.GetId(), 10)),
			[]byte(cfg.GetConnectionName()),
			[]byte(cfg.GetTargetDatabase()),
			[]byte(cfg.GetTargetShard()),
			[]byte(st.GetPhase().String()),
			[]byte(st.GetActiveDirection().String()),
			[]byte(strconv.FormatInt(st.GetTotalRelations(), 10)),
			[]byte(strconv.FormatInt(st.GetReadyRelations(), 10)),
			[]byte(boolText(st.GetCaughtUp())),
			[]byte(strconv.FormatUint(st.GetLagBytes(), 10)),
			[]byte(strconv.FormatFloat(st.GetLagSeconds(), 'f', 3, 64)),
			[]byte(st.GetLastError()),
		}))
	}
	return result, nil
}

func (m *MigrationDDL) showConnections(ctx context.Context, s *ast.ShowConnectionsStmt, includeFields bool) (*sqltypes.Result, error) {
	client, err := m.backend.client()
	if err != nil {
		return nil, err
	}
	resp, err := client.ListConnections(ctx, &migratorpb.ListConnectionsRequest{})
	if err != nil {
		return nil, err
	}
	cols := []string{"name", "host", "port", "dbname", "user", "sslmode"}
	result := &sqltypes.Result{}
	if includeFields {
		result.Fields = textFields(cols)
	}
	for _, e := range resp.GetConnections() {
		if s.Name != "" && e.GetName() != s.Name {
			continue
		}
		kv := parseConnInfo(e.GetDsn())
		result.Rows = append(result.Rows, sqltypes.MakeRow([][]byte{
			[]byte(e.GetName()),
			[]byte(kv["host"]),
			[]byte(kv["port"]),
			[]byte(kv["dbname"]),
			[]byte(kv["user"]),
			[]byte(kv["sslmode"]),
		}))
	}
	result.CommandTag = fmt.Sprintf("SELECT %d", len(result.Rows))
	return result, nil
}

// ---- helpers ----

func commandTag(tag string) *sqltypes.Result {
	return &sqltypes.Result{CommandTag: tag}
}

func textFields(names []string) []*query.Field {
	fields := make([]*query.Field, len(names))
	for i, n := range names {
		fields[i] = textField(n)
	}
	return fields
}

func boolText(b bool) string {
	if b {
		return "t"
	}
	return "f"
}

// nameList extracts identifier strings from a NodeList of *String.
func nameList(list *ast.NodeList) []string {
	if list == nil {
		return nil
	}
	out := make([]string, 0, len(list.Items))
	for _, it := range list.Items {
		if s, ok := it.(*ast.String); ok {
			out = append(out, s.SVal)
		}
	}
	return out
}

// defElemValue returns the option value as a plain, unescaped string. This
// serves two different grammar productions: CONNECTION OPTIONS values are
// always Sconst (postgres.y's generic_option_arg: "the spec only requires
// string literals"), but WITH (...) option values are def_arg, which also
// allows a bare NumericOnly (e.g. sequence_margin = 1). For an *ast.String,
// SVal is read directly — it's already the unescaped value, unlike
// SqlString(), which re-serializes it AS a SQL literal (quoted, with any
// backslash/quote re-escaped) and is the wrong direction for recovering the
// original value. Any other literal type (Integer, Float, ...) never needs
// unescaping, so SqlString() is exact for those.
func defElemValue(d *ast.DefElem) string {
	if d.Arg == nil {
		return ""
	}
	if s, ok := d.Arg.(*ast.String); ok {
		return s.SVal
	}
	return d.Arg.SqlString()
}

// connectionDSN turns a CONNECTION OPTIONS (...) list into a libpq conninfo
// string. It requires a host and fills defaults for the keys most likely to be
// omitted, rather than leaning on libpq's own defaults — those resolve in the
// target backend's context, where an omitted host means a LOCAL socket (not the
// external source) and an omitted user/dbname means the backend's OS user. The
// applied defaults: port=5432, dbname=postgres (the default database on stock
// PostgreSQL and Supabase images), sslmode=disable. Explicitly-supplied options
// win; a host must always be given.
func connectionDSN(list *ast.NodeList) (string, error) {
	seen := make(map[string]bool)
	var parts []string
	if list != nil {
		for _, it := range list.Items {
			d, ok := it.(*ast.DefElem)
			if !ok {
				continue
			}
			// Lowercase the stored key too, not just the seen-set: ALTER
			// CONNECTION's applyConnectionOptions always looks up options by
			// lowercased name (option names are case-insensitive SQL
			// identifiers), and SHOW CONNECTIONS reads fixed lowercase keys —
			// a mixed-case key stored here would be invisible to both.
			key := strings.ToLower(d.Defname)
			seen[key] = true
			parts = append(parts, key+"="+quoteConnInfoValue(defElemValue(d)))
		}
	}
	if !seen["host"] {
		return "", errors.New("CONNECTION requires a host option")
	}
	for _, def := range []struct{ key, val string }{
		{"port", "5432"},
		{"dbname", "postgres"},
		{"sslmode", "disable"},
	} {
		if !seen[def.key] {
			parts = append(parts, def.key+"="+def.val)
		}
	}
	return strings.Join(parts, " "), nil
}

// quoteConnInfoValue escapes v for embedding as a libpq keyword=value conninfo
// value, always producing the single-quoted form so empty strings, embedded
// spaces, backslashes, and quotes all round-trip correctly through libpq's own
// parser (see pgconn.parseKeywordValueSettings, and parseConnInfo below, which
// both reverse this exact escaping: \\ -> \ and \' -> ').
func quoteConnInfoValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)
	return "'" + v + "'"
}

// isConnInfoSpace reports whether b is whitespace by libpq conninfo's own
// rules (matches pgconn's asciiSpace table: space, tab, newline, CR, vertical
// tab, form feed).
func isConnInfoSpace(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	default:
		return false
	}
}

// unescapeConnInfoValue reverses quoteConnInfoValue's escaping.
func unescapeConnInfoValue(s string) string {
	s = strings.ReplaceAll(s, `\\`, `\`)
	return strings.ReplaceAll(s, `\'`, `'`)
}

// parseConnInfo splits a libpq-style keyword=value conninfo string into a
// key/value map. A value is either a run of non-whitespace characters, or a
// '...'-quoted string — both forms may backslash-escape a literal backslash
// or quote — matching libpq's own tokenizer (pgconn.parseKeywordValueSettings)
// so this stays the exact inverse of quoteConnInfoValue/connInfoString: a
// value containing a space or quote must round-trip through
// GetConnection -> parseConnInfo -> applyConnectionOptions -> connInfoString ->
// UpdateConnection (ALTER CONNECTION's merge path) without being corrupted or
// truncated at the first space.
func parseConnInfo(dsn string) map[string]string {
	kv := map[string]string{}
	s := dsn
	for len(s) > 0 && isConnInfoSpace(s[0]) {
		s = s[1:]
	}
	for len(s) > 0 {
		eqIdx := strings.IndexByte(s, '=')
		if eqIdx < 0 {
			break
		}
		key := strings.ToLower(strings.TrimSpace(s[:eqIdx]))
		s = s[eqIdx+1:]
		for len(s) > 0 && isConnInfoSpace(s[0]) {
			s = s[1:]
		}

		var raw string
		switch {
		case len(s) == 0:
			raw = ""
		case s[0] != '\'':
			end := 0
			for end < len(s) && !isConnInfoSpace(s[end]) {
				if s[end] == '\\' {
					end++
					if end >= len(s) {
						break
					}
				}
				end++
			}
			raw = s[:end]
			s = s[end:]
		default:
			s = s[1:]
			end := 0
			for end < len(s) && s[end] != '\'' {
				if s[end] == '\\' {
					end++
					if end >= len(s) {
						break
					}
				}
				end++
			}
			raw = s[:min(end, len(s))]
			if end < len(s) {
				end++ // consume the closing quote
			}
			s = s[end:]
		}
		for len(s) > 0 && isConnInfoSpace(s[0]) {
			s = s[1:]
		}

		if key != "" {
			kv[key] = unescapeConnInfoValue(raw)
		}
	}
	return kv
}

// connInfoString serializes a conninfo map back to libpq's space-separated
// "k=v" form, in sorted key order for a deterministic result. Every value is
// quoted via quoteConnInfoValue so a value containing a space, quote, or
// backslash round-trips through parseConnInfo unchanged.
func connInfoString(kv map[string]string) string {
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + quoteConnInfoValue(kv[k])
	}
	return strings.Join(parts, " ")
}

// applyConnectionOptions merges an ALTER CONNECTION OPTIONS list onto an
// existing conninfo map, in place, per each option's ADD/SET/DROP action
// (DefElem.Defaction) — unlike CREATE CONNECTION's connectionDSN, which always
// starts from nothing, this only ever touches the options actually named:
//   - ADD requires the option is not already set (else "already exists").
//   - SET requires the option is already set (else "does not exist").
//   - DROP requires the option is already set (else "does not exist"); removes it.
//   - unspecified (no ADD/SET/DROP keyword) upserts: sets regardless of
//     whether the option was already present.
func applyConnectionOptions(current map[string]string, list *ast.NodeList) error {
	if list == nil {
		return nil
	}
	for _, it := range list.Items {
		d, ok := it.(*ast.DefElem)
		if !ok {
			continue
		}
		name := strings.ToLower(d.Defname)
		_, exists := current[name]
		switch d.Defaction {
		case ast.DEFELEM_ADD:
			if exists {
				return fmt.Errorf("option %q already exists", name)
			}
			current[name] = defElemValue(d)
		case ast.DEFELEM_SET:
			if !exists {
				return fmt.Errorf("option %q does not exist", name)
			}
			current[name] = defElemValue(d)
		case ast.DEFELEM_DROP:
			if !exists {
				return fmt.Errorf("option %q does not exist", name)
			}
			delete(current, name)
		default: // DEFELEM_UNSPEC
			current[name] = defElemValue(d)
		}
	}
	return nil
}

// selectionObjects converts a pub_obj_list (from the FOR clause) into a single
// migrator SelectionObject: a table list or a schema list, never a mix —
// mirroring Postgres's own restriction that CREATE PUBLICATION's FOR clause is
// one form or the other. Per-table column lists, WHERE filters, and ONLY
// (excluding partition descendants) are not carried on the wire, so a caller
// that specifies any of them gets an explicit error rather than a silently
// dropped clause.
func selectionObjects(list *ast.NodeList) (*migratorpb.SelectionObject, error) {
	if list == nil {
		return nil, nil
	}
	var tables, schemas []string
	for _, it := range list.Items {
		spec, ok := it.(*ast.PublicationObjSpec)
		if !ok {
			continue
		}
		switch spec.PubObjType {
		case ast.PUBLICATIONOBJ_TABLE:
			if len(schemas) > 0 {
				return nil, errors.New("migrator: a migration's FOR clause cannot mix tables and schemas")
			}
			pt := spec.PubTable
			if pt.Relation != nil && !pt.Relation.Inh {
				return nil, errors.New("migrator: ONLY (excluding partition descendants) is not yet supported")
			}
			if pt.Columns != nil && len(pt.Columns.Items) > 0 {
				return nil, errors.New("migrator: per-table column lists are not yet supported")
			}
			if pt.WhereClause != nil {
				return nil, errors.New("migrator: per-table WHERE row filters are not yet supported")
			}
			tables = append(tables, rangeVarName(pt.Relation))
		case ast.PUBLICATIONOBJ_TABLES_IN_SCHEMA:
			if len(tables) > 0 {
				return nil, errors.New("migrator: a migration's FOR clause cannot mix tables and schemas")
			}
			schemas = append(schemas, spec.Name)
		default:
			return nil, errors.New("unsupported table-selection object")
		}
	}
	switch {
	case len(tables) > 0:
		return &migratorpb.SelectionObject{Object: &migratorpb.SelectionObject_Table{
			Table: &migratorpb.TableSpec{QualifiedNames: tables},
		}}, nil
	case len(schemas) > 0:
		return &migratorpb.SelectionObject{Object: &migratorpb.SelectionObject_Schema{
			Schema: &migratorpb.SchemaSpec{Schemata: schemas},
		}}, nil
	default:
		return nil, nil
	}
}

func rangeVarName(rv *ast.RangeVar) string {
	if rv == nil {
		return ""
	}
	if rv.SchemaName != "" {
		return rv.SchemaName + "." + rv.RelName
	}
	return rv.RelName
}

// applyActivateOptions maps an ACTIVATE WITH (...) option list onto
// ActivateMigrationRequest. Recognized options: max_lag_bytes (a byte count or a
// size literal like '8 MiB', see humansize.ParseBytes) and wait_timeout (a
// duration string like '30s', or a bare integer in seconds).
func applyActivateOptions(req *migratorpb.ActivateMigrationRequest, list *ast.NodeList) error {
	if list == nil {
		return nil
	}
	for _, it := range list.Items {
		d, ok := it.(*ast.DefElem)
		if !ok {
			continue
		}
		val := defElemValue(d)
		switch strings.ToLower(d.Defname) {
		case "max_lag_bytes":
			n, err := humansize.ParseBytes(val)
			if err != nil {
				return fmt.Errorf("max_lag_bytes: %w", err)
			}
			req.MaxLagBytes = &n
		case "wait_timeout":
			secs, err := parseTimeoutSeconds(val)
			if err != nil {
				return err
			}
			req.WaitTimeoutSeconds = &secs
		default:
			return fmt.Errorf("unknown ACTIVATE option %q", d.Defname)
		}
	}
	return nil
}

// parseTimeoutSeconds accepts a Go duration string ('30s', '2m') or a bare integer
// number of seconds, returning whole seconds. It rejects sub-second and negative
// values (the RPC field is integer seconds).
func parseTimeoutSeconds(val string) (int64, error) {
	if n, err := strconv.ParseInt(val, 10, 64); err == nil {
		if n < 0 {
			return 0, fmt.Errorf("wait_timeout must not be negative: %q", val)
		}
		return n, nil
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return 0, fmt.Errorf("wait_timeout must be a duration (e.g. '30s') or integer seconds: %q", val)
	}
	if d < 0 {
		return 0, fmt.Errorf("wait_timeout must not be negative: %q", val)
	}
	return int64(d / time.Second), nil
}

// applyMigrationOptions maps a WITH (...) option list onto CreateMigrationRequest.
func applyMigrationOptions(req *migratorpb.CreateMigrationRequest, list *ast.NodeList) error {
	if list == nil {
		return nil
	}
	for _, it := range list.Items {
		d, ok := it.(*ast.DefElem)
		if !ok {
			continue
		}
		val := defElemValue(d)
		switch strings.ToLower(d.Defname) {
		case "copy_data":
			// SQL keeps the positive "copy_data" spelling; the wire field is the
			// inverted skip_copy_data (see its proto comment).
			req.SkipCopyData = !isTruthy(val)
		case "skip_schema_copy":
			req.SkipSchemaCopy = isTruthy(val)
		case "sequence_margin":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return fmt.Errorf("sequence_margin must be an integer: %q", val)
			}
			req.Migration.SequenceMargin = n
		case "quiesce_roles":
			req.QuiesceRoles = parseRoleList(val)
		default:
			return fmt.Errorf("unknown migration option %q", d.Defname)
		}
	}
	return nil
}

// applyUpdateOptions maps ALTER MIGRATION SET (...) onto UpdateMigrationRequest,
// returning the field-mask paths for the options that were set.
func applyUpdateOptions(req *migratorpb.UpdateMigrationRequest, list *ast.NodeList) ([]string, error) {
	var paths []string
	if list == nil {
		return paths, nil
	}
	for _, it := range list.Items {
		d, ok := it.(*ast.DefElem)
		if !ok {
			continue
		}
		val := defElemValue(d)
		switch strings.ToLower(d.Defname) {
		case "sequence_margin":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("sequence_margin must be an integer: %q", val)
			}
			req.Migration.SequenceMargin = n
			paths = append(paths, "sequence_margin")
		default:
			return nil, fmt.Errorf("option %q cannot be changed with ALTER MIGRATION ... SET", d.Defname)
		}
	}
	return paths, nil
}

// parseRoleList splits a comma-separated quiesce_roles option value into trimmed,
// non-empty role names (e.g. "app, reporting" -> ["app","reporting"]).
func parseRoleList(v string) []string {
	var roles []string
	for r := range strings.SplitSeq(v, ",") {
		if r = strings.TrimSpace(r); r != "" {
			roles = append(roles, r)
		}
	}
	return roles
}

func isTruthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "on", "1", "yes", "t":
		return true
	default:
		return false
	}
}
