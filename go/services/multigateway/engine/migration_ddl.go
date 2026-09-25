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
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// ConnectionStore holds named source connections (name -> libpq conninfo). It is
// the gateway-side store for CREATE CONNECTION objects; a migration created FROM
// a connection resolves the connection to a DSN before calling the migrator.
type ConnectionStore interface {
	Set(name, dsn string)
	Drop(name string) bool
	Get(name string) (string, bool)
	List() []ConnectionEntry
}

// ConnectionEntry is one stored connection.
type ConnectionEntry struct {
	Name string
	DSN  string
}

// InMemoryConnectionStore is a process-local ConnectionStore. It is not
// persisted, so connections are lost when the gateway restarts — sufficient for
// experimenting with the interface; a topo-backed store can replace it later.
type InMemoryConnectionStore struct {
	mu    sync.RWMutex
	conns map[string]string
}

func NewInMemoryConnectionStore() *InMemoryConnectionStore {
	return &InMemoryConnectionStore{conns: map[string]string{}}
}

func (s *InMemoryConnectionStore) Set(name, dsn string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns[name] = dsn
}

func (s *InMemoryConnectionStore) Drop(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.conns[name]
	delete(s.conns, name)
	return ok
}

func (s *InMemoryConnectionStore) Get(name string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dsn, ok := s.conns[name]
	return dsn, ok
}

func (s *InMemoryConnectionStore) List() []ConnectionEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ConnectionEntry, 0, len(s.conns))
	for name, dsn := range s.conns {
		out = append(out, ConnectionEntry{Name: name, DSN: dsn})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// MigrationBackend gives the migration DDL primitive what it needs: a migrator
// gRPC client (resolved lazily so it follows the shard primary) and the
// connection store. TargetDatabase/TargetShard are the target for new
// migrations (the database/cluster the gateway fronts).
type MigrationBackend struct {
	// Client returns a migrator client aimed at the current shard primary, or
	// nil if none is reachable.
	Client func() migratorpb.MigratorClient
	Conns  ConnectionStore

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
	if m.backend == nil || m.backend.Conns == nil {
		return nil, errors.New("the migration interface is not configured on this gateway")
	}
	switch s := m.stmt.(type) {
	case *ast.CreateConnectionStmt:
		if _, exists := m.backend.Conns.Get(s.Name); exists && s.IfNotExists {
			return commandTag("CREATE CONNECTION"), nil
		} else if exists {
			return nil, fmt.Errorf("connection %q already exists", s.Name)
		}
		m.backend.Conns.Set(s.Name, optionsToDSN(s.Options))
		return commandTag("CREATE CONNECTION"), nil

	case *ast.AlterConnectionStmt:
		if _, exists := m.backend.Conns.Get(s.Name); !exists {
			return nil, fmt.Errorf("connection %q does not exist", s.Name)
		}
		// Simplified: replace the stored DSN with the supplied options.
		m.backend.Conns.Set(s.Name, optionsToDSN(s.Options))
		return commandTag("ALTER CONNECTION"), nil

	case *ast.DropConnectionStmt:
		for _, name := range nameList(s.Names) {
			if !m.backend.Conns.Drop(name) && !s.IfExists {
				return nil, fmt.Errorf("connection %q does not exist", name)
			}
		}
		return commandTag("DROP CONNECTION"), nil

	case *ast.ShowConnectionsStmt:
		return m.showConnections(s, includeFields), nil

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
	dsn, ok := m.backend.Conns.Get(s.Connection)
	if !ok {
		return nil, fmt.Errorf("connection %q does not exist", s.Connection)
	}
	client, err := m.backend.client()
	if err != nil {
		return nil, err
	}
	req := &migratorpb.CreateMigrationRequest{
		Name:           s.Name,
		SourceDsn:      dsn,
		TargetDatabase: m.backend.TargetDatabase,
		TargetShard:    m.backend.TargetShard,
		AllTables:      s.ForAllTables,
	}
	if !s.ForAllTables {
		objs, err := selectionObjects(s.Objects)
		if err != nil {
			return nil, err
		}
		req.Objects = objs
	}
	if err := applyMigrationOptions(req, s.Options); err != nil {
		return nil, err
	}
	if _, err := client.CreateMigration(ctx, req); err != nil {
		return nil, err
	}
	return commandTag("CREATE MIGRATION"), nil
}

func (m *MigrationDDL) alterMigration(ctx context.Context, s *ast.AlterMigrationStmt) (*sqltypes.Result, error) {
	client, err := m.backend.client()
	if err != nil {
		return nil, err
	}
	switch s.Action {
	case ast.MigrationActionStart:
		_, err = client.StartMigration(ctx, &migratorpb.StartMigrationRequest{Name: s.Name})
	case ast.MigrationActionActivate:
		req := &migratorpb.ActivateMigrationRequest{Name: s.Name}
		if err = applyActivateOptions(req, s.Options); err != nil {
			return nil, err
		}
		_, err = client.ActivateMigration(ctx, req)
	case ast.MigrationActionDeactivate:
		_, err = client.DeactivateMigration(ctx, &migratorpb.DeactivateMigrationRequest{Name: s.Name})
	case ast.MigrationActionSetConnection:
		dsn, ok := m.backend.Conns.Get(s.Connection)
		if !ok {
			return nil, fmt.Errorf("connection %q does not exist", s.Connection)
		}
		_, err = client.UpdateMigration(ctx, &migratorpb.UpdateMigrationRequest{
			Name:       s.Name,
			SourceDsn:  dsn,
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"source_dsn"}},
		})
	case ast.MigrationActionSetOptions:
		req := &migratorpb.UpdateMigrationRequest{Name: s.Name}
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
			Name:  name,
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

func (m *MigrationDDL) showMigrations(ctx context.Context, s *ast.ShowMigrationsStmt, includeFields bool) (*sqltypes.Result, error) {
	client, err := m.backend.client()
	if err != nil {
		return nil, err
	}
	resp, err := client.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Name: s.Name})
	if err != nil {
		return nil, err
	}
	cols := []string{
		"name", "id", "source", "target_database", "target_shard",
		"phase", "active_direction", "total_relations", "ready_relations",
		"caught_up", "lag_bytes", "lag_seconds", "last_error",
	}
	result := &sqltypes.Result{CommandTag: fmt.Sprintf("SELECT %d", len(resp.GetMigrations()))}
	if includeFields {
		result.Fields = textFields(cols)
	}
	for _, mig := range resp.GetMigrations() {
		result.Rows = append(result.Rows, sqltypes.MakeRow([][]byte{
			[]byte(mig.GetName()),
			[]byte(mig.GetId()),
			[]byte(mig.GetSource()),
			[]byte(mig.GetTargetDatabase()),
			[]byte(mig.GetTargetShard()),
			[]byte(mig.GetPhase().String()),
			[]byte(mig.GetActiveDirection().String()),
			[]byte(strconv.FormatInt(mig.GetTotalRelations(), 10)),
			[]byte(strconv.FormatInt(mig.GetReadyRelations(), 10)),
			[]byte(boolText(mig.GetCaughtUp())),
			[]byte(strconv.FormatUint(mig.GetLagBytes(), 10)),
			[]byte(strconv.FormatFloat(mig.GetLagSeconds(), 'f', 3, 64)),
			[]byte(mig.GetLastError()),
		}))
	}
	return result, nil
}

func (m *MigrationDDL) showConnections(s *ast.ShowConnectionsStmt, includeFields bool) *sqltypes.Result {
	cols := []string{"name", "host", "port", "dbname", "user", "sslmode"}
	result := &sqltypes.Result{}
	if includeFields {
		result.Fields = textFields(cols)
	}
	entries := m.backend.Conns.List()
	for _, e := range entries {
		if s.Name != "" && e.Name != s.Name {
			continue
		}
		kv := parseConnInfo(e.DSN)
		result.Rows = append(result.Rows, sqltypes.MakeRow([][]byte{
			[]byte(e.Name),
			[]byte(kv["host"]),
			[]byte(kv["port"]),
			[]byte(kv["dbname"]),
			[]byte(kv["user"]),
			[]byte(kv["sslmode"]),
		}))
	}
	result.CommandTag = fmt.Sprintf("SELECT %d", len(result.Rows))
	return result
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

// defElemValue returns the option value as a plain string, stripping any
// surrounding quotes the deparser adds around string literals.
func defElemValue(d *ast.DefElem) string {
	if d.Arg == nil {
		return ""
	}
	return strings.Trim(d.Arg.SqlString(), "'\"")
}

// optionsToDSN turns a generic OPTIONS (...) list into a libpq conninfo string.
func optionsToDSN(list *ast.NodeList) string {
	if list == nil {
		return ""
	}
	parts := make([]string, 0, len(list.Items))
	for _, it := range list.Items {
		d, ok := it.(*ast.DefElem)
		if !ok {
			continue
		}
		parts = append(parts, d.Defname+"="+defElemValue(d))
	}
	return strings.Join(parts, " ")
}

// parseConnInfo splits a space-separated libpq conninfo into key/value pairs.
func parseConnInfo(dsn string) map[string]string {
	kv := map[string]string{}
	for tok := range strings.FieldsSeq(dsn) {
		if i := strings.IndexByte(tok, '='); i > 0 {
			kv[tok[:i]] = tok[i+1:]
		}
	}
	return kv
}

// selectionObjects converts a pub_obj_list (from the FOR clause) into migrator
// SelectionObjects.
func selectionObjects(list *ast.NodeList) ([]*migratorpb.SelectionObject, error) {
	if list == nil {
		return nil, nil
	}
	out := make([]*migratorpb.SelectionObject, 0, len(list.Items))
	for _, it := range list.Items {
		spec, ok := it.(*ast.PublicationObjSpec)
		if !ok {
			continue
		}
		switch spec.PubObjType {
		case ast.PUBLICATIONOBJ_TABLE:
			ts := &migratorpb.TableSpec{QualifiedName: rangeVarName(spec.PubTable.Relation)}
			if spec.PubTable.Relation != nil {
				ts.IncludeDescendants = spec.PubTable.Relation.Inh
			}
			ts.Columns = append(ts.Columns, nameList(spec.PubTable.Columns)...)
			if spec.PubTable.WhereClause != nil {
				ts.Where = spec.PubTable.WhereClause.SqlString()
			}
			out = append(out, &migratorpb.SelectionObject{
				Object: &migratorpb.SelectionObject_Table{Table: ts},
			})
		case ast.PUBLICATIONOBJ_TABLES_IN_SCHEMA:
			out = append(out, &migratorpb.SelectionObject{
				Object: &migratorpb.SelectionObject_Schema{Schema: spec.Name},
			})
		default:
			return nil, errors.New("unsupported table-selection object")
		}
	}
	return out, nil
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
// ActivateMigrationRequest. Recognized options: max_lag_bytes (integer bytes) and
// wait_timeout (a duration string like '30s', or a bare integer in seconds).
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
			n, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				return fmt.Errorf("max_lag_bytes must be a non-negative integer: %q", val)
			}
			req.MaxLagBytes = n
		case "wait_timeout":
			secs, err := parseTimeoutSeconds(val)
			if err != nil {
				return err
			}
			req.WaitTimeoutSeconds = secs
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
			b := isTruthy(val)
			req.CopyData = &b
		case "skip_schema_copy":
			req.SkipSchemaCopy = isTruthy(val)
		case "sequence_margin":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return fmt.Errorf("sequence_margin must be an integer: %q", val)
			}
			req.SequenceMargin = n
		case "source_publication":
			req.SourcePublication = val
		case "publish_via_partition_root":
			req.PublishViaPartitionRoot = isTruthy(val)
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
			req.SequenceMargin = n
			paths = append(paths, "sequence_margin")
		default:
			return nil, fmt.Errorf("option %q cannot be changed with ALTER MIGRATION ... SET", d.Defname)
		}
	}
	return paths, nil
}

func isTruthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "on", "1", "yes", "t":
		return true
	default:
		return false
	}
}
