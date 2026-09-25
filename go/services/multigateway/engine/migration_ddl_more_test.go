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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/sqltypes"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
)

// failMigrator returns errBackend from every RPC, to drive the RPC-error paths.
type failMigrator struct{}

var errBackend = errors.New("backend boom")

func (failMigrator) CreateMigration(context.Context, *migratorpb.CreateMigrationRequest, ...grpc.CallOption) (*migratorpb.CreateMigrationResponse, error) {
	return nil, errBackend
}

func (failMigrator) StartMigration(context.Context, *migratorpb.StartMigrationRequest, ...grpc.CallOption) (*migratorpb.StartMigrationResponse, error) {
	return nil, errBackend
}

func (failMigrator) UpdateMigration(context.Context, *migratorpb.UpdateMigrationRequest, ...grpc.CallOption) (*migratorpb.UpdateMigrationResponse, error) {
	return nil, errBackend
}

func (failMigrator) GetMigrations(context.Context, *migratorpb.GetMigrationsRequest, ...grpc.CallOption) (*migratorpb.GetMigrationsResponse, error) {
	return nil, errBackend
}

func (failMigrator) ActivateMigration(context.Context, *migratorpb.ActivateMigrationRequest, ...grpc.CallOption) (*migratorpb.ActivateMigrationResponse, error) {
	return nil, errBackend
}

func (failMigrator) DeactivateMigration(context.Context, *migratorpb.DeactivateMigrationRequest, ...grpc.CallOption) (*migratorpb.DeactivateMigrationResponse, error) {
	return nil, errBackend
}

func (failMigrator) DropMigration(context.Context, *migratorpb.DropMigrationRequest, ...grpc.CallOption) (*migratorpb.DropMigrationResponse, error) {
	return nil, errBackend
}

// --- accessors and portal path ---

func TestMigrationDDL_Accessors(t *testing.T) {
	p := NewMigrationDDL("SHOW MIGRATIONS", nil, nil)
	assert.Equal(t, "", p.GetTableGroup())
	assert.Equal(t, "SHOW MIGRATIONS", p.GetQuery())
	assert.Equal(t, "MigrationDDL(SHOW MIGRATIONS)", p.String())
}

// portalRunSQL executes via the extended-query portal path, letting the caller
// choose whether column metadata is attached (includeDescribe).
func portalRunSQL(t *testing.T, backend *MigrationBackend, sql string, includeDescribe bool) (*sqltypes.Result, error) {
	t.Helper()
	stmts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	p := NewMigrationDDL(sql, stmts[0], backend)
	var got *sqltypes.Result
	execErr := p.PortalStreamExecute(context.Background(), nil, nil, nil, nil, 0, includeDescribe, PlanExecInfo{},
		func(_ context.Context, r *sqltypes.Result) error { got = r; return nil })
	return got, execErr
}

func TestMigrationDDL_PortalStreamExecute(t *testing.T) {
	backend := newTestBackend(&fakeMigrator{})
	backend.Conns.Set("c", "host=h port=5432 dbname=app")

	// includeDescribe=false omits the column metadata.
	res, err := portalRunSQL(t, backend, "SHOW CONNECTION c", false)
	require.NoError(t, err)
	assert.Empty(t, res.Fields)
	require.Len(t, res.Rows, 1)

	// includeDescribe=true attaches fields.
	res, err = portalRunSQL(t, backend, "SHOW CONNECTION c", true)
	require.NoError(t, err)
	assert.NotEmpty(t, res.Fields)
}

// --- backend / client error paths ---

func TestMigrationDDL_BackendConfigErrors(t *testing.T) {
	stmt, _ := parser.ParseSQL("CREATE MIGRATION m CONNECTION c FOR ALL TABLES")

	// Conns present but Client unset: helpers that need the migrator fail.
	noClient := &MigrationBackend{Conns: NewInMemoryConnectionStore()}
	noClient.Conns.Set("c", "host=h")
	p := NewMigrationDDL("sql", stmt[0], noClient)
	err := p.StreamExecute(context.Background(), nil, nil, nil, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	assert.ErrorContains(t, err, "not configured")

	// Client set but returns nil: no migrator reachable.
	nilClient := &MigrationBackend{
		Conns:  NewInMemoryConnectionStore(),
		Client: func() migratorpb.MigratorClient { return nil },
	}
	nilClient.Conns.Set("c", "host=h")
	p = NewMigrationDDL("sql", stmt[0], nilClient)
	err = p.StreamExecute(context.Background(), nil, nil, nil, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	assert.ErrorContains(t, err, "no migrator")

	// Conns unset entirely: execute rejects before dispatch.
	noConns := &MigrationBackend{Client: func() migratorpb.MigratorClient { return &fakeMigrator{} }}
	p = NewMigrationDDL("sql", stmt[0], noConns)
	err = p.StreamExecute(context.Background(), nil, nil, nil, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	assert.ErrorContains(t, err, "not configured")
}

func TestMigrationDDL_UnsupportedStatement(t *testing.T) {
	// A non-migration statement reaches the dispatch default arm.
	stmts, err := parser.ParseSQL("SELECT 1")
	require.NoError(t, err)
	p := NewMigrationDDL("SELECT 1", stmts[0], newTestBackend(&fakeMigrator{}))
	err = p.StreamExecute(context.Background(), nil, nil, nil, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	assert.ErrorContains(t, err, "unsupported migration statement")
}

// --- RPC-error propagation for each migration verb ---

func TestMigrationDDL_RPCErrors(t *testing.T) {
	backend := newTestBackend(failMigrator{})
	backend.Conns.Set("c", "host=h")

	for _, sql := range []string{
		"CREATE MIGRATION m CONNECTION c FOR ALL TABLES",
		"ALTER MIGRATION m START",
		"ALTER MIGRATION m ACTIVATE",
		"ALTER MIGRATION m DEACTIVATE",
		"ALTER MIGRATION m CONNECTION c",
		"ALTER MIGRATION m SET (sequence_margin = 1)",
		"SHOW MIGRATION m",
	} {
		_, err := runSQL(t, backend, sql)
		assert.ErrorContains(t, err, "boom", "sql: %s", sql)
	}

	// DROP without IF EXISTS propagates the RPC error...
	_, err := runSQL(t, backend, "DROP MIGRATION m")
	assert.ErrorContains(t, err, "boom")
	// ...but IF EXISTS swallows it and reports success.
	_, err = runSQL(t, backend, "DROP MIGRATION IF EXISTS m")
	assert.NoError(t, err)
}

// --- ALTER MIGRATION validation errors (before any RPC) ---

func TestMigrationDDL_AlterValidation(t *testing.T) {
	backend := newTestBackend(&fakeMigrator{})

	// SET CONNECTION to an unknown connection.
	_, err := runSQL(t, backend, "ALTER MIGRATION m CONNECTION missing")
	assert.ErrorContains(t, err, "missing")

	// SET with an option that cannot be changed via ALTER ... SET.
	_, err = runSQL(t, backend, "ALTER MIGRATION m SET (copy_data = true)")
	assert.ErrorContains(t, err, "copy_data")

	// SET with a non-integer sequence_margin.
	_, err = runSQL(t, backend, "ALTER MIGRATION m SET (sequence_margin = notanint)")
	assert.ErrorContains(t, err, "integer")
}

func TestMigrationDDL_CreateAlreadyExistsAndIfNotExists(t *testing.T) {
	backend := newTestBackend(&fakeMigrator{})

	_, err := runSQL(t, backend, "CREATE CONNECTION c OPTIONS (host 'h')")
	require.NoError(t, err)

	// Re-create without IF NOT EXISTS: conflict.
	_, err = runSQL(t, backend, "CREATE CONNECTION c OPTIONS (host 'h2')")
	assert.ErrorContains(t, err, "already exists")

	// Re-create with IF NOT EXISTS: no-op success, DSN unchanged.
	_, err = runSQL(t, backend, "CREATE CONNECTION IF NOT EXISTS c OPTIONS (host 'h2')")
	require.NoError(t, err)
	dsn, _ := backend.Conns.Get("c")
	assert.Equal(t, "host=h", dsn)

	// ALTER a missing connection.
	_, err = runSQL(t, backend, "ALTER CONNECTION nope OPTIONS (host 'x')")
	assert.ErrorContains(t, err, "does not exist")
}

func TestMigrationDDL_CreateMigrationOptionErrors(t *testing.T) {
	backend := newTestBackend(&fakeMigrator{})
	backend.Conns.Set("c", "host=h")

	// Unknown WITH option.
	_, err := runSQL(t, backend, "CREATE MIGRATION m CONNECTION c FOR ALL TABLES WITH (bogus = 1)")
	assert.ErrorContains(t, err, "bogus")

	// Non-integer sequence_margin.
	_, err = runSQL(t, backend, "CREATE MIGRATION m CONNECTION c FOR ALL TABLES WITH (sequence_margin = nope)")
	assert.ErrorContains(t, err, "integer")

	// All the accepted options in one go.
	_, err = runSQL(t, backend, "CREATE MIGRATION m CONNECTION c FOR ALL TABLES WITH (copy_data = false, skip_schema_copy = true, sequence_margin = 7, source_publication = pub, publish_via_partition_root = true)")
	require.NoError(t, err)
}

// --- pure helpers ---

func defElem(name, val string) *ast.DefElem {
	return ast.NewDefElem(name, ast.NewString(val))
}

func TestMigrationDDLHelpers_isTruthy(t *testing.T) {
	for _, v := range []string{"true", "on", "1", "yes", "t", " TRUE ", "Yes"} {
		assert.True(t, isTruthy(v), "want truthy: %q", v)
	}
	for _, v := range []string{"false", "off", "0", "no", "f", "", "maybe"} {
		assert.False(t, isTruthy(v), "want falsey: %q", v)
	}
}

func TestMigrationDDLHelpers_boolText(t *testing.T) {
	assert.Equal(t, "t", boolText(true))
	assert.Equal(t, "f", boolText(false))
}

func TestMigrationDDLHelpers_defElemValue(t *testing.T) {
	assert.Equal(t, "", defElemValue(ast.NewDefElem("x", nil)), "nil arg yields empty")
	assert.Equal(t, "h", defElemValue(defElem("host", "h")), "quotes stripped")
}

func TestMigrationDDLHelpers_optionsToDSN(t *testing.T) {
	assert.Equal(t, "", optionsToDSN(nil))
	list := ast.NewNodeList(
		defElem("host", "db"),
		ast.NewString("junk"), // non-DefElem entry is skipped
		defElem("port", "5432"),
	)
	assert.Equal(t, "host=db port=5432", optionsToDSN(list))
}

func TestMigrationDDLHelpers_parseConnInfo(t *testing.T) {
	kv := parseConnInfo("host=h port=5432  dbname=app noeq =leadingeq")
	assert.Equal(t, "h", kv["host"])
	assert.Equal(t, "5432", kv["port"])
	assert.Equal(t, "app", kv["dbname"])
	assert.NotContains(t, kv, "noeq") // token without '='
	assert.NotContains(t, kv, "")     // '=' at index 0 is ignored
	assert.Empty(t, parseConnInfo(""))
}

func TestMigrationDDLHelpers_nameList(t *testing.T) {
	assert.Nil(t, nameList(nil))
	list := ast.NewNodeList(ast.NewString("a"), ast.NewInteger(1), ast.NewString("b"))
	assert.Equal(t, []string{"a", "b"}, nameList(list), "non-String entries are skipped")
}

func TestMigrationDDLHelpers_rangeVarName(t *testing.T) {
	assert.Equal(t, "", rangeVarName(nil))
	assert.Equal(t, "orders", rangeVarName(ast.NewRangeVar("orders", "", "")))
	assert.Equal(t, "public.orders", rangeVarName(ast.NewRangeVar("orders", "public", "")))
}

func TestMigrationDDLHelpers_selectionObjects(t *testing.T) {
	nilObjs, err := selectionObjects(nil)
	require.NoError(t, err)
	assert.Nil(t, nilObjs)

	// A table object with a column list, WHERE filter, and inheritance flag.
	rel := ast.NewRangeVar("orders", "public", "")
	rel.Inh = true
	tbl := ast.NewPublicationObjSpecTable(ast.PUBLICATIONOBJ_TABLE,
		ast.NewPublicationTable(rel, ast.NewString("id > 0"), ast.NewNodeList(ast.NewString("id"))))
	schema := ast.NewPublicationObjSpecName(ast.PUBLICATIONOBJ_TABLES_IN_SCHEMA, "sales")
	objs, err := selectionObjects(ast.NewNodeList(tbl, ast.NewString("skip"), schema))
	require.NoError(t, err)
	require.Len(t, objs, 2)
	ts := objs[0].GetTable()
	assert.Equal(t, "public.orders", ts.GetQualifiedName())
	assert.True(t, ts.GetIncludeDescendants())
	assert.Equal(t, []string{"id"}, ts.GetColumns())
	assert.NotEmpty(t, ts.GetWhere())
	assert.Equal(t, "sales", objs[1].GetSchema())

	// An unsupported object type (CURRENT_SCHEMA) is rejected.
	cur := ast.NewPublicationObjSpec(ast.PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA)
	_, err = selectionObjects(ast.NewNodeList(cur))
	assert.ErrorContains(t, err, "unsupported")
}

func TestMigrationDDLHelpers_applyMigrationOptions(t *testing.T) {
	req := &migratorpb.CreateMigrationRequest{}
	require.NoError(t, applyMigrationOptions(req, nil))

	list := ast.NewNodeList(
		defElem("copy_data", "false"),
		ast.NewString("skip"), // non-DefElem skipped
		defElem("skip_schema_copy", "true"),
		defElem("sequence_margin", "9"),
		defElem("source_publication", "pub1"),
		defElem("publish_via_partition_root", "yes"),
	)
	require.NoError(t, applyMigrationOptions(req, list))
	require.NotNil(t, req.CopyData)
	assert.False(t, req.GetCopyData())
	assert.True(t, req.GetSkipSchemaCopy())
	assert.Equal(t, int64(9), req.GetSequenceMargin())
	assert.Equal(t, "pub1", req.GetSourcePublication())
	assert.True(t, req.GetPublishViaPartitionRoot())

	assert.ErrorContains(t, applyMigrationOptions(&migratorpb.CreateMigrationRequest{},
		ast.NewNodeList(defElem("sequence_margin", "x"))), "integer")
	assert.ErrorContains(t, applyMigrationOptions(&migratorpb.CreateMigrationRequest{},
		ast.NewNodeList(defElem("nope", "1"))), "unknown migration option")
}

func TestMigrationDDLHelpers_applyActivateOptions(t *testing.T) {
	// nil list leaves the request at defaults.
	req := &migratorpb.ActivateMigrationRequest{}
	require.NoError(t, applyActivateOptions(req, nil))
	assert.Zero(t, req.GetMaxLagBytes())
	assert.Zero(t, req.GetWaitTimeoutSeconds())

	// Bytes plus a duration string.
	req = &migratorpb.ActivateMigrationRequest{}
	require.NoError(t, applyActivateOptions(req, ast.NewNodeList(
		ast.NewString("skip"), // non-DefElem skipped
		defElem("max_lag_bytes", "8388608"),
		defElem("wait_timeout", "30s"),
	)))
	assert.Equal(t, uint64(8388608), req.GetMaxLagBytes())
	assert.Equal(t, int64(30), req.GetWaitTimeoutSeconds())

	// wait_timeout also accepts bare integer seconds.
	req = &migratorpb.ActivateMigrationRequest{}
	require.NoError(t, applyActivateOptions(req, ast.NewNodeList(defElem("wait_timeout", "45"))))
	assert.Equal(t, int64(45), req.GetWaitTimeoutSeconds())

	// Errors: bad bytes, bad duration, negative timeout, unknown option.
	assert.ErrorContains(t, applyActivateOptions(&migratorpb.ActivateMigrationRequest{},
		ast.NewNodeList(defElem("max_lag_bytes", "-1"))), "non-negative integer")
	assert.ErrorContains(t, applyActivateOptions(&migratorpb.ActivateMigrationRequest{},
		ast.NewNodeList(defElem("wait_timeout", "soon"))), "duration")
	assert.ErrorContains(t, applyActivateOptions(&migratorpb.ActivateMigrationRequest{},
		ast.NewNodeList(defElem("wait_timeout", "-5s"))), "negative")
	assert.ErrorContains(t, applyActivateOptions(&migratorpb.ActivateMigrationRequest{},
		ast.NewNodeList(defElem("nope", "1"))), "unknown ACTIVATE option")
}

func TestMigrationDDLHelpers_applyUpdateOptions(t *testing.T) {
	req := &migratorpb.UpdateMigrationRequest{}
	paths, err := applyUpdateOptions(req, nil)
	require.NoError(t, err)
	assert.Empty(t, paths)

	paths, err = applyUpdateOptions(req, ast.NewNodeList(
		ast.NewString("skip"), // non-DefElem skipped
		defElem("sequence_margin", "12"),
	))
	require.NoError(t, err)
	assert.Equal(t, []string{"sequence_margin"}, paths)
	assert.Equal(t, int64(12), req.GetSequenceMargin())

	_, err = applyUpdateOptions(&migratorpb.UpdateMigrationRequest{},
		ast.NewNodeList(defElem("sequence_margin", "x")))
	assert.ErrorContains(t, err, "integer")
	_, err = applyUpdateOptions(&migratorpb.UpdateMigrationRequest{},
		ast.NewNodeList(defElem("copy_data", "true")))
	assert.ErrorContains(t, err, "cannot be changed")
}

func TestMigrationDDLHelpers_connectionStoreList(t *testing.T) {
	s := NewInMemoryConnectionStore()
	assert.Empty(t, s.List())
	s.Set("b", "host=2")
	s.Set("a", "host=1")
	list := s.List()
	require.Len(t, list, 2)
	assert.Equal(t, "a", list[0].Name, "List is sorted by name")
	assert.Equal(t, "b", list[1].Name)
}
