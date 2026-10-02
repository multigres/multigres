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

func (failMigrator) GetMigration(context.Context, *migratorpb.GetMigrationRequest, ...grpc.CallOption) (*migratorpb.GetMigrationResponse, error) {
	return nil, errBackend
}

func (failMigrator) ListMigrations(context.Context, *migratorpb.ListMigrationsRequest, ...grpc.CallOption) (*migratorpb.ListMigrationsResponse, error) {
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

func (failMigrator) GetMigrationJournal(context.Context, *migratorpb.GetMigrationJournalRequest, ...grpc.CallOption) (*migratorpb.GetMigrationJournalResponse, error) {
	return nil, errBackend
}

func (failMigrator) CreateConnection(context.Context, *migratorpb.CreateConnectionRequest, ...grpc.CallOption) (*migratorpb.CreateConnectionResponse, error) {
	return nil, errBackend
}

func (failMigrator) UpdateConnection(context.Context, *migratorpb.UpdateConnectionRequest, ...grpc.CallOption) (*migratorpb.UpdateConnectionResponse, error) {
	return nil, errBackend
}

func (failMigrator) GetConnection(context.Context, *migratorpb.GetConnectionRequest, ...grpc.CallOption) (*migratorpb.GetConnectionResponse, error) {
	return nil, errBackend
}

func (failMigrator) ListConnections(context.Context, *migratorpb.ListConnectionsRequest, ...grpc.CallOption) (*migratorpb.ListConnectionsResponse, error) {
	return nil, errBackend
}

func (failMigrator) DropConnection(context.Context, *migratorpb.DropConnectionRequest, ...grpc.CallOption) (*migratorpb.DropConnectionResponse, error) {
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
	fake := &fakeMigrator{conns: map[string]*migratorpb.Connection{"c": {Id: 1, Name: "c", Dsn: "host=h port=5432 dbname=app"}}}
	backend := newTestBackend(fake)

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

	// Client unset: helpers that need the migrator fail.
	noClient := &MigrationBackend{}
	p := NewMigrationDDL("sql", stmt[0], noClient)
	err := p.StreamExecute(context.Background(), nil, nil, nil, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	assert.ErrorContains(t, err, "not configured")

	// Client set but returns nil: no migrator reachable.
	nilClient := &MigrationBackend{
		Client: func() migratorpb.MigratorClient { return nil },
	}
	p = NewMigrationDDL("sql", stmt[0], nilClient)
	err = p.StreamExecute(context.Background(), nil, nil, nil, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	assert.ErrorContains(t, err, "no migrator")

	// Backend entirely nil: execute rejects before dispatch.
	p = NewMigrationDDL("sql", stmt[0], nil)
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
	fake := &fakeMigrator{}
	backend := newTestBackend(fake)

	_, err := runSQL(t, backend, "CREATE CONNECTION c OPTIONS (host 'h')")
	require.NoError(t, err)

	// Re-create without IF NOT EXISTS: conflict.
	_, err = runSQL(t, backend, "CREATE CONNECTION c OPTIONS (host 'h2')")
	assert.ErrorContains(t, err, "already exists")

	// Re-create with IF NOT EXISTS: no-op success, DSN unchanged (still the first
	// create's DSN, with the CREATE CONNECTION defaults filled in).
	_, err = runSQL(t, backend, "CREATE CONNECTION IF NOT EXISTS c OPTIONS (host 'h2')")
	require.NoError(t, err)
	assert.Equal(t, "host='h' port=5432 dbname=postgres sslmode=disable", fake.conns["c"].GetDsn())

	// ALTER a missing connection.
	_, err = runSQL(t, backend, "ALTER CONNECTION nope OPTIONS (host 'x')")
	assert.ErrorContains(t, err, "does not exist")
}

// TestMigrationDDL_AlterConnectionOptionsMerge proves ALTER CONNECTION merges
// onto the existing option set instead of replacing it wholesale: ADD/SET/DROP
// (and a bare, action-less option) each touch only the option they name,
// leaving every other option as it was.
func TestMigrationDDL_AlterConnectionOptionsMerge(t *testing.T) {
	fake := &fakeMigrator{}
	backend := newTestBackend(fake)

	_, err := runSQL(t, backend, "CREATE CONNECTION c OPTIONS (host 'h', user 'u', dbname 'app')")
	require.NoError(t, err)

	// SET changes one option, leaving the others (including the CREATE-time
	// defaults) untouched.
	_, err = runSQL(t, backend, "ALTER CONNECTION c OPTIONS (SET port '6543')")
	require.NoError(t, err)
	kv := parseConnInfo(fake.conns["c"].GetDsn())
	assert.Equal(t, "6543", kv["port"])
	assert.Equal(t, "h", kv["host"])
	assert.Equal(t, "u", kv["user"])
	assert.Equal(t, "app", kv["dbname"])
	assert.Equal(t, "disable", kv["sslmode"])

	// SET on an option that isn't set yet is rejected.
	_, err = runSQL(t, backend, "ALTER CONNECTION c OPTIONS (SET sslrootcert 'x')")
	assert.ErrorContains(t, err, "does not exist")

	// ADD introduces a brand-new option, leaving the rest alone.
	_, err = runSQL(t, backend, "ALTER CONNECTION c OPTIONS (ADD sslrootcert 'x')")
	require.NoError(t, err)
	kv = parseConnInfo(fake.conns["c"].GetDsn())
	assert.Equal(t, "x", kv["sslrootcert"])
	assert.Equal(t, "u", kv["user"], "ADD must not disturb unrelated options")

	// ADD on an option that already exists is rejected.
	_, err = runSQL(t, backend, "ALTER CONNECTION c OPTIONS (ADD sslrootcert 'y')")
	assert.ErrorContains(t, err, "already exists")

	// DROP removes just the named option.
	_, err = runSQL(t, backend, "ALTER CONNECTION c OPTIONS (DROP sslrootcert)")
	require.NoError(t, err)
	kv = parseConnInfo(fake.conns["c"].GetDsn())
	assert.NotContains(t, kv, "sslrootcert")
	assert.Equal(t, "app", kv["dbname"], "DROP must not disturb unrelated options")

	// DROP on an option that no longer exists is rejected.
	_, err = runSQL(t, backend, "ALTER CONNECTION c OPTIONS (DROP sslrootcert)")
	assert.ErrorContains(t, err, "does not exist")

	// A bare option (no ADD/SET/DROP keyword) upserts regardless of whether it
	// already exists.
	_, err = runSQL(t, backend, "ALTER CONNECTION c OPTIONS (dbname 'app2')")
	require.NoError(t, err)
	assert.Equal(t, "app2", parseConnInfo(fake.conns["c"].GetDsn())["dbname"])
}

func TestMigrationDDL_CreateMigrationOptionErrors(t *testing.T) {
	fake := &fakeMigrator{conns: map[string]*migratorpb.Connection{"c": {Id: 1, Name: "c", Dsn: "host=h"}}}
	backend := newTestBackend(fake)

	// Unknown WITH option.
	_, err := runSQL(t, backend, "CREATE MIGRATION m CONNECTION c FOR ALL TABLES WITH (bogus = 1)")
	assert.ErrorContains(t, err, "bogus")

	// Non-integer sequence_margin.
	_, err = runSQL(t, backend, "CREATE MIGRATION m CONNECTION c FOR ALL TABLES WITH (sequence_margin = nope)")
	assert.ErrorContains(t, err, "integer")

	// All the accepted options in one go.
	_, err = runSQL(t, backend, "CREATE MIGRATION m CONNECTION c FOR ALL TABLES WITH (copy_data = false, skip_schema_copy = true, sequence_margin = 7)")
	require.NoError(t, err)

	// The two options the backend never implemented are gone from the wire
	// entirely now, so they are rejected like any other unknown option.
	_, err = runSQL(t, backend, "CREATE MIGRATION m CONNECTION c FOR ALL TABLES WITH (source_publication = pub)")
	assert.ErrorContains(t, err, "source_publication")
	_, err = runSQL(t, backend, "CREATE MIGRATION m CONNECTION c FOR ALL TABLES WITH (publish_via_partition_root = true)")
	assert.ErrorContains(t, err, "publish_via_partition_root")
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
	assert.Equal(t, "h", defElemValue(defElem("host", "h")), "reads the literal's unescaped value")
}

func TestMigrationDDLHelpers_connectionDSN(t *testing.T) {
	// host is required.
	_, err := connectionDSN(nil)
	assert.ErrorContains(t, err, "host")

	// Explicit options win (quoted); missing ones get the CREATE CONNECTION
	// defaults (unquoted literals).
	list := ast.NewNodeList(
		defElem("host", "db"),
		ast.NewString("junk"), // non-DefElem entry is skipped
		defElem("port", "5433"),
	)
	dsn, err := connectionDSN(list)
	require.NoError(t, err)
	assert.Equal(t, "host='db' port='5433' dbname=postgres sslmode=disable", dsn)
}

func TestMigrationDDLHelpers_parseConnInfo(t *testing.T) {
	kv := parseConnInfo("host=h port=5432  dbname=app")
	assert.Equal(t, "h", kv["host"])
	assert.Equal(t, "5432", kv["port"])
	assert.Equal(t, "app", kv["dbname"])
	assert.Empty(t, parseConnInfo(""))

	// Keys fold to lowercase (ALTER CONNECTION's merge and SHOW CONNECTIONS
	// both look up fixed lowercase keys).
	assert.Equal(t, "h", parseConnInfo("Host=h")["host"])

	// A quoted value keeps an embedded space instead of being split at it.
	kv = parseConnInfo(`host=h password='has space'`)
	assert.Equal(t, "has space", kv["password"])

	// Escaped backslash and quote inside a quoted value round-trip.
	kv = parseConnInfo(`host=h password='a\\b\'c'`)
	assert.Equal(t, `a\b'c`, kv["password"])

	// parseConnInfo is the exact inverse of connInfoString/quoteConnInfoValue.
	original := map[string]string{"host": "h", "password": `has space ' and \ backslash`}
	assert.Equal(t, original, parseConnInfo(connInfoString(original)))
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
	nilObj, err := selectionObjects(nil)
	require.NoError(t, err)
	assert.Nil(t, nilObj)

	// Multiple plain table entries fold into one TableSpec.
	rel1 := ast.NewRangeVar("orders", "public", "")
	rel1.Inh = true
	tbl1 := ast.NewPublicationObjSpecTable(ast.PUBLICATIONOBJ_TABLE, ast.NewPublicationTable(rel1, nil, nil))
	rel2 := ast.NewRangeVar("items", "public", "")
	rel2.Inh = true
	tbl2 := ast.NewPublicationObjSpecTable(ast.PUBLICATIONOBJ_TABLE, ast.NewPublicationTable(rel2, nil, nil))
	obj, err := selectionObjects(ast.NewNodeList(tbl1, ast.NewString("skip"), tbl2))
	require.NoError(t, err)
	assert.Equal(t, []string{"public.orders", "public.items"}, obj.GetTable().GetQualifiedNames())

	// Multiple schema entries fold into one SchemaSpec.
	schema1 := ast.NewPublicationObjSpecName(ast.PUBLICATIONOBJ_TABLES_IN_SCHEMA, "sales")
	schema2 := ast.NewPublicationObjSpecName(ast.PUBLICATIONOBJ_TABLES_IN_SCHEMA, "reporting")
	obj, err = selectionObjects(ast.NewNodeList(schema1, schema2))
	require.NoError(t, err)
	assert.Equal(t, []string{"sales", "reporting"}, obj.GetSchema().GetSchemata())

	// Mixing a table and a schema entry is rejected — a migration's FOR clause is
	// one form or the other, mirroring CREATE PUBLICATION.
	_, err = selectionObjects(ast.NewNodeList(tbl1, schema1))
	assert.ErrorContains(t, err, "cannot mix")

	// A column list, WHERE filter, and ONLY are all accepted on the wire (the
	// wire surface used to carry them) but not backed, so each must fail with an
	// explicit error rather than silently dropping the clause.
	relOnly := ast.NewRangeVar("orders", "public", "")
	relOnly.Inh = false
	only := ast.NewPublicationObjSpecTable(ast.PUBLICATIONOBJ_TABLE, ast.NewPublicationTable(relOnly, nil, nil))
	_, err = selectionObjects(ast.NewNodeList(only))
	assert.ErrorContains(t, err, "ONLY")

	withColumns := ast.NewPublicationObjSpecTable(ast.PUBLICATIONOBJ_TABLE,
		ast.NewPublicationTable(rel1, nil, ast.NewNodeList(ast.NewString("id"))))
	_, err = selectionObjects(ast.NewNodeList(withColumns))
	assert.ErrorContains(t, err, "column")

	withWhere := ast.NewPublicationObjSpecTable(ast.PUBLICATIONOBJ_TABLE,
		ast.NewPublicationTable(rel1, ast.NewString("id > 0"), nil))
	_, err = selectionObjects(ast.NewNodeList(withWhere))
	assert.ErrorContains(t, err, "WHERE")

	// An unsupported object type (CURRENT_SCHEMA) is rejected.
	cur := ast.NewPublicationObjSpec(ast.PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA)
	_, err = selectionObjects(ast.NewNodeList(cur))
	assert.ErrorContains(t, err, "unsupported")
}

func TestMigrationDDLHelpers_applyMigrationOptions(t *testing.T) {
	req := &migratorpb.CreateMigrationRequest{Migration: &migratorpb.Migration{}}
	require.NoError(t, applyMigrationOptions(req, nil))

	list := ast.NewNodeList(
		defElem("copy_data", "false"),
		ast.NewString("skip"), // non-DefElem skipped
		defElem("skip_schema_copy", "true"),
		defElem("sequence_margin", "9"),
	)
	require.NoError(t, applyMigrationOptions(req, list))
	assert.True(t, req.GetSkipCopyData(), "copy_data=false inverts to skip_copy_data=true")
	assert.True(t, req.GetSkipSchemaCopy())
	assert.Equal(t, int64(9), req.GetMigration().GetSequenceMargin())

	assert.ErrorContains(t, applyMigrationOptions(&migratorpb.CreateMigrationRequest{},
		ast.NewNodeList(defElem("sequence_margin", "x"))), "integer")
	assert.ErrorContains(t, applyMigrationOptions(&migratorpb.CreateMigrationRequest{},
		ast.NewNodeList(defElem("nope", "1"))), "unknown migration option")
	// The two removed backend options are no longer recognized at all.
	assert.ErrorContains(t, applyMigrationOptions(&migratorpb.CreateMigrationRequest{},
		ast.NewNodeList(defElem("source_publication", "pub1"))), "unknown migration option")
	assert.ErrorContains(t, applyMigrationOptions(&migratorpb.CreateMigrationRequest{},
		ast.NewNodeList(defElem("publish_via_partition_root", "yes"))), "unknown migration option")
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

	// max_lag_bytes also accepts a human-readable size literal.
	req = &migratorpb.ActivateMigrationRequest{}
	require.NoError(t, applyActivateOptions(req, ast.NewNodeList(defElem("max_lag_bytes", "8 MiB"))))
	assert.Equal(t, uint64(8388608), req.GetMaxLagBytes())

	// Errors: bad bytes, bad duration, negative timeout, unknown option.
	assert.ErrorContains(t, applyActivateOptions(&migratorpb.ActivateMigrationRequest{},
		ast.NewNodeList(defElem("max_lag_bytes", "-1"))), "max_lag_bytes")
	assert.ErrorContains(t, applyActivateOptions(&migratorpb.ActivateMigrationRequest{},
		ast.NewNodeList(defElem("max_lag_bytes", "1 PiB"))), "unknown unit")
	assert.ErrorContains(t, applyActivateOptions(&migratorpb.ActivateMigrationRequest{},
		ast.NewNodeList(defElem("wait_timeout", "soon"))), "duration")
	assert.ErrorContains(t, applyActivateOptions(&migratorpb.ActivateMigrationRequest{},
		ast.NewNodeList(defElem("wait_timeout", "-5s"))), "negative")
	assert.ErrorContains(t, applyActivateOptions(&migratorpb.ActivateMigrationRequest{},
		ast.NewNodeList(defElem("nope", "1"))), "unknown ACTIVATE option")
}

func TestMigrationDDLHelpers_applyUpdateOptions(t *testing.T) {
	req := &migratorpb.UpdateMigrationRequest{Migration: &migratorpb.Migration{}}
	paths, err := applyUpdateOptions(req, nil)
	require.NoError(t, err)
	assert.Empty(t, paths)

	paths, err = applyUpdateOptions(req, ast.NewNodeList(
		ast.NewString("skip"), // non-DefElem skipped
		defElem("sequence_margin", "12"),
	))
	require.NoError(t, err)
	assert.Equal(t, []string{"sequence_margin"}, paths)
	assert.Equal(t, int64(12), req.GetMigration().GetSequenceMargin())

	_, err = applyUpdateOptions(&migratorpb.UpdateMigrationRequest{Migration: &migratorpb.Migration{}},
		ast.NewNodeList(defElem("sequence_margin", "x")))
	assert.ErrorContains(t, err, "integer")
	_, err = applyUpdateOptions(&migratorpb.UpdateMigrationRequest{Migration: &migratorpb.Migration{}},
		ast.NewNodeList(defElem("copy_data", "true")))
	assert.ErrorContains(t, err, "cannot be changed")
}
