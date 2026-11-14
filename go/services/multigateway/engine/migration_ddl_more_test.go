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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
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

func (failMigrator) GetMigration(context.Context, *migratorpb.GetMigrationRequest, ...grpc.CallOption) (*migratorpb.GetMigrationResponse, error) {
	return nil, errBackend
}

func (failMigrator) ListMigrations(context.Context, *migratorpb.ListMigrationsRequest, ...grpc.CallOption) (*migratorpb.ListMigrationsResponse, error) {
	return nil, errBackend
}

func (failMigrator) SetMigrationDirection(context.Context, *migratorpb.SetMigrationDirectionRequest, ...grpc.CallOption) (*migratorpb.SetMigrationDirectionResponse, error) {
	return nil, errBackend
}

func (failMigrator) DropMigration(context.Context, *migratorpb.DropMigrationRequest, ...grpc.CallOption) (*migratorpb.DropMigrationResponse, error) {
	return nil, errBackend
}

func (failMigrator) ReseedReverseLink(context.Context, *migratorpb.ReseedReverseLinkRequest, ...grpc.CallOption) (*migratorpb.ReseedReverseLinkResponse, error) {
	return nil, errBackend
}

func (failMigrator) GetMigrationJournal(context.Context, *migratorpb.GetMigrationJournalRequest, ...grpc.CallOption) (*migratorpb.GetMigrationJournalResponse, error) {
	return nil, errBackend
}

func (failMigrator) CreateConnection(context.Context, *migratorpb.CreateConnectionRequest, ...grpc.CallOption) (*migratorpb.CreateConnectionResponse, error) {
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
	execErr := p.PortalStreamExecute(context.Background(), nil, privilegedTestConn(), nil, nil, 0, includeDescribe, PlanExecInfo{},
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
	err := p.StreamExecute(context.Background(), nil, privilegedTestConn(), nil, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	assert.ErrorContains(t, err, "not configured")

	// Client set but returns nil: no migrator reachable.
	nilClient := &MigrationBackend{
		Client: func() migratorpb.MigratorClient { return nil },
	}
	p = NewMigrationDDL("sql", stmt[0], nilClient)
	err = p.StreamExecute(context.Background(), nil, privilegedTestConn(), nil, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	assert.ErrorContains(t, err, "no migrator")

	// Backend entirely nil: execute rejects before dispatch.
	p = NewMigrationDDL("sql", stmt[0], nil)
	err = p.StreamExecute(context.Background(), nil, privilegedTestConn(), nil, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	assert.ErrorContains(t, err, "not configured")
}

func TestMigrationDDL_UnsupportedStatement(t *testing.T) {
	// A non-migration statement reaches the dispatch default arm. SELECT has its
	// own case (the multigres.stat_migration pseudo-view), so this uses a
	// statement type with no migration-related handling at all.
	stmts, err := parser.ParseSQL("SET foo = 'bar'")
	require.NoError(t, err)
	p := NewMigrationDDL("SET foo = 'bar'", stmts[0], newTestBackend(&fakeMigrator{}))
	err = p.StreamExecute(context.Background(), nil, privilegedTestConn(), nil, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	assert.ErrorContains(t, err, "unsupported migration statement")
}

// --- RPC-error propagation for each migration verb ---

func TestMigrationDDL_RPCErrors(t *testing.T) {
	backend := newTestBackend(failMigrator{})

	for _, sql := range []string{
		"CREATE MIGRATION m CONNECTION c FOR ALL TABLES",
		"ALTER MIGRATION m PHASE IMPORT",
		"ALTER MIGRATION m PHASE EXPORT",
		"SELECT * FROM multigres.stat_migration WHERE migration_name = 'm'",
		"SHOW CONNECTION onprem",
	} {
		_, err := runSQL(t, backend, sql)
		assert.ErrorContains(t, err, "boom", "sql: %s", sql)
	}

	// DROP without IF EXISTS propagates the RPC error...
	_, err := runSQL(t, backend, "DROP MIGRATION m")
	assert.ErrorContains(t, err, "boom")
	// ...and so does IF EXISTS, for anything other than NotFound: it swallows
	// "not found" specifically (see TestMigrationDDL_DropIfExistsSwallowsOnlyNotFound),
	// not every error.
	_, err = runSQL(t, backend, "DROP MIGRATION IF EXISTS m")
	assert.ErrorContains(t, err, "boom")

	// ALTER MIGRATION IF EXISTS has the same not-every-error contract (see
	// TestMigrationDDL_AlterIfExistsSwallowsOnlyNotFound for the NotFound half).
	_, err = runSQL(t, backend, "ALTER MIGRATION IF EXISTS m PHASE IMPORT")
	assert.ErrorContains(t, err, "boom")
}

// notFoundMigrator returns NotFound from DropMigration/SetMigrationDirection
// and panics on every other method (never called in these tests), to verify
// IF EXISTS swallows specifically that code.
type notFoundMigrator struct{ migratorpb.MigratorClient }

func (notFoundMigrator) DropMigration(context.Context, *migratorpb.DropMigrationRequest, ...grpc.CallOption) (*migratorpb.DropMigrationResponse, error) {
	return nil, status.Error(codes.NotFound, "migration not found")
}

func (notFoundMigrator) SetMigrationDirection(context.Context, *migratorpb.SetMigrationDirectionRequest, ...grpc.CallOption) (*migratorpb.SetMigrationDirectionResponse, error) {
	return nil, status.Error(codes.NotFound, "migration not found")
}

// TestMigrationDDL_DropIfExistsSwallowsOnlyNotFound is the regression test for
// the finding this guards against: IF EXISTS must swallow a NotFound
// DropMigration error (this test) but nothing else
// (TestMigrationDDL_RPCErrors's "boom" case covers that half).
func TestMigrationDDL_DropIfExistsSwallowsOnlyNotFound(t *testing.T) {
	backend := newTestBackend(notFoundMigrator{})

	_, err := runSQL(t, backend, "DROP MIGRATION m")
	assert.ErrorContains(t, err, "not found")

	_, err = runSQL(t, backend, "DROP MIGRATION IF EXISTS m")
	assert.NoError(t, err)
}

// TestMigrationDDL_AlterIfExistsSwallowsOnlyNotFound is the regression test
// for the finding this guards against: ALTER MIGRATION IF EXISTS parsed the
// flag but alterMigration never read it, so a NotFound SetMigrationDirection
// error always propagated instead of being swallowed like DROP's does.
func TestMigrationDDL_AlterIfExistsSwallowsOnlyNotFound(t *testing.T) {
	backend := newTestBackend(notFoundMigrator{})

	_, err := runSQL(t, backend, "ALTER MIGRATION m PHASE IMPORT")
	assert.ErrorContains(t, err, "not found")

	_, err = runSQL(t, backend, "ALTER MIGRATION IF EXISTS m PHASE IMPORT")
	assert.NoError(t, err)
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
	assert.Equal(t, "host='h'", fake.conns["c"].GetDsn())
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

func TestMigrationDDLHelpers_parseBoolOption(t *testing.T) {
	for _, v := range []string{"true", "on", "1", "yes", "t", " TRUE ", "Yes"} {
		got, err := parseBoolOption("opt", v)
		require.NoError(t, err)
		assert.True(t, got, "want truthy: %q", v)
	}
	for _, v := range []string{"false", "off", "0", "no", "f", " FALSE "} {
		got, err := parseBoolOption("opt", v)
		require.NoError(t, err)
		assert.False(t, got, "want falsey: %q", v)
	}
	// An unrecognized value (e.g. a typo) is a syntax error, not a silent
	// fallback to false — see TestMigrationDDLHelpers_applyMigrationOptions's
	// "copy_data=typo" case for the regression this guards against.
	for _, v := range []string{"", "maybe", "troo"} {
		_, err := parseBoolOption("opt", v)
		assert.ErrorContains(t, err, "opt must be a boolean value", "want error for: %q", v)
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

// An option that is not a libpq connection parameter is refused at the gateway, which can say
// which one: the migrator checks the string it is given and cannot name a parameter.
func TestMigrationDDLHelpers_connectionDSNRefusesAnUnknownOption(t *testing.T) {
	_, err := connectionDSN(ast.NewNodeList(defElem("host", "h"), defElem("hots", "x")))
	require.ErrorContains(t, err, `option "hots" is not a libpq connection parameter`)

	// libpq's own parameters are all accepted, in any case.
	dsn, err := connectionDSN(ast.NewNodeList(defElem("HOST", "h"), defElem("SslRootCert", "/ca.pem"), defElem("connect_timeout", "5")))
	require.NoError(t, err)
	require.Contains(t, dsn, "sslrootcert='/ca.pem'")
}

// An option given twice is refused, as CREATE SERVER ... OPTIONS does (42710), whatever the case it
// is written in, and not left to the last one.
func TestMigrationDDLHelpers_connectionDSNRefusesADuplicateOption(t *testing.T) {
	_, err := connectionDSN(ast.NewNodeList(defElem("host", "a"), defElem("HOST", "b")))
	require.ErrorContains(t, err, `option "HOST" provided more than once`)
	assert.Equal(t, mterrors.PgSSDuplicateObject, mterrors.ExtractSQLSTATE(err))

	fake := &fakeMigrator{}
	_, err = runSQL(t, newTestBackend(fake), "CREATE CONNECTION dup OPTIONS (host 'a', port '1', port '2')")
	require.ErrorContains(t, err, `option "port" provided more than once`)
	assert.Empty(t, fake.conns, "nothing is created")
}

// The gateway renders the options as they were written and decides nothing: no default is applied
// and no host (or anything else) is required here, since the migrator does both, once, for every
// connection (backingconn.FromDSN).
func TestMigrationDDLHelpers_connectionDSNAppliesNoDefaultAndRequiresNothing(t *testing.T) {
	dsn, err := connectionDSN(nil)
	require.NoError(t, err)
	assert.Empty(t, dsn)

	dsn, err = connectionDSN(ast.NewNodeList(defElem("host", "")))
	require.NoError(t, err)
	assert.Equal(t, "host=''", dsn, "an empty host is the migrator's to refuse")

	// Only what was given: no port, no database, no sslmode (not even for system roots). pgutil
	// renders keys in sorted order, every value quoted. A non-DefElem entry is skipped.
	dsn, err = connectionDSN(ast.NewNodeList(
		defElem("host", "db"),
		ast.NewString("junk"),
		defElem("port", "5433"),
		defElem("sslrootcert", "system"),
	))
	require.NoError(t, err)
	assert.Equal(t, "host='db' port='5433' sslrootcert='system'", dsn)
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
	// wire surface used to carry them) but not backed, so each must fail with a
	// typed feature_not_supported error (SQLSTATE 0A000) rather than silently
	// dropping the clause.
	relOnly := ast.NewRangeVar("orders", "public", "")
	relOnly.Inh = false
	only := ast.NewPublicationObjSpecTable(ast.PUBLICATIONOBJ_TABLE, ast.NewPublicationTable(relOnly, nil, nil))
	_, err = selectionObjects(ast.NewNodeList(only))
	assert.ErrorContains(t, err, "ONLY")
	assert.Equal(t, "0A000", mterrors.ExtractSQLSTATE(err))

	withColumns := ast.NewPublicationObjSpecTable(ast.PUBLICATIONOBJ_TABLE,
		ast.NewPublicationTable(rel1, nil, ast.NewNodeList(ast.NewString("id"))))
	_, err = selectionObjects(ast.NewNodeList(withColumns))
	assert.ErrorContains(t, err, "column")
	assert.Equal(t, "0A000", mterrors.ExtractSQLSTATE(err))

	withWhere := ast.NewPublicationObjSpecTable(ast.PUBLICATIONOBJ_TABLE,
		ast.NewPublicationTable(rel1, ast.NewString("id > 0"), nil))
	_, err = selectionObjects(ast.NewNodeList(withWhere))
	assert.ErrorContains(t, err, "WHERE")
	assert.Equal(t, "0A000", mterrors.ExtractSQLSTATE(err))

	// An unsupported object type (CURRENT_SCHEMA) is rejected with the same
	// typed feature_not_supported error as the other not-yet-backed forms above.
	cur := ast.NewPublicationObjSpec(ast.PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA)
	_, err = selectionObjects(ast.NewNodeList(cur))
	assert.ErrorContains(t, err, "CURRENT_SCHEMA")
	assert.Equal(t, "0A000", mterrors.ExtractSQLSTATE(err))
}

func TestMigrationDDLHelpers_applyMigrationOptions(t *testing.T) {
	req := &migratorpb.CreateMigrationRequest{Migration: &migratorpb.MigrationRecord{}}
	require.NoError(t, applyMigrationOptions(req, nil))

	list := ast.NewNodeList(
		defElem("copy_data", "false"),
		ast.NewString("skip"), // non-DefElem skipped
		defElem("skip_schema_copy", "true"),
		defElem("sequence_margin", "9"),
	)
	require.NoError(t, applyMigrationOptions(req, list))
	assert.True(t, req.GetOptions().GetSkipCopyData(), "copy_data=false inverts to skip_copy_data=true")
	assert.True(t, req.GetOptions().GetSkipSchemaCopy())
	assert.Equal(t, int64(9), req.GetMigration().GetSequenceMargin())

	assert.ErrorContains(t, applyMigrationOptions(&migratorpb.CreateMigrationRequest{},
		ast.NewNodeList(defElem("sequence_margin", "x"))), "integer")
	assert.ErrorContains(t, applyMigrationOptions(&migratorpb.CreateMigrationRequest{},
		ast.NewNodeList(defElem("nope", "1"))), "unknown migration option")
	// A typo'd boolean must be a syntax error, not silently fall through to
	// false (which, for copy_data, inverts to skip_copy_data=true — omitting
	// the initial copy while streaming still appears caught up).
	assert.ErrorContains(t, applyMigrationOptions(&migratorpb.CreateMigrationRequest{},
		ast.NewNodeList(defElem("copy_data", "typo"))), "copy_data must be a boolean value")
	assert.ErrorContains(t, applyMigrationOptions(&migratorpb.CreateMigrationRequest{},
		ast.NewNodeList(defElem("skip_schema_copy", "typo"))), "skip_schema_copy must be a boolean value")
	// The two removed backend options are no longer recognized at all.
	assert.ErrorContains(t, applyMigrationOptions(&migratorpb.CreateMigrationRequest{},
		ast.NewNodeList(defElem("source_publication", "pub1"))), "unknown migration option")
	assert.ErrorContains(t, applyMigrationOptions(&migratorpb.CreateMigrationRequest{},
		ast.NewNodeList(defElem("publish_via_partition_root", "yes"))), "unknown migration option")
	// quiesce_roles isn't recognized yet (the coordinator doesn't read it, and
	// the exact shape this takes isn't settled); it's just another unknown
	// option until the commit that wires it up gives it a case of its own.
	assert.ErrorContains(t, applyMigrationOptions(&migratorpb.CreateMigrationRequest{},
		ast.NewNodeList(defElem("quiesce_roles", "app, reporting"))), "unknown migration option")
}

func TestMigrationDDLHelpers_applyDropWithOptions(t *testing.T) {
	req := &migratorpb.DropMigrationRequest{}
	require.NoError(t, applyDropWithOptions(req, nil))
	assert.Zero(t, req.GetWaitTimeoutSeconds())

	req = &migratorpb.DropMigrationRequest{}
	require.NoError(t, applyDropWithOptions(req, ast.NewNodeList(
		ast.NewString("skip"), // non-DefElem skipped
		defElem("wait_timeout", "45"),
	)))
	assert.Equal(t, int64(45), req.GetWaitTimeoutSeconds())

	assert.ErrorContains(t, applyDropWithOptions(&migratorpb.DropMigrationRequest{},
		ast.NewNodeList(defElem("wait_timeout", "soon"))), "duration")
	assert.ErrorContains(t, applyDropWithOptions(&migratorpb.DropMigrationRequest{},
		ast.NewNodeList(defElem("nope", "1"))), "unknown DROP MIGRATION option")
}

func TestMigrationDDLHelpers_applyPhaseOptions(t *testing.T) {
	// nil list leaves the request at defaults.
	req := &migratorpb.SetMigrationDirectionRequest{}
	require.NoError(t, applyPhaseOptions(req, nil))
	assert.Zero(t, req.GetWaitTimeoutSeconds())

	// A duration string.
	req = &migratorpb.SetMigrationDirectionRequest{}
	require.NoError(t, applyPhaseOptions(req, ast.NewNodeList(
		ast.NewString("skip"), // non-DefElem skipped
		defElem("wait_timeout", "30s"),
	)))
	assert.Equal(t, int64(30), req.GetWaitTimeoutSeconds())

	// wait_timeout also accepts bare integer seconds.
	req = &migratorpb.SetMigrationDirectionRequest{}
	require.NoError(t, applyPhaseOptions(req, ast.NewNodeList(defElem("wait_timeout", "45"))))
	assert.Equal(t, int64(45), req.GetWaitTimeoutSeconds())

	// Errors: bad duration, negative timeout, unknown option.
	assert.ErrorContains(t, applyPhaseOptions(&migratorpb.SetMigrationDirectionRequest{},
		ast.NewNodeList(defElem("wait_timeout", "soon"))), "duration")
	assert.ErrorContains(t, applyPhaseOptions(&migratorpb.SetMigrationDirectionRequest{},
		ast.NewNodeList(defElem("wait_timeout", "-5s"))), "negative")
	assert.ErrorContains(t, applyPhaseOptions(&migratorpb.SetMigrationDirectionRequest{},
		ast.NewNodeList(defElem("nope", "1"))), "unknown PHASE option")
}

// TestIsStatMigrationSelect covers the planner-facing wrapper directly: the
// rest of this file only drives recognizeStatMigrationSelect indirectly
// through runSQL/execute, never this one-line entry point planner.go actually
// calls.
func TestIsStatMigrationSelect(t *testing.T) {
	stmts, err := parser.ParseSQL("SELECT * FROM multigres.stat_migration")
	require.NoError(t, err)
	ss, ok := stmts[0].(*ast.SelectStmt)
	require.True(t, ok)
	assert.True(t, IsStatMigrationSelect(ss))

	stmts, err = parser.ParseSQL("SELECT * FROM pg_catalog.pg_class")
	require.NoError(t, err)
	ss, ok = stmts[0].(*ast.SelectStmt)
	require.True(t, ok)
	assert.False(t, IsStatMigrationSelect(ss))
}

// TestShowConnectionsDescription covers the Describe-time answer for SHOW
// CONNECTION[S]: the fixed column list, with no RPC and no live data.
func TestShowConnectionsDescription(t *testing.T) {
	desc := ShowConnectionsDescription()
	require.True(t, desc.HasFields)
	require.Len(t, desc.Fields, len(showConnectionsColumns))
	for i, col := range showConnectionsColumns {
		assert.Equal(t, col, desc.Fields[i].Name)
	}
	assert.Empty(t, desc.Parameters)
}

// TestStatMigrationSelectDescription covers both of IsStatMigrationSelect's
// outcomes: a recognized query answers with its exact projected columns (not
// always the full statMigrationColumns list), and an unrecognized one reports
// ok=false so the caller falls through to ordinary routing.
func TestStatMigrationSelectDescription(t *testing.T) {
	stmts, err := parser.ParseSQL("SELECT migration_id, lag_bytes FROM multigres.stat_migration")
	require.NoError(t, err)
	selectStmt, ok := stmts[0].(*ast.SelectStmt)
	require.True(t, ok)
	desc, ok := StatMigrationSelectDescription(selectStmt)
	require.True(t, ok)
	require.True(t, desc.HasFields)
	require.Len(t, desc.Fields, 2)
	assert.Equal(t, "migration_id", desc.Fields[0].Name)
	assert.Equal(t, "lag_bytes", desc.Fields[1].Name)
	assert.Empty(t, desc.Parameters)

	stmts, err = parser.ParseSQL("SELECT * FROM pg_catalog.pg_class")
	require.NoError(t, err)
	selectStmt, ok = stmts[0].(*ast.SelectStmt)
	require.True(t, ok)
	_, ok = StatMigrationSelectDescription(selectStmt)
	assert.False(t, ok)
}

func TestColumnRefName(t *testing.T) {
	name, ok := columnRefName(ast.NewColumnRef(ast.NewString("migration_id")))
	assert.True(t, ok)
	assert.Equal(t, "migration_id", name)

	name, ok = columnRefName(ast.NewColumnRef(ast.NewString("stat_migration"), ast.NewString("migration_id")))
	assert.True(t, ok)
	assert.Equal(t, "migration_id", name)

	// A different qualifier is not ok.
	_, ok = columnRefName(ast.NewColumnRef(ast.NewString("other"), ast.NewString("migration_id")))
	assert.False(t, ok)

	// More than two parts, or an empty Fields list, is not ok.
	_, ok = columnRefName(ast.NewColumnRef(ast.NewString("a"), ast.NewString("b"), ast.NewString("c")))
	assert.False(t, ok)
	_, ok = columnRefName(&ast.ColumnRef{})
	assert.False(t, ok)
}

// whereClauseOf parses "SELECT 1 FROM t WHERE <cond>" and returns the
// resulting WHERE expression, for feeding directly to
// recognizeStatMigrationWhere without hand-building its AST.
func whereClauseOf(t *testing.T, cond string) ast.Node {
	t.Helper()
	stmts, err := parser.ParseSQL("SELECT 1 FROM t WHERE " + cond)
	require.NoError(t, err)
	ss, ok := stmts[0].(*ast.SelectStmt)
	require.True(t, ok)
	return ss.WhereClause
}

// TestRecognizeStatMigrationWhere covers the WHERE-recognizer's branches
// beyond the "migration_name = 'x'" and "migration_id = 1" cases
// TestMigrationDDL_StatMigrationSelect already drives end to end: operand
// order, migration_id as a string literal, a bad op, an unrecognized column,
// and a non-numeric migration_id string.
func TestRecognizeStatMigrationWhere(t *testing.T) {
	// No filter.
	ref, ok := recognizeStatMigrationWhere(nil)
	assert.True(t, ok)
	assert.Nil(t, ref)

	// Reversed operand order: literal = column.
	ref, ok = recognizeStatMigrationWhere(whereClauseOf(t, "'orders_move' = migration_name"))
	require.True(t, ok)
	assert.Equal(t, "orders_move", ref.GetName())

	// migration_id as a quoted string literal.
	ref, ok = recognizeStatMigrationWhere(whereClauseOf(t, "migration_id = '5'"))
	require.True(t, ok)
	assert.Equal(t, int64(5), ref.GetId())

	// migration_id as a non-numeric string is not ok.
	_, ok = recognizeStatMigrationWhere(whereClauseOf(t, "migration_id = 'nope'"))
	assert.False(t, ok)

	// A real migration_id (a UnixNano timestamp) always overflows int32, so the
	// lexer emits an *ast.Float, not an *ast.Integer, for the bare literal.
	ref, ok = recognizeStatMigrationWhere(whereClauseOf(t, "migration_id = 1762709523000000000"))
	require.True(t, ok)
	assert.Equal(t, int64(1762709523000000000), ref.GetId())

	// A genuinely fractional literal is not ok.
	_, ok = recognizeStatMigrationWhere(whereClauseOf(t, "migration_id = 5.5"))
	assert.False(t, ok)

	// migration_name compared against a non-string literal is not ok.
	_, ok = recognizeStatMigrationWhere(whereClauseOf(t, "migration_name = 5"))
	assert.False(t, ok)

	// A non-equality operator is not ok.
	_, ok = recognizeStatMigrationWhere(whereClauseOf(t, "migration_id > 1"))
	assert.False(t, ok)

	// An unrecognized column is not ok.
	_, ok = recognizeStatMigrationWhere(whereClauseOf(t, "migration_foo = 1"))
	assert.False(t, ok)

	// Something that isn't an A_Expr at all is not ok.
	_, ok = recognizeStatMigrationWhere(ast.NewString("not an expr"))
	assert.False(t, ok)
}

func TestQuoteCompositeField(t *testing.T) {
	assert.Equal(t, `""`, quoteCompositeField(""))
	assert.Equal(t, "plain", quoteCompositeField("plain"))
	// A character that would otherwise be ambiguous in the paren/comma format
	// forces quoting, with '"' and '\' backslash-escaped.
	assert.Equal(t, `"a,b"`, quoteCompositeField("a,b"))
	assert.Equal(t, `"a(b)"`, quoteCompositeField("a(b)"))
	assert.Equal(t, `"a\"b"`, quoteCompositeField(`a"b`))
	assert.Equal(t, `"a\\b"`, quoteCompositeField(`a\b`))
	// Leading/trailing whitespace also forces quoting even with no other
	// special character.
	assert.Equal(t, `" a"`, quoteCompositeField(" a"))
}
