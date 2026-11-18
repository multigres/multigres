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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/backingconn"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/tools/pgutil"
	"github.com/multigres/multigres/go/tools/units"
)

// MigrationBackend gives the migration DDL primitive what it needs: a migrator
// gRPC client, resolved lazily so it follows the shard primary. Named
// connections are no longer gateway-local; CREATE/ALTER/DROP/SHOW CONNECTION
// all go through the same client, so they persist in the shard's Postgres and
// are visible from any gateway replica. TargetDatabase, TargetShard, and
// TargetTableGroup are the target for new migrations.
type MigrationBackend struct {
	// Client returns a migrator client aimed at the current shard primary, or
	// nil if none is reachable.
	Client func() migratorpb.MigratorClient

	TargetDatabase   string
	TargetShard      string
	TargetTableGroup string
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

// String returns a debug representation, logged verbatim by the executor on
// every plan. CREATE CONNECTION carries the source password as a SQL literal
// in m.sql, so that statement kind is redacted to just its kind and
// connection name; every other migration/connection statement never carries
// a secret and is still logged in full.
func (m *MigrationDDL) String() string {
	if redacted, ok := ast.RedactIfCredentialBearing(m.stmt); ok {
		return "MigrationDDL(" + redacted + ")"
	}
	return "MigrationDDL(" + m.sql + ")"
}

func (m *MigrationDDL) StreamExecute(
	ctx context.Context,
	_ IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	_ []*ast.A_Const,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if err := checkMigrationDDLPrivilege(conn); err != nil {
		return err
	}
	if err := rejectMigrationDDLInTransaction(conn, state); err != nil {
		return err
	}
	result, err := m.execute(ctx, true, conn.User())
	if err != nil {
		return err
	}
	return callback(ctx, result)
}

func (m *MigrationDDL) PortalStreamExecute(
	ctx context.Context,
	_ IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	_ int32,
	includeDescribe bool,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if err := checkMigrationDDLPrivilege(conn); err != nil {
		return err
	}
	if err := rejectMigrationDDLInTransaction(conn, state); err != nil {
		return err
	}
	if returnsRows(m.stmt) && RequestsNonTextResultFormat(portalInfo) {
		return mterrors.NewFeatureNotSupported("binary result format is not supported for this statement")
	}
	result, err := m.execute(ctx, includeDescribe, conn.User())
	if err != nil {
		return err
	}
	return callback(ctx, result)
}

// rejectMigrationDDLInTransaction rejects a migration/connection DDL statement
// run inside a transaction block, the same way PG's own PreventInTransactionBlock
// rejects statements like DISCARD ALL (see discard_all_primitive.go). These
// statements take effect immediately via a migrator RPC call, not through the
// backend Postgres connection the surrounding transaction controls, so a
// ROLLBACK after one cannot undo it — only reachable for CREATE/ALTER/DROP
// MIGRATION/CONNECTION; SHOW CONNECTION[S] and the stat_migration SELECT are
// read-only and fine inside a transaction, but are rejected here too for
// simplicity, since nothing in this gateway path should distinguish them.
func rejectMigrationDDLInTransaction(conn *server.Conn, state *handler.MultigatewayConnectionState) error {
	if conn != nil && conn.IsInTransaction() {
		return mterrors.NewInTransactionBlock("migration/connection DDL")
	}
	// An extended-protocol pipeline (several Execute messages before a Sync)
	// is PostgreSQL's own implicit transaction, even without an explicit
	// BEGIN: conn.IsInTransaction() alone misses it, since that only reflects
	// a real BEGIN. notePipelinedExecute leaves this flag clear across a
	// successful migration DDL execution (migration_ddl is in its no-op
	// case, mirroring tablegroup DDL), so this only fires for a pipeline an
	// earlier, different statement actually opened.
	if state != nil && state.Pipelining() {
		return mterrors.NewInPipeline("migration/connection DDL")
	}
	return nil
}

// checkMigrationDDLPrivilege gates every migration/connection DDL statement on
// the caller being able to perform the logical-replication setup a migration
// drives. Without this, an authenticated client with no such capability could
// still trigger migration setup, a cutover, or read SHOW CONNECTION's stored
// source credentials via the gateway, which PostgreSQL itself would never let
// that role do directly.
func checkMigrationDDLPrivilege(conn *server.Conn) error {
	if conn != nil && conn.CanCreateMigration() {
		return nil
	}
	return mterrors.NewPgError("ERROR", mterrors.PgSSInsufficientPrivilege,
		"insufficient privilege to manage migrations or connections", "")
}

// execute dispatches on the statement type. includeFields controls whether a
// result set attaches column metadata.
func (m *MigrationDDL) execute(ctx context.Context, includeFields bool, callerRole string) (*sqltypes.Result, error) {
	if m.backend == nil {
		return nil, errors.New("the migration interface is not configured on this gateway")
	}
	switch s := m.stmt.(type) {
	case *ast.CreateConnectionStmt:
		client, err := m.backend.client()
		if err != nil {
			return nil, err
		}
		_, existsErr := client.GetConnection(ctx, &migratorpb.GetConnectionRequest{Ref: connByName(s.Name)})
		switch {
		case existsErr == nil:
			if s.IfNotExists {
				return commandTag("CREATE CONNECTION"), nil
			}
			return nil, mterrors.NewPgError("ERROR", mterrors.PgSSDuplicateObject,
				fmt.Sprintf("connection %q already exists", s.Name), "")
		case mterrors.Code(mterrors.FromGRPC(existsErr)) != mtrpcpb.Code_NOT_FOUND:
			// The existence check itself failed for an unrelated reason (e.g. a
			// transient gRPC error) — propagate it rather than silently treating
			// it as "doesn't exist" and attempting to create a possible duplicate.
			return nil, existsErr
		}
		dsn, err := connectionDSN(s.Options)
		if err != nil {
			return nil, err
		}
		if _, err := client.CreateConnection(ctx, &migratorpb.CreateConnectionRequest{
			Connection: &migratorpb.Connection{
				Name: s.Name,
				Dsn:  dsn,
			},
			CallerRole: callerRole,
		}); err != nil {
			return nil, err
		}
		return commandTag("CREATE CONNECTION"), nil

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
		return m.createMigration(ctx, s, callerRole)

	case *ast.AlterMigrationStmt:
		return m.alterMigration(ctx, s, callerRole)

	case *ast.DropMigrationStmt:
		return m.dropMigration(ctx, s, callerRole)

	case *ast.SelectStmt:
		return m.statMigration(ctx, s, includeFields)

	default:
		return nil, mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
			fmt.Sprintf("unsupported migration statement %T", m.stmt), "")
	}
}

func (m *MigrationDDL) createMigration(ctx context.Context, s *ast.CreateMigrationStmt, callerRole string) (*sqltypes.Result, error) {
	client, err := m.backend.client()
	if err != nil {
		return nil, err
	}
	req := &migratorpb.CreateMigrationRequest{
		Migration: &migratorpb.MigrationRecord{
			Name:           s.Name,
			ConnectionName: s.Connection,
			Target: &clustermetadatapb.ShardRef{
				Database:   m.backend.TargetDatabase,
				TableGroup: m.backend.TargetTableGroup,
				Shard:      m.backend.TargetShard,
			},
		},
		Options:    &migratorpb.MigrationOptions{},
		CallerRole: callerRole,
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
// ALTER/DROP/SHOW statements, which only ever carry a name, not an id.
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

func (m *MigrationDDL) alterMigration(ctx context.Context, s *ast.AlterMigrationStmt, callerRole string) (*sqltypes.Result, error) {
	client, err := m.backend.client()
	if err != nil {
		return nil, err
	}
	req := &migratorpb.SetMigrationDirectionRequest{Ref: refByName(s.Name), Direction: directionToProto(s.Direction), CallerRole: callerRole}
	if s.When != nil {
		// Only the EXPORT cutover's readiness gate is backed: lag_bytes < size,
		// translated onto max_lag_bytes. PHASE IMPORT has no backed use for WHEN, and
		// any other field/operator combination on either direction is rejected the
		// same way the rest of this grammar handles not-yet-implemented forms.
		field, op, value, ok := migrationCondParts(s.When)
		if !ok || s.Direction != ast.MigrationDirectionExport || field != "lag_bytes" || op != "<" {
			return nil, mterrors.NewFeatureNotSupported(fmt.Sprintf(
				"WHEN (%s) is not supported for ALTER MIGRATION ... PHASE %s; only PHASE EXPORT WHEN (lag_bytes < size) is backed",
				s.When.SqlString(), s.Direction,
			))
		}
		n, perr := units.ParseBytes(condValueString(value))
		if perr != nil {
			return nil, fmt.Errorf("lag_bytes: %w", perr)
		}
		req.MaxLagBytes = &n
	}
	if err = applyPhaseOptions(req, s.Options); err != nil {
		return nil, err
	}
	if _, err = client.SetMigrationDirection(ctx, req); err != nil {
		// IF EXISTS swallows "not found"; other errors propagate. FromGRPC is needed
		// before the code check: the server returns a bare status.Error(codes.NotFound,
		// ...) here, with no mtrpcpb.RPCError detail for mterrors.Code to read directly.
		if s.IfExists && mterrors.Code(mterrors.FromGRPC(err)) == mtrpcpb.Code_NOT_FOUND {
			return commandTag("ALTER MIGRATION"), nil
		}
		return nil, err
	}
	return commandTag("ALTER MIGRATION"), nil
}

func (m *MigrationDDL) dropMigration(ctx context.Context, s *ast.DropMigrationStmt, callerRole string) (*sqltypes.Result, error) {
	client, err := m.backend.client()
	if err != nil {
		return nil, err
	}
	for _, name := range nameList(s.Names) {
		req := &migratorpb.DropMigrationRequest{Ref: refByName(name), Force: s.Force, CallerRole: callerRole}
		if s.When != nil {
			// Only lag_bytes = 0 is backed: DropMigrationRequest has no threshold field,
			// only the boolean Wait, which already models blocking until fully caught up.
			// Any other field/operator/value is rejected the same way the rest of this
			// grammar handles not-yet-implemented forms.
			notSupported := mterrors.NewFeatureNotSupported(fmt.Sprintf(
				"WHEN (%s) is not supported for DROP MIGRATION; only WHEN (lag_bytes = 0) is backed", s.When.SqlString(),
			))
			field, op, value, ok := migrationCondParts(s.When)
			if !ok || field != "lag_bytes" || op != "=" {
				return nil, notSupported
			}
			n, perr := units.ParseBytes(condValueString(value))
			if perr != nil || n != 0 {
				return nil, notSupported
			}
			req.Wait = true
		}
		if err := applyDropWithOptions(req, s.Options); err != nil {
			return nil, err
		}
		if _, err := client.DropMigration(ctx, req); err != nil {
			// IF EXISTS swallows "not found"; other errors propagate. FromGRPC is needed
			// before the code check: the server returns a bare status.Error(codes.NotFound,
			// ...) here, with no mtrpcpb.RPCError detail for mterrors.Code to read directly.
			if s.IfExists && mterrors.Code(mterrors.FromGRPC(err)) == mtrpcpb.Code_NOT_FOUND {
				continue
			}
			return nil, err
		}
	}
	return commandTag("DROP MIGRATION"), nil
}

// fetchMigrations fetches either the one migration addressed by ref, or every
// migration when ref is nil, listing ids via ListMigrations and then fetching
// each one's full info via GetMigration.
func fetchMigrations(ctx context.Context, client migratorpb.MigratorClient, ref *migratorpb.MigrationRef) ([]*migratorpb.GetMigrationResponse, error) {
	if ref != nil {
		resp, err := client.GetMigration(ctx, &migratorpb.GetMigrationRequest{Ref: ref})
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

// statMigrationColumns is the canonical, view-definition-ordered column list
// for multigres.stat_migration. A bare SELECT * expands to this list in this
// order.
var statMigrationColumns = []string{
	"migration_id", "migration_name", "connection_name", "migration_target",
	"migration_phase", "active_direction", "total_relations", "ready_relations",
	"lag_bytes", "lag_seconds", "last_error",
}

// showConnectionsColumns is the fixed column list SHOW CONNECTION[S] returns.
var showConnectionsColumns = []string{"name", "host", "port", "dbname", "user", "sslmode"}

// IsStatMigrationSelect reports whether ss is a supported pseudo-view query
// against multigres.stat_migration, for the planner's T_SelectStmt routing
// decision.
func IsStatMigrationSelect(ss *ast.SelectStmt) bool {
	_, _, ok := recognizeStatMigrationSelect(ss)
	return ok
}

// recognizeStatMigrationSelect reports whether ss is a supported pseudo-view
// query against multigres.stat_migration: a column-list projection or *, no
// joins or aggregation, with at most one equality filter on migration_name or
// migration_id. If so, it returns the migration it filters to, nil meaning
// every migration, and the column list to project, in request order.
//
// Anything outside that shape returns ok=false, and the caller falls through
// to ordinary query routing, which still resolves correctly against the real
// multigres.stat_migration view, subject to the normal serving gate. This
// recognizer only identifies the shapes worth the gate-bypass optimization,
// not the full set of valid queries against the view.
func recognizeStatMigrationSelect(ss *ast.SelectStmt) (ref *migratorpb.MigrationRef, columns []string, ok bool) {
	if ss.DistinctClause != nil || ss.IntoClause != nil || ss.GroupClause != nil ||
		ss.GroupDistinct || ss.HavingClause != nil || ss.WindowClause != nil ||
		ss.ValuesLists != nil || ss.SortClause != nil || ss.LimitOffset != nil ||
		ss.LimitCount != nil || ss.LockingClause != nil || ss.WithClause != nil ||
		ss.Larg != nil || ss.Rarg != nil {
		return nil, nil, false
	}
	if ss.FromClause == nil || len(ss.FromClause.Items) != 1 {
		return nil, nil, false
	}
	rv, isRangeVar := ss.FromClause.Items[0].(*ast.RangeVar)
	if !isRangeVar || rv.Alias != nil || rv.SchemaName != "multigres" || rv.RelName != "stat_migration" {
		return nil, nil, false
	}
	columns, ok = recognizeStatMigrationTargetList(ss.TargetList)
	if !ok {
		return nil, nil, false
	}
	ref, ok = recognizeStatMigrationWhere(ss.WhereClause)
	if !ok {
		return nil, nil, false
	}
	return ref, columns, true
}

// recognizeStatMigrationTargetList accepts a bare "*", which expands to
// statMigrationColumns, or a list of unaliased, unqualified-or-self-qualified
// references to columns of multigres.stat_migration.
func recognizeStatMigrationTargetList(list *ast.NodeList) ([]string, bool) {
	if list == nil || len(list.Items) == 0 {
		return nil, false
	}
	if len(list.Items) == 1 {
		if rt, isRT := list.Items[0].(*ast.ResTarget); isRT && rt.Name == "" {
			if cr, isCR := rt.Val.(*ast.ColumnRef); isCR && cr.Fields != nil && len(cr.Fields.Items) == 1 {
				if _, isStar := cr.Fields.Items[0].(*ast.A_Star); isStar {
					return append([]string(nil), statMigrationColumns...), true
				}
			}
		}
	}
	cols := make([]string, 0, len(list.Items))
	for _, item := range list.Items {
		rt, isRT := item.(*ast.ResTarget)
		if !isRT || rt.Name != "" {
			return nil, false
		}
		cr, isCR := rt.Val.(*ast.ColumnRef)
		if !isCR {
			return nil, false
		}
		name, ok := columnRefName(cr)
		if !ok || !isStatMigrationColumn(name) {
			return nil, false
		}
		cols = append(cols, name)
	}
	return cols, true
}

// columnRefName extracts a bare or self-qualified column name; any other
// shape is not ok.
func columnRefName(cr *ast.ColumnRef) (string, bool) {
	if cr.Fields == nil {
		return "", false
	}
	switch items := cr.Fields.Items; len(items) {
	case 1:
		s, isStr := items[0].(*ast.String)
		if !isStr {
			return "", false
		}
		return s.SVal, true
	case 2:
		qualifier, isStr := items[0].(*ast.String)
		if !isStr || qualifier.SVal != "stat_migration" {
			return "", false
		}
		name, isStr2 := items[1].(*ast.String)
		if !isStr2 {
			return "", false
		}
		return name.SVal, true
	default:
		return "", false
	}
}

func isStatMigrationColumn(name string) bool {
	return slices.Contains(statMigrationColumns, name)
}

// migrationCondParts unpacks a WHEN (cond) expression into its field name,
// operator, and constant value; ok is false for anything other than a simple
// ColumnRef compared to a constant.
func migrationCondParts(cond *ast.A_Expr) (field, op string, value ast.Node, ok bool) {
	if cond == nil || cond.Kind != ast.AEXPR_OP || cond.Name == nil || len(cond.Name.Items) != 1 {
		return "", "", nil, false
	}
	opName, isStr := cond.Name.Items[0].(*ast.String)
	if !isStr {
		return "", "", nil, false
	}
	cr, isCR := cond.Lexpr.(*ast.ColumnRef)
	if !isCR {
		return "", "", nil, false
	}
	name, nameOK := columnRefName(cr)
	if !nameOK {
		return "", "", nil, false
	}
	ac, isAC := cond.Rexpr.(*ast.A_Const)
	if !isAC || ac.Isnull {
		return "", "", nil, false
	}
	return name, opName.SVal, ac.Val, true
}

// recognizeStatMigrationWhere reports whether where is either nil, meaning no
// filter, or a single equality comparison of migration_name or migration_id
// against a literal, in either operand order.
func recognizeStatMigrationWhere(where ast.Node) (*migratorpb.MigrationRef, bool) {
	if where == nil {
		return nil, true
	}
	expr, isExpr := where.(*ast.A_Expr)
	if !isExpr || expr.Kind != ast.AEXPR_OP || expr.Name == nil || len(expr.Name.Items) != 1 {
		return nil, false
	}
	opName, isStr := expr.Name.Items[0].(*ast.String)
	if !isStr || opName.SVal != "=" {
		return nil, false
	}
	col, lit, ok := columnAndLiteral(expr.Lexpr, expr.Rexpr)
	if !ok {
		col, lit, ok = columnAndLiteral(expr.Rexpr, expr.Lexpr)
	}
	if !ok {
		return nil, false
	}
	switch col {
	case "migration_name":
		s, isStr := lit.(*ast.String)
		if !isStr {
			return nil, false
		}
		return refByName(s.SVal), true
	case "migration_id":
		switch v := lit.(type) {
		case *ast.Integer:
			return refByID(int64(v.IVal)), true
		case *ast.String:
			id, err := strconv.ParseInt(v.SVal, 10, 64)
			if err != nil {
				return nil, false
			}
			return refByID(id), true
		case *ast.Float:
			// Real migration IDs are UnixNano timestamps, always past int32 —
			// the lexer (mirroring Postgres's own scanner) lexes any integer
			// literal that overflows int32 as a Float, not an Integer, so this
			// is the form migration_id = <real id> actually takes. ParseInt
			// itself rejects anything with a decimal point or exponent, so a
			// genuinely fractional literal still falls through to ok=false.
			id, err := strconv.ParseInt(v.FVal, 10, 64)
			if err != nil {
				return nil, false
			}
			return refByID(id), true
		default:
			return nil, false
		}
	default:
		return nil, false
	}
}

// columnAndLiteral reports whether lhs is a reference to migration_name or
// migration_id and rhs is a literal constant, returning that column name and
// the literal's value node.
func columnAndLiteral(lhs, rhs ast.Node) (col string, lit ast.Value, ok bool) {
	cr, isCR := lhs.(*ast.ColumnRef)
	if !isCR {
		return "", nil, false
	}
	name, nameOK := columnRefName(cr)
	if !nameOK || (name != "migration_name" && name != "migration_id") {
		return "", nil, false
	}
	ac, isAC := rhs.(*ast.A_Const)
	if !isAC || ac.Isnull {
		return "", nil, false
	}
	return name, ac.Val, true
}

// statMigration answers a recognized pseudo-view query directly via the
// migrator RPC. ok is always true here since the planner only routes a
// *ast.SelectStmt to this primitive after confirming that itself.
func (m *MigrationDDL) statMigration(ctx context.Context, ss *ast.SelectStmt, includeFields bool) (*sqltypes.Result, error) {
	ref, columns, ok := recognizeStatMigrationSelect(ss)
	if !ok {
		return nil, mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
			"unsupported query against multigres.stat_migration", "")
	}
	client, err := m.backend.client()
	if err != nil {
		return nil, err
	}
	migrations, err := fetchMigrations(ctx, client, ref)
	if err != nil {
		return nil, err
	}
	result := &sqltypes.Result{CommandTag: fmt.Sprintf("SELECT %d", len(migrations))}
	if includeFields {
		result.Fields = textFields(columns)
	}
	for _, mig := range migrations {
		row := make([][]byte, len(columns))
		for i, col := range columns {
			row[i] = statMigrationColumnValue(mig, col)
		}
		result.Rows = append(result.Rows, sqltypes.MakeRow(row))
	}
	return result, nil
}

// ShowConnectionsVariable is the pseudo-GUC name the bare plural
// "SHOW CONNECTIONS" parses as: it has no dedicated grammar production (to
// avoid keyword collisions with ordinary identifiers), so it parses as an
// ordinary SHOW of a GUC that doesn't exist, and the planner (planVariableShowStmt)
// and IsShowConnections both recognize it by this name instead.
const ShowConnectionsVariable = "connections"

// IsShowConnections reports whether stmt is either form that lists stored
// connections: SHOW CONNECTION name, which parses as *ast.ShowConnectionsStmt
// directly, or the bare plural SHOW CONNECTIONS, which parses as an ordinary
// *ast.VariableShowStmt naming ShowConnectionsVariable. The name compares
// case-insensitively, matching PostgreSQL's handling of unquoted (lowercased)
// and quoted (case-preserving) identifiers.
func IsShowConnections(stmt ast.Stmt) bool {
	switch s := stmt.(type) {
	case *ast.ShowConnectionsStmt:
		return true
	case *ast.VariableShowStmt:
		return strings.ToLower(s.Name) == ShowConnectionsVariable
	default:
		return false
	}
}

// returnsRows reports whether stmt is one of the two migration/connection DDL
// shapes that answer with a row set (SHOW CONNECTION[S], a recognized
// multigres.stat_migration query) rather than a bare command tag. Only these
// two have a result-format question to ask in the first place.
func returnsRows(stmt ast.Stmt) bool {
	if IsShowConnections(stmt) {
		return true
	}
	ss, ok := stmt.(*ast.SelectStmt)
	return ok && IsStatMigrationSelect(ss)
}

// RequestsNonTextResultFormat reports whether portalInfo's Bind requested a
// non-text (binary) format for any result column. MigrationDDL always
// synthesizes its rows as text, so a binary request can't be honored —
// callers reject it explicitly rather than silently answering with
// mismatched metadata and bytes. portalInfo is nil for a statement describe
// (no Bind has happened yet), which always answers as text.
func RequestsNonTextResultFormat(portalInfo *preparedstatement.PortalInfo) bool {
	if portalInfo == nil {
		return false
	}
	for _, f := range portalInfo.Portal.GetResultFormats() {
		if f != 0 {
			return true
		}
	}
	return false
}

// ShowConnectionsDescription answers Describe for SHOW CONNECTION[S] at the
// gateway: a fixed column set, independent of any live connection data, so no
// migrator RPC round trip (or even a configured migration backend) is needed
// to answer it.
func ShowConnectionsDescription() *query.StatementDescription {
	return &query.StatementDescription{
		Parameters: []*query.ParameterDescription{},
		Fields:     textFields(showConnectionsColumns),
		HasFields:  true,
	}
}

// StatMigrationSelectDescription answers Describe for a recognized
// multigres.stat_migration pseudo-view query at the gateway, with the exact
// projected column list ss requests — the same recognizer planMigrationDDL's
// routing decision (IsStatMigrationSelect) uses, so Describe and Execute
// agree on shape without a migrator RPC round trip. ok is false for anything
// outside the recognized shape, matching IsStatMigrationSelect; the caller
// falls through to ordinary routing in that case, same as Execute does.
//
// This answers locally regardless of whether a migration backend is
// configured: multigres.stat_migration is a real Postgres view with this
// exact column set (recognizeStatMigrationSelect's doc comment), so ordinary
// routing — the only other path reachable when a backend isn't configured —
// would describe it identically.
func StatMigrationSelectDescription(ss *ast.SelectStmt) (desc *query.StatementDescription, ok bool) {
	_, columns, ok := recognizeStatMigrationSelect(ss)
	if !ok {
		return nil, false
	}
	return &query.StatementDescription{
		Parameters: []*query.ParameterDescription{},
		Fields:     textFields(columns),
		HasFields:  true,
	}, true
}

// statMigrationColumnValue projects one column of multigres.stat_migration
// from a migration's RPC response. migration_phase/active_direction strip the
// proto enum's MIGRATION_PHASE_/MIGRATION_DIRECTION_ prefix so the rendered
// text matches the real view's plain string column, not the proto enum's full
// name.
func statMigrationColumnValue(mig *migratorpb.GetMigrationResponse, col string) []byte {
	cfg, st := mig.GetMigration(), mig.GetStatus()
	switch col {
	case "migration_id":
		return []byte(strconv.FormatInt(cfg.GetId(), 10))
	case "migration_name":
		return []byte(cfg.GetName())
	case "connection_name":
		return []byte(cfg.GetConnectionName())
	case "migration_target":
		t := cfg.GetTarget()
		return []byte(formatShardKeyComposite(t.GetDatabase(), t.GetTableGroup(), t.GetShard()))
	case "migration_phase":
		return []byte(strings.TrimPrefix(st.GetPhase().String(), "MIGRATION_PHASE_"))
	case "active_direction":
		return []byte(strings.TrimPrefix(st.GetActiveDirection().String(), "MIGRATION_DIRECTION_"))
	case "total_relations":
		return []byte(strconv.FormatInt(st.GetTotalRelations(), 10))
	case "ready_relations":
		return []byte(strconv.FormatInt(st.GetReadyRelations(), 10))
	case "lag_bytes":
		return []byte(strconv.FormatUint(st.GetLagBytes(), 10))
	case "lag_seconds":
		return []byte(strconv.FormatFloat(st.GetLagSeconds(), 'f', 3, 64))
	case "last_error":
		return []byte(st.GetLastError())
	default:
		// Unreachable: recognizeStatMigrationSelect only ever returns names from
		// statMigrationColumns.
		return nil
	}
}

// formatShardKeyComposite renders the shard key the same way Postgres formats
// a composite-type column, so a client comparing the gateway's answer to the
// real view's migration_target column sees identical text.
func formatShardKeyComposite(database, tableGroup, shard string) string {
	return "(" + strings.Join([]string{
		quoteCompositeField(database),
		quoteCompositeField(tableGroup),
		quoteCompositeField(shard),
	}, ",") + ")"
}

// quoteCompositeField quotes one composite field the way Postgres's record
// output function does: empty as "", and double-quoted with backslash/quote
// escaping when it contains a character that would otherwise be ambiguous in
// the comma-separated, paren-delimited format.
func quoteCompositeField(s string) string {
	if s == "" {
		return `""`
	}
	if !strings.ContainsAny(s, `,()"\`) && strings.TrimSpace(s) == s {
		return s
	}
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		if r == '"' || r == '\\' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	b.WriteByte('"')
	return b.String()
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
	result := &sqltypes.Result{}
	if includeFields {
		result.Fields = textFields(showConnectionsColumns)
	}
	for _, e := range resp.GetConnections() {
		if s.Name != "" && e.GetName() != s.Name {
			continue
		}
		if e.GetDsn() == "" {
			// A connection that cannot be read is listed with an empty dsn. Its name is all there is
			// to show, and the other columns are NULL: every connection that can be read has a
			// value in each of them.
			result.Rows = append(result.Rows, sqltypes.MakeRow([][]byte{[]byte(e.GetName()), nil, nil, nil, nil, nil}))
			continue
		}
		kv := pgutil.ParseConnInfo(e.GetDsn())
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

// directionToProto converts the grammar's MigrationDirection, PHASE IMPORT or
// PHASE EXPORT, to the proto enum SetMigrationDirectionRequest carries.
func directionToProto(d ast.MigrationDirection) migratorpb.MigrationDirection {
	if d == ast.MigrationDirectionExport {
		return migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT
	}
	return migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT
}

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

// defElemValue returns the option value as a plain, unescaped string.
// CONNECTION OPTIONS values are always Sconst, but WITH (...) option values
// are def_arg, which also allows a bare NumericOnly. For an *ast.String, SVal
// is the unescaped value directly; SqlString() instead re-serializes it as a
// quoted SQL literal, the wrong direction for recovering the original value.
// Other literal types never need unescaping, so SqlString() is exact for
// those.
func defElemValue(d *ast.DefElem) string {
	if d.Arg == nil {
		return ""
	}
	if s, ok := d.Arg.(*ast.String); ok {
		return s.SVal
	}
	return d.Arg.SqlString()
}

// connectionDSN turns a CONNECTION OPTIONS (...) list into a libpq conninfo string exactly as
// written: option names lowercased, values quoted. It applies no default and requires nothing: the
// migrator does both, once, for every connection it is given (backingconn.FromDSN). The one thing
// it says is which option is wrong when the name is not a libpq parameter at all, since the
// migrator, which checks a string, cannot name a parameter without repeating text of one.
func connectionDSN(list *ast.NodeList) (string, error) {
	kv := make(map[string]string)
	if list != nil {
		for _, it := range list.Items {
			d, ok := it.(*ast.DefElem)
			if !ok {
				continue
			}
			// Lowercase the stored key too, not just the lookup: option names are
			// case-insensitive SQL identifiers, and SHOW CONNECTIONS reads fixed lowercase
			// keys, so a mixed-case key stored here would be invisible to it.
			name := strings.ToLower(d.Defname)
			if !backingconn.IsParam(name) {
				return "", fmt.Errorf("CONNECTION option %q is not a libpq connection parameter", d.Defname)
			}
			// As CREATE SERVER ... OPTIONS does: an option given twice is an error, not a
			// last-one-wins.
			if _, dup := kv[name]; dup {
				return "", mterrors.NewPgError("ERROR", mterrors.PgSSDuplicateObject,
					fmt.Sprintf("option %q provided more than once", d.Defname), "")
			}
			kv[name] = defElemValue(d)
		}
	}
	return pgutil.ConnInfoString(kv), nil
}

// selectionObjects converts a pub_obj_list from the FOR clause into a single
// migrator SelectionObject: a table list or a schema list, never a mix,
// mirroring Postgres's own restriction that CREATE PUBLICATION's FOR clause is
// one form or the other. Per-table column lists, WHERE filters, and ONLY
// exclusions are not carried on the wire, so a caller that specifies any of
// them gets an explicit error rather than a silently dropped clause.
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
				return nil, mterrors.NewFeatureNotSupported("ONLY (excluding partition descendants) is not yet supported")
			}
			if pt.Columns != nil && len(pt.Columns.Items) > 0 {
				return nil, mterrors.NewFeatureNotSupported("per-table column lists are not yet supported")
			}
			if pt.WhereClause != nil {
				return nil, mterrors.NewFeatureNotSupported("per-table WHERE row filters are not yet supported")
			}
			tables = append(tables, rangeVarName(pt.Relation))
		case ast.PUBLICATIONOBJ_TABLES_IN_SCHEMA:
			if len(tables) > 0 {
				return nil, errors.New("migrator: a migration's FOR clause cannot mix tables and schemas")
			}
			schemas = append(schemas, spec.Name)
		default:
			return nil, mterrors.NewFeatureNotSupported("FOR TABLES IN SCHEMA CURRENT_SCHEMA is not yet supported")
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

// applyPhaseOptions maps a PHASE ... WITH (...) option list onto
// SetMigrationDirectionRequest. The only recognized option is wait_timeout.
// The readiness threshold itself is WHEN (lag_bytes < size)'s job, not WITH's.
func applyPhaseOptions(req *migratorpb.SetMigrationDirectionRequest, list *ast.NodeList) error {
	if list == nil {
		return nil
	}
	for _, it := range list.Items {
		d, ok := it.(*ast.DefElem)
		if !ok {
			continue
		}
		switch strings.ToLower(d.Defname) {
		case "wait_timeout":
			secs, err := parseTimeoutSeconds(defElemValue(d))
			if err != nil {
				return err
			}
			req.WaitTimeoutSeconds = &secs
		default:
			return fmt.Errorf("unknown PHASE option %q", d.Defname)
		}
	}
	return nil
}

// applyDropWithOptions maps a DROP MIGRATION ... WITH (...) option list onto
// DropMigrationRequest. The only recognized option is wait_timeout, bounding
// how long WHEN's condition is waited on.
func applyDropWithOptions(req *migratorpb.DropMigrationRequest, list *ast.NodeList) error {
	if list == nil {
		return nil
	}
	for _, it := range list.Items {
		d, ok := it.(*ast.DefElem)
		if !ok {
			continue
		}
		switch strings.ToLower(d.Defname) {
		case "wait_timeout":
			secs, err := parseTimeoutSeconds(defElemValue(d))
			if err != nil {
				return err
			}
			req.WaitTimeoutSeconds = secs
		default:
			return fmt.Errorf("unknown DROP MIGRATION option %q", d.Defname)
		}
	}
	return nil
}

// condValueString extracts a MigrationCond.Value node's textual form, the same
// way defElemValue does for a DefElem's Arg. Used so units.ParseBytes can
// parse a WHEN clause's constant regardless of whether it was written as a
// bare number or a quoted size literal.
func condValueString(n ast.Node) string {
	if s, ok := n.(*ast.String); ok {
		return s.SVal
	}
	return n.SqlString()
}

// parseTimeoutSeconds accepts a Go duration string or a bare integer number of
// seconds, returning whole seconds. It rejects sub-second and negative values
// because the RPC field is integer seconds.
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
	if req.Options == nil {
		req.Options = &migratorpb.MigrationOptions{}
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
			// inverted skip_copy_data.
			copyData, err := parseBoolOption("copy_data", val)
			if err != nil {
				return err
			}
			req.Options.SkipCopyData = !copyData
		case "skip_schema_copy":
			skip, err := parseBoolOption("skip_schema_copy", val)
			if err != nil {
				return err
			}
			req.Options.SkipSchemaCopy = skip
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

// parseRoleList splits a comma-separated quiesce_roles option value into
// trimmed, non-empty role names.
func parseRoleList(v string) []string {
	var roles []string
	for r := range strings.SplitSeq(v, ",") {
		if r = strings.TrimSpace(r); r != "" {
			roles = append(roles, r)
		}
	}
	return roles
}

// parseBoolOption parses a WITH-option's string value as a boolean, using
// Postgres's own boolean grammar (sqltypes.ParseBool) and rejecting anything
// that isn't one of the recognized spellings. A typo must be a syntax error,
// not silently fall through to false and change which migration option the
// caller asked for.
func parseBoolOption(name, v string) (bool, error) {
	b, ok := sqltypes.ParseBool(v)
	if !ok {
		return false, fmt.Errorf("%s must be a boolean value: %q", name, v)
	}
	return b, nil
}
