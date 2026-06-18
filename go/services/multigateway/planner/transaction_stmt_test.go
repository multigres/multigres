// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planner

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// newPortalInfoFor parses sql and wraps the resulting single statement as a
// PortalInfo the way the handler would at Bind time.
func newPortalInfoFor(t *testing.T, sql string) *preparedstatement.PortalInfo {
	t.Helper()
	psi, err := preparedstatement.NewPreparedStatementInfo(
		protoutil.NewPreparedStatement("ppstmt", sql, nil),
	)
	require.NoError(t, err)
	portal := protoutil.NewPortal("ppstmt", psi.Name, nil, nil, nil)
	return preparedstatement.NewPortalInfo(psi, portal)
}

// planPortal plans sql on the extended-protocol (portal) path, mirroring how the
// executor drives the portal flow through Plan with IsPortal set. Routable
// statements come back as a plain Route (which reissues the portal to the
// backend); gateway-local statements come back as their dedicated primitive.
func planPortal(t *testing.T, p *Planner, conn *server.Conn, sql string) (*engine.Plan, error) {
	t.Helper()
	pi := newPortalInfoFor(t, sql)
	return p.Plan(pi.PreparedStatementInfo.Query, pi.PreparedStatementInfo.AstStmt(), conn, PlanOptions{IsPortal: true})
}

// TestPlanPortal_TransactionStmt pins that BEGIN/COMMIT/ROLLBACK over the
// extended protocol resolve to the TransactionPrimitive so they are handled
// locally by the gateway. If this returns nil the executor sends the
// statement to a pooled backend connection, which leaks open (or aborted)
// transactions across clients when the connection is recycled.
func TestPlanPortal_TransactionStmt(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantKind ast.TransactionStmtKind
	}{
		{name: "BEGIN", sql: "BEGIN", wantKind: ast.TRANS_STMT_BEGIN},
		{name: "START TRANSACTION", sql: "START TRANSACTION", wantKind: ast.TRANS_STMT_START},
		{name: "COMMIT", sql: "COMMIT", wantKind: ast.TRANS_STMT_COMMIT},
		{name: "END (COMMIT alias)", sql: "END", wantKind: ast.TRANS_STMT_COMMIT},
		{name: "ROLLBACK", sql: "ROLLBACK", wantKind: ast.TRANS_STMT_ROLLBACK},
		{name: "BEGIN ISOLATION LEVEL SERIALIZABLE", sql: "BEGIN ISOLATION LEVEL SERIALIZABLE", wantKind: ast.TRANS_STMT_BEGIN},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
			p := NewPlanner("default", logger, nil)
			testConn := server.NewTestConn(&bytes.Buffer{})

			plan, err := planPortal(t, p, testConn.Conn, tt.sql)
			require.NoError(t, err)
			require.NotNil(t, plan, "the portal path must return a non-nil plan for %s — a plan that did not own dispatch would send the statement to a pooled backend connection and leak transaction state", tt.sql)

			prim, ok := plan.Primitive.(*engine.TransactionPrimitive)
			require.True(t, ok, "expected TransactionPrimitive, got %T", plan.Primitive)
			assert.Equal(t, tt.wantKind, prim.Kind)
			assert.Equal(t, tt.sql, prim.Query, "original SQL text must be preserved so BEGIN options (isolation level, access mode) survive to the deferred-BEGIN path")
		})
	}
}

// TestPlanPortal_RegularSelect pins the non-transaction default: a routable
// query plans to a plain Route, whose PortalStreamExecute reissues the portal to
// the backend — exactly the portal fast path, with no gateway-local handling.
func TestPlanPortal_RegularSelect(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	plan, err := planPortal(t, p, testConn.Conn, "SELECT 1")
	require.NoError(t, err)
	require.NotNil(t, plan)
	_, ok := plan.Primitive.(*engine.Route)
	assert.True(t, ok, "a routable SELECT must plan to a plain Route (reissues the portal), got %T", plan.Primitive)
}

// TestPlanPortal_SavepointFallsThrough confirms that savepoint variants
// are still planned through the TransactionPrimitive in the extended path.
// Plan() routes every TransactionStmt kind through planTransactionStmt, and
// the primitive's StreamExecute forwards savepoint variants to the backend —
// the important invariant is that the gateway owns dispatch rather than
// letting a raw portal Bind+Execute run on a pooled connection.
func TestPlanPortal_SavepointFallsThrough(t *testing.T) {
	tests := []string{
		"SAVEPOINT sp1",
		"RELEASE SAVEPOINT sp1",
		"ROLLBACK TO SAVEPOINT sp1",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
			p := NewPlanner("default", logger, nil)
			testConn := server.NewTestConn(&bytes.Buffer{})

			plan, err := planPortal(t, p, testConn.Conn, sql)
			require.NoError(t, err)

			// Savepoint statements still go through the TransactionPrimitive
			// (that's what planTransactionStmt returns for every Kind), but the
			// primitive's StreamExecute passes them through to the backend.
			// The key point is the gateway owns dispatch — no direct portal
			// Bind+Execute on a pooled connection for any TransactionStmt.
			require.NotNil(t, plan)
			_, ok := plan.Primitive.(*engine.TransactionPrimitive)
			assert.True(t, ok, "every TransactionStmt must go through TransactionPrimitive, got %T", plan.Primitive)
		})
	}
}
