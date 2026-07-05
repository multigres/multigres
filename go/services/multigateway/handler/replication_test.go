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

package handler

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
)

func TestReplicationStubError(t *testing.T) {
	cases := []struct {
		stmt   ast.Stmt
		opName string
		msg    string
	}{
		{ast.NewIdentifySystemCmd(), "IDENTIFY_SYSTEM", "IDENTIFY_SYSTEM is not yet supported"},
		{
			ast.NewCreateReplicationSlotCmd("s", ast.ReplicationKindLogical, "p", false),
			"CREATE_REPLICATION_SLOT", "CREATE_REPLICATION_SLOT is not yet supported",
		},
		{ast.NewDropReplicationSlotCmd("s", false), "DROP_REPLICATION_SLOT", "DROP_REPLICATION_SLOT is not yet supported"},
		{ast.NewAlterReplicationSlotCmd("s", nil), "ALTER_REPLICATION_SLOT", "ALTER_REPLICATION_SLOT is not yet supported"},
		{ast.NewReadReplicationSlotCmd("s"), "READ_REPLICATION_SLOT", "READ_REPLICATION_SLOT is not yet supported"},
		{
			ast.NewStartReplicationCmd(ast.ReplicationKindLogical, "s", 0, 0, nil),
			"START_REPLICATION", "START_REPLICATION is not yet supported",
		},
	}
	for _, c := range cases {
		op, err := replicationStubError(c.stmt)
		if op != c.opName {
			t.Errorf("%T: want op %q got %q", c.stmt, c.opName, op)
		}
		if err == nil || !strings.Contains(err.Error(), c.msg) {
			t.Errorf("%T: want err containing %q, got %v", c.stmt, c.msg, err)
		}
		if got := mterrors.ExtractSQLSTATE(err); got != "0A000" {
			t.Errorf("%T: want SQLSTATE 0A000, got %q", c.stmt, got)
		}
	}
}

func TestReplicationStubError_ShowDelegates(t *testing.T) {
	op, err := replicationStubError(ast.NewVariableShowStmt("server_version"))
	if op != "SHOW" {
		t.Errorf("want op SHOW, got %q", op)
	}
	if err != nil {
		t.Errorf("SHOW should not stub-error; got %v", err)
	}
}

func newReplicationModeConn(mode server.ReplicationMode) *server.Conn {
	conn := &server.Conn{}
	server.WithTestReplicationMode(mode)(conn)
	return conn
}

func TestHandleQuery_DispatchesReplicationCommand(t *testing.T) {
	h := NewMultigatewayHandler(&mockExecutor{}, slog.Default(), 0)
	conn := newReplicationModeConn(server.ReplicationLogical)

	err := h.HandleQuery(context.Background(), conn, "IDENTIFY_SYSTEM",
		func(context.Context, *sqltypes.Result) error { return nil })
	if err == nil {
		t.Fatal("expected stub error, got nil")
	}
	if got := mterrors.ExtractSQLSTATE(err); got != "0A000" {
		t.Errorf("want SQLSTATE 0A000, got %q (%v)", got, err)
	}
	if !strings.Contains(err.Error(), "IDENTIFY_SYSTEM") {
		t.Errorf("want error mentioning IDENTIFY_SYSTEM, got %v", err)
	}
}

func TestHandleQuery_FallsThroughToSQL_WhenNotReplicationCommand(t *testing.T) {
	// Even with ReplicationLogical, a non-replication command must
	// reach the regular SQL path (the mock executor handles SELECT 1).
	h := NewMultigatewayHandler(&mockExecutor{}, slog.Default(), 0)
	conn := newReplicationModeConn(server.ReplicationLogical)

	called := false
	err := h.HandleQuery(context.Background(), conn, "SELECT 1",
		func(context.Context, *sqltypes.Result) error {
			called = true
			return nil
		})
	if err != nil {
		t.Fatalf("want no error from SQL fall-through, got %v", err)
	}
	if !called {
		t.Error("callback not invoked — SQL path was not reached")
	}
}

func TestHandleQuery_IgnoresReplicationMode_WhenNotInReplicationMode(t *testing.T) {
	// Without ReplicationLogical, IDENTIFY_SYSTEM must NOT receive
	// the stub error — it should fall through to the SQL parser, which
	// will reject it as a syntax error.
	h := NewMultigatewayHandler(&mockExecutor{}, slog.Default(), 0)
	conn := newReplicationModeConn(server.ReplicationOff)

	err := h.HandleQuery(context.Background(), conn, "IDENTIFY_SYSTEM",
		func(context.Context, *sqltypes.Result) error { return nil })
	if err == nil {
		t.Fatal("expected SQL parse error, got nil")
	}
	if strings.Contains(err.Error(), "is not yet supported") {
		t.Errorf("must not return replication stub error when not in replication mode: %v", err)
	}
}
