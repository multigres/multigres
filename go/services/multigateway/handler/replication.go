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
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/replparser"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
)

// handleReplicationCommand parses queryStr as a replication-protocol command
// and emits the corresponding stub response. Returns handled=true if the
// command was claimed (either successfully stubbed or a parse error), and
// handled=false if the query should fall through to the regular SQL path
// (e.g. SHOW, which the SQL grammar also accepts).
func (h *MultigatewayHandler) handleReplicationCommand(
	ctx context.Context,
	conn *server.Conn,
	queryStr string,
	queryStart time.Time,
) (bool, error) {
	parseStart := time.Now()
	stmt, err := replparser.ParseReplicationCommand(queryStr)
	parseDuration := time.Since(parseStart)
	if err != nil {
		h.recordQueryCompletion(ctx, conn, "REPLICATION", "simple",
			parseDuration, 0, time.Since(queryStart), 0, nil, err)
		return true, err
	}

	op, stubErr := replicationStubError(stmt)
	if stubErr == nil {
		// VariableShowStmt — the SQL grammar handles SHOW, so let the
		// caller continue down the regular path.
		return false, nil
	}
	h.recordQueryCompletion(ctx, conn, op, "simple",
		parseDuration, 0, time.Since(queryStart), 0, nil, stubErr)
	return true, stubErr
}

// replicationStubError maps a replication-command AST node to an op name
// and a `feature_not_supported` (SQLSTATE 0A000) error.
//
// Returns (opName, nil) for *ast.VariableShowStmt — SHOW is delegated to the
// normal SQL path by the caller and is not a stub failure case.
func replicationStubError(stmt ast.Stmt) (string, error) {
	switch stmt.(type) {
	case *ast.IdentifySystemCmd:
		return notSupported("IDENTIFY_SYSTEM")
	case *ast.CreateReplicationSlotCmd:
		return notSupported("CREATE_REPLICATION_SLOT")
	case *ast.DropReplicationSlotCmd:
		return notSupported("DROP_REPLICATION_SLOT")
	case *ast.AlterReplicationSlotCmd:
		return notSupported("ALTER_REPLICATION_SLOT")
	case *ast.ReadReplicationSlotCmd:
		return notSupported("READ_REPLICATION_SLOT")
	case *ast.StartReplicationCmd:
		return notSupported("START_REPLICATION")
	case *ast.VariableShowStmt:
		return "SHOW", nil
	}
	return "REPLICATION", mterrors.NewFeatureNotSupported("replication command is not yet supported")
}

func notSupported(op string) (string, error) {
	return op, mterrors.NewFeatureNotSupported(op + " is not yet supported")
}
