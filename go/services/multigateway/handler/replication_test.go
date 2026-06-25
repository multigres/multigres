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
	"testing"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
)

func newReplicationModeConn(mode server.ReplicationMode) *server.Conn {
	conn := &server.Conn{}
	server.WithTestReplicationMode(mode)(conn)
	return conn
}

// Logical-replication connections are tunneled from serve() before they reach
// HandleQuery, so HandleQuery treats every query as ordinary SQL regardless of
// replication mode. A plain SELECT must still flow through the normal path even
// when the connection negotiated replication=database.
func TestHandleQuery_RunsSQLRegardlessOfReplicationMode(t *testing.T) {
	h := NewMultiGatewayHandler(&mockExecutor{}, slog.Default(), 0)
	conn := newReplicationModeConn(server.ReplicationLogical)

	called := false
	err := h.HandleQuery(context.Background(), conn, "SELECT 1",
		func(context.Context, *sqltypes.Result) error {
			called = true
			return nil
		})
	if err != nil {
		t.Fatalf("want no error from SQL path, got %v", err)
	}
	if !called {
		t.Error("callback not invoked — SQL path was not reached")
	}
}
