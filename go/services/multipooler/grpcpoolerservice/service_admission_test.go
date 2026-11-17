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

package grpcpoolerservice

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protoutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
)

// TestUnaryHandlers_RejectMismatchedTarget checks that every unary query
// handler admits its request: a target for another shard is a routing bug
// (MTD01) and must be rejected before it reaches the executor, including the
// calls that act on an existing reserved connection.
func TestUnaryHandlers_RejectMismatchedTarget(t *testing.T) {
	p := poolerserver.NewQueryPoolerServer(slog.New(slog.DiscardHandler), nil, &clustermetadatapb.ID{Name: "p"}, "tg", "0", nil, 0, false)
	srv := &poolerService{pooler: p}
	target := protoutil.NewTarget("", "tg", "1", query.Mode_MODE_WRITABLE)
	reserved := &query.ExecuteOptions{ReservedConnectionId: 7}
	ctx := context.Background()

	calls := map[string]func() error{
		"ExecuteQuery": func() error {
			_, err := srv.ExecuteQuery(ctx, &multipoolerpb.ExecuteQueryRequest{Target: target, Query: "SELECT 1"})
			return err
		},
		"Describe": func() error {
			_, err := srv.Describe(ctx, &multipoolerpb.DescribeRequest{Target: target})
			return err
		},
		"ConcludeTransaction": func() error {
			_, err := srv.ConcludeTransaction(ctx, &multipoolerpb.ConcludeTransactionRequest{Target: target, Options: reserved})
			return err
		},
		"DiscardTempTables": func() error {
			_, err := srv.DiscardTempTables(ctx, &multipoolerpb.DiscardTempTablesRequest{Target: target, Options: reserved})
			return err
		},
		"ReleaseReservedConnection": func() error {
			_, err := srv.ReleaseReservedConnection(ctx, &multipoolerpb.ReleaseReservedConnectionRequest{Target: target, Options: reserved})
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := mterrors.FromGRPC(call())
			assert.True(t, mterrors.IsErrorCode(err, mterrors.MTD01.ID), "expected MTD01 for a mismatched shard, got: %v", err)
		})
	}
}
