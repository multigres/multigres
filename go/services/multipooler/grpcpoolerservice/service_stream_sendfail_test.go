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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// What the handlers return when the gRPC stream itself fails, on both the
// success and the error path. The two handlers must behave identically: a
// send failure on the final flush is returned as a gRPC status; a send
// failure while the execution itself failed is dropped in favour of the
// execution error, since nothing returned reaches the gateway on a broken
// stream and the PostgreSQL diagnostics are what matter.

var errStreamGone = errors.New("transport is closing")

func TestPortalStreamExecute_SendFailureOnFinalFlushIsReturned(t *testing.T) {
	stream := &mockPortalStream{failOnCall: 1, sendErr: errStreamGone}
	exec := &mockStreamQueryService{
		results:  []*sqltypes.Result{rowResult("SELECT 1")},
		reserved: &query.ReservedState{ReservedConnectionId: 3},
	}

	err := (&poolerService{}).portalStreamExecuteTo(stream, exec, portalReq())

	require.Error(t, err)
	_, isStatus := status.FromError(err)
	require.True(t, isStatus, "send failures are returned as gRPC statuses")
	require.Contains(t, status.Convert(err).Message(), "transport is closing")
	require.Empty(t, stream.sent)
}

func TestPortalStreamExecute_SendFailureOnErrorPathKeepsExecutionError(t *testing.T) {
	stream := &mockPortalStream{failOnCall: 1, sendErr: errStreamGone}
	exec := &mockStreamQueryService{
		results:  []*sqltypes.Result{rowResult("partial")},
		reserved: &query.ReservedState{ReservedConnectionId: 3},
		err:      errors.New("relation does not exist"),
	}

	err := (&poolerService{}).portalStreamExecuteTo(stream, exec, portalReq())

	require.Error(t, err)
	require.Contains(t, status.Convert(err).Message(), "relation does not exist",
		"the execution error wins over the send error")
	require.NotContains(t, status.Convert(err).Message(), "transport is closing")
	require.Equal(t, 1, stream.calls, "the pending chunk with the reservation state is still attempted")
}

func TestPortalStreamExecute_SendFailureOnIntermediateChunkStopsExecution(t *testing.T) {
	stream := &mockPortalStream{failOnCall: 1, sendErr: errStreamGone}
	callbacks := 0
	exec := &mockStreamQueryService{
		results:       []*sqltypes.Result{intermediateChunk("rows-1"), intermediateChunk("rows-2"), rowResult("SELECT 3")},
		reserved:      &query.ReservedState{ReservedConnectionId: 3},
		afterCallback: func(int) { callbacks++ },
	}

	err := (&poolerService{}).portalStreamExecuteTo(stream, exec, portalReq())

	require.Error(t, err)
	require.Contains(t, status.Convert(err).Message(), "transport is closing")
	require.Equal(t, 0, callbacks, "the callback error stops the executor after the first chunk")
	require.Equal(t, 2, stream.calls, "the failed chunk, then one attempt to deliver the reservation state on the error path")
	require.Empty(t, stream.sent)
}

func TestPortalStreamExecute_SendFailureOnNoticeStopsExecution(t *testing.T) {
	stream := &mockPortalStream{failOnCall: 1, sendErr: errStreamGone}
	// The notice is sent ahead of its data, so it is the first Send call.
	exec := &mockStreamQueryService{
		results: []*sqltypes.Result{{
			CommandTag: "SELECT 1",
			Notices:    []*mterrors.PgDiagnostic{{MessageType: 'N', Severity: "NOTICE", Message: "heads up"}},
		}},
	}

	err := (&poolerService{}).portalStreamExecuteTo(stream, exec, portalReq())

	require.Error(t, err)
	require.Contains(t, status.Convert(err).Message(), "transport is closing")
	require.Equal(t, 1, stream.calls)
}

func TestStreamExecute_SendFailureOnFinalFlushIsReturned(t *testing.T) {
	stream := &mockExecStream{failOnCall: 1, sendErr: errStreamGone}
	exec := &mockStreamQueryService{
		results:  []*sqltypes.Result{rowResult("SELECT 1")},
		reserved: &query.ReservedState{ReservedConnectionId: 3},
	}
	req := &multipoolerpb.StreamExecuteRequest{Query: "select 1", Options: &query.ExecuteOptions{User: "postgres"}}

	err := (&poolerService{}).streamExecuteTo(stream, exec, req)

	require.Error(t, err)
	_, isStatus := status.FromError(err)
	require.True(t, isStatus)
	require.Contains(t, status.Convert(err).Message(), "transport is closing")
	require.Empty(t, stream.sent)
}

func TestStreamExecute_SendFailureOnErrorPathKeepsExecutionError(t *testing.T) {
	stream := &mockExecStream{failOnCall: 1, sendErr: errStreamGone}
	exec := &mockStreamQueryService{
		results:  []*sqltypes.Result{rowResult("partial")},
		reserved: &query.ReservedState{ReservedConnectionId: 3},
		err:      errors.New("division by zero"),
	}
	req := &multipoolerpb.StreamExecuteRequest{Query: "select 1/0", Options: &query.ExecuteOptions{User: "postgres"}}

	err := (&poolerService{}).streamExecuteTo(stream, exec, req)

	require.Error(t, err)
	require.Contains(t, status.Convert(err).Message(), "division by zero")
	require.NotContains(t, status.Convert(err).Message(), "transport is closing")
}

func TestStreamExecute_SendFailureOnIntermediateChunkStopsExecution(t *testing.T) {
	stream := &mockExecStream{failOnCall: 1, sendErr: errStreamGone}
	callbacks := 0
	exec := &mockStreamQueryService{
		results:       []*sqltypes.Result{intermediateChunk("rows-1"), intermediateChunk("rows-2"), rowResult("SELECT 3")},
		reserved:      &query.ReservedState{ReservedConnectionId: 3},
		afterCallback: func(int) { callbacks++ },
	}
	req := &multipoolerpb.StreamExecuteRequest{Query: "select big", Options: &query.ExecuteOptions{User: "postgres"}}

	err := (&poolerService{}).streamExecuteTo(stream, exec, req)

	require.Error(t, err)
	require.Contains(t, status.Convert(err).Message(), "transport is closing")
	require.Equal(t, 0, callbacks)
	require.Equal(t, 2, stream.calls, "the failed chunk, then one attempt to deliver the reservation state on the error path")
	require.Empty(t, stream.sent)
}

func TestStreamExecute_SendFailureOnNoticeStopsExecution(t *testing.T) {
	stream := &mockExecStream{failOnCall: 1, sendErr: errStreamGone}
	exec := &mockStreamQueryService{
		results: []*sqltypes.Result{{
			CommandTag: "SELECT 1",
			Notices:    []*mterrors.PgDiagnostic{{MessageType: 'N', Severity: "NOTICE", Message: "heads up"}},
		}},
	}
	req := &multipoolerpb.StreamExecuteRequest{Query: "select 1", Options: &query.ExecuteOptions{User: "postgres"}}

	err := (&poolerService{}).streamExecuteTo(stream, exec, req)

	require.Error(t, err)
	require.Contains(t, status.Convert(err).Message(), "transport is closing")
	require.Equal(t, 1, stream.calls)
}
