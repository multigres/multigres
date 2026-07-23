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
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

func TestReservationAwareSelectsRuntimeSessionPath(t *testing.T) {
	reserved := &recordingPrimitive{}
	unreserved := &recordingPrimitive{}
	primitive := NewReservationAware("RESET work_mem", reserved, unreserved)
	state := handler.NewMultigatewayConnectionState()
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	require.NoError(t, primitive.StreamExecute(context.Background(), nil, conn, state, nil, PlanExecInfo{}, nil))
	require.Equal(t, 1, unreserved.streamCalls)
	require.Zero(t, reserved.streamCalls)

	target := &querypb.Target{}
	state.SetReservedConnection(target, &querypb.ReservedState{ReservedConnectionId: 1})
	require.NoError(t, primitive.StreamExecute(context.Background(), nil, conn, state, nil, PlanExecInfo{}, nil))
	require.Equal(t, 1, unreserved.streamCalls)
	require.Equal(t, 1, reserved.streamCalls)
}
