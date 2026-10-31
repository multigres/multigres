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

package poolerserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protoutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/pb/query"
)

// requireAdmitted asserts the request passed admission: the test server has no
// executor, so an admitted request fails fetching it instead.
func requireAdmitted(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	assert.False(t, mterrors.IsErrorCode(err, mterrors.MTF01.ID), "expected admission to pass, got: %v", err)
	assert.Contains(t, err.Error(), "executor not initialized")
}

// TestQueryService_AdmissionPerMethod checks each method's admission kind in
// stage 1 of a graceful drain, where single queries and existing reservations
// are served but new reservations are refused.
func TestQueryService_AdmissionPerMethod(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.drainPhase = drainReserved
	qs := s.QueryService()
	ctx := context.Background()
	reserved := &query.ExecuteOptions{ReservedConnectionId: 7}
	withReasons := &query.ReservationOptions{Reasons: protoutil.ReasonTransaction}

	_, _, err := qs.ExecuteQuery(ctx, nil, "SELECT 1", nil)
	requireAdmitted(t, err)
	_, err = qs.Describe(ctx, nil, nil, nil, nil)
	requireAdmitted(t, err)
	_, err = qs.StreamExecute(ctx, nil, "SELECT 1", nil, nil, nil)
	requireAdmitted(t, err)

	_, err = qs.StreamExecute(ctx, nil, "BEGIN", nil, withReasons, nil)
	requireMTF01(t, err)
	_, err = qs.PortalStreamExecute(ctx, nil, nil, nil, &query.ExecuteOptions{MaxRows: 10}, nil, nil, nil)
	requireMTF01(t, err)
	_, _, _, err = qs.CopyReady(ctx, nil, "COPY t FROM STDIN", nil, nil)
	requireMTF01(t, err)
	_, _, _, _, err = qs.CopyOutReady(ctx, nil, "COPY t TO STDOUT", nil, nil)
	requireMTF01(t, err)

	_, _, err = qs.ConcludeTransaction(ctx, nil, reserved, 0, nil, false, false, nil)
	requireAdmitted(t, err)
	_, _, err = qs.DiscardTempTables(ctx, nil, reserved)
	requireAdmitted(t, err)
	_, err = qs.ReleaseReservedConnection(ctx, nil, reserved, false)
	requireAdmitted(t, err)
	_, err = qs.StreamExecute(ctx, nil, "SELECT 1", reserved, withReasons, nil)
	requireAdmitted(t, err)
}

// TestQueryService_CopyFollowUpsNotAdmitted checks that the calls continuing a
// COPY are not admitted again, even once the pooler stops serving.
func TestQueryService_CopyFollowUpsNotAdmitted(t *testing.T) {
	s := newStartRequestTestServer() // not serving
	qs := s.QueryService()
	ctx := context.Background()

	requireAdmitted(t, qs.CopySendData(ctx, nil, []byte("1\n"), nil))
	_, _, err := qs.CopyFinalize(ctx, nil, nil, nil)
	requireAdmitted(t, err)
	_, err = qs.CopyAbort(ctx, nil, "stop", nil)
	requireAdmitted(t, err)
	_, _, err = qs.CopyOutStream(ctx, nil, nil, nil)
	requireAdmitted(t, err)
}

// TestQueryService_InvalidReasons checks that unknown reservation reason bits
// are rejected as INVALID_ARGUMENT, which the gRPC handler still returns as
// codes.InvalidArgument with the same message.
func TestQueryService_InvalidReasons(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	qs := s.QueryService()
	bogus := &query.ReservationOptions{Reasons: 1 << 31}

	_, err := qs.StreamExecute(context.Background(), nil, "SELECT 1", nil, bogus, nil)
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
	st, ok := status.FromError(mterrors.ToGRPC(err))
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "invalid reservation reasons")

	_, err = qs.PortalStreamExecute(context.Background(), nil, nil, nil, nil, nil, bogus, nil)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
}
