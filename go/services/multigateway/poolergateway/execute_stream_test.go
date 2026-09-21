// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package poolergateway

import (
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/queryrpc"
	pb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

type reusableServer struct {
	pb.UnimplementedMultipoolerServiceServer
	streams atomic.Int32
	calls   atomic.Int32
}

func (s *reusableServer) ExecuteStream(stream pb.MultipoolerService_ExecuteStreamServer) error {
	s.streams.Add(1)
	return queryrpc.Serve(stream, func(req *pb.StreamExecuteRequest, out pb.MultipoolerService_StreamExecuteServer) error {
		s.calls.Add(1)
		if err := out.Send(&pb.StreamExecuteResponse{ReservedState: &query.ReservedState{ReservedConnectionId: req.Options.GetReservedConnectionId()}}); err != nil {
			return err
		}
		if req.Query == "unavailable" {
			return status.Error(codes.Unavailable, "may already have executed")
		}
		return nil
	})
}

func TestReusableStreamReservationAndRetryBoundary(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := &reusableServer{}
	server := grpc.NewServer()
	pb.RegisterMultipoolerServiceServer(server, s)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	qs := queryServiceAt(t, lis.Addr().String())
	t.Cleanup(func() { require.NoError(t, qs.Close()) })
	for i := uint64(1); i <= 4; i++ {
		state, err := qs.StreamExecute(t.Context(), &query.Target{}, "ok", &query.ExecuteOptions{ReservedConnectionId: i}, nil, noopStreamCallback)
		require.NoError(t, err)
		require.Equal(t, i, state.ReservedConnectionId)
	}
	_, err = qs.StreamExecute(t.Context(), &query.Target{}, "unavailable", nil, nil, noopStreamCallback)
	require.Error(t, err)
	require.False(t, mterrors.IsPreExecutionUnavailable(err))
	require.EqualValues(t, 1, s.streams.Load())
	require.EqualValues(t, 5, s.calls.Load())
}
