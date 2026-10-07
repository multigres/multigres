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

package queryrpc

import (
	"context"

	"google.golang.org/grpc"

	pb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// Service is the query-service subset eligible for sequential stream reuse.
// Reuse invokes these existing handlers, including their per-operation admission,
// caller annotation, reservation validation and error/status-detail handling.
// COPY, authentication, subscriptions, health and replication remain separate.
type Service interface {
	StreamExecute(*pb.StreamExecuteRequest, pb.MultipoolerService_StreamExecuteServer) error
	PortalStreamExecute(*pb.PortalStreamExecuteRequest, pb.MultipoolerService_PortalStreamExecuteServer) error
	ExecuteQuery(context.Context, *pb.ExecuteQueryRequest) (*pb.ExecuteQueryResponse, error)
	Describe(context.Context, *pb.DescribeRequest) (*pb.DescribeResponse, error)
	ConcludeTransaction(context.Context, *pb.ConcludeTransactionRequest) (*pb.ConcludeTransactionResponse, error)
	DiscardTempTables(context.Context, *pb.DiscardTempTablesRequest) (*pb.DiscardTempTablesResponse, error)
	ReleaseReservedConnection(context.Context, *pb.ReleaseReservedConnectionRequest) (*pb.ReleaseReservedConnectionResponse, error)
}

var supportedOperations = []pb.ExecuteStreamOperation{
	pb.ExecuteStreamOperation_STREAM_EXECUTE,
	pb.ExecuteStreamOperation_PORTAL_STREAM_EXECUTE,
	pb.ExecuteStreamOperation_EXECUTE_QUERY,
	pb.ExecuteStreamOperation_DESCRIBE,
	pb.ExecuteStreamOperation_CONCLUDE_TRANSACTION,
	pb.ExecuteStreamOperation_DISCARD_TEMP_TABLES,
	pb.ExecuteStreamOperation_RELEASE_RESERVED_CONNECTION,
}

// operationStream retains transport send failures independently of handler
// errors: an incomplete response must never be followed by a completion frame.
type operationStream struct {
	grpc.ServerStream
	ctx     context.Context
	stream  pb.MultipoolerService_ExecuteStreamServer
	sendErr error
}

func (s *operationStream) Context() context.Context { return s.ctx }
func (s *operationStream) send(response *pb.ExecuteStreamResponse) error {
	if s.sendErr == nil {
		s.sendErr = s.stream.Send(response)
	}
	return s.sendErr
}

type sqlStream struct{ *operationStream }

func (s sqlStream) Send(response *pb.StreamExecuteResponse) error {
	return s.send(&pb.ExecuteStreamResponse{Result: &pb.ExecuteStreamResponse_Response{Response: response}})
}

type portalStream struct{ *operationStream }

func (s portalStream) Send(response *pb.PortalStreamExecuteResponse) error {
	return s.send(&pb.ExecuteStreamResponse{Result: &pb.ExecuteStreamResponse_PortalStreamExecute{PortalStreamExecute: response}})
}

func dispatch(req *pb.ExecuteStreamRequest, service Service, stream *operationStream) error {
	switch Operation(req) {
	case pb.ExecuteStreamOperation_STREAM_EXECUTE:
		return service.StreamExecute(req.GetRequest(), sqlStream{stream})
	case pb.ExecuteStreamOperation_PORTAL_STREAM_EXECUTE:
		return service.PortalStreamExecute(req.GetPortalStreamExecute(), portalStream{stream})
	case pb.ExecuteStreamOperation_EXECUTE_QUERY:
		response, err := service.ExecuteQuery(stream.Context(), req.GetExecuteQuery())
		if err != nil {
			return err
		}
		return stream.send(&pb.ExecuteStreamResponse{Result: &pb.ExecuteStreamResponse_ExecuteQuery{ExecuteQuery: response}})
	case pb.ExecuteStreamOperation_DESCRIBE:
		response, err := service.Describe(stream.Context(), req.GetDescribe())
		if err != nil {
			return err
		}
		return stream.send(&pb.ExecuteStreamResponse{Result: &pb.ExecuteStreamResponse_Describe{Describe: response}})
	case pb.ExecuteStreamOperation_CONCLUDE_TRANSACTION:
		response, err := service.ConcludeTransaction(stream.Context(), req.GetConcludeTransaction())
		if err != nil {
			return err
		}
		return stream.send(&pb.ExecuteStreamResponse{Result: &pb.ExecuteStreamResponse_ConcludeTransaction{ConcludeTransaction: response}})
	case pb.ExecuteStreamOperation_DISCARD_TEMP_TABLES:
		response, err := service.DiscardTempTables(stream.Context(), req.GetDiscardTempTables())
		if err != nil {
			return err
		}
		return stream.send(&pb.ExecuteStreamResponse{Result: &pb.ExecuteStreamResponse_DiscardTempTables{DiscardTempTables: response}})
	case pb.ExecuteStreamOperation_RELEASE_RESERVED_CONNECTION:
		response, err := service.ReleaseReservedConnection(stream.Context(), req.GetReleaseReservedConnection())
		if err != nil {
			return err
		}
		return stream.send(&pb.ExecuteStreamResponse{Result: &pb.ExecuteStreamResponse_ReleaseReservedConnection{ReleaseReservedConnection: response}})
	default:
		panic("unvalidated ExecuteStream operation")
	}
}
