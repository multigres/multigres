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
	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// Request wraps an existing query-service request without copying its payload.
// Unsupported types return nil and must never be submitted.
func Request(message proto.Message) *pb.ExecuteStreamRequest {
	switch req := message.(type) {
	case *pb.CopyBidiExecuteRequest:
		return &pb.ExecuteStreamRequest{Operation: &pb.ExecuteStreamRequest_CopyBidiExecute{CopyBidiExecute: req}}
	case *pb.StreamExecuteRequest:
		return &pb.ExecuteStreamRequest{Operation: &pb.ExecuteStreamRequest_Request{Request: req}}
	case *pb.PortalStreamExecuteRequest:
		return &pb.ExecuteStreamRequest{Operation: &pb.ExecuteStreamRequest_PortalStreamExecute{PortalStreamExecute: req}}
	case *pb.ExecuteQueryRequest:
		return &pb.ExecuteStreamRequest{Operation: &pb.ExecuteStreamRequest_ExecuteQuery{ExecuteQuery: req}}
	case *pb.DescribeRequest:
		return &pb.ExecuteStreamRequest{Operation: &pb.ExecuteStreamRequest_Describe{Describe: req}}
	case *pb.ConcludeTransactionRequest:
		return &pb.ExecuteStreamRequest{Operation: &pb.ExecuteStreamRequest_ConcludeTransaction{ConcludeTransaction: req}}
	case *pb.DiscardTempTablesRequest:
		return &pb.ExecuteStreamRequest{Operation: &pb.ExecuteStreamRequest_DiscardTempTables{DiscardTempTables: req}}
	case *pb.ReleaseReservedConnectionRequest:
		return &pb.ExecuteStreamRequest{Operation: &pb.ExecuteStreamRequest_ReleaseReservedConnection{ReleaseReservedConnection: req}}
	default:
		return nil
	}
}

// Operation identifies a nonempty typed request. Unknown operation fields from
// newer peers decode as an empty oneof and are rejected before dispatch.
func Operation(req *pb.ExecuteStreamRequest) pb.ExecuteStreamOperation {
	if req.GetCopyBidiExecute() != nil && req.GetCopyBidiExecute().Phase == pb.CopyBidiExecuteRequest_INITIATE {
		return pb.ExecuteStreamOperation_COPY_BIDI_EXECUTE
	}
	if req.GetRequest() != nil {
		return pb.ExecuteStreamOperation_STREAM_EXECUTE
	}
	if req.GetPortalStreamExecute() != nil {
		return pb.ExecuteStreamOperation_PORTAL_STREAM_EXECUTE
	}
	if req.GetExecuteQuery() != nil {
		return pb.ExecuteStreamOperation_EXECUTE_QUERY
	}
	if req.GetDescribe() != nil {
		return pb.ExecuteStreamOperation_DESCRIBE
	}
	if req.GetConcludeTransaction() != nil {
		return pb.ExecuteStreamOperation_CONCLUDE_TRANSACTION
	}
	if req.GetDiscardTempTables() != nil {
		return pb.ExecuteStreamOperation_DISCARD_TEMP_TABLES
	}
	if req.GetReleaseReservedConnection() != nil {
		return pb.ExecuteStreamOperation_RELEASE_RESERVED_CONNECTION
	}
	return pb.ExecuteStreamOperation_EXECUTE_STREAM_OPERATION_UNSPECIFIED
}

// Response unwraps the typed result and identifies which operation owns it.
// Clients reject a result belonging to another operation before exposing it.
func Response(frame *pb.ExecuteStreamResponse) (pb.ExecuteStreamOperation, proto.Message) {
	if value := frame.GetCopyBidiExecute(); value != nil {
		return pb.ExecuteStreamOperation_COPY_BIDI_EXECUTE, value
	}
	if value := frame.GetResponse(); value != nil {
		return pb.ExecuteStreamOperation_STREAM_EXECUTE, value
	}
	if value := frame.GetPortalStreamExecute(); value != nil {
		return pb.ExecuteStreamOperation_PORTAL_STREAM_EXECUTE, value
	}
	if value := frame.GetExecuteQuery(); value != nil {
		return pb.ExecuteStreamOperation_EXECUTE_QUERY, value
	}
	if value := frame.GetDescribe(); value != nil {
		return pb.ExecuteStreamOperation_DESCRIBE, value
	}
	if value := frame.GetConcludeTransaction(); value != nil {
		return pb.ExecuteStreamOperation_CONCLUDE_TRANSACTION, value
	}
	if value := frame.GetDiscardTempTables(); value != nil {
		return pb.ExecuteStreamOperation_DISCARD_TEMP_TABLES, value
	}
	if value := frame.GetReleaseReservedConnection(); value != nil {
		return pb.ExecuteStreamOperation_RELEASE_RESERVED_CONNECTION, value
	}
	return pb.ExecuteStreamOperation_EXECUTE_STREAM_OPERATION_UNSPECIFIED, nil
}

func operationName(op pb.ExecuteStreamOperation) string {
	switch op {
	case pb.ExecuteStreamOperation_COPY_BIDI_EXECUTE:
		return "CopyBidiExecute"
	case pb.ExecuteStreamOperation_STREAM_EXECUTE:
		return "StreamExecute"
	case pb.ExecuteStreamOperation_PORTAL_STREAM_EXECUTE:
		return "PortalStreamExecute"
	case pb.ExecuteStreamOperation_EXECUTE_QUERY:
		return "ExecuteQuery"
	case pb.ExecuteStreamOperation_DESCRIBE:
		return "Describe"
	case pb.ExecuteStreamOperation_CONCLUDE_TRANSACTION:
		return "ConcludeTransaction"
	case pb.ExecuteStreamOperation_DISCARD_TEMP_TABLES:
		return "DiscardTempTables"
	case pb.ExecuteStreamOperation_RELEASE_RESERVED_CONNECTION:
		return "ReleaseReservedConnection"
	default:
		return "Unknown"
	}
}
