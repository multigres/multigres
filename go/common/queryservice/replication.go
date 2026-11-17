// Copyright 2025 Supabase, Inc.
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

package queryservice

import (
	"errors"
	"io"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// ReplicationStream is a replication tunnel to a pooler: opaque replication
// protocol bytes in both directions, after the open handshake
// (OpenReplication). Send carries data to the pooler; Recv returns data from
// it, or a structured infrastructure error, and io.EOF or an error when the
// tunnel ends. The gRPC stream for MultipoolerService.StreamReplication
// implements it; in minigres an in-process stream does.
type ReplicationStream interface {
	Send(*multipoolerpb.StreamReplicationRequest) error
	Recv() (*multipoolerpb.StreamReplicationResponse, error)
}

// OpenReplication performs a replication tunnel's open handshake on stream,
// whatever the transport: it sends init, which must be the first message,
// then waits for the pooler to open the backend. A structured backend error is
// returned as its *mterrors.PgDiagnostic, so the caller can re-emit it
// verbatim as an ErrorResponse; any other backend error as the error the
// pooler ended the stream with. convert, when set, turns the transport's
// errors into mterrors errors (mterrors.FromGRPC for gRPC).
func OpenReplication(stream ReplicationStream, init *multipoolerpb.StreamReplicationInit, convert func(error) error) error {
	if convert == nil {
		convert = func(err error) error { return err }
	}
	if err := stream.Send(&multipoolerpb.StreamReplicationRequest{
		Msg: &multipoolerpb.StreamReplicationRequest_Init{Init: init},
	}); err != nil {
		return mterrors.Wrapf(convert(err), "failed to send replication Init")
	}

	// Await the backend-opened handshake.
	resp, err := stream.Recv()
	if err != nil {
		return mterrors.Wrapf(convert(err), "failed to receive replication Ready")
	}
	if replErr := resp.GetError(); replErr != nil {
		if diag := replErr.GetDiagnostic(); diag != nil {
			return mterrors.PgDiagnosticFromProto(diag)
		}
		// The pooler ends the stream right after a handshake error, so the
		// stream's end carries the cause (for example a pool wait timeout or a
		// dial failure) and this Recv does not block.
		if _, err := stream.Recv(); err != nil && !errors.Is(err, io.EOF) {
			return mterrors.Wrapf(convert(err), "replication open failed")
		}
		return mterrors.New(mtrpcpb.Code_INTERNAL, "replication stream returned an error without a diagnostic")
	}
	if resp.GetReady() == nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL, "expected Ready as the first replication response")
	}
	return nil
}
