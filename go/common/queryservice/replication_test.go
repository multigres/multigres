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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// scriptedReplicationStream records what is sent and answers the first Recv
// with a fixed reply or error, and later ones with endErr (io.EOF if unset).
type scriptedReplicationStream struct {
	sent    []*multipoolerpb.StreamReplicationRequest
	sendErr error
	resp    *multipoolerpb.StreamReplicationResponse
	recvErr error
	endErr  error
	recvs   int
}

func (s *scriptedReplicationStream) Send(req *multipoolerpb.StreamReplicationRequest) error {
	s.sent = append(s.sent, req)
	return s.sendErr
}

func (s *scriptedReplicationStream) Recv() (*multipoolerpb.StreamReplicationResponse, error) {
	s.recvs++
	if s.recvs == 1 {
		return s.resp, s.recvErr
	}
	if s.endErr != nil {
		return nil, s.endErr
	}
	return nil, io.EOF
}

func TestOpenReplication(t *testing.T) {
	init := &multipoolerpb.StreamReplicationInit{User: "u"}
	diag := mterrors.NewPgError("FATAL", mterrors.PgSSAuthFailed, `password authentication failed for user "u"`, "")

	t.Run("ready", func(t *testing.T) {
		s := &scriptedReplicationStream{resp: &multipoolerpb.StreamReplicationResponse{
			Msg: &multipoolerpb.StreamReplicationResponse_Ready{Ready: &multipoolerpb.StreamReplicationReady{}},
		}}
		require.NoError(t, OpenReplication(s, init, nil))
		require.Len(t, s.sent, 1)
		assert.Same(t, init, s.sent[0].GetInit(), "init must be the first message")
	})

	t.Run("structured backend error is its diagnostic", func(t *testing.T) {
		s := &scriptedReplicationStream{resp: &multipoolerpb.StreamReplicationResponse{
			Msg: &multipoolerpb.StreamReplicationResponse_Error{Error: &multipoolerpb.StreamReplicationError{
				Diagnostic: mterrors.PgDiagnosticToProto(diag),
			}},
		}}
		err := OpenReplication(s, init, nil)
		var got *mterrors.PgDiagnostic
		require.ErrorAs(t, err, &got)
		assert.Equal(t, diag.Code, got.Code)
		assert.Equal(t, diag.Message, got.Message)
	})

	noDiagnostic := &multipoolerpb.StreamReplicationResponse{
		Msg: &multipoolerpb.StreamReplicationResponse_Error{Error: &multipoolerpb.StreamReplicationError{}},
	}

	t.Run("error without diagnostic is the stream's end error", func(t *testing.T) {
		cause := mterrors.New(mtrpcpb.Code_UNAVAILABLE, "pool wait timeout")
		converted := 0
		convert := func(err error) error { converted++; return err }
		s := &scriptedReplicationStream{resp: noDiagnostic, endErr: cause}
		err := OpenReplication(s, init, convert)
		assert.ErrorIs(t, err, cause)
		assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err))
		assert.Contains(t, err.Error(), "pool wait timeout")
		assert.Equal(t, 1, converted, "the end error goes through convert")
	})

	t.Run("error without diagnostic and a clean end", func(t *testing.T) {
		s := &scriptedReplicationStream{resp: noDiagnostic}
		assert.Equal(t, mtrpcpb.Code_INTERNAL, mterrors.Code(OpenReplication(s, init, nil)))
	})

	t.Run("first reply not ready", func(t *testing.T) {
		s := &scriptedReplicationStream{resp: &multipoolerpb.StreamReplicationResponse{
			Msg: &multipoolerpb.StreamReplicationResponse_Data{Data: []byte("x")},
		}}
		assert.Equal(t, mtrpcpb.Code_INTERNAL, mterrors.Code(OpenReplication(s, init, nil)))
	})

	t.Run("transport errors go through convert", func(t *testing.T) {
		converted := mterrors.New(mtrpcpb.Code_UNAVAILABLE, "converted")
		convert := func(error) error { return converted }

		err := OpenReplication(&scriptedReplicationStream{sendErr: errors.New("send")}, init, convert)
		assert.ErrorIs(t, err, converted)
		assert.Contains(t, err.Error(), "failed to send replication Init")

		err = OpenReplication(&scriptedReplicationStream{recvErr: errors.New("recv")}, init, convert)
		assert.ErrorIs(t, err, converted)
		assert.Contains(t, err.Error(), "failed to receive replication Ready")
	})
}
