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

package pgbackrestd

import (
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"connectrpc.com/connect"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// restartWithFence stops a sidecar and starts another on the same state
// directory after replacing its durable restore fence with data.
func restartWithFence(t *testing.T, data []byte) (*Server, error) {
	t.Helper()
	first := newTestServer(t, nil)
	cfg := first.cfg
	require.NoError(t, first.Close())
	require.NoError(t, os.WriteFile(cfg.stateSub(restoreRecordFile), data, 0o600))
	s, err := newForTest(cfg)
	if s != nil {
		t.Cleanup(func() { s.Close() })
	}
	return s, err
}

func marshalFence(t *testing.T, rec *pb.RestoreResult) []byte {
	t.Helper()
	data, err := proto.Marshal(rec)
	require.NoError(t, err)
	return data
}

// A restore fence that cannot be read or is internally inconsistent stops the
// sidecar at start: serving without it could replay an operation or lose the
// record of one that already ran.
func TestLoadRestoreRecord_RefusesAnUntrustworthyFence(t *testing.T) {
	op := func(id string, seq uint64, incarnation string) *pb.RestoreOperation {
		return &pb.RestoreOperation{Id: id, Sequence: seq, Incarnation: incarnation}
	}
	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{"bytes that are not a record", []byte{0xff, 0xff, 0xff, 0xff}, "unreadable restore fence"},
		{"an operation id that is not a UUID", marshalFence(t, &pb.RestoreResult{Operation: op("not-a-uuid", 1, "inc")}), "invalid restore fence"},
		{"no operation at all", marshalFence(t, &pb.RestoreResult{}), "invalid restore fence"},
		{"a zero sequence", marshalFence(t, &pb.RestoreResult{Operation: op(uuid.NewString(), 0, "inc")}), "sequence or incarnation"},
		{"no incarnation", marshalFence(t, &pb.RestoreResult{Operation: op(uuid.NewString(), 1, "")}), "sequence or incarnation"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := restartWithFence(t, tc.data)
			require.Error(t, err)
			assert.Nil(t, s)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// An operation that was still running when the sidecar died is never replayed:
// the restart records it as failed.
func TestLoadRestoreRecord_SettlesAnInterruptedOperationAsFailed(t *testing.T) {
	id := uuid.NewString()
	s, err := restartWithFence(t, marshalFence(t, &pb.RestoreResult{
		Operation: &pb.RestoreOperation{Id: id, Sequence: 3, Incarnation: "earlier"},
		ExitCode:  -1,
	}))
	require.NoError(t, err)

	rec := s.restoreRecord
	assert.Equal(t, id, rec.GetOperation().GetId())
	assert.True(t, rec.Terminal)
	assert.False(t, rec.Succeeded)
	assert.Equal(t, int32(-1), rec.ExitCode)

	stored, err := os.ReadFile(s.cfg.stateSub(restoreRecordFile))
	require.NoError(t, err)
	var onDisk pb.RestoreResult
	require.NoError(t, proto.Unmarshal(stored, &onDisk))
	assert.True(t, onDisk.Terminal, "the settled record is durable")
}

func TestRunRestoreOperation_RefusesWhatCannotBeAdmitted(t *testing.T) {
	newReq := func(s *Server, id string) *pb.RunRequest {
		return &pb.RunRequest{Command: pb.Command_COMMAND_RESTORE, Restore: &pb.RestoreOperation{Id: id, Sequence: 1, Incarnation: s.incarnation}}
	}
	run := func(s *Server, req *pb.RunRequest) error {
		out := newStreamSender(discardRestoreStream{})
		_, err := s.runRestoreOperation(t.Context(), req, out)
		require.NoError(t, out.finish(nil))
		return err
	}

	t.Run("an operation id that is not a UUID", func(t *testing.T) {
		s := newTestServer(t, nil)
		err := run(s, newReq(s, "restore-1"))
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("a fence that could not be persisted stops every later restore", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root ignores directory permissions")
		}
		s := newTestServer(t, nil)
		_, err := s.configure(&pb.ConfigureRequest{Revision: 1, Repos: repos(1, 1)})
		require.NoError(t, err)
		stateDir := s.cfg.stateSub(dirState)
		require.NoError(t, os.Chmod(stateDir, 0o500))
		t.Cleanup(func() { _ = os.Chmod(stateDir, 0o700) })

		require.Error(t, run(s, newReq(s, uuid.NewString())), "the admission record cannot be written")

		require.NoError(t, os.Chmod(stateDir, 0o700))
		err = run(s, newReq(s, uuid.NewString()))
		require.Error(t, err)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		assert.Contains(t, err.Error(), "persistence failed")
	})
}
