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

package pgbackrestd

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/constants"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
)

type delayedRestoreControl struct {
	*controlService
	id       string
	decoded  chan struct{}
	release  chan struct{}
	finished chan error
}

func (c *delayedRestoreControl) Run(ctx context.Context, req *connect.Request[pb.RunRequest], stream *connect.ServerStream[pb.RunOutput]) error {
	if req.Msg.GetRestore().GetId() == c.id {
		close(c.decoded)
		<-c.release
		// Prove the identity fence, not merely the cancelled-context defense.
		ctx = context.WithoutCancel(ctx)
		err := c.controlService.Run(ctx, req, stream)
		c.finished <- err
		return err
	}
	return c.controlService.Run(ctx, req, stream)
}

func consumeRestore(ctx context.Context, client pgbackrestdserviceconnect.PgBackRestdClient, req *pb.RunRequest) error {
	stream, err := client.Run(ctx, connect.NewRequest(req))
	if err != nil {
		return err
	}
	defer stream.Close()
	for stream.Receive() {
	}
	return stream.Err()
}

// Regression for a decoded request paused before admission over the actual
// Unix Connect transport. B cleans up and finishes before A resumes. A must
// not resurrect, even with a live context and no currently running restore.
func TestRestoreFenceLateUnixAdmission(t *testing.T) {
	f := newFakeBackrest(t, nil, false)
	s := newTestServer(t, nil)
	_, err := s.configure(&pb.ConfigureRequest{Revision: 1, Repos: repos(1, 1)})
	require.NoError(t, err)
	op := &pb.RestoreOperation{Id: uuid.NewString(), Sequence: 1, Incarnation: s.incarnation}
	a := &pb.RunRequest{Command: pb.Command_COMMAND_RESTORE, Args: []string{"--type=standby", "--set=20250104-100000F"}, Restore: op}
	delayed := &delayedRestoreControl{controlService: &controlService{s: s}, id: op.Id, decoded: make(chan struct{}), release: make(chan struct{}), finished: make(chan error, 1)}
	path, handler := pgbackrestdserviceconnect.NewPgBackRestdHandler(delayed)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	// Testkit has no short Unix-path helper; macOS limits sun_path to 104 bytes.
	dir, err := os.MkdirTemp("/tmp", "restore-fence-")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "c.sock")
	ln, err := net.Listen("unix", socket)
	require.NoError(t, err)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	t.Cleanup(func() {
		require.NoError(t, srv.Close())
		if err := <-served; !errors.Is(err, http.ErrServerClosed) {
			require.NoError(t, err)
		}
	})
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client := pgbackrestdserviceconnect.NewPgBackRestdClient(&http.Client{Transport: transport}, "http://sidecar")
	done := make(chan error, 1)
	callCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { done <- consumeRestore(callCtx, client, a) }()
	<-delayed.decoded
	cancel()
	require.Equal(t, connect.CodeCanceled, connect.CodeOf(<-done))
	st, err := client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
	require.NoError(t, err)
	require.False(t, st.Msg.RestoreInProgress)
	require.NoError(t, os.MkdirAll(s.cfg.PGData, 0o700))
	victim := filepath.Join(s.cfg.PGData, "partial")
	require.NoError(t, os.WriteFile(victim, []byte("partial"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(s.cfg.PGData), constants.RestoreSentinelFile), []byte("interrupted"), 0o600))
	b := proto.Clone(a).(*pb.RunRequest)
	b.Restore.Id = uuid.NewString()
	b.Restore.Cleanup = true
	require.NoError(t, consumeRestore(t.Context(), client, b))
	_, err = os.Stat(victim)
	require.ErrorIs(t, err, os.ErrNotExist)
	close(delayed.release)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(<-delayed.finished))
	require.NoError(t, consumeRestore(t.Context(), client, b), "identical replay must return the result")
	require.Equal(t, 1, strings.Count(strings.Join(f.commands(t), "\n"), "restore"))
	b.Args = []string{"--type=standby", "--set=20250105-100000F"}
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(consumeRestore(t.Context(), client, b)))
}

func TestRestoreFenceRestartAndCancelledAdmission(t *testing.T) {
	newFakeBackrest(t, nil, false)
	s := newTestServer(t, nil)
	_, err := s.configure(&pb.ConfigureRequest{Revision: 1, Repos: repos(1, 1)})
	require.NoError(t, err)
	req := &pb.RunRequest{Command: pb.Command_COMMAND_RESTORE, Args: []string{"--type=standby"}, Restore: &pb.RestoreOperation{Id: uuid.NewString(), Sequence: 1, Incarnation: s.incarnation}}
	out := newStreamSender(discardRestoreStream{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = s.runRestoreOperation(ctx, req, out)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, s.restoreRecord)
	_, err = s.runRestoreOperation(t.Context(), req, out)
	require.NoError(t, err)
	require.NoError(t, out.finish(nil))
	require.NoError(t, s.Close())
	s2, err := newServer(s.cfg, s.decision)
	require.NoError(t, err)
	t.Cleanup(func() { s2.Close() })
	require.NotEqual(t, s.incarnation, s2.incarnation)
	require.Equal(t, req.Restore.Id, s2.restoreRecord.Operation.Id)
	out2 := newStreamSender(discardRestoreStream{})
	defer func() { require.NoError(t, out2.finish(nil)) }()
	code, err := s2.runRestoreOperation(t.Context(), req, out2)
	require.NoError(t, err)
	require.Zero(t, code, "an admitted completed operation is replayable across restart without execution")
	req.Restore.Id = uuid.NewString()
	_, err = s2.runRestoreOperation(t.Context(), req, out2)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "old unadmitted requests cannot enter the new incarnation")
}

type discardRestoreStream struct{}

func (discardRestoreStream) Send(*pb.RunOutput) error { return nil }

// Both pre-stop failure and missing quiescence must leave the old directory
// and prefetch queue intact. No "stop succeeded" or stale global success bit
// can substitute for a worker ACK.
func TestRestoreCleanupFailsClosed(t *testing.T) {
	for _, failure := range []string{"pre-start", "worker-ack", "postmaster", "missing-marker", "symlink-marker", "admission-persistence"} {
		t.Run(failure, func(t *testing.T) {
			opts := fakeOpts{}
			if failure == "pre-start" {
				opts.failStartCalls = []int{1}
			}
			f := newFakeBackrestOpts(t, opts)
			s := newTestServer(t, nil)
			_, err := s.configure(&pb.ConfigureRequest{Revision: 1, Repos: repos(1, 1)})
			require.NoError(t, err)
			s.lastRestoreOK.Store(true)
			require.NoError(t, os.MkdirAll(s.cfg.PGData, 0o700))
			partial := filepath.Join(s.cfg.PGData, "PG_VERSION")
			require.NoError(t, os.WriteFile(partial, []byte("partial"), 0o600))
			require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(s.cfg.PGData), constants.RestoreSentinelFile), []byte("interrupted"), 0o600))
			queue := s.cfg.stateSub("spool/archive/" + Stanza + "/in")
			require.NoError(t, os.MkdirAll(queue, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(queue, "wal"), []byte("old"), 0o600))
			if failure == "worker-ack" {
				w := &ownedWorker{done: make(chan struct{}), err: context.DeadlineExceeded}
				close(w.done)
				s.workers = map[*ownedWorker]struct{}{w: {}}
				t.Cleanup(func() { s.workersMu.Lock(); delete(s.workers, w); s.workersMu.Unlock() })
			}
			marker := filepath.Join(filepath.Dir(s.cfg.PGData), constants.RestoreSentinelFile)
			switch failure {
			case "postmaster":
				require.NoError(t, os.WriteFile(filepath.Join(s.cfg.PGData, constants.PostmasterPIDFile), []byte("123"), 0o600))
			case "missing-marker":
				require.NoError(t, os.Remove(marker))
			case "symlink-marker":
				require.NoError(t, os.Remove(marker))
				require.NoError(t, os.Symlink(partial, marker))
			case "admission-persistence":
				require.NoError(t, os.Mkdir(s.cfg.stateSub(restoreRecordFile), 0o700))
			}
			out := newStreamSender(discardRestoreStream{})
			defer func() { require.NoError(t, out.finish(nil)) }()
			req := &pb.RunRequest{Command: pb.Command_COMMAND_RESTORE, Restore: &pb.RestoreOperation{Id: uuid.NewString(), Sequence: 1, Incarnation: s.incarnation, Cleanup: true}}
			code, err := s.runRestoreOperation(t.Context(), req, out)
			require.True(t, err != nil || code != 0)
			require.FileExists(t, partial)
			require.FileExists(t, filepath.Join(queue, "wal"))
			require.NotContains(t, f.commands(t), "restore")
			require.False(t, s.restoreRecord.Succeeded)
		})
	}
}

func TestRestoreFenceRejectsABA(t *testing.T) {
	fake := newFakeBackrest(t, nil, false)
	s := newTestServer(t, nil)
	_, err := s.configure(&pb.ConfigureRequest{Revision: 1, Repos: repos(1, 1)})
	require.NoError(t, err)
	out := newStreamSender(discardRestoreStream{})
	defer func() { require.NoError(t, out.finish(nil)) }()
	a := &pb.RunRequest{Command: pb.Command_COMMAND_RESTORE, Restore: &pb.RestoreOperation{Id: uuid.NewString(), Sequence: 1, Incarnation: s.incarnation}}
	b := proto.Clone(a).(*pb.RunRequest)
	b.Restore.Id = uuid.NewString()
	b.Restore.Sequence = 2
	b.Restore.Predecessor = a.Restore.Id
	c := proto.Clone(a).(*pb.RunRequest)
	c.Restore.Sequence = 3
	c.Restore.Predecessor = b.Restore.Id
	for _, req := range []*pb.RunRequest{a, b, c} {
		code, err := s.runRestoreOperation(t.Context(), req, out)
		require.NoError(t, err)
		require.Zero(t, code)
	}
	// C reused A's UUID. B's old predecessor UUID now matches again, but its
	// sequence must never be admitted again or allowed to clean up C's data.
	_, err = s.runRestoreOperation(t.Context(), b, out)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	require.Equal(t, 3, strings.Count(strings.Join(fake.commands(t), "\n"), "restore"))
}
