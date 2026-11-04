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
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// Retains the independent review's F4 signal/error transition probe. The HTTP
// request was decoded before stop and keeps a live context during HTTP drain.
func TestSignalStopMustFenceRestoreAdmission(t *testing.T) {
	s := newTestServer(t, nil)
	s.configuredFlag.Store(true)
	old := &pb.RestoreResult{Operation: &pb.RestoreOperation{Id: uuid.NewString(), Sequence: 1, Incarnation: s.incarnation}, Terminal: true, Succeeded: true}
	s.restoreRecord = old
	require.NoError(t, s.saveRestoreRecord())
	before, err := os.ReadFile(s.cfg.stateSub(restoreRecordFile))
	require.NoError(t, err)
	req := &pb.RunRequest{Command: pb.Command_COMMAND_RESTORE, Restore: &pb.RestoreOperation{Id: uuid.NewString(), Sequence: 2, Predecessor: old.Operation.Id, Incarnation: s.incarnation}}
	// Hold worker admission to observe the earlier gate-stop boundary while the
	// shared stop helper is still running. Restore admission must already be shut.
	stopped := make(chan struct{})
	func() {
		s.workersMu.Lock()
		defer s.workersMu.Unlock()
		go func() { s.stopAdmission(); close(stopped) }()
		require.Eventually(t, func() bool { s.gate.mu.Lock(); defer s.gate.mu.Unlock(); return s.gate.stopped }, time.Second, time.Millisecond)
		s.restoreMu.Lock()
		restoreClosed := s.restoreClosed
		s.restoreMu.Unlock()
		require.True(t, restoreClosed, "visible stopped gate must already fence restore CAS")
	}()
	<-stopped
	out := newStreamSender(discardRestoreStream{})
	_, err = s.runRestoreOperation(t.Context(), req, out)
	require.NoError(t, out.finish(nil))
	require.ErrorContains(t, err, "sidecar closing")
	require.True(t, proto.Equal(old, s.restoreRecord), "late restore replaced the successful fence")
	after, err := os.ReadFile(s.cfg.stateSub(restoreRecordFile))
	require.NoError(t, err)
	require.Equal(t, before, after, "late admission must not modify the durable journal")
}

func TestGateCloseHonorsPermanentStop(t *testing.T) {
	g := NewGate()
	release, ok := g.TryEnter()
	require.True(t, ok)
	closed := make(chan error, 1)
	go func() { _, err := g.Close(t.Context(), time.Minute); closed <- err }()
	require.Eventually(t, func() bool { g.mu.Lock(); defer g.mu.Unlock(); return g.closed }, time.Second, time.Millisecond)
	g.StopAdmission()
	select {
	case err := <-closed:
		require.ErrorContains(t, err, "stopped")
	case <-time.After(time.Second):
		t.Fatal("stop did not wake Gate.Close")
	}
	// Shutdown still waits for admitted handlers instead of taking a reopenable
	// restore lease. A timed-out drainer cannot reopen a stopped gate either.
	require.Error(t, g.drainStopped(t.Context(), time.Millisecond))
	release()
	require.NoError(t, g.drainStopped(t.Context(), time.Second))
	_, err := g.Close(t.Context(), time.Second)
	require.ErrorContains(t, err, "stopped")
	_, ok = g.TryEnter()
	require.False(t, ok)
}

func TestGateReopenCannotUndoPermanentStop(t *testing.T) {
	g := NewGate()
	reopen, err := g.Close(t.Context(), time.Second)
	require.NoError(t, err)
	g.StopAdmission()
	reopen()
	_, err = g.Close(t.Context(), time.Second)
	require.ErrorContains(t, err, "stopped")
	_, ok := g.TryEnter()
	require.False(t, ok)
	require.NoError(t, g.drainStopped(t.Context(), time.Second))
}
