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

package manager

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// Retains the review's real next-recovery-tick interleaving: Run A is decoded
// but held before admission; the next tick sees idle and completes info. A is
// admitted before info's stream ends. The manager must not delete its writes.
// The separate sidecar Unix-transport test proves superseded admission fails.
func TestRestoreNextTickPreservesLateWriter(t *testing.T) {
	f := newAsyncRestoreFixture(t)
	original := f.a.fake.run
	admit, decoded := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(admit) }) })
	f.a.fake.run = func(ctx context.Context, req *pb.RunRequest, stream *connect.ServerStream[pb.RunOutput]) error {
		if req.Command == pb.Command_COMMAND_RESTORE && first.CompareAndSwap(false, true) {
			close(decoded)
			<-admit
		}
		err := original(ctx, req, stream)
		if req.Command == pb.Command_COMMAND_INFO {
			once.Do(func() { close(admit) })
			select {
			case <-f.a.midway:
			case <-time.After(5 * time.Second):
				t.Error("late restore never started")
			}
		}
		return err
	}
	// Cancellation belongs after the request has reached the admission barrier.
	// A short startup deadline can expire before Run is decoded on a busy race
	// runner, leaving this test waiting forever for a request that never arrived.
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	t.Cleanup(cancelFirst)
	firstDone := make(chan error, 1)
	go func() { firstDone <- f.restoreWithContext(t, firstCtx) }()
	select {
	case <-decoded:
	case <-time.After(10 * time.Second):
		t.Fatal("first restore did not reach admission barrier")
	}
	cancelFirst()
	select {
	case err := <-firstDone:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled first restore did not return")
	}
	require.True(t, f.hasSentinel())
	require.True(t, f.a.fakeIdle())
	err := f.pm.restoreAndStartPostgresForTest(t, f)
	require.Error(t, err)
	require.False(t, f.a.fakeIdle())
	_, statErr := os.Stat(filepath.Join(f.dataDir, "half"))
	require.NoError(t, statErr, "next tick must not delete PGDATA under the admitted writer")
	require.True(t, f.hasSentinel())
	f.a.finish(false)
	require.Eventually(t, func() bool { return f.a.fakeIdle() }, 5*time.Second, 10*time.Millisecond)
}
