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

package command

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
)

func TestRewindRuns(t *testing.T) {
	t.Run("a request whose id was fenced first is refused", func(t *testing.T) {
		var r rewindRuns
		terminated, err := r.abort(t.Context(), "run-1")
		require.NoError(t, err)
		assert.False(t, terminated, "nothing was running")

		_, _, err = r.begin(t.Context(), "run-1")
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))

		_, finish, err := r.begin(t.Context(), "run-2")
		require.NoError(t, err, "other ids are unaffected")
		finish()
	})

	t.Run("aborting a running run cancels it and waits for it to finish", func(t *testing.T) {
		var r rewindRuns
		runCtx, finish, err := r.begin(t.Context(), "run-1")
		require.NoError(t, err)

		aborted := make(chan bool)
		go func() {
			terminated, err := r.abort(t.Context(), "run-1")
			assert.NoError(t, err)
			aborted <- terminated
		}()

		<-runCtx.Done()
		select {
		case <-aborted:
			t.Fatal("abort returned before the run finished")
		case <-time.After(50 * time.Millisecond):
		}

		finish()
		assert.True(t, <-aborted)
	})

	t.Run("a run that already finished reports nothing terminated", func(t *testing.T) {
		var r rewindRuns
		_, finish, err := r.begin(t.Context(), "run-1")
		require.NoError(t, err)
		finish()

		terminated, err := r.abort(t.Context(), "run-1")
		require.NoError(t, err)
		assert.False(t, terminated)

		_, _, err = r.begin(t.Context(), "run-1")
		require.Error(t, err, "the id is fenced all the same")
	})

	t.Run("aborting another id leaves the running run alone", func(t *testing.T) {
		var r rewindRuns
		runCtx, finish, err := r.begin(t.Context(), "run-1")
		require.NoError(t, err)
		defer finish()

		terminated, err := r.abort(t.Context(), "run-2")
		require.NoError(t, err)
		assert.False(t, terminated)
		assert.NoError(t, runCtx.Err())
	})

	t.Run("a second run is refused while one is running", func(t *testing.T) {
		var r rewindRuns
		_, finish, err := r.begin(t.Context(), "run-1")
		require.NoError(t, err)

		_, _, err = r.begin(t.Context(), "run-2")
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))

		finish()
		_, finish, err = r.begin(t.Context(), "run-2")
		require.NoError(t, err, "allowed once the first has finished")
		finish()
	})

	t.Run("a run without an id is tracked like any other but cannot be fenced", func(t *testing.T) {
		var r rewindRuns
		_, finish, err := r.begin(t.Context(), "")
		require.NoError(t, err)

		_, _, err = r.begin(t.Context(), "run-1")
		require.Error(t, err, "a tracked run cannot overlap it")
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		_, _, err = r.begin(t.Context(), "")
		require.Error(t, err, "nor can another one without an id")

		finish()
		_, finish, err = r.begin(t.Context(), "run-1")
		require.NoError(t, err, "allowed once it has finished")
		finish()
	})

	t.Run("a tracked run cannot be overlapped by a run without an id", func(t *testing.T) {
		var r rewindRuns
		_, finish, err := r.begin(t.Context(), "run-1")
		require.NoError(t, err)
		defer finish()

		_, _, err = r.begin(t.Context(), "")
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("abort gives up when the run does not finish in time", func(t *testing.T) {
		var r rewindRuns
		_, finish, err := r.begin(t.Context(), "run-1")
		require.NoError(t, err)
		defer finish()

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, err = r.abort(ctx, "run-1")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("the fenced set is bounded", func(t *testing.T) {
		var r rewindRuns
		for i := 0; i <= maxFencedRewindRuns; i++ {
			_, err := r.abort(t.Context(), fmt.Sprintf("run-%d", i))
			require.NoError(t, err)
		}
		_, err := r.abort(t.Context(), "run-0")
		require.NoError(t, err, "fencing an id already forgotten is harmless")
		assert.LessOrEqual(t, len(r.fenced), maxFencedRewindRuns)
		assert.LessOrEqual(t, len(r.order), maxFencedRewindRuns)

		_, finish, err := r.begin(t.Context(), "run-1")
		require.NoError(t, err, "the oldest ids are forgotten")
		finish()
		_, _, err = r.begin(t.Context(), fmt.Sprintf("run-%d", maxFencedRewindRuns))
		require.Error(t, err, "the newest are kept")
	})
}

// newRewindTestService returns a pgctld service whose pg_rewind is the given shell
// body, with a data directory that crash recovery is not needed for.
func newRewindTestService(t *testing.T, rewindBody string) (*PgCtldService, string) {
	t.Helper()
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "pg_data")
	t.Setenv(constants.PgDataDirEnvVar, dataDir)

	binDir := filepath.Join(baseDir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))
	testutil.CreateMockPostgreSQLBinaries(t, binDir)
	testutil.MockBinary(t, binDir, "pg_controldata", `echo "Database cluster state:               shut down"`)
	testutil.MockBinary(t, binDir, "pg_rewind", rewindBody)
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16\n"), 0o644))

	service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, baseDir, "localhost", 0, "")
	require.NoError(t, err)
	t.Cleanup(func() { service.Close() })
	return service, baseDir
}

func TestPgCtldService_PgRewindAbort(t *testing.T) {
	t.Run("a request for a fenced run is refused and pg_rewind never starts", func(t *testing.T) {
		service, baseDir := newRewindTestService(t, "")
		ran := filepath.Join(baseDir, "pg-rewind-ran")
		testutil.MockBinary(t, filepath.Join(baseDir, "bin"), "pg_rewind", fmt.Sprintf("touch %q", ran))

		resp, err := service.PgRewindAbort(t.Context(), &pb.PgRewindAbortRequest{RunId: "run-1"})
		require.NoError(t, err)
		assert.False(t, resp.GetTerminated())

		_, err = service.PgRewind(t.Context(), &pb.PgRewindRequest{
			SourceHost: "127.0.0.1", SourcePort: 5433, RunId: "run-1",
		})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.NoFileExists(t, ran, "pg_rewind must not start for a fenced run")
		assert.NoFileExists(t, filepath.Join(baseDir, constants.RewindRunningFile), "no record for a refused run")
	})

	t.Run("a running pg_rewind is terminated and its record is gone when abort returns", func(t *testing.T) {
		service, baseDir := newRewindTestService(t, "")
		started := filepath.Join(baseDir, "pg-rewind-started")
		testutil.MockBinary(t, filepath.Join(baseDir, "bin"), "pg_rewind", fmt.Sprintf("touch %q\nexec sleep 60", started))
		running := filepath.Join(baseDir, constants.RewindRunningFile)

		rewindDone := make(chan error, 1)
		go func() {
			_, err := service.PgRewind(context.Background(), &pb.PgRewindRequest{
				SourceHost: "127.0.0.1", SourcePort: 5433, RunId: "run-1",
			})
			rewindDone <- err
		}()
		require.Eventually(t, func() bool {
			_, err := os.Stat(started)
			return err == nil
		}, 30*time.Second, 20*time.Millisecond, "pg_rewind never started")
		require.FileExists(t, running)

		resp, err := service.PgRewindAbort(t.Context(), &pb.PgRewindAbortRequest{RunId: "run-1"})
		require.NoError(t, err)
		assert.True(t, resp.GetTerminated())
		assert.NoFileExists(t, running, "the record is gone by the time abort returns")

		select {
		case err := <-rewindDone:
			require.Error(t, err, "the aborted run fails")
		case <-time.After(30 * time.Second):
			t.Fatal("PgRewind did not return after the abort")
		}
	})

	t.Run("an empty run id is rejected", func(t *testing.T) {
		service, _ := newRewindTestService(t, "")
		_, err := service.PgRewindAbort(t.Context(), &pb.PgRewindAbortRequest{})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("abort reports an error when the run does not exit in time", func(t *testing.T) {
		service, baseDir := newRewindTestService(t, "")
		started := filepath.Join(baseDir, "pg-rewind-started")
		testutil.MockBinary(t, filepath.Join(baseDir, "bin"), "pg_rewind", fmt.Sprintf("touch %q\ntrap '' TERM\nexec sleep 60", started))

		go func() {
			_, _ = service.PgRewind(context.Background(), &pb.PgRewindRequest{
				SourceHost: "127.0.0.1", SourcePort: 5433, RunId: "run-1",
			})
		}()
		require.Eventually(t, func() bool {
			_, err := os.Stat(started)
			return err == nil
		}, 30*time.Second, 20*time.Millisecond)

		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		_, err := service.PgRewindAbort(ctx, &pb.PgRewindAbortRequest{RunId: "run-1"})
		require.Error(t, err, "a run that has not exited must not be reported as stopped")
	})
}
