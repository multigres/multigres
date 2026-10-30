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

package pgbackrestsidecar

import (
	"context"
	"database/sql"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

var walKey = regexp.MustCompile(`/archive/.*/[0-9A-F]{24}-`)

// matchWALFetch matches the repository GET behind an archive-get of a segment.
func matchWALFetch(r *http.Request) bool {
	return r.Method == http.MethodGet && walKey.MatchString(r.URL.Path)
}

// matchBackupData matches the GETs a restore makes for the backup's files.
func matchBackupData(r *http.Request) bool {
	return r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/backup/") &&
		(strings.Contains(r.URL.Path, "/pg_data/") || strings.Contains(r.URL.Path, "/bundle/"))
}

// startRestoringPooler adds a pooler whose data directory does not exist. Its
// multipooler restores it from the newest backup through the sidecar, then
// replays the archive: nothing streams to it, so every row it ends up with came
// through the sidecar.
func startRestoringPooler(t *testing.T, c *sidecarCluster, name string) *shardsetup.MultipoolerInstance {
	t.Helper()
	inst := c.setup.CreateMultipoolerInstance(t, name, utils.GetFreePort(t), utils.GetFreePort(t), utils.GetFreePort(t))
	// The sidecar first: postgres's archive_command dials its WAL socket.
	require.NoError(t, inst.PgBackRestd.Start(c.setup.Context(), t))
	require.NoError(t, inst.Pgctld.Start(t.Context(), t))
	require.NoError(t, inst.Multipooler.Start(t.Context(), t))
	return inst
}

// seedArchive writes rows on the primary, each in its own archived segment, so
// a restored pooler has WAL to replay beyond the bootstrap backup. It returns
// the number of rows the table holds.
func seedArchive(t *testing.T, c *sidecarCluster, segments int) int {
	t.Helper()
	primary := c.setup.GetPrimary(t)
	db := openSocketDB(t, primary)
	for range segments {
		waitArchived(t, c, primary, writeAndSwitch(t, db, "seed"), 2*time.Minute)
	}
	var n int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM sidecar_e2e").Scan(&n))
	return n
}

func rowCountIs(db *sql.DB, want int) func() bool {
	return func() bool {
		var n int
		return db.QueryRow("SELECT count(*) FROM sidecar_e2e").Scan(&n) == nil && n == want
	}
}

// TestSidecar_StopRestoreCommandLeavesNoPartial holds the repository read behind
// a standby's restore_command (the sidecar's archive-get is waiting on S3), then
// stops the restore command through pgctld. The curl and its sidecar request
// are cancelled, no %p or %p.fetch is left in pg_wal, postgres keeps recovering
// (a killed restore_command that is not wrapped would make it abort), and once
// the repository answers the same pooler finishes replay.
//
// It fails without the wrapper's --rename: a killed curl leaves a partial
// RECOVERYXLOG, which postgres does not size-check for history files and would
// replay as a truncated segment. Without the wrapper at all, postgres exits.
// Curl creates %p.fetch only when the first body bytes arrive and the sidecar
// sends a fetched segment in one burst, so the file this guards against exists
// for a moment only; the assertions are on the end state.
func TestSidecar_StopRestoreCommandLeavesNoPartial(t *testing.T) {
	c := newSidecarCluster(t, 2)
	want := seedArchive(t, c, 2)

	slow := c.front.addRule(holdRule(matchWALFetch))
	defer slow.release()
	inst := startRestoringPooler(t, c, "pooler-3")
	select {
	case <-slow.hit:
	case <-time.After(5 * time.Minute):
		t.Fatal("the restored pooler never fetched a WAL segment from the archive")
	}

	walDir := filepath.Join(inst.Pgctld.PoolerDir, "pg_data", "pg_wal")
	pidFile := filepath.Join(inst.Pgctld.PoolerDir, "restore_command.pid")
	require.Eventually(t, func() bool { _, err := os.Stat(pidFile); return err == nil },
		time.Minute, 200*time.Millisecond, "the restore wrapper should be running")

	pg, err := shardsetup.NewPgctldClient(inst.Pgctld.GrpcPort)
	require.NoError(t, err)
	defer pg.Close()
	resp, err := pg.StopRestoreCommand(t.Context(), &pgctldpb.StopRestoreCommandRequest{})
	require.NoError(t, err)
	assert.True(t, resp.Found, "the running restore command should be found")

	for _, leftover := range []string{"RECOVERYXLOG", "RECOVERYXLOG.fetch"} {
		assert.NoFileExists(t, filepath.Join(walDir, leftover))
	}
	partials, err := filepath.Glob(filepath.Join(walDir, "*.fetch"))
	require.NoError(t, err)
	assert.Empty(t, partials)

	// Not yet consistent, so postgres accepts no connections: its pid file is
	// the evidence that the stopped command did not abort recovery.
	time.Sleep(10 * time.Second)
	pidData, err := os.ReadFile(filepath.Join(inst.Pgctld.PoolerDir, "pg_data", "postmaster.pid"))
	require.NoError(t, err, "postgres must still be running, not aborted by the stopped command")
	pid, err := strconv.Atoi(strings.SplitN(string(pidData), "\n", 2)[0])
	require.NoError(t, err)
	require.NoError(t, syscall.Kill(pid, 0), "the postmaster must be alive")

	slow.release()
	db := openSocketDB(t, inst)
	require.Eventually(t, rowCountIs(db, want), 3*time.Minute, time.Second, "replay should finish once the archive answers")
	partials, err = filepath.Glob(filepath.Join(walDir, "*.fetch"))
	require.NoError(t, err)
	assert.Empty(t, partials)
}

// pgDataEntries counts the top-level entries of a pooler's data directory.
func pgDataEntries(inst *shardsetup.MultipoolerInstance) int {
	entries, err := os.ReadDir(filepath.Join(inst.Pgctld.PoolerDir, "pg_data"))
	if err != nil {
		return 0
	}
	return len(entries)
}

// watchDataDir polls the data directory and reports, on stop, whether postgres
// ever ran in it (postmaster.pid) and whether it ever shrank.
type dataDirWatch struct {
	stop    chan struct{}
	done    chan struct{}
	started bool
	shrank  bool
}

func watchDataDir(inst *shardsetup.MultipoolerInstance) *dataDirWatch {
	w := &dataDirWatch{stop: make(chan struct{}), done: make(chan struct{})}
	pidPath := filepath.Join(inst.Pgctld.PoolerDir, "pg_data", "postmaster.pid")
	go func() {
		defer close(w.done)
		peak := 0
		for {
			if _, err := os.Stat(pidPath); err == nil {
				w.started = true
			}
			n := pgDataEntries(inst)
			if n < peak {
				w.shrank = true
			}
			peak = max(peak, n)
			select {
			case <-w.stop:
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()
	return w
}

func (w *dataDirWatch) finish() *dataDirWatch {
	close(w.stop)
	<-w.done
	return w
}

// TestSidecar_KilledMidRestoreRecovers SIGKILLs the sidecar while its restore
// child is reading the backup. The multipooler must not start postgres on the
// half-written directory or delete it while the restore may still be running;
// once the sidecar is back, pushes work (no stop file left behind) and the
// restore is retried from scratch to completion.
//
// It fails if a failed restore RPC makes multipooler remove PGDATA or start
// postgres while the sidecar is down (the watch below sees the directory shrink
// or postmaster.pid appear), or if the killed sidecar leaves its stop file so that the
// push below is refused.
func TestSidecar_KilledMidRestoreRecovers(t *testing.T) {
	c := newSidecarCluster(t, 2)
	want := seedArchive(t, c, 1)

	hold := c.front.addRule(holdRule(matchBackupData))
	defer hold.release()
	inst := startRestoringPooler(t, c, "pooler-3")
	watch := watchDataDir(inst)
	select {
	case <-hold.hit:
	case <-time.After(5 * time.Minute):
		t.Fatal("the restore never read the backup")
	}
	marker := filepath.Join(inst.Pgctld.PoolerDir, constants.RestoreSentinelFile)
	require.Eventually(t, func() bool { _, err := os.Stat(marker); return err == nil },
		30*time.Second, 100*time.Millisecond, "multipooler should have written its restore marker before the restore")
	require.True(t, sidecarStatus(t, inst).RestoreInProgress)

	// Only the sidecar's own pid, as an OOM kill would: its restore child is not
	// signalled by us.
	killCtx, cancelKill := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelKill()
	_, killed := executil.KillPID(killCtx, sidecarPID(inst))
	require.True(t, killed)
	require.Eventually(t, func() bool { return !inst.PgBackRestd.IsRunning() }, 30*time.Second, 100*time.Millisecond)
	// The orphaned restore child (if it survived) must be gone before the
	// repository answers it again, or it would write into the retry's directory.
	time.Sleep(3 * time.Second)
	hold.release()
	// While the sidecar is unreachable multipooler cannot know whether a restore
	// is still writing, so it must neither delete the directory nor start
	// postgres on it. Give its monitor several ticks to do either.
	time.Sleep(20 * time.Second)
	w := watch.finish()
	assert.False(t, w.shrank, "PGDATA must not be deleted while the sidecar, and so the restore, is unaccounted for")
	assert.False(t, w.started, "postgres must not start on a directory a restore may still be writing")
	assert.FileExists(t, marker, "the marker must stay until the restore is retried")

	require.NoError(t, inst.PgBackRestd.Start(c.setup.Context(), t))
	st := sidecarStatus(t, inst)
	assert.False(t, st.RestoreInProgress, "a fresh sidecar has no restore running")
	assert.False(t, st.LastRestoreSucceeded, "and no restore has succeeded in its lifetime")
	assert.Eventually(t, func() bool { return sidecarStatus(t, inst).ServerUp }, time.Minute, time.Second, "the 8432 server should come back")

	shardsetup.WaitForEvent(t, inst.Multipooler.LogFile, "restore.attempt", "success", 5*time.Minute)
	db := openSocketDB(t, inst)
	require.Eventually(t, rowCountIs(db, want), 3*time.Minute, time.Second, "the retried restore should deliver every row")

	// Pushes work again: re-pushing a segment the repository already holds is
	// accepted, and is refused only if the stop file survived.
	var seg string
	for name := range repoSegments(c.s3) {
		if _, err := os.Stat(filepath.Join(c.setup.GetPrimary(t).Pgctld.PoolerDir, "pg_data", "pg_wal", name)); err == nil {
			seg = name
			break
		}
	}
	require.NotEmpty(t, seg, "the primary should still hold an archived segment")
	data, err := os.ReadFile(filepath.Join(c.setup.GetPrimary(t).Pgctld.PoolerDir, "pg_data", "pg_wal", seg))
	require.NoError(t, err)
	copyPath := filepath.Join(inst.Pgctld.PoolerDir, "pg_data", "pg_wal", seg)
	if _, err := os.Stat(copyPath); err != nil {
		require.NoError(t, os.WriteFile(copyPath, data, 0o600))
	}
	// An async push finds its work through archive_status, as postgres leaves it.
	ready := filepath.Join(inst.Pgctld.PoolerDir, "pg_data", "pg_wal", "archive_status", seg+".ready")
	require.NoError(t, os.WriteFile(ready, nil, 0o600))
	code, body := walRequest(t, inst, http.MethodPost, seg)
	assert.Equal(t, http.StatusOK, code, "a push after the restart must succeed: %s", body)
}

// asyncWorkers lists the pgBackRest async archive workers whose command line
// names stateDir.
func asyncWorkers(t *testing.T, stateDir string) []string {
	t.Helper()
	out, err := exec.Command("ps", "-axo", "pid=,command=").Output()
	require.NoError(t, err)
	var found []string
	for line := range strings.SplitSeq(string(out), "\n") {
		if strings.Contains(line, "archive-push:async") && strings.Contains(line, stateDir) {
			found = append(found, strings.TrimSpace(line))
		}
	}
	return found
}

// TestSidecar_RestoreGate503 runs a restore through a sidecar and probes it
// while it runs: a WAL push, a WAL fetch and Run(backup) are refused with 503 or
// Unavailable, Status says a restore is in progress, the 8432 server is not
// listening, and an async archive worker that was started before is gone. After
// the restore all of that is undone.
//
// The restore is requested on a standby's sidecar with its postgres stopped: pgBackRest refuses to
// restore over a non-empty PGDATA, so the data is never touched, and
// everything up to that refusal (the gate, the pause of 8432, "stop --force")
// is what runs. The repository read is held so the probes happen mid-restore.
//
// It fails if the gate does not close before the restore (a push gets 200 or
// 5xx), if the 8432 server keeps serving through the restore, or if the stop
// barrier is skipped (the worker below survives).
func TestSidecar_RestoreGate503(t *testing.T) {
	c := newSidecarCluster(t, 2)
	primary := c.setup.GetPrimary(t)
	waitArchived(t, c, primary, writeAndSwitch(t, openSocketDB(t, primary), "warm"), 2*time.Minute)
	// pgBackRest refuses to restore while postgres runs, so the victim is a
	// standby whose postgres is stopped (and kept stopped).
	victim := c.setup.GetStandbys()[0]
	resume := c.setup.StopPostgres(t, victim.Name, "fast")
	defer resume()
	stateDir := victim.PgBackRestd.Paths.StateDir
	serverAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(victim.PgBackRestd.Config.ServerPort))
	require.NoError(t, dialOK(serverAddr), "the 8432 server should be up before the restore")

	// An async worker that outlives its foreground: push a segment the
	// repository does not have (a copy of an archived one under a new name),
	// hold its PUT, then cancel the request so the foreground dies. The worker
	// (its own session) stays, blocked on the held PUT.
	var src string
	for name := range repoSegments(c.s3) {
		if _, err := os.Stat(filepath.Join(primary.Pgctld.PoolerDir, "pg_data", "pg_wal", name)); err == nil {
			src = name
			break
		}
	}
	require.NotEmpty(t, src)
	data, err := os.ReadFile(filepath.Join(primary.Pgctld.PoolerDir, "pg_data", "pg_wal", src))
	require.NoError(t, err)
	const fake = "0000000100000000000000F0"
	require.NoError(t, os.WriteFile(filepath.Join(victim.Pgctld.PoolerDir, "pg_data", "pg_wal", fake), data, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(victim.Pgctld.PoolerDir, "pg_data", "pg_wal", "archive_status", fake+".ready"), nil, 0o600))
	put := c.front.addRule(holdRule(func(r *http.Request) bool {
		return r.Method == http.MethodPut && strings.Contains(r.URL.Path, fake)
	}))
	defer put.release()
	pushCtx, cancelPush := context.WithCancel(t.Context())
	pushDone := make(chan struct{})
	go func() {
		defer close(pushDone)
		req, _ := http.NewRequestWithContext(pushCtx, http.MethodPost, "http://pgbackrest/wal/"+fake, nil)
		if resp, err := unixClient(victim.PgBackRestd.Paths.WALSocket).Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}()
	select {
	case <-put.hit:
	case <-time.After(2 * time.Minute):
		t.Fatal("the push never reached the repository")
	}
	cancelPush()
	<-pushDone
	require.Eventually(t, func() bool { return len(asyncWorkers(t, stateDir)) > 0 },
		30*time.Second, 500*time.Millisecond, "an async archive worker should outlive its foreground push")

	hold := c.front.addRule(holdRule(func(r *http.Request) bool {
		return r.Method == http.MethodGet && strings.Contains(r.URL.Path, "backup.info")
	}))
	defer hold.release()
	runCtx, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()
	restoreDone := make(chan error, 1)
	go func() {
		stream, err := controlClient(victim).Run(runCtx, connect.NewRequest(&pgbackrestdservice.RunRequest{Command: pgbackrestdservice.Command_COMMAND_RESTORE}))
		if err != nil {
			restoreDone <- err
			return
		}
		for stream.Receive() {
		}
		restoreDone <- stream.Err()
	}()
	select {
	case <-hold.hit:
	case err := <-restoreDone:
		t.Fatalf("the restore ended before the repository read it: %v", err)
	case <-time.After(2 * time.Minute):
		t.Fatal("the restore never read the repository")
	}

	// --- during the restore
	require.True(t, sidecarStatus(t, victim).RestoreInProgress)
	code, body := walRequest(t, victim, http.MethodPost, "000000010000000000000001")
	assert.Equal(t, http.StatusServiceUnavailable, code, "WAL push during restore: %s", body)
	code, body = walRequest(t, victim, http.MethodGet, "000000010000000000000001")
	assert.Equal(t, http.StatusServiceUnavailable, code, "WAL fetch during restore: %s", body)
	stream, err := controlClient(victim).Run(t.Context(), connect.NewRequest(&pgbackrestdservice.RunRequest{Command: pgbackrestdservice.Command_COMMAND_BACKUP}))
	if err == nil {
		for stream.Receive() {
		}
		err = stream.Err()
	}
	require.Error(t, err, "Run(backup) during restore")
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
	assert.Error(t, dialOK(serverAddr), "the 8432 server must be down during the restore")
	assert.False(t, sidecarStatus(t, victim).ServerUp)
	assert.Empty(t, asyncWorkers(t, stateDir), "the async worker must be gone: stop --force is the barrier")

	// --- after
	hold.release()
	put.release()
	select {
	case <-restoreDone:
	case <-time.After(5 * time.Minute):
		t.Fatal("the restore never finished")
	}
	require.Eventually(t, func() bool {
		st := sidecarStatus(t, victim)
		return !st.RestoreInProgress && st.ServerUp && !st.RestoreStartPending
	}, 2*time.Minute, time.Second, "the gate should reopen and the 8432 server come back")
	require.NoError(t, dialOK(serverAddr), "the 8432 server should accept again")
	// The primary cannot commit while its only standby is stopped, so the proof
	// that the WAL API is open again is a fetch of an archived segment.
	code, body = walRequest(t, victim, http.MethodGet, src)
	assert.Equal(t, http.StatusOK, code, "WAL fetch after the restore: %.200s", body)
}

func dialOK(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return err
	}
	return conn.Close()
}
