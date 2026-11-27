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
	"net/http"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// sessionTTL is the life of the sessions the sidecar mints (shardsetup asks the
// s3mock for 10 minutes). The sidecar never hands pgBackRest a credential with
// five minutes or less left, and pgBackRest renews a process's credential when
// it is that close to expiry.
const sessionTTL = 10 * time.Minute

// TestSidecar_AsyncBacklogAcrossTwoExpiries shows that pgBackRest's
// asynchronous archive-push workers, which are long-lived processes with no
// foreground command behind them, renew their credential through the creds
// helper: one set of workers drains a backlog for longer than two session
// lifetimes, and s3mock refuses an expired session.
//
// The backlog is built so that it is one batch. An asynchronous archive-push
// reads the list of ready segments once when it starts and pushes all of them
// before it exits, with process-max (2) local workers that live as long as it
// does. Every WAL PUT is slowed to 40s (under pgBackRest's 60s I/O timeout),
// one segment is produced to start a first batch, and while its PUT is held
// the rest are produced. The next batch is all of them, 35 rounds of 40s, so
// the same two workers push for more than 23 minutes against a 10 minute
// session.
//
// What it requires, beyond every segment arriving with no failed archive
// attempt and no request refused for its credentials:
//   - the workers of that batch are the same processes (pid and start time)
//     for more than two session lifetimes, and no other archive-push worker
//     runs beside them, so every PUT in that time is theirs;
//   - in that time PUTs succeeded with three different sessions, each still
//     valid when used, and PUTs succeeded after the first and after the second
//     of them had expired.
//
// A worker that kept the credential it started with would have its PUT
// refused once that session expired: the s3front records the refusal and
// postgres counts a failed archive.
//
// Linux only: elsewhere the sidecar runs archive-push with --no-archive-async,
// so there is no long-lived worker. TestSidecar_BackupAcrossExpiry covers
// renewal by a long-lived process on every platform.
func TestSidecar_AsyncBacklogAcrossTwoExpiries(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("asynchronous archive workers run on Linux only; off Linux the sidecar passes --no-archive-async")
	}
	c := newSidecarCluster(t, 2)
	primary := c.setup.GetPrimary(t)
	db := openSocketDB(t, primary)
	waitArchived(t, c, primary, writeAndSwitch(t, db, "warm"), 2*time.Minute)
	var failedBefore int64
	require.NoError(t, db.QueryRow("SELECT failed_count FROM pg_stat_archiver").Scan(&failedBefore))

	const putDelay = 40 * time.Second
	slow := c.front.addRule(delayRule(func(r *http.Request) bool {
		return r.Method == http.MethodPut && walKey.MatchString(r.URL.Path)
	}, putDelay))
	defer slow.release()

	// The first batch: one segment, whose PUT is now being delayed.
	first := writeAndSwitch(t, db, "first batch")
	select {
	case <-slow.hit:
	case <-time.After(2 * time.Minute):
		t.Fatal("the first segment's PUT never reached the repository")
	}
	// The second batch, produced while the first is still being pushed.
	const backlog = 70
	produced := make([]string, 0, backlog)
	for range backlog {
		produced = append(produced, writeAndSwitch(t, db, "backlog"))
	}
	require.NotContains(t, repoSegments(c.s3), first, "the backlog must be complete before the first batch ends")

	// Watch the primary sidecar's archive-push processes until the backlog is
	// drained.
	watch := watchArchivePushWorkers(t, primary.PgBackRestd.Paths.StateDir)
	for _, seg := range append([]string{first}, produced...) {
		waitArchived(t, c, primary, seg, backlog/2*putDelay+10*time.Minute)
	}
	workers := watch.stop()

	var failed int64
	require.NoError(t, db.QueryRow("SELECT failed_count FROM pg_stat_archiver").Scan(&failed))
	assert.Equal(t, failedBefore, failed, "no archive attempt may have failed")
	assert.Empty(t, c.front.rejectedRequests(), "s3mock must not have refused any request for its credentials")

	// One asynchronous archive-push and its local workers lived for more than
	// two session lifetimes.
	async := workers.longest("archive-push:async")
	require.NotNil(t, async, "no archive-push:async process was seen: %s", workers)
	require.Greater(t, async.last.Sub(async.first), 2*sessionTTL,
		"one asynchronous archive-push must drain for more than two session lifetimes: %s", workers)
	locals := workers.during("archive-push:local", async.first, async.last)
	require.NotEmpty(t, locals, "no archive-push:local worker was seen: %s", workers)
	for _, w := range locals {
		assert.Greater(t, w.last.Sub(w.first), 2*sessionTTL,
			"every local worker of the batch must live as long as the batch, with none starting beside them: %s", workers)
	}

	// Their PUTs, in the time they were watched.
	var tokens []string
	var puts []walPut
	for _, p := range c.front.walPutLog() {
		if p.at.Before(async.first) || p.at.After(async.last) {
			continue
		}
		require.Equal(t, http.StatusOK, p.status, "a WAL PUT failed during the batch")
		exp, ok := c.front.sessionExpiry(p.token)
		require.True(t, ok, "a WAL PUT carried a session token s3mock never issued")
		require.True(t, p.at.Before(exp), "s3mock accepted a PUT at %s for a session that expired at %s", p.at, exp)
		if !slices.Contains(tokens, p.token) {
			tokens = append(tokens, p.token)
		}
		puts = append(puts, p)
	}
	require.GreaterOrEqual(t, len(tokens), 3, "the workers must have used three sessions: the one they started with and two renewals")
	for i, token := range tokens[:2] {
		exp, _ := c.front.sessionExpiry(token)
		assert.True(t, slices.ContainsFunc(puts, func(p walPut) bool { return p.at.After(exp) }),
			"no PUT succeeded after session %d of the batch expired at %s", i+1, exp)
	}
	t.Logf("batch %s to %s: %d WAL PUTs over %d sessions; workers: %s",
		async.first.Format(time.TimeOnly), async.last.Format(time.TimeOnly), len(puts), len(tokens), workers)
}

// TestSidecar_BackupAcrossExpiry takes a backup that runs past a credential
// expiry (every repository request is delayed so the backup outlives a 10
// minute session) and requires it to complete: pgBackRest's main and backup
// processes renew through the creds helper. The negative subtest breaks the
// credential source so every renewal returns the first session; the same backup
// must then fail, which shows the positive case really depends on renewal.
//
// It fails if the sidecar hands out a credential that expires mid-backup, or
// (negative) if the backup somehow succeeds on an expired session.
func TestSidecar_BackupAcrossExpiry(t *testing.T) {
	c := newSidecarCluster(t, 2)
	standby := c.setup.GetStandbys()[0]
	client, err := shardsetup.NewMultipoolerClient(standby.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer client.Close()

	// Slow the backup's repository requests while it runs, so it outlives the
	// session. 40s stays under pgBackRest's 60s I/O timeout. The WAL archive
	// is left alone: a backup closes a segment when it starts and gives the
	// segment 60s to be archived, and a push takes several requests.
	slow := c.front.addRule(delayRule(func(r *http.Request) bool {
		if r.URL.Path == "/_session" {
			return false
		}
		return !strings.Contains(r.URL.Path, "/archive/") && !strings.Contains(r.URL.Query().Get("prefix"), "/archive/")
	}, 40*time.Second))
	defer slow.release()
	slow.off.Store(true)

	backup := func(t *testing.T) (time.Duration, string, error) {
		t.Helper()
		// Slow only the first stretch, longer than a session: the work that
		// follows the backup (info, expire) must not inherit the delay.
		// The duration is measured on the wall clock, like the delay's end and
		// the session's expiry (see wallNow).
		start := wallNow()
		until := start.Add(sessionTTL + time.Minute)
		slow.until.Store(&until)
		slow.off.Store(false)
		defer slow.off.Store(true)
		resp, err := client.Manager.Backup(t.Context(), &multipoolermanagerdatapb.BackupRequest{Type: "full"})
		id := ""
		if resp != nil {
			id = resp.BackupId
		}
		return wallNow().Sub(start), id, err
	}

	t.Run("RenewalWorks", func(t *testing.T) {
		took, id, err := backup(t)
		require.NoError(t, err, "a backup across a credential expiry must complete")
		require.NotEmpty(t, id)
		assert.Greater(t, took, sessionTTL, "the backup must outlive one session or it proves nothing")
		assert.Empty(t, c.front.rejectedRequests(), "no request may have been refused for its credentials")
		list, err := client.Manager.GetBackups(t.Context(), &multipoolermanagerdatapb.GetBackupsRequest{})
		require.NoError(t, err)
		var status multipoolermanagerdatapb.BackupMetadata_Status
		for _, b := range list.Backups {
			if b.BackupId == id {
				status = b.Status
			}
		}
		assert.Equal(t, multipoolermanagerdatapb.BackupMetadata_COMPLETE, status)
	})

	t.Run("RenewalBroken", func(t *testing.T) {
		// Every mint from now on returns the first session, whose expiry is in
		// the past by the time the cache needs a new one. Within five minutes
		// the sidecar has no usable credential and refuses to hand one out.
		c.front.replayFirstSession()
		logs := sidecarLogMarks(c)
		mintsBefore := c.front.sessionCount()
		took, _, err := backup(t)
		require.Error(t, err, "with renewal broken the backup must fail (it ran %v)", took)
		// The failure must be the renewal: a sidecar asked the source again,
		// was answered with the expired session and refused to use it. Any
		// other backup failure would pass the line above too.
		assert.Greater(t, c.front.sessionCount(), mintsBefore, "a sidecar must have asked the credential source again")
		assert.Contains(t, logs.added(t), "source returned a credential expiring at",
			"a sidecar must have refused the replayed session as too close to its expiry")
	})
}
