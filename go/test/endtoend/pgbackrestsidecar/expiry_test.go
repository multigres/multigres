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

// TestSidecar_AsyncBacklogAcrossTwoExpiries builds an archive backlog that the
// asynchronous archive-push worker drains for longer than two session
// lifetimes, long after the foreground push that started it has exited. The
// worker is one long-lived pgBackRest process, so it must renew its credential
// itself through the creds helper; s3mock refuses an expired session.
//
// Each repository PUT is slowed (40s, under pgBackRest's 60s I/O timeout and
// postgres's retry cadence) while a segment is produced every 15s. pgBackRest
// pushes two at a time (process-max=2), so it drains 3 a minute against 4
// produced, the backlog grows and the worker never idles. No segment may fail, none may be
// refused for its credentials, and all of them end up in the repository.
//
// It fails if the worker keeps using the credential it started with: s3mock
// rejects the expired session, the PUT fails, postgres counts a failed archive
// and the s3front records the rejected request.
func TestSidecar_AsyncBacklogAcrossTwoExpiries(t *testing.T) {
	c := newSidecarCluster(t, 2)
	primary := c.setup.GetPrimary(t)
	db := openSocketDB(t, primary)
	waitArchived(t, c, primary, writeAndSwitch(t, db, "warm"), 2*time.Minute)
	sessionsBefore := c.front.sessionCount()
	var failedBefore int64
	require.NoError(t, db.QueryRow("SELECT failed_count FROM pg_stat_archiver").Scan(&failedBefore))

	slow := c.front.addRule(delayRule(func(r *http.Request) bool {
		return r.Method == http.MethodPut && walKey.MatchString(r.URL.Path)
	}, 40*time.Second))
	defer slow.release()

	start := time.Now()
	var produced []string
	for time.Since(start) < 2*sessionTTL+2*time.Minute {
		produced = append(produced, writeAndSwitch(t, db, "backlog"))
		time.Sleep(15 * time.Second)
	}
	t.Logf("produced %d segments in %v; %d archived so far", len(produced), time.Since(start).Round(time.Second), len(repoSegments(c.s3)))
	require.Greater(t, len(produced), len(repoSegments(c.s3))-1, "the worker should still be behind: the test must exercise a live backlog")

	slow.release()
	for _, seg := range produced {
		waitArchived(t, c, primary, seg, 10*time.Minute)
	}

	var failed int64
	require.NoError(t, db.QueryRow("SELECT failed_count FROM pg_stat_archiver").Scan(&failed))
	assert.Equal(t, failedBefore, failed, "no archive attempt may have failed")
	assert.Empty(t, c.front.rejectedRequests(), "s3mock must not have refused any request for its credentials")
	assert.GreaterOrEqual(t, c.front.sessionCount()-sessionsBefore, 2, "the credential must have been renewed")
	assert.Greater(t, time.Since(c.front.firstSessionAt()), 2*sessionTTL, "the run must span two session lifetimes")
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

	// Slow every repository request while a backup runs, so it outlives the
	// session. 40s stays under pgBackRest's 60s I/O timeout.
	slow := c.front.addRule(delayRule(func(r *http.Request) bool { return r.URL.Path != "/_session" }, 40*time.Second))
	defer slow.release()
	slow.off.Store(true)

	backup := func(t *testing.T) (time.Duration, string, error) {
		t.Helper()
		// Slow only the first stretch, longer than a session: the work that
		// follows the backup (info, expire) must not inherit the delay.
		slow.until.Store(time.Now().Add(sessionTTL + time.Minute).UnixNano())
		slow.off.Store(false)
		defer slow.off.Store(true)
		start := time.Now()
		resp, err := client.Manager.Backup(t.Context(), &multipoolermanagerdatapb.BackupRequest{Type: "full"})
		id := ""
		if resp != nil {
			id = resp.BackupId
		}
		return time.Since(start), id, err
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
		took, _, err := backup(t)
		assert.Error(t, err, "with renewal broken the backup must fail (it ran %v)", took)
	})
}
