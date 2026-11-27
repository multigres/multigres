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

	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/s3mock"
)

// TestSidecar_RefreshedStaticKeysReplaceRevokedOnes is the path `multigres
// cluster refresh-credentials` takes for a sidecar that reads its S3 keys from
// a key directory, against a repository that enforces them: s3mock accepts
// only the session it issued and has not revoked.
//
//  1. With the first keys, WAL is archived.
//  2. The first keys are revoked. The next segment is refused by the
//     repository and stays queued.
//  3. New keys are written to the key directory and nothing else is done. The
//     segment is still refused: the sidecar reads the directory only while it
//     handles a Configure, so the file alone changes nothing.
//  4. local.ReloadSidecarS3Keys, which is what refresh-credentials calls,
//     re-sends the Configure the sidecar holds. The queued segment and the
//     next one are archived, and every WAL PUT the repository accepts after
//     the revocation carries the new session.
//
// It fails if the reload does not make the sidecar use the new keys (4 never
// archives), and step 3 fails if a sidecar starts to pick the keys up on its
// own, which would make the reload untested.
func TestSidecar_RefreshedStaticKeysReplaceRevokedOnes(t *testing.T) {
	requireSidecarHost(t)
	t.Parallel()
	clusterSlots <- struct{}{}
	t.Cleanup(func() { <-clusterSlots })

	s3Server, err := s3mock.NewServer(0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s3Server.Stop() })
	require.NoError(t, s3Server.CreateBucket(bucket))
	s3Server.RequireSessions()
	front := newS3Front(t, s3Server)
	first := s3Server.IssueSession(time.Hour)

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithS3Backup(bucket, "us-east-1", front.URL()),
		shardsetup.WithS3CAPEM(front.CertPEM()),
		shardsetup.WithS3StaticKey(first.AccessKeyID, first.SecretAccessKey, first.SessionToken),
	)
	t.Cleanup(cleanup)
	c := &sidecarCluster{setup: setup, s3: s3Server, front: front}
	primary := setup.GetPrimary(t)
	db := openSocketDB(t, primary)
	failedCount := func() int64 {
		var n int64
		require.NoError(t, db.QueryRow("SELECT failed_count FROM pg_stat_archiver").Scan(&n))
		return n
	}
	// waitForFailure waits for an archive attempt to fail after this call.
	waitForFailure := func(why string) {
		t.Helper()
		before := failedCount()
		require.Eventually(t, func() bool { return failedCount() > before }, 5*time.Minute, time.Second, why)
	}

	// 1. The first keys work.
	waitArchived(t, c, primary, writeAndSwitch(t, db, "first keys"), 2*time.Minute)

	// 2. Revoked: the repository refuses the next segment.
	s3Server.RevokeSession(first.SessionToken)
	revokedAt := wallNow()
	queued := writeAndSwitch(t, db, "queued while the keys are revoked")
	waitForFailure("an archive attempt must fail once the repository has revoked the keys")
	require.NotEmpty(t, front.rejectedRequests(), "the repository must have refused a request for its credentials")
	require.NotContains(t, repoSegments(s3Server), queued)

	// 3. New keys on disk only: still refused.
	second := s3Server.IssueSession(time.Hour)
	for _, inst := range setup.Multipoolers {
		require.NoError(t, local.WriteS3KeyBundle(inst.PgBackRestd.Config.S3StaticKeyDir,
			second.AccessKeyID, second.SecretAccessKey, second.SessionToken))
	}
	waitForFailure("writing the key file alone must not change the keys a running sidecar uses")
	require.NotContains(t, repoSegments(s3Server), queued)

	// 4. Reload, as refresh-credentials does.
	for name, inst := range setup.Multipoolers {
		got, err := local.ReloadSidecarS3Keys(t.Context(), inst.PgBackRestd.Paths, sidecarPID(inst))
		require.NoError(t, err, "reload the keys of %s", name)
		require.Equal(t, local.S3KeysReloaded, got, "%s holds a configuration, so it must have reloaded", name)
	}
	waitArchived(t, c, primary, queued, 5*time.Minute)
	waitArchived(t, c, primary, writeAndSwitch(t, db, "second keys"), 5*time.Minute)

	accepted := 0
	for _, put := range front.walPutLog() {
		if put.at.Before(revokedAt) || put.status != http.StatusOK {
			continue
		}
		accepted++
		assert.Equal(t, second.SessionToken, put.token, "a WAL PUT accepted after the revocation must carry the new session")
	}
	assert.GreaterOrEqual(t, accepted, 2, "both segments must have been stored after the revocation")
}
