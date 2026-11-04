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
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/tools/testpoll"
)

// TestSidecar_RestartedWithOtherTLSSettingsIsNotConfigured restarts only the
// leader's sidecar, with verification of the S3 endpoint switched off, while
// its multipooler and postgres keep running. Multipooler compared the
// sidecar's settings with the topology when it started and does not start
// again, and the sidecar's saved configuration would let it archive at once.
// Neither happens: the restarted sidecar does not load the configuration saved
// under the other settings, it refuses a Configure that names the fingerprint
// multipooler expects, and multipooler does not configure it. WAL stays queued
// in postgres until the sidecar is restarted with the topology's settings.
//
// It fails if the sidecar loads state another fingerprint saved, or if a
// segment reaches the repository while verification is off.
func TestSidecar_RestartedWithOtherTLSSettingsIsNotConfigured(t *testing.T) {
	c := newDirectCluster(t, 2, nil)
	primary := c.setup.GetPrimary(t)
	db := openSocketDB(t, primary)
	waitArchived(t, c, primary, writeAndSwitch(t, db, "before"), 2*time.Minute)

	before := sidecarStatus(t, primary)
	require.True(t, before.Configured)
	require.False(t, before.S3TlsVerifyDisabled)
	var failedBefore int64
	require.NoError(t, db.QueryRow("SELECT failed_count FROM pg_stat_archiver").Scan(&failedBefore))

	sidecar := primary.PgBackRestd
	matching := sidecar.Config.Location
	other := proto.Clone(matching).(*clustermetadatapb.BackupLocation)
	other.GetS3().CaFile = ""
	other.GetS3().SkipTlsVerify = true

	require.NoError(t, sidecar.Stop(30*time.Second))
	sidecar.Config.Location = other
	require.NoError(t, sidecar.Start(c.setup.Context(), t))

	restarted := sidecarStatus(t, primary)
	require.True(t, restarted.S3TlsVerifyDisabled, "the sidecar was restarted with verification off")
	require.NotEqual(t, before.LocationFingerprint, restarted.LocationFingerprint)
	require.False(t, restarted.Configured, "a sidecar must not load the configuration saved under other TLS settings")

	// A Configure that names no fingerprint is what a multipooler build that
	// compares nothing would send to a sidecar it finds unconfigured: refused.
	unbound, unboundCancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer unboundCancel()
	_, err := controlClient(primary).Configure(unbound, connect.NewRequest(&pgbackrestdservice.ConfigureRequest{
		Revision: before.Revision,
		Repos:    before.Repos,
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	require.False(t, sidecarStatus(t, primary).Configured)

	// What multipooler would send, sent directly: refused, and nothing applied.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = controlClient(primary).Configure(ctx, connect.NewRequest(&pgbackrestdservice.ConfigureRequest{
		Revision:            before.Revision,
		Repos:               before.Repos,
		LocationFingerprint: before.LocationFingerprint,
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	seg := writeAndSwitch(t, db, "verify-off")
	require.Eventually(t, func() bool {
		var failed int64
		return db.QueryRow("SELECT failed_count FROM pg_stat_archiver").Scan(&failed) == nil && failed > failedBefore
	}, 3*time.Minute, time.Second, "archive_command should fail while the sidecar is not configured")
	// Several of multipooler's sync intervals: it keeps finding the sidecar
	// and keeps not configuring it.
	testpoll.Never(t, func() bool {
		return sidecarStatus(t, primary).Configured || repoSegments(c.s3)[seg]
	}, 35*time.Second, time.Second, "multipooler configured a sidecar whose TLS settings differ from the topology's, or %s was archived through it", seg)
	require.FileExists(t, filepath.Join(primary.Pgctld.PoolerDir, "pg_data", "pg_wal", "archive_status", seg+".ready"),
		"%s must stay queued", seg)

	require.NoError(t, sidecar.Stop(30*time.Second))
	sidecar.Config.Location = matching
	require.NoError(t, sidecar.Start(c.setup.Context(), t))
	waitArchived(t, c, primary, seg, 3*time.Minute)
	after := sidecarStatus(t, primary)
	assert.True(t, after.Configured)
	assert.False(t, after.S3TlsVerifyDisabled)
	assert.Equal(t, before.LocationFingerprint, after.LocationFingerprint)
}

// TestSidecar_LocationChangeReachesARunningPooler changes a TLS setting of the
// backup location the way a rollout does, with multipooler and postgres
// running throughout: the leader's sidecar is restarted with another CA file
// path, and the database record in the topology is changed to name that path.
// Until the record names it, multipooler does not configure the sidecar: what
// a sidecar reports about itself is not what authorizes it. Once the record
// does, multipooler reads it again on its next sync pass and configures the
// sidecar, without being restarted, and the WAL that queued meanwhile is
// archived.
//
// It fails if multipooler keeps the location it read at startup (the sidecar
// would stay unconfigured), or if it configures the sidecar before the record
// names its settings.
func TestSidecar_LocationChangeReachesARunningPooler(t *testing.T) {
	c := newDirectCluster(t, 2, nil)
	primary := c.setup.GetPrimary(t)
	db := openSocketDB(t, primary)
	waitArchived(t, c, primary, writeAndSwitch(t, db, "before"), 2*time.Minute)
	before := sidecarStatus(t, primary)
	require.True(t, before.Configured)

	sidecar := primary.PgBackRestd
	old := sidecar.Config.Location
	ca, err := os.ReadFile(old.GetS3().GetCaFile())
	require.NoError(t, err)
	movedCA := filepath.Join(c.setup.TempDir, "s3-ca-moved.pem")
	require.NoError(t, os.WriteFile(movedCA, ca, 0o644))
	moved := proto.Clone(old).(*clustermetadatapb.BackupLocation)
	moved.GetS3().CaFile = movedCA

	require.NoError(t, sidecar.Stop(30*time.Second))
	sidecar.Config.Location = moved
	require.NoError(t, sidecar.Start(c.setup.Context(), t))
	restarted := sidecarStatus(t, primary)
	require.NotEqual(t, before.LocationFingerprint, restarted.LocationFingerprint)
	require.False(t, restarted.Configured)
	require.Equal(t, before.Revision, restarted.Revision, "the revision of the state it set aside stays as the floor")
	require.NotEmpty(t, restarted.SetAsideRepos, "the list of the state it set aside is offered")

	seg := writeAndSwitch(t, db, "moved-ca")
	testpoll.Never(t, func() bool {
		return sidecarStatus(t, primary).Configured || repoSegments(c.s3)[seg]
	}, 25*time.Second, time.Second, "multipooler configured a sidecar whose settings the database record does not name")

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	require.NoError(t, c.setup.TopoServer.UpdateDatabaseFields(ctx, "postgres", func(record *clustermetadatapb.Database) error {
		record.BackupLocation = proto.Clone(moved).(*clustermetadatapb.BackupLocation)
		return nil
	}))

	require.Eventually(t, func() bool { return sidecarStatus(t, primary).Configured },
		time.Minute, time.Second, "multipooler should adopt the record's new location and configure the sidecar")
	waitArchived(t, c, primary, seg, 3*time.Minute)
	after := sidecarStatus(t, primary)
	assert.Equal(t, restarted.LocationFingerprint, after.LocationFingerprint)
	assert.GreaterOrEqual(t, after.Revision, before.Revision)
	assert.Empty(t, after.SetAsideRepos)
}
