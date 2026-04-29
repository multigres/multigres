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

package multipooler

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/tools/s3mock"
)

// TestBackup_ReplicaDownDuringBackup verifies that backups are resilient to
// STANDBY postgres death (no primary failover involved). The matrix varies
// the relationship between the backup source and the kill target:
//
//   - StandbySource/KillSource:      backup from a standby; that same standby's
//     postgres dies.
//   - StandbySource/KillOtherStandby: backup from a standby; a DIFFERENT
//     standby dies.
//   - PrimarySource/KillStandby:      backup from primary; a standby dies.
//
// In all three cases the backup MUST succeed. Empirically, pgBackRest in
// standby mode reads files and runs control commands against the PRIMARY via
// the pgBackRest TLS server (pg2), not against the standby's local postgres
// (pg1). Killing a standby therefore does not affect the data path. The
// design originally predicted KillSource would fail; that prediction was
// wrong, and recording it here as a test is what proves the resilience.
//
// Sub-cases run sequentially (see TestBackup_ConcurrentMatrix for the
// rationale around shared-host parallel backup load).
func TestBackup_ReplicaDownDuringBackup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	t.Parallel()

	cases := []struct {
		name       string
		source     string // "Primary" | "Standby"
		killTarget string // "Source" | "OtherStandby" | "Standby"
	}{
		{"StandbySource_KillSource", "Standby", "Source"},
		{"StandbySource_KillOtherStandby", "Standby", "OtherStandby"},
		{"PrimarySource_KillStandby", "Primary", "Standby"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			runReplicaDownCase(t, c.source, c.killTarget)
		})
	}
}

func runReplicaDownCase(t *testing.T, source, killTarget string) {
	tlog := newBackupTestLogger(t)
	tlog.Log("matrix case Source=%s Kill=%s", source, killTarget)

	gate := s3mock.NewGate(s3mock.MatchDataUpload)
	s3Server, err := s3mock.NewServer(0, s3mock.WithGate(gate))
	require.NoError(t, err)
	defer func() { _ = s3Server.Stop() }()
	require.NoError(t, s3Server.CreateBucket("multigres"))

	// 3 multipoolers: primary + 2 standbys.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithS3Backup("multigres", "us-east-1", s3Server.Endpoint()),
	)
	defer cleanup()

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary)
	primaryName := setup.PrimaryName

	// Collect standbys in a stable order.
	var standbyNames []string
	for name := range setup.Multipoolers {
		if name != primaryName {
			standbyNames = append(standbyNames, name)
		}
	}
	require.Len(t, standbyNames, 2, "expected exactly 2 standbys")
	tlog.Log("primary=%s standbys=%v", primaryName, standbyNames)

	// Pick the backup source.
	var sourceName string
	backupClient := createBackupClient(t, primary.Multipooler.GrpcPort)
	forcePrimary := true
	overrides := map[string]string(nil)
	if source == "Standby" {
		sourceName = standbyNames[0]
		sourceInst := setup.Multipoolers[sourceName]
		backupClient = createBackupClient(t, sourceInst.Multipooler.GrpcPort)
		forcePrimary = false
		overrides = map[string]string{
			"pg2_path": filepath.Join(primary.Pgctld.PoolerDir, "pg_data"),
		}
	} else {
		sourceName = primaryName
	}

	// Pick the kill target based on the test case. The mapping depends on the
	// source: "Source" means kill the same node we back up from; "OtherStandby"
	// means kill the standby that's NOT the source; "Standby" means kill any
	// standby (used when source is primary).
	var killName string
	switch killTarget {
	case "Source":
		killName = sourceName
	case "OtherStandby":
		// Both source and kill target are standbys — pick the one that isn't
		// the source.
		for _, n := range standbyNames {
			if n != sourceName {
				killName = n
				break
			}
		}
	case "Standby":
		killName = standbyNames[0]
	default:
		t.Fatalf("unknown killTarget %q", killTarget)
	}
	require.NotEmpty(t, killName, "kill target name not resolved")
	tlog.Log("source=%s kill=%s", sourceName, killName)

	// Disable postgres restarts on the kill target so the multipooler doesn't
	// auto-restart its postgres before we can observe the effect.
	mc := createBackupClient(t, setup.Multipoolers[killName].Multipooler.GrpcPort)
	_, err = mc.SetPostgresRestartsEnabled(t.Context(),
		&multipoolermanagerdata.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err, "failed to disable postgres restarts on %s", killName)

	// Wait for the multipooler's automatic post-init backup to finish before
	// arming the gate. Otherwise our test backup races for the pgBackRest
	// stanza lock and fails fast with exit 56.
	waitForBootstrapBackup(t, backupClient)

	gate.Arm()
	jobID := fmt.Sprintf("replica-down-%s-%s", source, killTarget)

	backupErrCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		_, err := backupClient.Backup(ctx, &multipoolermanagerdata.BackupRequest{
			Type:         "full",
			JobId:        jobID,
			ForcePrimary: forcePrimary,
			Overrides:    overrides,
		})
		backupErrCh <- err
	}()

	hit, err := waitForGateOrEarlyError(t, gate, backupErrCh, 2*time.Minute)
	require.NoError(t, err, "timed out waiting for first backup data PUT")
	tlog.Log("paused at %s", hit.Key)

	setup.KillPostgres(t, killName)
	tlog.Log("killed postgres on %s", killName)
	gate.Release()

	backupErr := waitChanWithTimeout(t, backupErrCh, 2*time.Minute, "backup did not return")
	tlog.Log("backup returned: %v", backupErr)

	require.NoError(t, backupErr, "backup must succeed when a standby dies (Source=%s Kill=%s) — pgBackRest reads from primary via TLS", source, killTarget)
	assertJobComplete(t, backupClient, jobID)

	// Cluster sanity: primary stays primary (no failover triggered by killing
	// a standby) and is still serving.
	primaryAfter := setup.GetPrimary(t)
	require.NotNil(t, primaryAfter, "primary should still be elected after standby kill")
	assert.Equal(t, primaryName, setup.PrimaryName, "primary should not change when only a standby dies")
}
