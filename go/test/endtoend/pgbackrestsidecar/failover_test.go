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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// failoverCluster boots a three-pooler sidecar cluster with multiorch.
func failoverCluster(t *testing.T) *sidecarCluster {
	t.Helper()
	return newSidecarCluster(t, 3,
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
}

// failoverAndRejoin shuts the primary down gracefully, lets multiorch elect a
// new one, writes on it so the timelines diverge, and lets the old primary come
// back: multiorch demotes it and it runs pg_rewind against the new primary,
// which copies the new primary's postgresql.auto.conf over its own. It returns
// the old and the new primary.
func failoverAndRejoin(t *testing.T, c *sidecarCluster) (oldP, newP *shardsetup.MultipoolerInstance) {
	t.Helper()
	setup := c.setup
	setup.StartMultiorchs(t.Context(), t)

	oldP = setup.GetPrimary(t)
	oldName := setup.PrimaryName
	resume := setup.ShutdownPostgres(t, oldName)
	defer resume()

	newName := shardsetup.WaitForNewPrimary(t, setup, oldName, time.Minute)
	require.NotEmpty(t, newName)
	newP = setup.GetMultipoolerInstance(newName)
	db := openSocketDB(t, newP)
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS timeline_test (id SERIAL PRIMARY KEY, data TEXT)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO timeline_test (data) VALUES ('new_primary_data')")
	require.NoError(t, err)

	resume()
	setup.RequireRecovery(t, "multiorch", shardsetup.RecoveryScenarioStalePrimaryDemote)
	shardsetup.RequirePoolerCondition(t, []*shardsetup.MultipoolerInstance{oldP},
		func(r shardsetup.PoolerStatusResult) (bool, string) {
			if r.Status.PoolerType != clustermetadatapb.PoolerType_REPLICA {
				return false, fmt.Sprintf("type=%v, waiting for REPLICA", r.Status.PoolerType)
			}
			if r.Status.ReplicationStatus == nil || r.Status.ReplicationStatus.PrimaryConnInfo == nil {
				return false, "replication not yet configured"
			}
			return true, ""
		},
		"the old primary should rejoin as a replica after pg_rewind",
	)
	return oldP, newP
}

// archiveCommandLine returns the archive_command line of postgresql.auto.conf.
func archiveCommandLine(conf string) string {
	for line := range strings.SplitSeq(conf, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "archive_command") {
			return line
		}
	}
	return ""
}

// TestSidecar_RewindForeignSocketConverges rejoins a primary through pg_rewind.
// pg_rewind copies the new primary's postgresql.auto.conf, whose archive_command
// names the new primary's WAL socket (every local pooler has its own). After
// the rewind the rejoined pooler must archive through its own socket.
//
// It fails if normalisation is skipped after pg_rewind (the copied command
// still names the other pooler's socket, so this pooler would push its WAL
// through someone else's sidecar, or fail once that one's sandbox refuses the
// file) or if it only fixes the in-process path form.
func TestSidecar_RewindForeignSocketConverges(t *testing.T) {
	c := failoverCluster(t)
	oldP, newP := failoverAndRejoin(t, c)

	conf := readAutoConf(t, oldP.Pgctld.PoolerDir)
	line := archiveCommandLine(conf)
	require.NotEmpty(t, line, "the rejoined pooler should have an archive_command")
	assert.Contains(t, line, "--unix-socket "+oldP.PgBackRestd.Paths.WALSocket+" ", "it must name its own socket")
	assert.NotContains(t, conf, newP.PgBackRestd.Paths.WALSocket, "it must not name the pooler it was rewound from")
	if line := restoreCommandLineOf(conf); line != "" {
		assert.NotContains(t, line, newP.PgBackRestd.Paths.WALSocket)
	}
}

func restoreCommandLineOf(conf string) string {
	for line := range strings.SplitSeq(conf, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "restore_command") {
			return line
		}
	}
	return ""
}

// TestSidecar_DemoteRestoreCommandStaysRemoved demotes a primary. A demoted
// pooler streams only from the leader, so restore_command must be absent after
// the demote and stay absent across postgres restarts, whose pre-start
// normalisation rewrites commands that exist and must never add one.
//
// It fails if normalisation (or the writers of these commands) re-add restore_command
// before a start: the demoted pooler would then replay from the archive and
// could diverge from the leader's timeline.
func TestSidecar_DemoteRestoreCommandStaysRemoved(t *testing.T) {
	c := failoverCluster(t)
	oldP, _ := failoverAndRejoin(t, c)

	requireNoRestoreCommand := func(when string) {
		t.Helper()
		assert.Empty(t, restoreCommandLineOf(readAutoConf(t, oldP.Pgctld.PoolerDir)), "restore_command in auto.conf %s", when)
		var got string
		require.NoError(t, openSocketDB(t, oldP).QueryRow("SHOW restore_command").Scan(&got))
		assert.Empty(t, got, "restore_command %s", when)
	}
	requireNoRestoreCommand("after the demote")

	// Two restarts: each one runs the monitor's pre-start normalisation.
	for i := range 2 {
		resume := c.setup.StopPostgres(t, oldP.Name, "fast")
		resume()
		require.Eventually(t, func() bool {
			db := openSocketDB(t, oldP)
			var in bool
			return db.QueryRow("SELECT pg_is_in_recovery()").Scan(&in) == nil && in
		}, 2*time.Minute, time.Second, "postgres should restart as a standby")
		requireNoRestoreCommand(fmt.Sprintf("after restart %d", i+1))
	}
}
