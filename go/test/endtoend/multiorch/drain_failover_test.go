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

package multiorch

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// TestPrimaryDrain_TriggersFailover exercises the golden operator path: an
// explicit Drain RPC on the current primary triggers the EmergencyDemote-style
// demote-to-standby, multiorch observes REQUESTING_DEMOTION, elects a new
// primary, and client writes via multigateway land on the new primary.
func TestPrimaryDrain_TriggersFailover(t *testing.T) {
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(3),
		shardsetup.WithMultigateway(),
	)
	t.Cleanup(cleanup)
	setup.StartMultiOrchs(t.Context(), t)

	oldPrimary := setup.GetPrimary(t)
	oldPrimaryName := oldPrimary.Name

	// Capture the old primary's consensus term for the resignation-signal check.
	client, err := shardsetup.NewMultipoolerClient(oldPrimary.Multipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	statusResp, err := client.Manager.Status(context.Background(), &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)
	oldTerm := statusResp.Status.ConsensusTerm.GetPrimaryTerm()
	require.NotZero(t, oldTerm, "old primary should have a non-zero primary term")

	// Act: Drain RPC on the primary.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := client.Manager.Drain(ctx, &multipoolermanagerdatapb.DrainRequest{})
	require.NoError(t, err, "Drain RPC should succeed")
	require.True(t, resp.PrimaryPathTaken, "Drain on primary should take the primary path")

	// Assert: new primary elected.
	newPrimaryName := waitForNewPrimary(t, setup, oldPrimaryName, 20*time.Second)
	require.NotEqual(t, oldPrimaryName, newPrimaryName)

	// Assert: former primary signals REQUESTING_DEMOTION for the old term.
	// AvailabilityStatus lives on the Consensus.Status response, not Manager.Status.
	require.Eventually(t, func() bool {
		statusCtx, statusCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer statusCancel()
		s, err := client.Consensus.Status(statusCtx, &consensusdatapb.StatusRequest{})
		if err != nil {
			return false
		}
		ls := s.GetAvailabilityStatus().GetLeadershipStatus()
		return ls != nil &&
			ls.Signal == clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION &&
			ls.PrimaryTerm == oldTerm
	}, 10*time.Second, 500*time.Millisecond, "former primary should advertise REQUESTING_DEMOTION for old term")

	// Assert: write via multigateway succeeds and is readable.
	dsn := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	pgxConn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer pgxConn.Close(context.Background())

	_, err = pgxConn.Exec(ctx, `CREATE TABLE IF NOT EXISTS drain_failover_test (id int primary key, note text)`)
	require.NoError(t, err, "CREATE TABLE against new primary via gateway should succeed")
	_, err = pgxConn.Exec(ctx, `INSERT INTO drain_failover_test (id, note) VALUES (1, 'after-drain')`)
	require.NoError(t, err, "INSERT via gateway should succeed")

	var note string
	err = pgxConn.QueryRow(ctx, `SELECT note FROM drain_failover_test WHERE id = 1`).Scan(&note)
	require.NoError(t, err)
	assert.Equal(t, "after-drain", note)
}

// TestPrimaryDrain_NoHealthyReplica_DoesNotSplitBrain proves the coordinator
// does not zombie-promote when the primary drains with no eligible candidate.
// The pooler demotes regardless (it does not introspect cluster state), and the
// former primary's postgres goes read-only — but multiorch must NOT promote
// anyone.
func TestPrimaryDrain_NoHealthyReplica_DoesNotSplitBrain(t *testing.T) {
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(3),
	)
	t.Cleanup(cleanup)
	setup.StartMultiOrchs(t.Context(), t)

	oldPrimary := setup.GetPrimary(t)
	replicas := setup.GetStandbys()
	require.Len(t, replicas, 2)

	// Stop both replica multipooler processes so multiorch has no eligible target.
	for _, r := range replicas {
		r.Multipooler.TerminateGracefully(t.Logf, 10*time.Second)
	}

	client, err := shardsetup.NewMultipoolerClient(oldPrimary.Multipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	// Give multiorch a few recovery cycles to mark both replicas unreachable.
	time.Sleep(5 * time.Second)

	// Act.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := client.Manager.Drain(ctx, &multipoolermanagerdatapb.DrainRequest{})
	require.NoError(t, err)
	require.True(t, resp.PrimaryPathTaken, "Drain should take primary path even in degraded cluster")

	// Assert: former primary's postgres is read-only.
	oldPrimaryDSN := shardsetup.GetTestUserDSN("localhost", oldPrimary.Pgctld.PgPort, "sslmode=disable", "connect_timeout=5")
	require.Eventually(t, func() bool {
		pgCtx, pgCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer pgCancel()
		conn, err := pgx.Connect(pgCtx, oldPrimaryDSN)
		if err != nil {
			return true // postgres may be unavailable during demote — that also proves non-writable
		}
		defer conn.Close(context.Background())
		_, err = conn.Exec(pgCtx, `CREATE TABLE degraded_drain_test (id int)`)
		return err != nil // any write error proves read-only / non-writable
	}, 15*time.Second, 500*time.Millisecond, "former primary should reject writes after Drain")

	// Assert: no pooler *other than the old primary* becomes PRIMARY. Hold 20s =
	// ≥15 recovery cycles past the 4s grace. The stopped replicas are excluded
	// from the probe because their gRPC is down (Err != nil continue). The old
	// primary is explicitly allowed because its topology record cannot be cleared
	// without a successor — the assertion is that multiorch did not promote a
	// different pooler.
	assertNoNewPrimaryElected(t, setup, oldPrimary.Name, 20*time.Second, 2*time.Second)
}

// TestPrimaryDrain_SIGTERM_TriggersFailover exercises the safety-net path:
// SIGTERM to the primary multipooler process triggers the in-process auto-Drain,
// and multiorch elects a new primary. This is the real k8s ops path when
// preStop didn't run (forced delete, node pressure, kubelet eviction).
func TestPrimaryDrain_SIGTERM_TriggersFailover(t *testing.T) {
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(3),
		shardsetup.WithMultigateway(),
	)
	t.Cleanup(cleanup)
	setup.StartMultiOrchs(t.Context(), t)

	oldPrimary := setup.GetPrimary(t)
	oldPrimaryName := oldPrimary.Name

	// Act: SIGTERM the primary. Generous grace window (30s) to allow the
	// in-process Drain on shutdown to complete the demote.
	oldPrimary.Multipooler.TerminateGracefully(t.Logf, 30*time.Second)

	// Assert: new primary elected.
	newPrimaryName := waitForNewPrimary(t, setup, oldPrimaryName, 30*time.Second)
	require.NotEqual(t, oldPrimaryName, newPrimaryName)

	// Assert: write via multigateway succeeds.
	dsn := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pgxConn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer pgxConn.Close(context.Background())

	_, err = pgxConn.Exec(ctx, `CREATE TABLE IF NOT EXISTS sigterm_drain_test (id int primary key)`)
	require.NoError(t, err)
	_, err = pgxConn.Exec(ctx, `INSERT INTO sigterm_drain_test (id) VALUES (1)`)
	require.NoError(t, err)
}
