// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multipooler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestManagerHealthStream_SnapshotOnSetPrimaryConnInfo verifies that a snapshot
// is pushed to an open ManagerHealthStream promptly after SetPrimaryConnInfo is
// called. SetPrimaryConnInfo calls broadcastHealth() at the end of its work, so
// the stream client should not need to wait for the periodic poll ticker.
func TestManagerHealthStream_SnapshotOnSetPrimaryConnInfo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup, WithoutReplication())

	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Connect to the standby's manager service.
	standbyClient, err := shardsetup.NewMultipoolerClient(setup.StandbyMultipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { standbyClient.Close() })

	// Open a ManagerHealthStream. Cancel it at test teardown.
	streamCtx, cancelStream := context.WithCancel(context.Background())
	t.Cleanup(cancelStream)

	stream, err := standbyClient.Manager.ManagerHealthStream(streamCtx)
	require.NoError(t, err)

	// Send the start message to open the stream.
	require.NoError(t, stream.Send(&multipoolermanagerdatapb.ManagerHealthStreamClientMessage{
		Message: &multipoolermanagerdatapb.ManagerHealthStreamClientMessage_Start{
			Start: &multipoolermanagerdatapb.ManagerHealthStreamStartRequest{},
		},
	}))

	// The first server message is a start response confirming the timing values.
	startResp, err := stream.Recv()
	require.NoError(t, err, "expected start response on stream open")
	require.NotNil(t, startResp.GetStart(), "first message should be a start response")

	// The second message is the initial health snapshot.
	initial, err := stream.Recv()
	require.NoError(t, err, "expected initial snapshot after start response")
	require.Equal(t, multipoolermanagerdatapb.SnapshotTrigger_SNAPSHOT_TRIGGER_INITIAL,
		initial.GetSnapshot().GetTrigger(), "second message should be an initial snapshot")

	// Call SetPrimaryConnInfo on the standby. This configures primary_conninfo
	// (without starting replication) and calls broadcastHealth() at the end,
	// which should cause the stream to send a broadcast-triggered snapshot.
	primary := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      setup.CellName,
			Name:      setup.PrimaryMultipooler.Name,
		},
		Hostname: "localhost",
		PortMap:  map[string]int32{"postgres": int32(setup.PrimaryPgctld.PgPort)},
	}
	_, err = standbyClient.Consensus.SetPrimaryConnInfo(t.Context(), &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
		Primary:               primary,
		StopReplicationBefore: false,
		StartReplicationAfter: false,
		Force:                 true,
	})
	require.NoError(t, err, "SetPrimaryConnInfo should succeed on standby")

	// Receive snapshots until we see one with SNAPSHOT_TRIGGER_BROADCAST.
	// We skip over any heartbeat snapshots that may arrive concurrently.
	// We use the test context so there is no arbitrary deadline shorter than the
	// overall test timeout.
	for {
		snapCh := make(chan *multipoolermanagerdatapb.ManagerHealthStreamResponse, 1)
		go func() {
			msg, err := stream.Recv()
			if err == nil {
				snapCh <- msg
			}
		}()

		select {
		case snap := <-snapCh:
			require.NotNil(t, snap.GetSnapshot(), "received message should contain a snapshot")
			if snap.GetSnapshot().GetTrigger() == multipoolermanagerdatapb.SnapshotTrigger_SNAPSHOT_TRIGGER_BROADCAST {
				return // test passed
			}
			// Not a broadcast snapshot (e.g. a concurrent heartbeat); keep waiting.
		case <-t.Context().Done():
			t.Fatal("no broadcast snapshot received before test timeout — SetPrimaryConnInfo may not be triggering a broadcast")
		}
	}
}
