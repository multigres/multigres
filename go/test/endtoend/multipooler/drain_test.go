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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// TestObservedInvoluntaryDrain_SelfReportsDRAINING proves the pooler's heartbeat
// loop re-publishes ServingSignal.DRAINING after observing its own MultiPooler
// record transition to PoolerType.DRAINED in topology. This is the bridge that
// makes voluntary and involuntary drains look identical to the gateway.
func TestObservedInvoluntaryDrain_SelfReportsDRAINING(t *testing.T) {
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
	)
	t.Cleanup(cleanup)

	// Pick a replica as the subject — primary stays up so bootstrap/durability
	// is satisfied. The draining pooler just needs a running heartbeat loop.
	replicas := setup.GetStandbys()
	require.NotEmpty(t, replicas)
	inst := replicas[0]

	client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	// Act: flip PoolerType to DRAINED in topology directly.
	poolerID := setup.GetMultipoolerID(inst.Name)
	require.NotNil(t, poolerID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = setup.TopoServer.UpdateMultiPoolerFields(ctx, poolerID, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_DRAINED
		return nil
	})
	cancel()
	require.NoError(t, err)

	// Assert: within one heartbeat interval (30s) + slack, pooler self-reports DRAINING.
	require.Eventually(t, func() bool {
		sCtx, sCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer sCancel()
		s, err := client.Manager.Status(sCtx, &multipoolermanagerdatapb.StatusRequest{})
		if err != nil || s.Status == nil || s.Status.QueryServingStatus == nil {
			return false
		}
		return s.Status.QueryServingStatus.Signal == clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING
	}, 45*time.Second, 2*time.Second, "pooler should self-report DRAINING within one heartbeat (30s) + slack")
}

// TestPermanentDrain_WritesTombstone proves the Drain-RPC → permanence-sticky →
// Shutdown → writeDrainTombstoneIfNeeded chain. After a permanent Drain RPC
// and a graceful SIGTERM, the pooler's MultiPooler topology record must have
// RemovedAt set and RemoveReason = SCALE_DOWN.
func TestPermanentDrain_WritesTombstone(t *testing.T) {
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
	)
	t.Cleanup(cleanup)

	replicas := setup.GetStandbys()
	require.NotEmpty(t, replicas)
	replicaA := replicas[0]
	replicaID := setup.GetMultipoolerID(replicaA.Name)
	require.NotNil(t, replicaID)

	client, err := shardsetup.NewMultipoolerClient(replicaA.Multipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	// Act: permanent Drain RPC, then graceful shutdown.
	dCtx, dCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = client.Manager.Drain(dCtx, &multipoolermanagerdatapb.DrainRequest{
		RemoveFromTopology: true,
		RemoveReason:       clustermetadatapb.RemoveReason_REMOVE_REASON_SCALE_DOWN,
	})
	dCancel()
	require.NoError(t, err)

	replicaA.Multipooler.TerminateGracefully(t.Logf, 15*time.Second)

	// Assert: tombstone fields set on the topology record.
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		mp, err := setup.TopoServer.GetMultiPooler(ctx, replicaID)
		if err != nil || mp == nil || mp.MultiPooler == nil {
			return false
		}
		return mp.MultiPooler.RemovedAt != nil &&
			mp.MultiPooler.RemoveReason == clustermetadatapb.RemoveReason_REMOVE_REASON_SCALE_DOWN
	}, 10*time.Second, 250*time.Millisecond, "tombstone fields should be set after permanent drain shutdown")
}
