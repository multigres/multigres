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

package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// latentTopo is an in-memory topology whose RegisterMultipooler takes a moment between reading the
// record and writing it back, as a topology server reached over the network does. It is
// topoclient's RegisterMultipooler otherwise.
type latentTopo struct {
	topoclient.Store
	latency time.Duration
}

func (l *latentTopo) RegisterMultipooler(ctx context.Context, mp *clustermetadatapb.Multipooler, allowUpdate bool) error {
	err := l.CreateMultipooler(ctx, mp)
	if !errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}) || !allowUpdate {
		return err
	}
	old, err := l.GetMultipooler(ctx, mp.Id)
	if err != nil {
		return err
	}
	time.Sleep(l.latency)
	old.Multipooler = mp
	return l.UpdateMultipooler(ctx, old)
}

// TestPoolerRecord_RegistrationDoesNotRaceThePublisher: a pooler that restarts finds its previous
// record in the topology, so its initial registration updates it, and a state change that lands
// as it registers makes the publisher update it too. Both read the record and write it back at
// the version they read, so one of them loses with a version conflict. The registration retries
// at once, but a publisher that lost has to wait for its next retry, and the record stays as the
// registration wrote it, which may be older than the state the pooler is in.
func TestPoolerRecord_RegistrationDoesNotRaceThePublisher(t *testing.T) {
	const rounds = 20
	stale := 0
	for i := range rounds {
		mem, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
		ts := &latentTopo{Store: mem, latency: 2 * time.Millisecond}
		id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: fmt.Sprintf("restarted-%d", i)}
		previous := &clustermetadatapb.Multipooler{
			Id: id, Hostname: "localhost", PortMap: map[string]int32{"grpc": 1},
			ShardRef:      &clustermetadatapb.ShardRef{Database: "db", TableGroup: "tg", Shard: "0-inf"},
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		}
		require.NoError(t, ts.CreateMultipooler(t.Context(), previous))

		starting := previous
		starting.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
		r, err := newPoolerRecord(slog.New(slog.DiscardHandler), ts, starting)
		require.NoError(t, err)
		lockedCtx := newActionLockedCtx(t)

		// The pooler starts DISABLED and is SERVING a moment into its registration: the
		// registration has read its state, and the publisher has a change to write.
		go func() {
			time.Sleep(500 * time.Microsecond)
			_ = r.Mutate(lockedCtx, func(s *MutablePoolerRecordState) {
				s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
			})
		}()
		r.Register(t.Context(), func(string) {})

		// The topology must come to hold what the pooler is: long before the publisher's next
		// scheduled retry.
		deadline := time.Now().Add(300 * time.Millisecond)
		current := false
		for time.Now().Before(deadline) {
			info, err := ts.GetMultipooler(t.Context(), id)
			if err == nil && info.GetServingStatus() == clustermetadatapb.PoolerServingStatus_SERVING {
				current = true
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !current {
			stale++
		}
		r.Unregister(t.Context(), nil)
		mem.Close()
	}
	require.Zero(t, stale, "the topology record was left stale in %d of %d registrations", stale, rounds)
}
