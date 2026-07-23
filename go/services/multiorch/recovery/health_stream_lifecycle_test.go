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

package recovery

import (
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// TestHealthStream_StreamEOFWithoutSpecialSignal_KeepsBackoff verifies that
// an EOF on an ordinary stream goes through the reconnect-with-backoff path.
// Stream closure on graceful shutdown is driven by the topology deletion
// performed by the pooler's OnClose unregisterFunc, not by anything on the
// health stream itself — so a bare EOF (without an accompanying topology
// delete) must continue to back off as today.
func TestHealthStream_StreamEOFWithoutSpecialSignal_KeepsBackoff(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 4)
	fakeClient.OnManagerHealthStream = func(_ topoclient.ComponentID, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewTestCache(t)
	sm := newTestHealthStreamFactory(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	seedPooler(t, poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

	sm.NewForTest(t, poolerStore, poolerID)
	stream := <-streamCh
	completeHandshake(t, stream)

	// Send a regular snapshot.
	stream.Ch <- makeSnapshot(&multipoolermanagerdatapb.Status{
		PoolerType:    clustermetadata.PoolerType_PRIMARY,
		PostgresReady: true,
	})

	// Close the stream. Orchestrator must reconnect.
	close(stream.Ch)

	select {
	case <-streamCh:
		// Expected: a reconnect attempt arrived.
	case <-time.After(3 * time.Second):
		t.Fatal("expected reconnect attempt after spurious EOF without lifecycle signal")
	}
}
