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

package etcdtopo

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

// TestRecoveries_ReportsLostAndRegainedConnection stops etcd and brings it
// back on the same address, once with its data and once with an empty data
// directory (a topology rebuilt from scratch). Both must produce a recovery.
func TestRecoveries_ReportsLostAndRegainedConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration tests in short mode")
	}

	for _, tc := range []struct {
		name     string
		wipeData bool
	}{
		{name: "restart with data", wipeData: false},
		{name: "restart with empty data", wipeData: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			clientPort := utils.GetFreePort(t)
			peerPort := utils.GetFreePort(t)
			addr, _, etcdServer := StartEtcdWithOptions(t, EtcdOptions{
				ClientPort: clientPort,
				PeerPort:   peerPort,
				DataDir:    dataDir,
			})

			topo, err := NewServerWithOpts([]string{addr}, "/test", "", "", "")
			require.NoError(t, err)
			defer topo.Close()

			recoveries := topo.Recoveries()
			require.True(t, recoveries == topo.Recoveries(), "every call must return the same channel")

			select {
			case <-recoveries:
				t.Fatal("recovery reported before any outage")
			case <-time.After(200 * time.Millisecond):
			}

			require.NoError(t, etcdServer.Process.Signal(os.Interrupt))
			_ = etcdServer.Wait()
			require.Eventually(t, func() bool {
				return checkPortAvailable(clientPort) == nil && checkPortAvailable(peerPort) == nil
			}, 5*time.Second, 50*time.Millisecond, "ports should be released after etcd shutdown")

			restartDir := dataDir
			if tc.wipeData {
				restartDir = t.TempDir()
			}
			StartEtcdWithOptions(t, EtcdOptions{
				ClientPort: clientPort,
				PeerPort:   peerPort,
				DataDir:    restartDir,
			})

			select {
			case <-recoveries:
			case <-time.After(30 * time.Second):
				t.Fatal("no recovery reported after etcd came back")
			}
		})
	}
}

func TestRecoveries_ChannelClosesWithConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration tests in short mode")
	}

	addr, _, _ := StartEtcd(t)
	topo, err := NewServerWithOpts([]string{addr}, "/test", "", "", "")
	require.NoError(t, err)

	recoveries := topo.Recoveries()
	require.NoError(t, topo.Close())

	select {
	case _, ok := <-recoveries:
		require.False(t, ok, "closed connection must not report a recovery")
	case <-time.After(5 * time.Second):
		t.Fatal("recovery channel not closed after Close")
	}
}
