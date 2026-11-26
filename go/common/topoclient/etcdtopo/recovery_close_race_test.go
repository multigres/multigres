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
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
)

// Close sets cli to nil, so Recoveries must not depend on it.
func TestRecoveries_AfterClose(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration tests in short mode")
	}
	addr, _, _ := StartEtcd(t)

	topo, err := NewServerWithOpts([]string{addr}, "/test", "", "", "")
	require.NoError(t, err)
	require.NoError(t, topo.Close())

	var recoveries <-chan struct{}
	require.NotPanics(t, func() { recoveries = topo.Recoveries() })
	select {
	case _, ok := <-recoveries:
		require.False(t, ok, "recovery reported on a closed connection")
	case <-time.After(5 * time.Second):
		t.Fatal("Recoveries channel not closed after Close")
	}
}

// WrapperConn calls Recoveries from a goroutine, so a caller that closes the
// connection right after opening it, like a per-reconcile store in an
// operator, can get there first.
func TestWrapperConn_CloseBeforeForwardRecoveries(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration tests in short mode")
	}
	addr, _, _ := StartEtcd(t)

	// keep the scheduler busy so the forwardRecoveries goroutine starts late,
	// like an operator reconciling many clusters at once
	stop := make(chan struct{})
	defer close(stop)
	for range 4 * runtime.GOMAXPROCS(0) {
		go func() {
			for {
				select {
				case <-stop:
					return
				default: //nolint:staticcheck // SA5004: spins on purpose to keep the scheduler busy
				}
			}
		}()
	}

	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 50 {
				conn := topoclient.NewWrapperConn(func() (topoclient.Conn, error) {
					return NewServerWithOpts([]string{addr}, "/test", "", "", "")
				}, nil)
				_ = conn.Close()
			}
		})
	}
	wg.Wait()
}
