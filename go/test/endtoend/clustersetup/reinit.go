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

package clustersetup

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/multigres/multigres/go/common/topoclient"
)

// Helpers shared by the ReinitializeCluster implementations of the Multigres
// and Minigres harnesses, which tear a running topology down and start it
// again on a fresh PostgreSQL data directory.

// ClearDirContents removes everything inside dir but keeps dir itself. Failures
// are logged, not fatal: the restart that follows reports anything that matters.
func ClearDirContents(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Logf("ReinitializeCluster: warning: failed to read %s: %v", dir, err)
		return
	}
	for _, entry := range entries {
		entryPath := filepath.Join(dir, entry.Name())
		if err := os.RemoveAll(entryPath); err != nil {
			t.Logf("ReinitializeCluster: warning: failed to remove %s: %v", entryPath, err)
		}
	}
	t.Logf("ReinitializeCluster: cleared %s", dir)
}

// WaitForPortFree blocks until no process is listening on localhost:port, or
// the timeout elapses. It is used between stopping postgres and restarting it
// on the same port during ReinitializeCluster, so the new postmaster can bind.
//
// A successful net.Listen means the port is free (an active listener on the
// same port — e.g. a not-yet-exited postmaster — makes Listen fail even with
// SO_REUSEADDR). On timeout it logs and returns rather than failing: the
// subsequent start will surface a clearer error if the port is truly stuck.
func WaitForPortFree(t *testing.T, port int, timeout time.Duration) {
	t.Helper()
	addr := fmt.Sprintf(":%d", port)
	deadline := time.Now().Add(timeout)
	for {
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			_ = ln.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Logf("ReinitializeCluster: warning: port %d still in use after %s (%v); proceeding anyway", port, timeout, err)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// WipeTopologyForReinit removes the etcd keys that would otherwise carry stale
// shard-election and per-process registration state across a
// ReinitializeCluster call: a pooler that was PRIMARY in the previous suite
// would read its stale role on restart, find an empty data directory, and hang
// in recovery.
//
// It wipes:
//   - databases/<db>/<tablegroup>/*   (shard records + ShardInitClaim)
//   - <cell>/poolers|gateways|orchs/* (per-process registration state)
//
// It keeps:
//   - databases/<db>/Database         (preserves BackupLocation + DurabilityPolicy)
//   - cells/<cell>/Cell               (cell config the poolers need to reconnect)
//
// It connects to etcd directly (rather than via a topology store) because the
// public topoclient API only exposes per-record delete helpers and this needs a
// recursive prefix delete.
func WipeTopologyForReinit(t *testing.T, etcdAddr, cell, database, tableGroup string) {
	t.Helper()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("ReinitializeCluster: failed to open etcd client for topology wipe: %v", err)
	}
	defer func() {
		if cerr := cli.Close(); cerr != nil {
			t.Logf("ReinitializeCluster: warning: closing etcd client: %v", cerr)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefixes := []string{
		path.Join(GlobalTopologyRoot, topoclient.DatabasesPath, database, tableGroup) + "/",
		path.Join(TopologyRoot, cell, topoclient.PoolersPath) + "/",
		path.Join(TopologyRoot, cell, topoclient.GatewaysPath) + "/",
		path.Join(TopologyRoot, cell, topoclient.OrchsPath) + "/",
	}
	for _, p := range prefixes {
		if _, err := cli.Delete(ctx, p, clientv3.WithPrefix()); err != nil {
			t.Fatalf("ReinitializeCluster: failed to wipe topology prefix %s: %v", p, err)
		}
	}
	t.Logf("ReinitializeCluster: wiped stale topology state (shard records + pooler/gateway/orch registrations)")
}
