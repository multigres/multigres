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
	"os/exec"
	"path"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/etcdtopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// TopologyRoot is the root under which test topologies live: the global
// topology at TopologyRoot/global and each cell at TopologyRoot/<cell>.
const TopologyRoot = "/multigres"

// GlobalTopologyRoot is the global topology root that test processes are given.
var GlobalTopologyRoot = path.Join(TopologyRoot, "global")

// CreateTopologyCell opens the global topology on the given etcd and creates a
// cell in it.
func CreateTopologyCell(t *testing.T, etcdClientAddr, cell string) topoclient.Store {
	t.Helper()
	ts, err := topoclient.OpenServer(topoclient.DefaultTopoImplementation, GlobalTopologyRoot, []string{etcdClientAddr}, topoclient.NewDefaultTopoConfig())
	if err != nil {
		t.Fatalf("failed to open topology server: %v", err)
	}
	err = ts.CreateCell(context.Background(), cell, &clustermetadatapb.Cell{
		ServerAddresses: []string{etcdClientAddr},
		Root:            path.Join(TopologyRoot, cell),
	})
	if err != nil {
		t.Fatalf("failed to create cell: %v", err)
	}
	t.Logf("Created topology cell '%s' at etcd %s", cell, etcdClientAddr)
	return ts
}

// CreateDatabaseRecord creates the database record that poolers read at
// startup: its backup location and bootstrap durability policy.
func CreateDatabaseRecord(ts topoclient.Store, database string, backupLocation *clustermetadatapb.BackupLocation, durabilityPolicy string) error {
	bootstrapPolicy, err := consensus.ParseUserSpecifiedDurabilityPolicy(durabilityPolicy)
	if err != nil {
		return fmt.Errorf("invalid durability policy %q: %w", durabilityPolicy, err)
	}
	err = ts.CreateDatabase(context.Background(), database, &clustermetadatapb.Database{
		Name:                      database,
		BackupLocation:            backupLocation,
		BootstrapDurabilityPolicy: bootstrapPolicy,
	})
	if err != nil {
		return fmt.Errorf("failed to create database in topology: %w", err)
	}
	return nil
}

// StartEtcd starts etcd without registering t.Cleanup() handlers
// since cleanup is handled manually by TestMain via Cleanup().
// Follows the pattern from multipooler/setup_test.go:startEtcdForSharedSetup.
func StartEtcd(ctx context.Context, t *testing.T, dataDir string) (string, *executil.Cmd, error) {
	t.Helper()

	ctx, span := telemetry.Tracer().Start(ctx, "clustersetup/StartEtcd")
	defer span.End()

	// Check if etcd is available in PATH
	_, err := exec.LookPath("etcd")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "etcd not found in PATH")
		return "", nil, fmt.Errorf("etcd not found in PATH: %w", err)
	}

	// Get ports for etcd (client, peer, and metrics)
	clientPort := utils.GetFreePort(t)
	peerPort := utils.GetFreePort(t)
	metricsPort := utils.GetFreePort(t)

	span.SetAttributes(
		attribute.Int("etcd.client_port", clientPort),
		attribute.Int("etcd.peer_port", peerPort),
		attribute.Int("etcd.metrics_port", metricsPort),
	)

	name := "shardsetup_test"
	clientAddr := fmt.Sprintf("http://localhost:%v", clientPort)
	peerAddr := fmt.Sprintf("http://localhost:%v", peerPort)
	metricsAddr := fmt.Sprintf("http://localhost:%v", metricsPort)
	initialCluster := fmt.Sprintf("%v=%v", name, peerAddr)

	// Wrap etcd with run_in_test.sh for orphan protection. Stops gracefully when
	// runningCtx is cancelled so run_in_test.sh can terminate etcd cleanly.
	cmd := utils.CommandWithOrphanProtection(ctx, "etcd",
		"-name", name,
		"-advertise-client-urls", clientAddr,
		"-initial-advertise-peer-urls", peerAddr,
		"-listen-client-urls", clientAddr,
		"-listen-peer-urls", peerAddr,
		"-listen-metrics-urls", metricsAddr,
		"-initial-cluster", initialCluster,
		"-data-dir", dataDir)

	// Set MULTIGRES_TESTDATA_DIR for directory-deletion triggered cleanup
	cmd.AddEnv("MULTIGRES_TESTDATA_DIR=" + dataDir)

	if err := cmd.Start(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to start etcd")
		return "", nil, fmt.Errorf("failed to start etcd: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := etcdtopo.WaitForReady(waitCtx, metricsAddr); err != nil {
		// Stop the etcd process if it's not ready
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, _ = cmd.Stop(stopCtx)
		stopCancel()
		span.RecordError(err)
		span.SetStatus(codes.Error, "etcd not ready")
		return "", nil, err
	}

	return clientAddr, cmd, nil
}
