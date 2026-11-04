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

package local

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/executil"
)

func TestGlobalDiscoveryHonorsShutdownMarkers(t *testing.T) {
	child := executil.Command(context.Background(), "/bin/sleep", "60")
	require.NoError(t, child.Start())
	go func() { _ = child.Wait() }()
	t.Cleanup(func() { _, _ = child.Stop(context.Background()) })
	for _, serviceName := range []string{"etcd", "multiadmin"} {
		for _, completed := range []bool{true, false} {
			t.Run(serviceName+map[bool]string{true: "-completed", false: "-uncertain"}[completed], func(t *testing.T) {
				p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
				service := &LocalProvisionedService{ID: "saved", Service: serviceName, PID: child.Process.Pid, Metadata: map[string]any{"shutdown_attempted": true, "shutdown_complete": completed}}
				require.NoError(t, p.saveServiceState(service, ""))
				var got *LocalProvisionedService
				var err error
				if serviceName == "etcd" {
					got, err = p.findRunningEtcdService()
				} else {
					got, err = p.findRunningService(serviceName)
				}
				require.Nil(t, got)
				if completed {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "shutdown outcome is unknown")
				}
			})
		}
	}
}

func TestDatabaseDiscoveryScopesShutdownMarkersToTargetCell(t *testing.T) {
	child := executil.Command(context.Background(), "/bin/sleep", "60")
	require.NoError(t, child.Start())
	go func() { _ = child.Wait() }()
	t.Cleanup(func() { _, _ = child.Stop(context.Background()) })
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
	require.NoError(t, p.saveServiceState(&LocalProvisionedService{ID: "a-uncertain", Service: "pgctld", PID: child.Process.Pid, Metadata: map[string]any{"cell": "A", "shutdown_attempted": true}}, "db"))
	require.NoError(t, p.saveServiceState(&LocalProvisionedService{ID: "b-running", Service: "pgctld", PID: child.Process.Pid, Metadata: map[string]any{"cell": "B"}}, "db"))
	got, err := p.findRunningDbService("pgctld", "db", "B")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "b-running", got.ID)
	got, err = p.findRunningDbService("pgctld", "db", "A")
	require.Nil(t, got)
	require.ErrorContains(t, err, "shutdown outcome is unknown")
}
