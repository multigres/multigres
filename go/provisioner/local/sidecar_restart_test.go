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
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/pgbackreststate"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

// Real subprocess and Status/Stop transport; only PostgreSQL itself is modeled.
func runRestartPgctldFixture(args []string) int {
	port, httpPort := "", ""
	for i := 0; i+1 < len(args); i++ {
		switch args[i] {
		case "--grpc-port":
			port = args[i+1]
		case "--http-port":
			httpPort = args[i+1]
		}
	}
	httpListener, err := net.Listen("tcp", "127.0.0.1:"+httpPort)
	if err != nil {
		return 2
	}
	defer httpListener.Close()
	httpServer := &http.Server{
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
		ReadHeaderTimeout: time.Second,
	}
	defer httpServer.Close()
	go func() { _ = httpServer.Serve(httpListener) }()
	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		return 2
	}
	server := grpc.NewServer()
	pb.RegisterPgCtldServer(server, &cleanupPgctld{})
	if err := server.Serve(listener); err != nil {
		return 3
	}
	return 0
}

func restartProvisioner(t *testing.T, sidecarBinary string) (*localProvisioner, PgBackRestdPaths) {
	t.Helper()
	paths := ownerTestPaths(t)
	root := filepath.Dir(paths.Dir)
	executable, err := os.Executable()
	require.NoError(t, err)
	require.NoError(t, os.Symlink(executable, filepath.Join(root, "pgctld")))
	if sidecarBinary == "" {
		sidecarBinary = executable
	}
	require.NoError(t, os.Symlink(sidecarBinary, filepath.Join(root, "pgbackrestd")))
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("MULTIGRES_PGBACKREST_SANDBOX", "off")
	pooler := filepath.Join(root, "pooler")
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: root, Cells: map[string]CellServicesConfig{
		"zone": {Pgctld: PgctldConfig{Path: filepath.Join(root, "pgctld"), PoolerDir: pooler, PgPassword: "pw", GrpcPort: utils.GetFreePort(t), HttpPort: utils.GetFreePort(t), PgBackRestPort: utils.GetFreePort(t)}},
	}}}
	_, err = GeneratePgBackRestCerts(p.certDir())
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(pooler, "pg_data"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(pooler, "pg_data", "payload"), []byte("retained database"), 0o600))
	return p, paths
}

func provisionRestartFixture(t *testing.T, p *localProvisioner, paths PgBackRestdPaths) *sidecarOwner {
	t.Helper()
	_, err := p.provisionPgctld(t.Context(), "db", "default", "pooler", "zone")
	require.NoError(t, err)
	child, ok := localSidecarChildren.LoadAndDelete(paths.ownerFile())
	require.True(t, ok, "exercise recovered self-shutdown, not local child stop")
	t.Cleanup(func() { _, _ = child.(*executil.Cmd).Stop(context.Background()) })
	// The production provisioner owns/reaps pgctld. Retain a cleanup path for an
	// assertion failure while it is still live, without ignoring stop failures.
	t.Cleanup(func() { _ = p.Teardown(context.Background(), false) })
	owner, err := paths.loadOwner()
	require.NoError(t, err)
	require.Empty(t, owner.RetiredStateDir)
	require.NotEmpty(t, owner.InstanceID)
	return owner
}

func runNonCleanRestart(t *testing.T, entrypoint, binary string) {
	p, paths := restartProvisioner(t, binary)
	first := provisionRestartFixture(t, p, paths)
	lockPath := filepath.Join(paths.StateDir, pgbackreststate.InstanceLockName)
	before, err := os.Stat(lockPath)
	require.NoError(t, err)
	if entrypoint == "teardown" {
		require.NoError(t, p.Teardown(t.Context(), false))
	} else {
		require.NoError(t, p.Deprovision(t.Context(), &provisioner.DeprovisionRequest{Service: "pgctld", ServiceID: "pgctld-pooler", DatabaseName: "db", Clean: false}))
	}
	retired, err := paths.loadOwner()
	require.NoError(t, err)
	require.Equal(t, first.InstanceID, retired.InstanceID)
	require.NotEmpty(t, retired.RetiredStateDir)
	services, err := p.loadShutdownServices()
	require.NoError(t, err)
	require.Len(t, services, 1)
	require.Equal(t, true, services[0].Metadata["shutdown_complete"])
	require.Equal(t, float64(first.PID), services[0].Metadata["pgbackrestd_pid"], "retained metadata must not become legacy")
	second := provisionRestartFixture(t, p, paths)
	require.NotEqual(t, first.InstanceID, second.InstanceID)
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(paths.Dir), "pooler", "pg_data", "payload"))
	require.NoError(t, err)
	require.Equal(t, "retained database", string(raw))
	after, err := os.Stat(lockPath)
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after))
	require.NoError(t, p.Teardown(t.Context(), false))
}

func TestSidecarNonCleanStopProvisionCycle(t *testing.T) {
	for _, entrypoint := range []string{"teardown", "direct"} {
		t.Run(entrypoint, func(t *testing.T) { runNonCleanRestart(t, entrypoint, "") })
	}
}

func TestRealSidecarNonCleanStopProvisionCycle(t *testing.T) {
	if testing.Short() {
		t.Skip("actual daemon cycle")
	}
	binary := os.Getenv("MULTIGRES_TEST_SIDECAR_BINARY")
	if binary == "" {
		t.Skip("set actual daemon binary after make build")
	}
	for _, entrypoint := range []string{"teardown", "direct"} {
		t.Run(entrypoint, func(t *testing.T) { runNonCleanRestart(t, entrypoint, binary) })
	}
}

func TestSidecarRetirementDoesNotAuthorizeUnverifiedRestart(t *testing.T) {
	for _, failure := range []string{"legacy", "uncertain-service", "pending-worker", "wrong-installation", "missing-state", "wrong-legacy-pid"} {
		t.Run(failure, func(t *testing.T) {
			p, paths := restartProvisioner(t, "")
			provisionRestartFixture(t, p, paths)
			require.NoError(t, p.Teardown(t.Context(), false))
			switch failure {
			case "legacy":
				require.NoError(t, paths.removeOwner())
			case "pending-worker":
				require.NoError(t, os.WriteFile(filepath.Join(paths.StateDir, "worker-test.pending"), []byte("unknown"), 0o600))
			case "missing-state":
				require.NoError(t, os.Rename(paths.StateDir, paths.StateDir+"-unavailable"))
			case "wrong-installation":
				owner, err := paths.loadOwner()
				require.NoError(t, err)
				owner.RetiredStateDir = filepath.Join(paths.Dir, "another-state")
				require.NoError(t, paths.saveOwner(owner))
			case "uncertain-service", "wrong-legacy-pid":
				services, err := p.loadShutdownServices()
				require.NoError(t, err)
				require.Len(t, services, 1)
				if failure == "uncertain-service" {
					services[0].Metadata["shutdown_complete"] = false
				} else {
					services[0].Metadata["pgbackrestd_pid"] = float64(1)
				}
				require.NoError(t, p.saveServiceState(services[0], "db"))
			}
			_, err := p.provisionPgctld(t.Context(), "db", "default", "pooler", "zone")
			require.Error(t, err)
			require.False(t, socketAccepts(paths.ControlSocket))
			raw, err := os.ReadFile(filepath.Join(filepath.Dir(paths.Dir), "pooler", "pg_data", "payload"))
			require.NoError(t, err)
			require.Equal(t, "retained database", string(raw))
		})
	}
}
