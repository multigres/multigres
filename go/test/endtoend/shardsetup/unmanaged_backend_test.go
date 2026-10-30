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

package shardsetup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

const externalPostgresPassword = "external-test-password"

// startExternalPostgres starts a plain PostgreSQL that the cluster knows
// nothing about and returns its TCP port. It stands in for the customer's
// externally owned database.
func startExternalPostgres(t *testing.T) int {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	pw := filepath.Join(dir, "pw")
	require.NoError(t, os.WriteFile(pw, []byte(externalPostgresPassword), 0o600))
	out, err := executil.Command(ctx, "initdb", "-D", data, "-U", "postgres", "--pwfile", pw,
		"--auth-host=scram-sha-256", "--auth-local=trust").CombinedOutput()
	require.NoError(t, err, "%s", out)

	// Unix socket paths are length limited; keep them out of the long temp dir.
	sockDir, err := os.MkdirTemp("", "ext")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })

	port := utils.GetFreePort(t)
	// A valid locale keeps postgres single-threaded at startup on macOS.
	_, _, _, _ = startTestProcessEnv(t, []string{"LC_ALL=en_US.UTF-8"}, "postgres", "-D", data, "-h", "127.0.0.1", "-p", strconv.Itoa(port), "-k", sockDir)
	require.Eventually(t, func() bool {
		conn, err := pgx.Connect(ctx, externalDSN(port))
		if err != nil {
			return false
		}
		_ = conn.Close(ctx)
		return true
	}, 30*time.Second, 100*time.Millisecond, "external postgres did not come up")
	return port
}

// externalDSN names the external postgres as a URI.
func externalDSN(port int) string {
	return fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%d/postgres?sslmode=disable", externalPostgresPassword, port)
}

// externalKeywordDSN names it as keyword=value pairs, the other form a connection's dsn can take.
func externalKeywordDSN(port int) string {
	return fmt.Sprintf("host=127.0.0.1 port=%d dbname=postgres user=postgres password=%s sslmode=disable", port, externalPostgresPassword)
}

// startTestProcessEnv runs a binary with extra environment variables for the
// lifetime of the test. It returns a function that stops the process early and a
// channel that closes when the process has exited.
func startTestProcessEnv(t *testing.T, env []string, binary string, args ...string) (stop func(), exited <-chan struct{}, logPath string, cmd *executil.Cmd) {
	t.Helper()
	logPath = filepath.Join(t.TempDir(), binary+".log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	cmd = executil.Command(context.Background(), binary, args...).WithProcessGroup()
	cmd.SetStdout(logFile)
	cmd.SetStderr(logFile)
	cmd.SetEnv(append(utils.BaseTestEnv(), env...))
	require.NoError(t, cmd.Start())
	exitedCh := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exitedCh)
	}()
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = cmd.Stop(ctx)
	}
	t.Cleanup(func() {
		stop()
		_ = logFile.Close()
		if t.Failed() {
			data, _ := os.ReadFile(logPath)
			t.Logf("%s log:\n%s", binary, data)
		}
	})
	return stop, exitedCh, logPath, cmd
}

// seedBackingConnection inserts a named connection in the sharding catalog of the managed
// default primary (multigres.connections), the way an operator does. dsn is a libpq connection
// string in either form.
func seedBackingConnection(t *testing.T, s *ShardSetup, name, dsn string) {
	t.Helper()
	conn := s.ConnectPrimaryAdmin(t)
	_, err := conn.Exec(t.Context(), `INSERT INTO multigres.connections (name, dsn) VALUES ($1, $2)`, name, dsn)
	require.NoError(t, err)
}

// unmanagedPooler is a running unmanaged multipooler process.
type unmanagedPooler struct {
	name     string
	grpcPort int
	stop     func()
	exited   <-chan struct{}
	logPath  string
	cmd      *executil.Cmd
}

func startUnmanagedPooler(t *testing.T, s *ShardSetup, name, database, tablegroup, connection string, extraArgs ...string) *unmanagedPooler {
	t.Helper()
	grpcPort := utils.GetFreePort(t)
	args := []string{
		"--backing-connection=" + connection,
		"--database=" + database, "--table-group=" + tablegroup, "--shard=0-inf",
		"--cell=" + s.CellName, "--service-id=" + name, "--hostname=localhost",
		"--grpc-port=" + strconv.Itoa(grpcPort), "--http-port=" + strconv.Itoa(utils.GetFreePort(t)),
		"--service-map=grpc-pooler,grpc-poolermanager,grpc-consensus",
		"--topo-global-server-addresses=" + s.EtcdClientAddr, "--topo-global-root=/multigres/global",
	}
	stop, exited, logPath, cmd := startTestProcessEnv(t, nil, "multipooler", append(args, extraArgs...)...)
	return &unmanagedPooler{name: name, grpcPort: grpcPort, stop: stop, exited: exited, logPath: logPath, cmd: cmd}
}

// firstHealth reads one message from the pooler's health stream.
func (p *unmanagedPooler) firstHealth(ctx context.Context) (*multipoolerservicepb.StreamPoolerHealthResponse, error) {
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", p.grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	stream, err := multipoolerservicepb.NewMultipoolerServiceClient(conn).StreamPoolerHealth(ctx, &multipoolerservicepb.StreamPoolerHealthRequest{})
	if err != nil {
		return nil, err
	}
	return stream.Recv()
}

// TestUnmanagedPoolerServesExternalPostgres starts an unmanaged pooler against
// a plain external PostgreSQL next to a managed cohort. It bootstraps only from
// --backing-connection plus the connection row on the default primary, serves
// queries, registers as UNMANAGED, leaves no multigres objects on the external
// database, rejects management RPCs, and leaves the external PostgreSQL running
// when it stops.
func TestUnmanagedPoolerServesExternalPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	s := getSharedSetup(t)
	extPort := startExternalPostgres(t)
	seedBackingConnection(t, s, "serves-queries", externalKeywordDSN(extPort))

	const name = "unmanaged-serves"
	p := startUnmanagedPooler(t, s, name, "postgres", "migrateTG", "serves-queries")

	client, err := NewMultipoolerClient(p.grpcPort)
	require.NoError(t, err)
	defer client.Close()

	require.Eventually(t, func() bool {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, err := client.Pooler.ExecuteQuery(readCtx, "SELECT 1", 1)
		return err == nil
	}, 60*time.Second, 200*time.Millisecond, "unmanaged pooler never served a query")

	res, err := client.Pooler.ExecuteQuery(ctx, "SELECT current_setting('port')", 1)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, strconv.Itoa(extPort), string(res.Rows[0].Values[0]), "query must reach the external database")

	health, err := p.firstHealth(ctx)
	require.NoError(t, err)
	require.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, health.GetServingStatus())

	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: s.CellName, Name: name}
	require.Eventually(t, func() bool {
		rec, err := s.TopoServer.GetMultipooler(ctx, id)
		return err == nil && rec.ManagementMode == clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED &&
			rec.ServingStatus == clustermetadatapb.PoolerServingStatus_SERVING
	}, 30*time.Second, 200*time.Millisecond)

	// The management and consensus services are not registered on an unmanaged pooler:
	// their methods answer Unimplemented.
	_, err = client.Manager.Status(ctx, &multipoolermanagerdata.StatusRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err), "%v", err)
	_, err = client.Consensus.Recruit(ctx, &consensusdata.RecruitRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err), "%v", err)

	// Nothing was created on the external database.
	ext, err := pgx.Connect(ctx, externalDSN(extPort))
	require.NoError(t, err)
	defer ext.Close(context.Background())
	var schemas int
	require.NoError(t, ext.QueryRow(ctx, "SELECT count(*) FROM pg_namespace WHERE nspname = 'multigres'").Scan(&schemas))
	require.Zero(t, schemas, "unmanaged pooler must not create multigres objects on the external database")

	p.stop()
	require.NoError(t, ext.Ping(ctx), "external PostgreSQL must keep running after the pooler stops")
}

// requireNeverReady fails if the pooler reports its backend ready at any point during d, and
// returns the last health message it read.
func requireNeverReady(t *testing.T, p *unmanagedPooler, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	var seen int
	for time.Now().Before(deadline) {
		h, err := p.firstHealth(t.Context())
		if err == nil {
			seen++
			require.NotEqual(t, clustermetadatapb.PoolerServingStatus_SERVING, h.GetServingStatus())
		}
		time.Sleep(250 * time.Millisecond)
	}
	require.NotZero(t, seen, "the pooler never answered its health stream: it is not running")
}

// poolerRecord reads the pooler's topology record.
func poolerRecord(t *testing.T, s *ShardSetup, name string) *clustermetadatapb.Multipooler {
	t.Helper()
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: s.CellName, Name: name}
	rec, err := s.TopoServer.GetMultipooler(t.Context(), id)
	require.NoError(t, err)
	return rec.Multipooler
}

// TestUnmanagedPoolerWithAnUnreachableBackendNeverServes: the connection is valid but nothing
// answers at the endpoint it names, so the pooler never reports its backend ready and stays
// disabled.
func TestUnmanagedPoolerWithAnUnreachableBackendNeverServes(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	s := getSharedSetup(t)

	seedBackingConnection(t, s, "unreachable", externalDSN(utils.GetFreePort(t)))
	p := startUnmanagedPooler(t, s, "unmanaged-unreachable", "postgres", "unreachableTG", "unreachable")

	requireNeverReady(t, p, 8*time.Second)
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, poolerRecord(t, s, "unmanaged-unreachable").GetServingStatus())
}
