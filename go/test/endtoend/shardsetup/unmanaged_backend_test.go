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
	"encoding/json"
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

	"github.com/multigres/multigres/go/common/backingconn"
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
	startTestProcessEnv(t, []string{"LC_ALL=en_US.UTF-8"}, "postgres", "-D", data, "-h", "127.0.0.1", "-p", strconv.Itoa(port), "-k", sockDir)
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

// startTestProcessEnv runs a binary with extra environment variables for the
// lifetime of the test, and returns a function that stops it early.
func startTestProcessEnv(t *testing.T, env []string, binary string, args ...string) (stop func()) {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), binary+".log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	cmd := executil.Command(context.Background(), binary, args...).WithProcessGroup()
	cmd.SetStdout(logFile)
	cmd.SetStderr(logFile)
	cmd.SetEnv(append(utils.BaseTestEnv(), env...))
	require.NoError(t, cmd.Start())
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
	return stop
}

// seedExternalTablegroup records an external tablegroup in the sharding catalog the way an operator
// does: a named connection in multigres.connections, then CreateTableGroup naming it. It returns the
// tablegroup's catalog id and the id of the creation's operation. The rows are removed when the test
// ends, since the cluster is shared.
func seedExternalTablegroup(t *testing.T, s *ShardSetup, tablegroup, connection string, conn *backingconn.Conn) (id int64, operationID string) {
	t.Helper()
	conninfo, err := json.Marshal(conn)
	require.NoError(t, err)
	admin := s.ConnectPrimaryAdmin(t)
	_, err = admin.Exec(t.Context(), `INSERT INTO multigres.connections (name, dsn, conninfo) VALUES ($1, $2, $3::jsonb)`, connection, conn.DSN(), string(conninfo))
	require.NoError(t, err)

	primary := s.NewPrimaryClient(t)
	defer primary.Close()
	resp, err := primary.Manager.CreateTableGroup(t.Context(), &multipoolermanagerdata.CreateTableGroupRequest{
		Name: tablegroup, BackingConnection: connection,
	})
	require.NoError(t, err)
	id = resp.GetTablegroupId()

	t.Cleanup(func() {
		ctx := context.Background()
		for _, stmt := range []string{
			`DELETE FROM multigres.shard WHERE tablegroup_id = $1`,
			`DELETE FROM multigres.operation WHERE kind = 'create_tablegroup' AND (request->>'tablegroup_id')::bigint = $1`,
			`DELETE FROM multigres.tablegroup WHERE tablegroup_id = $1`,
		} {
			_, err := admin.Exec(ctx, stmt, id)
			require.NoError(t, err, stmt)
		}
		_, err := admin.Exec(ctx, `DELETE FROM multigres.connections WHERE name = $1`, connection)
		require.NoError(t, err)
	})
	return id, resp.GetOperationId()
}

// unmanagedPooler is a running unmanaged multipooler process.
type unmanagedPooler struct {
	grpcPort int
	stop     func()
}

// startUnmanagedPooler starts a pooler told only its tablegroup's catalog id; it learns from the
// catalog that it is unmanaged and which postgres it fronts.
func startUnmanagedPooler(t *testing.T, s *ShardSetup, name, database, tablegroup string, tablegroupID int64) *unmanagedPooler {
	t.Helper()
	grpcPort := utils.GetFreePort(t)
	args := []string{
		"--database=" + database, "--table-group=" + tablegroup, "--shard=0-inf",
		"--tablegroup-id=" + strconv.FormatInt(tablegroupID, 10),
		"--cell=" + s.CellName, "--service-id=" + name, "--hostname=localhost",
		"--grpc-port=" + strconv.Itoa(grpcPort), "--http-port=" + strconv.Itoa(utils.GetFreePort(t)),
		"--service-map=grpc-pooler,grpc-poolermanager,grpc-consensus",
		"--topo-global-server-addresses=" + s.EtcdClientAddr, "--topo-global-root=/multigres/global",
	}
	return &unmanagedPooler{grpcPort: grpcPort, stop: startTestProcessEnv(t, nil, "multipooler", args...)}
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

// requireServesQueries waits until the pooler behind client answers a query.
func requireServesQueries(t *testing.T, client *MultipoolerClient, msg string) {
	t.Helper()
	require.Eventually(t, func() bool {
		readCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		_, err := client.Pooler.ExecuteQuery(readCtx, "SELECT 1", 1)
		return err == nil
	}, 60*time.Second, 200*time.Millisecond, msg)
}

// TestUnmanagedPoolerServesExternalPostgres starts an unmanaged pooler against
// a plain external PostgreSQL next to a managed cohort. It serves queries and
// LISTEN/NOTIFY, registers as UNMANAGED, leaves no multigres objects on the
// external database, rejects management RPCs, leaves the external PostgreSQL
// running when it stops, and serves again after a restart.
func TestUnmanagedPoolerServesExternalPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	s := getSharedSetup(t)
	extPort := startExternalPostgres(t)
	tablegroupID, creation := seedExternalTablegroup(t, s, "externaltg", "serves-queries", &backingconn.Conn{
		Hosts: []string{"127.0.0.1"}, Ports: []int{extPort}, Database: "postgres", User: "postgres",
		Password: externalPostgresPassword, SSLMode: "disable",
	})

	const name = "unmanaged-serves"
	p := startUnmanagedPooler(t, s, name, "postgres", "externaltg", tablegroupID)

	client, err := NewMultipoolerClient(p.grpcPort)
	require.NoError(t, err)
	defer client.Close()

	requireServesQueries(t, client, "unmanaged pooler never served a query")

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
		return err == nil && rec.LeadershipMode == clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED &&
			rec.ServingStatus == clustermetadatapb.PoolerServingStatus_SERVING
	}, 30*time.Second, 200*time.Millisecond)
	rec := poolerRecord(t, s, name)
	require.Equal(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, rec.GetRoutingState().GetRole())
	require.Equal(t, tablegroupID, rec.GetTablegroupId())
	require.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, rec.GetServingStatus())

	// A tablegroup creation counts a pooler by its shard's routing primary, SERVING, for the
	// tablegroup's id: the record checked above.
	primary := s.NewPrimaryClient(t)
	defer primary.Close()
	require.Eventually(t, func() bool {
		resp, err := primary.Manager.GetOperation(ctx, &multipoolermanagerdata.GetOperationRequest{OperationId: creation})
		return err == nil && resp.GetOperation().GetShardsServing() == 1
	}, 30*time.Second, 200*time.Millisecond, "the creation never counted the unmanaged pooler as serving")

	// The management and consensus services are not registered on an unmanaged pooler:
	// their methods answer Unimplemented.
	_, err = client.Manager.Status(ctx, &multipoolermanagerdata.StatusRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err), "%v", err)
	_, err = client.Consensus.Recruit(ctx, &consensusdata.RecruitRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err), "%v", err)

	ext, err := pgx.Connect(ctx, externalDSN(extPort))
	require.NoError(t, err)
	defer ext.Close(context.Background())

	// A session's LISTEN stream sent to the pooler works, and a NOTIFY on the external database
	// reaches it.
	requireNotificationDelivered(t, p, ext, "unmanaged_listen", "hello")

	// Nothing was created on the external database, listening included.
	var schemas int
	require.NoError(t, ext.QueryRow(ctx, "SELECT count(*) FROM pg_namespace WHERE nspname = 'multigres'").Scan(&schemas))
	require.Zero(t, schemas, "unmanaged pooler must not create multigres objects on the external database")

	p.stop()
	require.NoError(t, ext.Ping(ctx), "external PostgreSQL must keep running after the pooler stops")

	// A restart under the same name goes by the pooler's own topology record, which says it is
	// unmanaged, and reads its connection from the catalog again.
	restarted := startUnmanagedPooler(t, s, name, "postgres", "externaltg", tablegroupID)
	again, err := NewMultipoolerClient(restarted.grpcPort)
	require.NoError(t, err)
	defer again.Close()
	requireServesQueries(t, again, "the restarted unmanaged pooler never served a query")
}

// requireNotificationDelivered opens a LISTEN stream on the pooler for channel, issues a NOTIFY
// on the external database and requires the stream to deliver it. The stream is retried until the
// pooler accepts it, since its listener starts as the pooler begins to serve.
func requireNotificationDelivered(t *testing.T, p *unmanagedPooler, ext *pgx.Conn, channel, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", p.grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	var stream multipoolerservicepb.MultipoolerService_NotificationStreamClient
	var lastErr error
	for deadline := time.Now().Add(30 * time.Second); stream == nil && time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
		s, err := multipoolerservicepb.NewMultipoolerServiceClient(conn).NotificationStream(ctx)
		if err == nil {
			err = s.Send(&multipoolerservicepb.NotificationStreamRequest{SubscribeChannels: []string{channel}})
		}
		var ack *multipoolerservicepb.NotificationStreamResponse
		if err == nil {
			ack, err = s.Recv()
		}
		if err == nil && !ack.GetReady() {
			err = fmt.Errorf("the first response is not the subscription's acknowledgement: %v", ack)
		}
		if lastErr = err; err == nil {
			stream = s
		}
	}
	require.NotNil(t, stream, "the pooler never accepted a LISTEN stream, last error: %v", lastErr)

	_, err = ext.Exec(ctx, "SELECT pg_notify($1, $2)", channel, payload)
	require.NoError(t, err)
	got, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, channel, got.GetNotification().GetChannel())
	require.Equal(t, payload, got.GetNotification().GetPayload())
}

// requireNeverReady fails if the pooler reports SERVING at any point during d, or if it never
// answers its health stream.
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

	unreachable, err := backingconn.FromDSN(externalDSN(utils.GetFreePort(t)))
	require.NoError(t, err)
	tablegroupID, _ := seedExternalTablegroup(t, s, "unreachabletg", "unreachable", unreachable)
	p := startUnmanagedPooler(t, s, "unmanaged-unreachable", "postgres", "unreachabletg", tablegroupID)

	requireNeverReady(t, p, 8*time.Second)
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, poolerRecord(t, s, "unmanaged-unreachable").GetServingStatus())
}
