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

package queryserving

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/pid"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_QueryCancel tests that a PostgreSQL client can cancel a
// running query through the multigateway using the CancelRequest protocol.
// This validates end-to-end cancel routing: client → multigateway → backend.
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestMultiGateway_QueryCancel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping query cancel test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping query cancel tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 150*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable")

			t.Run("pgx context cancel", func(t *testing.T) {
				conn, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err, "failed to connect with pgx")
				defer conn.Close(ctx)

				cancelCtx, cancel := context.WithCancel(ctx)
				defer cancel()

				go func() {
					time.Sleep(200 * time.Millisecond)
					cancel()
				}()

				start := time.Now()
				_, err = conn.Exec(cancelCtx, "SELECT pg_sleep(10)")
				elapsed := time.Since(start)

				require.Error(t, err, "query should be cancelled")
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) {
					assert.Equal(t, "57014", pgErr.Code, "should be query_canceled (57014)")
				}
				assert.Less(t, elapsed, 5*time.Second, "cancel should return well before the 10s sleep completes")
			})

			t.Run("database/sql ExecContext cancel", func(t *testing.T) {
				sqlConnStr := fmt.Sprintf(
					"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
					target.Port, shardsetup.TestPostgresPassword)
				db, err := sql.Open("postgres", sqlConnStr)
				require.NoError(t, err)
				defer db.Close()

				err = db.PingContext(ctx)
				require.NoError(t, err, "failed to ping database")

				cancelCtx, cancel := context.WithCancel(ctx)
				defer cancel()

				go func() {
					time.Sleep(200 * time.Millisecond)
					cancel()
				}()

				start := time.Now()
				_, err = db.ExecContext(cancelCtx, "SELECT pg_sleep(10)")
				elapsed := time.Since(start)

				require.Error(t, err, "ExecContext should be cancelled")
				assert.Less(t, elapsed, 5*time.Second, "cancel should return well before the 10s sleep completes")
			})

			t.Run("database/sql QueryContext cancel", func(t *testing.T) {
				sqlConnStr := fmt.Sprintf(
					"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
					target.Port, shardsetup.TestPostgresPassword)
				db, err := sql.Open("postgres", sqlConnStr)
				require.NoError(t, err)
				defer db.Close()

				err = db.PingContext(ctx)
				require.NoError(t, err, "failed to ping database")

				cancelCtx, cancel := context.WithCancel(ctx)
				defer cancel()

				go func() {
					time.Sleep(200 * time.Millisecond)
					cancel()
				}()

				start := time.Now()
				rows, err := db.QueryContext(cancelCtx, "SELECT pg_sleep(10)")
				elapsed := time.Since(start)
				if rows != nil {
					defer rows.Close()
				}

				require.Error(t, err, "QueryContext should be cancelled")
				assert.Less(t, elapsed, 5*time.Second, "cancel should return well before the 10s sleep completes")
			})

			t.Run("new connection works after cancel", func(t *testing.T) {
				conn, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err, "failed to connect with pgx")
				defer conn.Close(ctx)

				cancelCtx, cancel := context.WithCancel(ctx)
				defer cancel()

				go func() {
					time.Sleep(200 * time.Millisecond)
					cancel()
				}()

				_, err = conn.Exec(cancelCtx, "SELECT pg_sleep(10)")
				require.Error(t, err, "query should be cancelled")

				// pgx closes the connection after context cancellation, which is
				// expected client behavior. Verify that the multigateway's state
				// is not corrupted: a new connection should work fine.
				conn2, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err, "failed to connect with pgx")
				defer conn2.Close(ctx)

				var result int
				err = conn2.QueryRow(ctx, "SELECT 1").Scan(&result)
				require.NoError(t, err, "new connection should work after cancel")
				assert.Equal(t, 1, result)
			})

			t.Run("cancel does not affect other connections", func(t *testing.T) {
				conn1, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err, "failed to connect with pgx")
				defer conn1.Close(ctx)
				conn2, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err, "failed to connect with pgx")
				defer conn2.Close(ctx)

				cancelCtx, cancel := context.WithCancel(ctx)
				defer cancel()

				// Start a long query on conn1 in a goroutine.
				errCh := make(chan error, 1)
				go func() {
					_, err := conn1.Exec(cancelCtx, "SELECT pg_sleep(10)")
					errCh <- err
				}()

				// Wait for the query to start executing on the backend.
				time.Sleep(100 * time.Millisecond)

				// conn2 should work fine while conn1 has a running query.
				var result int
				err = conn2.QueryRow(ctx, "SELECT 1").Scan(&result)
				require.NoError(t, err, "conn2 should not be affected by conn1's pending query")
				assert.Equal(t, 1, result)

				// Cancel conn1's query.
				cancel()

				// conn1's query should return with an error.
				select {
				case err = <-errCh:
					require.Error(t, err, "conn1's query should be cancelled")
				case <-time.After(10 * time.Second):
					t.Fatal("timed out waiting for conn1's query to be cancelled")
				}

				// conn2 should still be usable after conn1's cancel.
				err = conn2.QueryRow(ctx, "SELECT 2").Scan(&result)
				require.NoError(t, err, "conn2 should still work after conn1's cancel")
				assert.Equal(t, 2, result)
			})
		})
	}
}

func TestMultiGateway_QueryCancel_ForwardedOverGRPCTLS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TLS cancel forwarding test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping query cancel tests")
	}

	caCert, serverCert, serverKey := generateGatewayGRPCTLSFiles(t)

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultigateway(),
		func(c *shardsetup.SetupConfig) {
			// Gateway that owns query execution: enable gRPC server TLS.
			c.MultigatewayExtraArgs = append(c.MultigatewayExtraArgs,
				"--grpc-cert", serverCert,
				"--grpc-key", serverKey,
			)
		},
	)
	defer cleanup()

	targetGateway := setup.Multigateway
	require.NotNil(t, targetGateway)
	targetPGPort := setup.MultigatewayPgPort

	// Create a second gateway that receives CancelRequest and forwards to target over gRPC TLS.
	sourceGateway := setup.CreateMultigatewayInstance(
		t,
		"multigateway-source",
		utils.GetFreePort(t),
		utils.GetFreePort(t),
		utils.GetFreePort(t),
	)
	sourceGateway.ExtraArgs = []string{
		"--multipooler-grpc-ca", caCert,
		"--multipooler-grpc-server-name", "localhost",
		"--multipooler-grpc-require-tls",
	}

	// Restore canonical setup pointers so shardsetup cleanup continues to manage the target gateway.
	setup.Multigateway = targetGateway
	setup.MultigatewayPgPort = targetPGPort

	require.NoError(t, sourceGateway.Start(setup.Context(), t), "failed to start source gateway")
	defer sourceGateway.TerminateGracefully(t, 5*time.Second)

	prefixes := waitForGatewayPrefixes(t, setup, targetGateway.ServiceID, sourceGateway.ServiceID)
	targetPrefix := prefixes[targetGateway.ServiceID]
	sourcePrefix := prefixes[sourceGateway.ServiceID]
	require.NotEqual(t, targetPrefix, sourcePrefix, "gateways must have different PID prefixes")

	ctx := utils.WithTimeout(t, 120*time.Second)
	conn := connectClientToGateway(t, ctx, targetPGPort)
	defer conn.Close()

	processID := conn.ProcessID()
	secretKey := conn.SecretKey()
	require.NotZero(t, processID)
	require.NotZero(t, secretKey)

	queryPrefix, _ := pid.DecodePID(processID)
	require.Equal(t, targetPrefix, queryPrefix, "query connection PID should belong to target gateway")

	errCh := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := conn.Query(ctx, "SELECT pg_sleep(10)")
		errCh <- err
	}()

	// Give the query time to start on the target backend.
	time.Sleep(250 * time.Millisecond)
	require.NoError(t, sendCancelRequest("127.0.0.1", sourceGateway.PgPort, processID, secretKey))

	select {
	case err := <-errCh:
		require.Error(t, err, "query should be cancelled by forwarded CancelRequest")
		var pgDiag *mterrors.PgDiagnostic
		require.True(t, errors.As(err, &pgDiag), "expected PgDiagnostic, got %T: %v", err, err)
		assert.Equal(t, "57014", pgDiag.Code, "should be query_canceled")
		assert.Less(t, time.Since(start), 5*time.Second, "cancel should return well before pg_sleep(10) completes")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for forwarded cancel")
	}

	// Verify the target gateway connection is still usable after cancel.
	_, err := conn.Query(ctx, "SELECT 1")
	require.NoError(t, err)
}

func connectClientToGateway(t *testing.T, ctx context.Context, pgPort int) *client.Conn {
	t.Helper()

	conn, err := client.Connect(ctx, ctx, &client.Config{
		Host:        "localhost",
		Port:        pgPort,
		User:        shardsetup.DefaultTestUser,
		Password:    shardsetup.TestPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	return conn
}

func sendCancelRequest(host string, port int, processID, secretKey uint32) error {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial cancel endpoint %s: %w", addr, err)
	}
	defer conn.Close()

	packet := make([]byte, 16)
	binary.BigEndian.PutUint32(packet[0:4], 16)
	binary.BigEndian.PutUint32(packet[4:8], protocol.CancelRequestCode)
	binary.BigEndian.PutUint32(packet[8:12], processID)
	binary.BigEndian.PutUint32(packet[12:16], secretKey)

	n, err := conn.Write(packet)
	if err != nil {
		return fmt.Errorf("write cancel packet: %w", err)
	}
	if n != len(packet) {
		return fmt.Errorf("short write for cancel packet: wrote %d, want %d", n, len(packet))
	}
	return nil
}

func waitForGatewayPrefixes(t *testing.T, setup *shardsetup.ShardSetup, serviceIDs ...string) map[string]uint32 {
	t.Helper()

	expected := make(map[string]struct{}, len(serviceIDs))
	for _, id := range serviceIDs {
		expected[id] = struct{}{}
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		gateways, err := setup.TopoServer.GetMultiGatewaysByCell(context.Background(), setup.CellName)
		if err == nil {
			prefixes := make(map[string]uint32, len(serviceIDs))
			for _, gw := range gateways {
				if gw.GetId() == nil {
					continue
				}
				name := gw.GetId().GetName()
				if _, ok := expected[name]; ok {
					prefixes[name] = gw.GetPidPrefix()
				}
			}
			if len(prefixes) == len(serviceIDs) {
				return prefixes
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for gateways in topo: %v", serviceIDs)
	return nil
}

func generateGatewayGRPCTLSFiles(t *testing.T) (caCert, serverCert, serverKey string) {
	t.Helper()

	dir := t.TempDir()
	caCert = filepath.Join(dir, "ca.crt")
	caKey := filepath.Join(dir, "ca.key")
	serverCert = filepath.Join(dir, "server.crt")
	serverKey = filepath.Join(dir, "server.key")

	require.NoError(t, local.GenerateCA(caCert, caKey))
	require.NoError(t, local.GenerateCert(caCert, caKey, serverCert, serverKey, "localhost", []string{"localhost"}))

	return caCert, serverCert, serverKey
}
