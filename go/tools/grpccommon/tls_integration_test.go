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

package grpccommon

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// startTLSServer creates and starts a gRPC server with the given TLS config.
// Returns the listener address and a cleanup function.
func startTLSServer(t *testing.T, certFile, keyFile, caFile string) (string, func()) {
	t.Helper()

	tlsConfig, err := BuildServerTLSConfig(certFile, keyFile, caFile, "")
	require.NoError(t, err)

	var opts []grpc.ServerOption
	if tlsConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	srv := grpc.NewServer(opts...)

	// Register a health service for testing.
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrv)
	healthSrv.SetServingStatus("test", healthpb.HealthCheckResponse_SERVING)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = srv.Serve(lis) }()

	return lis.Addr().String(), func() { srv.Stop() }
}

// healthCheck makes a gRPC health check RPC to the given address with the given dial options.
func healthCheck(ctx context.Context, addr string, opts ...grpc.DialOption) error {
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)
	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{Service: "test"})
	return err
}

func TestGRPC_TLS_ClientConnects(t *testing.T) {
	caCert, _, serverCert, serverKey := generateTestCerts(t, "server")

	addr, cleanup := startTLSServer(t, serverCert, serverKey, "")
	defer cleanup()

	// Client with CA should connect successfully.
	clientTLS, err := BuildClientTLSConfig("", "", caCert, "localhost")
	require.NoError(t, err)

	err = healthCheck(context.Background(), addr, grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	assert.NoError(t, err)

	// Generate a separate CA that did NOT sign the server cert.
	otherDir := t.TempDir()
	otherCACert, _ := generateTestCA(t, otherDir)

	// Client with wrong CA should fail.
	wrongTLS, err := BuildClientTLSConfig("", "", otherCACert, "localhost")
	require.NoError(t, err)

	err = healthCheck(context.Background(), addr, grpc.WithTransportCredentials(credentials.NewTLS(wrongTLS)))
	assert.Error(t, err, "client with wrong CA should fail TLS verification")

	// Insecure client should fail against TLS server.
	err = healthCheck(context.Background(), addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.Error(t, err, "insecure client should fail against TLS server")
}

func TestGRPC_MTLS_RequiresClientCert(t *testing.T) {
	dir := t.TempDir()

	// Generate CA + server cert + client cert.
	caCert, caKey := generateTestCA(t, dir)
	serverCert, serverKey := generateTestCert(t, dir, "server", caCert, caKey)
	clientCert, clientKey := generateTestCert(t, dir, "client", caCert, caKey)

	// Start server with mTLS (CA for client cert verification).
	addr, cleanup := startTLSServer(t, serverCert, serverKey, caCert)
	defer cleanup()

	// Client with cert should succeed.
	mtlsConfig, err := BuildClientTLSConfig(clientCert, clientKey, caCert, "localhost")
	require.NoError(t, err)

	err = healthCheck(context.Background(), addr, grpc.WithTransportCredentials(credentials.NewTLS(mtlsConfig)))
	assert.NoError(t, err)

	// Client without cert should fail (server requires client cert).
	noCertConfig, err := BuildClientTLSConfig("", "", caCert, "localhost")
	require.NoError(t, err)

	err = healthCheck(context.Background(), addr, grpc.WithTransportCredentials(credentials.NewTLS(noCertConfig)))
	assert.Error(t, err, "client without cert should fail mTLS")
}
