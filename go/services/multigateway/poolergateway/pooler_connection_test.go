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

package poolergateway

import (
	"context"
	"log/slog"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// TestPoolerConnection_TelemetryAttributes verifies that PoolerConnection
// creates gRPC clients with the correct OpenTelemetry span attributes.
// This test catches bugs where telemetry configuration is missing or incorrect.
func TestPoolerConnection_TelemetryAttributes(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	ctx := context.Background()

	err := setup.Telemetry.InitTelemetry(ctx, "test-pooler-gateway")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = setup.Telemetry.ShutdownTelemetry(context.Background())
	})

	// Create a test gRPC server with health check service
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	go func() {
		_ = server.Serve(lis)
	}()
	t.Cleanup(func() {
		server.Stop()
	})

	// Create a test pooler that points to our health check server
	pooler := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "test-cell",
			Name:      "test-pooler",
		},
		Hostname:   "127.0.0.1",
		TableGroup: constants.DefaultTableGroup,
		Shard:      "0",
		Type:       clustermetadatapb.PoolerType_PRIMARY,
		PortMap: map[string]int32{
			"grpc": int32(lis.Addr().(*net.TCPAddr).Port),
		},
	}

	logger := slog.Default()

	// Create a real PoolerConnection - this is what we're testing
	conn, err := NewPoolerConnection(pooler, logger, nil)
	require.NoError(t, err)
	defer conn.Close()

	// Make a gRPC call through the PoolerConnection to generate a span
	healthClient := grpc_health_v1.NewHealthClient(conn.conn)
	_, err = healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)

	// Force flush to ensure span is exported
	require.NoError(t, setup.ForceFlush(ctx))

	// Get exported spans
	spans := setup.SpanExporter.GetSpans()
	require.NotEmpty(t, spans, "expected at least one span from gRPC call")

	// Find the client span with our pooler.id attribute
	var foundPoolerAttr bool
	expectedPoolerID := topoclient.MultiPoolerIDString(pooler.Id)

	for _, span := range spans {
		// Look for our pooler.id attribute
		for _, attr := range span.Attributes {
			if attr.Key == "multigres.pooler.id" {
				foundPoolerAttr = true
				assert.Equal(t, expectedPoolerID, attr.Value.AsString(),
					"pooler.id attribute should match the pooler ID")
				break
			}
		}
	}

	assert.True(t, foundPoolerAttr,
		"gRPC client span should have multigres.pooler.id attribute - "+
			"if this fails, telemetry attributes are not properly configured in NewPoolerConnection")
}

// TestNewPoolerConnection verifies basic PoolerConnection creation.
func TestNewPoolerConnection(t *testing.T) {
	logger := slog.Default()
	pooler := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)

	// Create a new pooler connection
	// The connection will fail to actually connect (no server), but gRPC uses
	// non-blocking dial so NewPoolerConnection succeeds.
	conn, err := NewPoolerConnection(pooler, logger, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	// Verify basic properties
	assert.Equal(t, "multipooler-zone1-pooler1", conn.ID())
	assert.Equal(t, "zone1", conn.Cell())
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, conn.Type())
}

// TestPoolerConnection_ID verifies ID generation for different pooler configurations.
func TestPoolerConnection_ID(t *testing.T) {
	logger := slog.Default()

	tests := []struct {
		name     string
		cell     string
		poolName string
		expected string
	}{
		{"simple", "zone1", "pooler1", "multipooler-zone1-pooler1"},
		{"different cell", "us-west", "primary", "multipooler-us-west-primary"},
		{"numeric name", "zone2", "pooler-123", "multipooler-zone2-pooler-123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pooler := createTestMultiPooler(tt.poolName, tt.cell, constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
			conn, err := NewPoolerConnection(pooler, logger, nil)
			require.NoError(t, err)
			defer conn.Close()

			assert.Equal(t, tt.expected, conn.ID())
		})
	}
}
