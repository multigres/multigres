// Copyright 2025 Supabase, Inc.
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

package shardsetup

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	multiorchpb "github.com/multigres/multigres/go/pb/multiorch"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// MultipoolerClient wraps a gRPC connection to a multipooler and provides access to
// manager, consensus, and pooler service clients over the same connection.
// Follows the pattern from multipooler/setup_test.go:multipoolerClient.
type MultipoolerClient struct {
	conn      *grpc.ClientConn
	Manager   multipoolermanagerpb.MultiPoolerManagerClient
	Consensus consensuspb.MultiPoolerConsensusClient
	Pooler    *MultiPoolerTestClient
}

// NewMultipoolerClient creates a new MultipoolerClient connected to the given gRPC port.
// It establishes a single gRPC connection and creates clients for all multipooler services.
// Follows the pattern from multipooler/setup_test.go:newMultipoolerClient.
func NewMultipoolerClient(grpcPort int) (*MultipoolerClient, error) {
	addr := fmt.Sprintf("localhost:%d", grpcPort)

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	// Create pooler test client (uses its own connection internally)
	poolerClient, err := NewMultiPoolerTestClient(addr)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create pooler client: %w", err)
	}

	return &MultipoolerClient{
		conn:      conn,
		Manager:   multipoolermanagerpb.NewMultiPoolerManagerClient(conn),
		Consensus: consensuspb.NewMultiPoolerConsensusClient(conn),
		Pooler:    poolerClient,
	}, nil
}

// Close closes all underlying connections.
func (c *MultipoolerClient) Close() error {
	var errs []error
	if c.Pooler != nil {
		if err := c.Pooler.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// WaitForManagerReady waits for the manager to be in ready state.
// Follows the pattern from multipooler/setup_test.go:waitForManagerReady.
func WaitForManagerReady(t *testing.T, manager *ProcessInstance) {
	t.Helper()

	// Connect to the manager
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", manager.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := multipoolermanagerpb.NewMultiPoolerManagerClient(conn)

	// Use require.Eventually to wait for manager to be ready
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		req := &multipoolermanagerdatapb.StateRequest{}
		resp, err := client.State(ctx, req)
		if err != nil {
			return false
		}
		if resp.State == "error" {
			t.Fatalf("Manager failed to initialize: %s", resp.ErrorMessage)
		}
		return resp.State == "ready"
	}, 30*time.Second, 100*time.Millisecond, "Manager should become ready within 30 seconds")

	t.Logf("Manager %s is ready", manager.Name)
}

// QueryStringValue executes a query and extracts the first column of the first row as a string.
// Returns empty string and error if query fails or returns no rows.
// Follows the pattern from multipooler/setup_test.go:queryStringValue.
func QueryStringValue(ctx context.Context, client *MultiPoolerTestClient, query string) (string, error) {
	resp, err := client.ExecuteQuery(ctx, query, 1)
	if err != nil {
		return "", err
	}
	if len(resp.Rows) == 0 || len(resp.Rows[0].Values) == 0 {
		return "", nil
	}
	return string(resp.Rows[0].Values[0]), nil
}

// PgctldClient wraps the pgctld gRPC client.
type PgctldClient struct {
	conn *grpc.ClientConn
	pgctldpb.PgCtldClient
}

// NewPgctldClient creates a new PgctldClient connected to the given gRPC port.
func NewPgctldClient(grpcPort int) (*PgctldClient, error) {
	addr := fmt.Sprintf("localhost:%d", grpcPort)

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to pgctld: %w", err)
	}

	return &PgctldClient{
		conn:         conn,
		PgCtldClient: pgctldpb.NewPgCtldClient(conn),
	}, nil
}

// Close closes the underlying connection.
func (c *PgctldClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// MultiOrchClient wraps the multiorch gRPC client.
type MultiOrchClient struct {
	conn *grpc.ClientConn
	multiorchpb.MultiOrchServiceClient
}

// NewMultiOrchClient creates a new MultiOrchClient connected to the given gRPC port.
func NewMultiOrchClient(grpcPort int) (*MultiOrchClient, error) {
	addr := fmt.Sprintf("localhost:%d", grpcPort)

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to multiorch: %w", err)
	}

	return &MultiOrchClient{
		conn:                   conn,
		MultiOrchServiceClient: multiorchpb.NewMultiOrchServiceClient(conn),
	}, nil
}

// Close closes the underlying connection.
func (c *MultiOrchClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
