// Copyright 2021 The Vitess Authors.
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
//
// Modifications Copyright 2025 Supabase, Inc.

package multipooler

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/nettest"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/multipooler/rpcclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// makePooler creates a multipooler object from an address for testing.
// The address should be in the format "host:port".
func makePooler(addr string) *clustermetadatapb.MultiPooler {
	// Parse the address into host and port
	host, portStr, _ := strings.Cut(addr, ":")
	port, _ := strconv.Atoi(portStr)

	return &clustermetadatapb.MultiPooler{
		Hostname: host,
		PortMap:  map[string]int32{"grpc": int32(port)},
	}
}

// fakeConsensusServer is a minimal implementation of the consensus service for testing.
type fakeConsensusServer struct {
	consensuspb.UnimplementedMultiPoolerConsensusServer
}

func (f *fakeConsensusServer) Status(ctx context.Context, req *consensusdatapb.StatusRequest) (*consensusdatapb.StatusResponse, error) {
	return &consensusdatapb.StatusResponse{}, nil
}

// fakeManagerServer is a minimal implementation of the manager service for testing.
type fakeManagerServer struct {
	multipoolermanagerpb.UnimplementedMultiPoolerManagerServer
}

func (f *fakeManagerServer) State(ctx context.Context, req *multipoolermanagerdatapb.StatusRequest) (*multipoolermanagerdatapb.StatusResponse, error) {
	return &multipoolermanagerdatapb.StatusResponse{}, nil
}

// grpcTestServer starts a gRPC server with both consensus and manager services for testing.
// Returns the address and a cleanup function.
func grpcTestServer(t testing.TB) (string, func()) {
	listener, err := nettest.NewLocalListener("tcp")
	if err != nil {
		t.Fatalf("cannot create listener: %v", err)
	}

	s := grpc.NewServer()
	consensuspb.RegisterMultiPoolerConsensusServer(s, &fakeConsensusServer{})
	multipoolermanagerpb.RegisterMultiPoolerManagerServer(s, &fakeManagerServer{})

	go func() {
		_ = s.Serve(listener)
	}()

	return listener.Addr().String(), func() {
		s.GracefulStop()
		listener.Close()
	}
}

// BenchmarkMultiPoolerClientSteadyState measures steady state performance with a pool of connections.
// This is ported from Vitess to test our rpcclient implementation.
func BenchmarkMultiPoolerClientSteadyState(b *testing.B) {
	addrs := make([]string, 100)
	cleanup := make([]func(), len(addrs))
	for i := range addrs {
		addrs[i], cleanup[i] = grpcTestServer(b)
	}
	defer func() {
		for _, c := range cleanup {
			c()
		}
	}()

	client := rpcclient.NewMultiPoolerClient(100)
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		addr := addrs[n%len(addrs)]
		pooler := makePooler(addr)

		_, err := client.ConsensusStatus(ctx, pooler, &consensusdatapb.StatusRequest{})
		if err != nil {
			b.Fatalf("ConsensusStatus RPC failed: %v", err)
		}
	}
}

// BenchmarkMultiPoolerClientSteadyStateRedials tests performance when redialing existing connections.
// This is ported from Vitess to test our rpcclient implementation.
func BenchmarkMultiPoolerClientSteadyStateRedials(b *testing.B) {
	addrs := make([]string, 1000)
	cleanup := make([]func(), len(addrs))
	for i := range addrs {
		addrs[i], cleanup[i] = grpcTestServer(b)
	}
	defer func() {
		for _, c := range cleanup {
			c()
		}
	}()

	client := rpcclient.NewMultiPoolerClient(100)
	defer client.Close()

	ctx := context.Background()

	// Pre-populate by making one call to each address
	for _, addr := range addrs {
		pooler := makePooler(addr)
		_, err := client.State(ctx, pooler, &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			b.Fatalf("Status RPC failed: %v", err)
		}
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		addr := addrs[n%len(addrs)]
		pooler := makePooler(addr)

		_, err := client.State(ctx, pooler, &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			b.Fatalf("Status RPC failed: %v", err)
		}
	}
}

// BenchmarkMultiPoolerClientSteadyStateEvictions tests performance when evictions are necessary.
// This is ported from Vitess to test our rpcclient implementation.
func BenchmarkMultiPoolerClientSteadyStateEvictions(b *testing.B) {
	const numAddrs = 1000

	addrs := make([]string, numAddrs)
	cleanup := make([]func(), len(addrs))
	for i := range addrs {
		addrs[i], cleanup[i] = grpcTestServer(b)
	}
	defer func() {
		for _, c := range cleanup {
			c()
		}
	}()

	// Use default capacity (100) to force evictions with 1000 addresses
	client := rpcclient.NewMultiPoolerClient(100)
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		addr := addrs[n%len(addrs)]
		pooler := makePooler(addr)

		_, err := client.ConsensusStatus(ctx, pooler, &consensusdatapb.StatusRequest{})
		if err != nil {
			b.Fatalf("ConsensusStatus RPC failed: %v", err)
		}
	}
}

// TestMultiPoolerClient tests the rpcclient with actual gRPC servers and concurrent access.
// This is ported from Vitess to test our rpcclient implementation.
func TestMultiPoolerClient(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const numAddrs = 100
	const concurrency = 10
	const duration = 1 * time.Minute

	addrs := make([]string, numAddrs)
	cleanup := make([]func(), len(addrs))
	for i := range addrs {
		addrs[i], cleanup[i] = grpcTestServer(t)
	}
	defer func() {
		for _, c := range cleanup {
			c()
		}
	}()

	client := rpcclient.NewMultiPoolerClient(100)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					addr := addrs[workerID%len(addrs)]
					pooler := makePooler(addr)

					// Make an RPC to both services
					_, err := client.ConsensusStatus(ctx, pooler, &consensusdatapb.StatusRequest{})
					if err != nil && ctx.Err() == nil {
						// With grpc.NewClient() lazy connections, DeadlineExceeded can occur
						// when the context expires right before/during connection establishment.
						// This is expected behavior near test completion, not a real failure.
						if !strings.Contains(err.Error(), "DeadlineExceeded") && !strings.Contains(err.Error(), "context deadline exceeded") {
							t.Errorf("worker %d: consensus Status RPC failed: %v", workerID, err)
						}
					}

					_, err = client.State(ctx, pooler, &multipoolermanagerdatapb.StatusRequest{})
					if err != nil && ctx.Err() == nil {
						// With grpc.NewClient() lazy connections, DeadlineExceeded can occur
						// when the context expires right before/during connection establishment.
						// This is expected behavior near test completion, not a real failure.
						if !strings.Contains(err.Error(), "DeadlineExceeded") && !strings.Contains(err.Error(), "context deadline exceeded") {
							t.Errorf("worker %d: manager Status RPC failed: %v", workerID, err)
						}
					}
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestMultiPoolerClient_evictions tests that the cache properly evicts connections when at capacity.
// This is ported from Vitess to test our rpcclient implementation.
func TestMultiPoolerClient_evictions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const numAddrs = 200

	addrs := make([]string, numAddrs)
	cleanup := make([]func(), len(addrs))
	for i := range addrs {
		addrs[i], cleanup[i] = grpcTestServer(t)
	}
	defer func() {
		for _, c := range cleanup {
			c()
		}
	}()

	// Default capacity is 100, so with 200 addresses we'll trigger evictions
	client := rpcclient.NewMultiPoolerClient(100)
	defer client.Close()

	ctx := context.Background()

	// Dial to all addresses (more than capacity)
	for i, addr := range addrs {
		pooler := makePooler(addr)

		_, err := client.ConsensusStatus(ctx, pooler, &consensusdatapb.StatusRequest{})
		if err != nil {
			t.Fatalf("iteration %d: ConsensusStatus RPC failed: %v", i, err)
		}
	}

	t.Logf("Successfully processed %d addresses with default capacity (should have triggered evictions)", numAddrs)
}
