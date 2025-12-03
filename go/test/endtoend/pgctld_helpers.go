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

package endtoend

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// InitAndStartPostgreSQL is a helper that initializes and starts PostgreSQL via pgctld gRPC.
// This combines the common pattern of calling InitDataDir followed by Start.
func InitAndStartPostgreSQL(t *testing.T, grpcAddr string) error {
	t.Helper()

	// Connect to pgctld gRPC
	conn, err := grpc.NewClient(
		grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to pgctld gRPC at %s: %w", grpcAddr, err)
	}
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize the data directory
	t.Logf("Initializing PostgreSQL data directory via gRPC at %s", grpcAddr)
	initResp, err := client.InitDataDir(ctx, &pb.InitDataDirRequest{})
	if err != nil {
		return fmt.Errorf("call toInitDataDir RPC failed: %w", err)
	}
	t.Logf("Init response: %s", initResp.Message)

	// Start PostgreSQL
	t.Logf("Starting PostgreSQL via gRPC at %s", grpcAddr)
	startResp, err := client.Start(ctx, &pb.StartRequest{})
	if err != nil {
		return fmt.Errorf("call to Start RPC failed: %w", err)
	}

	t.Logf("PostgreSQL started: PID=%d, Message=%s", startResp.Pid, startResp.Message)
	return nil
}

// InitPostgreSQLDataDir initializes the PostgreSQL data directory via pgctld gRPC
func InitPostgreSQLDataDir(t *testing.T, grpcAddr string) error {
	t.Helper()

	// Connect to pgctld gRPC
	conn, err := grpc.NewClient(
		grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to pgctld gRPC at %s: %w", grpcAddr, err)
	}
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Initialize the data directory
	t.Logf("Initializing PostgreSQL data directory via gRPC at %s", grpcAddr)
	initResp, err := client.InitDataDir(ctx, &pb.InitDataDirRequest{})
	if err != nil {
		return fmt.Errorf("InitDataDir RPC failed: %w", err)
	}
	t.Logf("Init response: %s", initResp.Message)
	return nil
}

// StartPostgreSQL starts PostgreSQL via pgctld gRPC
func StartPostgreSQL(t *testing.T, grpcAddr string) error {
	t.Helper()

	// Connect to pgctld gRPC
	conn, err := grpc.NewClient(
		grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to pgctld gRPC at %s: %w", grpcAddr, err)
	}
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start PostgreSQL
	t.Logf("Starting PostgreSQL via gRPC at %s", grpcAddr)
	startResp, err := client.Start(ctx, &pb.StartRequest{})
	if err != nil {
		return fmt.Errorf("call to Start RPC failed: %w", err)
	}

	t.Logf("PostgreSQL started: PID=%d, Message=%s", startResp.Pid, startResp.Message)
	return nil
}
