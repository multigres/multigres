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

package multipooler

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/test/endtoend"
	"github.com/multigres/multigres/go/test/utils"

	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// TestGetAuthCredentials_ExistingUser tests fetching SCRAM credentials for an existing PostgreSQL user.
func TestGetAuthCredentials_ExistingUser(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping test")
	}

	// Get shared test setup
	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	// Connect to multipooler via gRPC
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := multipoolerpb.NewMultiPoolerServiceClient(conn)

	// Create a test user with a password via SQL
	poolerClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
	require.NoError(t, err)
	defer poolerClient.Close()

	// Create user with SCRAM-SHA-256 password
	testUser := fmt.Sprintf("testuser_%d", time.Now().UnixNano())
	testPassword := "test_password_123"
	_, err = poolerClient.ExecuteQuery(context.Background(),
		fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", testUser, testPassword), 0)
	require.NoError(t, err)

	// Cleanup user after test
	t.Cleanup(func() {
		_, _ = poolerClient.ExecuteQuery(context.Background(),
			fmt.Sprintf("DROP USER IF EXISTS %s", testUser), 0)
	})

	// Test GetAuthCredentials
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &multipoolerpb.GetAuthCredentialsRequest{
		Database: "postgres",
		Username: testUser,
	}

	resp, err := client.GetAuthCredentials(ctx, req)
	require.NoError(t, err, "GetAuthCredentials should succeed")
	require.NotNil(t, resp)

	// Verify response
	assert.True(t, resp.UserExists, "user should exist")
	assert.NotEmpty(t, resp.ScramHash, "scram_hash should not be empty")
	assert.Equal(t, int32(1), resp.HashVersion, "hash_version should be 1")

	// Verify the hash format is SCRAM-SHA-256
	assert.True(t, strings.HasPrefix(resp.ScramHash, "SCRAM-SHA-256$"),
		"scram_hash should start with SCRAM-SHA-256$, got: %s", resp.ScramHash)
}

// TestGetAuthCredentials_NonExistentUser tests fetching credentials for a non-existent user.
func TestGetAuthCredentials_NonExistentUser(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping test")
	}

	// Get shared test setup
	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	// Connect to multipooler via gRPC
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := multipoolerpb.NewMultiPoolerServiceClient(conn)

	// Test GetAuthCredentials for non-existent user
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &multipoolerpb.GetAuthCredentialsRequest{
		Database: "postgres",
		Username: "nonexistent_user_xyz_12345",
	}

	resp, err := client.GetAuthCredentials(ctx, req)
	require.NoError(t, err, "GetAuthCredentials should succeed even for non-existent user")
	require.NotNil(t, resp)

	// Verify response indicates user doesn't exist
	assert.False(t, resp.UserExists, "user should not exist")
	assert.Empty(t, resp.ScramHash, "scram_hash should be empty for non-existent user")
}

// TestGetAuthCredentials_PostgresUser tests fetching credentials for the default postgres superuser.
func TestGetAuthCredentials_PostgresUser(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping test")
	}

	// Get shared test setup
	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	// Connect to multipooler via gRPC
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := multipoolerpb.NewMultiPoolerServiceClient(conn)

	// Test GetAuthCredentials for postgres user
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &multipoolerpb.GetAuthCredentialsRequest{
		Database: "postgres",
		Username: "postgres",
	}

	resp, err := client.GetAuthCredentials(ctx, req)
	require.NoError(t, err, "GetAuthCredentials should succeed")
	require.NotNil(t, resp)

	// The postgres user exists (created during initdb)
	assert.True(t, resp.UserExists, "postgres user should exist")
	// Note: The postgres user may not have a password set (trust auth in tests),
	// so we don't assert on ScramHash content
}

// TestGetAuthCredentials_InvalidRequest tests that invalid requests are rejected.
func TestGetAuthCredentials_InvalidRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping test")
	}

	// Get shared test setup
	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	// Connect to multipooler via gRPC
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := multipoolerpb.NewMultiPoolerServiceClient(conn)

	tests := []struct {
		name    string
		req     *multipoolerpb.GetAuthCredentialsRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "empty username",
			req: &multipoolerpb.GetAuthCredentialsRequest{
				Database: "postgres",
				Username: "",
			},
			wantErr: true,
			errMsg:  "username",
		},
		{
			name: "empty database",
			req: &multipoolerpb.GetAuthCredentialsRequest{
				Database: "",
				Username: "postgres",
			},
			wantErr: true,
			errMsg:  "database",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			resp, err := client.GetAuthCredentials(ctx, tt.req)

			if tt.wantErr {
				require.Error(t, err, "expected error for invalid request")
				assert.Nil(t, resp)
				assert.Contains(t, err.Error(), tt.errMsg,
					"error message should mention the invalid field")
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
			}
		})
	}
}
