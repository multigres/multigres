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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/test/endtoend"
	"github.com/multigres/multigres/go/test/utils"

	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pgprotocol/scram"
)

// createTestUser creates a PostgreSQL user with the given password and registers
// cleanup to drop the user when the test completes. Returns the generated username.
func createTestUser(t *testing.T, client *endtoend.MultiPoolerTestClient, password string) string {
	t.Helper()

	username := fmt.Sprintf("testuser_%d", time.Now().UnixNano())
	_, err := client.ExecuteQuery(t.Context(),
		fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", username, password), 0)
	require.NoError(t, err, "failed to create test user")

	t.Cleanup(func() {
		_, _ = client.ExecuteQuery(context.Background(),
			fmt.Sprintf("DROP USER IF EXISTS %s", username), 0)
	})

	return username
}

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

	testUser := createTestUser(t, poolerClient, "test_password_123")

	// Test GetAuthCredentials
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	req := &multipoolerpb.GetAuthCredentialsRequest{
		Database: "postgres",
		Username: testUser,
	}

	resp, err := client.GetAuthCredentials(ctx, req)
	require.NoError(t, err, "GetAuthCredentials should succeed")
	require.NotNil(t, resp)

	// Verify the hash is valid and matches the password we set
	parsed, err := scram.ParseScramSHA256Hash(resp.ScramHash)
	require.NoError(t, err, "should be able to parse the SCRAM hash")

	// Recompute StoredKey from password and verify it matches
	saltedPassword := scram.ComputeSaltedPassword("test_password_123", parsed.Salt, parsed.Iterations)
	clientKey := scram.ComputeClientKey(saltedPassword)
	computedStoredKey := scram.ComputeStoredKey(clientKey)
	assert.Equal(t, parsed.StoredKey, computedStoredKey,
		"computed StoredKey should match the one in the hash (password verification)")
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
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	req := &multipoolerpb.GetAuthCredentialsRequest{
		Database: "postgres",
		Username: "nonexistent_user_xyz_12345",
	}

	_, err = client.GetAuthCredentials(ctx, req)
	require.Error(t, err, "GetAuthCredentials should return error for non-existent user")

	// Verify error is NotFound
	st, ok := status.FromError(err)
	require.True(t, ok, "error should be a gRPC status error")
	assert.Equal(t, codes.NotFound, st.Code(), "error code should be NotFound")
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
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	req := &multipoolerpb.GetAuthCredentialsRequest{
		Database: "postgres",
		Username: "postgres",
	}

	resp, err := client.GetAuthCredentials(ctx, req)
	require.NoError(t, err, "GetAuthCredentials should succeed for postgres user")
	require.NotNil(t, resp)
	// Note: The postgres user may not have a password set (trust auth in tests),
	// so we don't assert on ScramHash content - just that the call succeeds
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
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
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
