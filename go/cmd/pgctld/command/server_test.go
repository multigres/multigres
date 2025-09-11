// Copyright 2025 The Multigres Authors.
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

package command

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pgctld"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
)

func TestPgCtldServiceStart(t *testing.T) {
	tests := []struct {
		name          string
		request       *pb.StartRequest
		setupDataDir  func(string) string
		setupBinaries bool
		expectError   bool
		errorContains string
		checkResponse func(*testing.T, *pb.StartResponse)
	}{
		{
			name: "start with uninitialized data dir should fail",
			request: &pb.StartRequest{
				Port:      5432,
				ExtraArgs: []string{},
			},
			setupDataDir: func(baseDir string) string {
				return testutil.CreateDataDir(t, baseDir, false)
			},
			setupBinaries: true,
			expectError:   true,
			errorContains: "data directory not initialized",
		},
		{
			name: "start already running server",
			request: &pb.StartRequest{
				Port: 5432,
			},
			setupDataDir: func(baseDir string) string {
				dataDir := testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, dataDir, 12345)
				return dataDir
			},
			setupBinaries: true,
			expectError:   false,
			checkResponse: func(t *testing.T, resp *pb.StartResponse) {
				assert.Contains(t, resp.Message, "already running")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_start_test")
			defer cleanup()

			tt.setupDataDir(baseDir)

			poolerDir := baseDir
			pgHost := "localhost"
			pgPort := 5432
			pgUser := "postgres"
			pgDatabase := "postgres"
			timeout := 30

			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)

				// Mock PATH
				t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
			}

			service, err := NewPgCtldService(testLogger(), pgHost, pgPort, pgUser, pgDatabase, timeout, poolerDir)
			require.NoError(t, err)

			resp, err := service.Start(context.Background(), tt.request)

			if tt.expectError {
				fmt.Println(resp)
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				if tt.checkResponse != nil {
					tt.checkResponse(t, resp)
				}
			}
		})
	}
}

func TestPgCtldServiceStart_MissingPoolerDir(t *testing.T) {
	t.Run("missing pooler-dir", func(t *testing.T) {
		// Don't set pooler-dir - should fail at service creation
		cleanupPooler := pgctld.SetPoolerDirForTest("")
		defer cleanupPooler()

		_, err := NewPgCtldService(testLogger(), "", 0, "", "", 0, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pooler-dir needs to be set")
	})
}

func TestPgCtldServiceStop(t *testing.T) {
	tests := []struct {
		name          string
		request       *pb.StopRequest
		setupDataDir  func(string) string
		setupBinaries bool
		expectError   bool
		errorContains string
		checkResponse func(*testing.T, *pb.StopResponse)
	}{
		{
			name: "successful stop",
			request: &pb.StopRequest{
				Mode:    "fast",
				Timeout: 30,
			},
			setupDataDir: func(baseDir string) string {
				dataDir := testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, dataDir, 12345)
				return dataDir
			},
			setupBinaries: true,
			expectError:   false,
			checkResponse: func(t *testing.T, resp *pb.StopResponse) {
				assert.Contains(t, resp.Message, "successfully")
			},
		},
		{
			name: "stop not running server",
			request: &pb.StopRequest{
				Mode: "fast",
			},
			setupDataDir: func(baseDir string) string {
				return testutil.CreateDataDir(t, baseDir, true) // No PID file
			},
			setupBinaries: false,
			expectError:   false,
			checkResponse: func(t *testing.T, resp *pb.StopResponse) {
				assert.Contains(t, resp.Message, "not running")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_stop_test")
			defer cleanup()

			poolerDir := baseDir
			pgHost := "localhost"
			pgPort := 5432
			pgUser := "postgres"
			pgDatabase := "postgres"
			timeout := 30

			_ = tt.setupDataDir(baseDir)

			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)
				t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
			}

			service, err := NewPgCtldService(testLogger(), pgHost, pgPort, pgUser, pgDatabase, timeout, poolerDir)
			require.NoError(t, err)

			resp, err := service.Stop(context.Background(), tt.request)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				if tt.checkResponse != nil {
					tt.checkResponse(t, resp)
				}
			}
		})
	}
}

func TestPgCtldServiceStatus(t *testing.T) {
	tests := []struct {
		name         string
		request      *pb.StatusRequest
		setupDataDir func(string) string
		expected     pb.ServerStatus
	}{
		{
			name:    "status not initialized",
			request: &pb.StatusRequest{},
			setupDataDir: func(baseDir string) string {
				return testutil.CreateDataDir(t, baseDir, false)
			},
			expected: pb.ServerStatus_NOT_INITIALIZED,
		},
		{
			name:    "status stopped",
			request: &pb.StatusRequest{},
			setupDataDir: func(baseDir string) string {
				return testutil.CreateDataDir(t, baseDir, true)
			},
			expected: pb.ServerStatus_STOPPED,
		},
		{
			name:    "status running",
			request: &pb.StatusRequest{},
			setupDataDir: func(baseDir string) string {
				dataDir := testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, dataDir, 12345)
				return dataDir
			},
			expected: pb.ServerStatus_RUNNING,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_status_test")
			defer cleanup()

			poolerDir := baseDir
			pgHost := "localhost"
			pgPort := 5432
			pgUser := "postgres"
			pgDatabase := "postgres"
			timeout := 30

			_ = tt.setupDataDir(baseDir)

			service, err := NewPgCtldService(testLogger(), pgHost, pgPort, pgUser, pgDatabase, timeout, poolerDir)
			require.NoError(t, err)

			resp, err := service.Status(context.Background(), tt.request)

			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, tt.expected, resp.Status)
			// TODO: This assertion needs to be updated when we fix this test in detail
			assert.Equal(t, int32(5432), resp.Port)
		})
	}
}

func TestPgCtldServiceRestart(t *testing.T) {
	t.Run("successful restart", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_restart_test")
		defer cleanup()

		dataDir := testutil.CreateDataDir(t, baseDir, true)
		testutil.CreatePIDFile(t, dataDir, 12345)

		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0o755))
		testutil.CreateMockPostgreSQLBinaries(t, binDir)
		t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

		poolerDir := baseDir
		pgHost := "localhost"
		pgPort := 5432
		pgUser := "postgres"
		pgDatabase := "postgres"
		timeout := 30

		service, err := NewPgCtldService(testLogger(), pgHost, pgPort, pgUser, pgDatabase, timeout, poolerDir)
		require.NoError(t, err)

		request := &pb.RestartRequest{
			Mode:    "fast",
			Timeout: 30,
			Port:    5432,
		}

		resp, err := service.Restart(context.Background(), request)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Contains(t, resp.Message, "restarted successfully")
	})
}

func TestPgCtldServiceReloadConfig(t *testing.T) {
	t.Run("successful reload", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_reload_test")
		defer cleanup()

		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0o755))
		testutil.CreateMockPostgreSQLBinaries(t, binDir)
		t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

		poolerDir := baseDir
		pgHost := "localhost"
		pgPort := 5432
		pgUser := "postgres"
		pgDatabase := "postgres"
		timeout := 30

		dataDir := testutil.CreateDataDir(t, baseDir, true)
		testutil.CreatePIDFile(t, dataDir, 12345)

		cleanupViper := SetupTestPgCtldCleanup(t)
		defer cleanupViper()

		service, err := NewPgCtldService(testLogger(), pgHost, pgPort, pgUser, pgDatabase, timeout, poolerDir)
		require.NoError(t, err)

		request := &pb.ReloadConfigRequest{}

		resp, err := service.ReloadConfig(context.Background(), request)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Contains(t, resp.Message, "reloaded successfully")
	})

	t.Run("reload when not running", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_reload_test")
		defer cleanup()

		poolerDir := baseDir
		pgHost := "localhost"
		pgPort := 5432
		pgUser := "postgres"
		pgDatabase := "postgres"
		timeout := 30

		cleanupPooler := pgctld.SetPoolerDirForTest(baseDir)
		defer cleanupPooler()

		testutil.CreateDataDir(t, baseDir, true)
		// No PID file = not running

		cleanupViper := SetupTestPgCtldCleanup(t)
		defer cleanupViper()

		service, err := NewPgCtldService(testLogger(), pgHost, pgPort, pgUser, pgDatabase, timeout, poolerDir)
		require.NoError(t, err)

		request := &pb.ReloadConfigRequest{}

		_, err = service.ReloadConfig(context.Background(), request)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "not running")
	})
}

func TestPgCtldServiceVersion(t *testing.T) {
	t.Run("successful version retrieval", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_version_test")
		defer cleanup()

		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0o755))
		testutil.CreateMockPostgreSQLBinaries(t, binDir)
		t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

		cleanupPooler := pgctld.SetPoolerDirForTest(baseDir)
		defer cleanupPooler()

		cleanupViper := SetupTestPgCtldCleanup(t)
		defer cleanupViper()

		poolerDir := baseDir
		service, err := NewPgCtldService(testLogger(), pgHost, pgPort, pgUser, pgDatabase, timeout, poolerDir)
		require.NoError(t, err)

		request := &pb.VersionRequest{
			Host:     "localhost",
			Port:     5432,
			Database: "postgres",
			User:     "postgres",
		}

		resp, err := service.Version(context.Background(), request)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Contains(t, resp.Version, "PostgreSQL")
	})
}

func TestPgCtldServiceInitDataDir(t *testing.T) {
	t.Run("successful initialization", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_init_test")
		defer cleanup()

		cleanupPooler := pgctld.SetPoolerDirForTest(baseDir)
		defer cleanupPooler()

		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0o755))
		testutil.CreateMockPostgreSQLBinaries(t, binDir)
		t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

		poolerDir := baseDir
		service, err := NewPgCtldService(testLogger(), pgHost, pgPort, pgUser, pgDatabase, timeout, poolerDir)
		require.NoError(t, err)

		request := &pb.InitDataDirRequest{
			AuthLocal: "trust",
			AuthHost:  "md5",
		}

		resp, err := service.InitDataDir(context.Background(), request)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Contains(t, resp.Message, "initialized successfully")
	})

	t.Run("already initialized", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_init_test")
		defer cleanup()

		cleanupPooler := pgctld.SetPoolerDirForTest(baseDir)
		defer cleanupPooler()

		_ = testutil.CreateDataDir(t, baseDir, true)

		poolerDir := baseDir
		service, err := NewPgCtldService(testLogger(), pgHost, pgPort, pgUser, pgDatabase, timeout, poolerDir)
		require.NoError(t, err)

		request := &pb.InitDataDirRequest{}

		resp, err := service.InitDataDir(context.Background(), request)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Contains(t, resp.Message, "already initialized")
	})
}

// testLogger returns a no-op logger for testing
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
