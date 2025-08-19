/*
Copyright 2025 The Multigres Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package command

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	pb "github.com/multigres/multigres/go/pb"
)

func TestPgCtldService_Start(t *testing.T) {
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
			name: "successful start with uninitialized data dir",
			request: &pb.StartRequest{
				Port:       5432,
				SocketDir:  "/tmp",
				ConfigFile: "",
				ExtraArgs:  []string{},
			},
			setupDataDir: func(baseDir string) string {
				return testutil.CreateDataDir(t, baseDir, false)
			},
			setupBinaries: true,
			expectError:   false,
			checkResponse: func(t *testing.T, resp *pb.StartResponse) {
				assert.NotZero(t, resp.Pid)
				assert.Contains(t, resp.Message, "successfully")
			},
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
		{
			name: "missing data directory",
			request: &pb.StartRequest{
				DataDir: "", // Will be empty, triggering error
			},
			setupDataDir: func(baseDir string) string {
				return "" // Return empty to trigger error
			},
			setupBinaries: false,
			expectError:   true,
			errorContains: "data-dir is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_start_test")
			defer cleanup()

			dataDir := tt.setupDataDir(baseDir)

			// Set data dir in request if not specified
			if tt.request.DataDir == "" && dataDir != "" {
				tt.request.DataDir = dataDir
			}

			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)

				// Mock PATH
				t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
			}

			cleanupViper := testutil.SetupTestViper(t, dataDir)
			defer cleanupViper()

			service := &PgCtldService{
				logger: testLogger(),
			}

			resp, err := service.Start(context.Background(), tt.request)

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

func TestPgCtldService_Stop(t *testing.T) {
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

			dataDir := tt.setupDataDir(baseDir)
			tt.request.DataDir = dataDir

			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)
				t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
			}

			cleanupViper := testutil.SetupTestViper(t, dataDir)
			defer cleanupViper()

			service := &PgCtldService{
				logger: testLogger(),
			}

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

func TestPgCtldService_Status(t *testing.T) {
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

			dataDir := tt.setupDataDir(baseDir)
			tt.request.DataDir = dataDir

			cleanupViper := testutil.SetupTestViper(t, dataDir)
			defer cleanupViper()

			service := &PgCtldService{
				logger: testLogger(),
			}

			resp, err := service.Status(context.Background(), tt.request)

			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, tt.expected, resp.Status)
			assert.Equal(t, dataDir, resp.DataDir)
			assert.Equal(t, int32(5432), resp.Port)
		})
	}
}

func TestPgCtldService_Restart(t *testing.T) {
	t.Run("successful restart", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_restart_test")
		defer cleanup()

		dataDir := testutil.CreateDataDir(t, baseDir, true)
		testutil.CreatePIDFile(t, dataDir, 12345)

		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0755))
		testutil.CreateMockPostgreSQLBinaries(t, binDir)
		t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

		cleanupViper := testutil.SetupTestViper(t, dataDir)
		defer cleanupViper()

		service := &PgCtldService{
			logger: testLogger(),
		}

		request := &pb.RestartRequest{
			DataDir: dataDir,
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

func TestPgCtldService_ReloadConfig(t *testing.T) {
	t.Run("successful reload", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_reload_test")
		defer cleanup()

		dataDir := testutil.CreateDataDir(t, baseDir, true)
		testutil.CreatePIDFile(t, dataDir, 12345)

		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0755))
		testutil.CreateMockPostgreSQLBinaries(t, binDir)
		t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

		cleanupViper := testutil.SetupTestViper(t, dataDir)
		defer cleanupViper()

		service := &PgCtldService{
			logger: testLogger(),
		}

		request := &pb.ReloadConfigRequest{
			DataDir: dataDir,
		}

		resp, err := service.ReloadConfig(context.Background(), request)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Contains(t, resp.Message, "reloaded successfully")
	})

	t.Run("reload when not running", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_reload_test")
		defer cleanup()

		dataDir := testutil.CreateDataDir(t, baseDir, true)
		// No PID file = not running

		cleanupViper := testutil.SetupTestViper(t, dataDir)
		defer cleanupViper()

		service := &PgCtldService{
			logger: testLogger(),
		}

		request := &pb.ReloadConfigRequest{
			DataDir: dataDir,
		}

		_, err := service.ReloadConfig(context.Background(), request)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "not running")
	})
}

func TestPgCtldService_Version(t *testing.T) {
	t.Run("successful version retrieval", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_version_test")
		defer cleanup()

		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0755))
		testutil.CreateMockPostgreSQLBinaries(t, binDir)
		t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

		cleanupViper := testutil.SetupTestViper(t, "")
		defer cleanupViper()

		service := &PgCtldService{
			logger: testLogger(),
		}

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

func TestPgCtldService_InitDataDir(t *testing.T) {
	t.Run("successful initialization", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_init_test")
		defer cleanup()

		dataDir := filepath.Join(baseDir, "data")

		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0755))
		testutil.CreateMockPostgreSQLBinaries(t, binDir)
		t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

		service := &PgCtldService{
			logger: testLogger(),
		}

		request := &pb.InitDataDirRequest{
			DataDir:   dataDir,
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

		dataDir := testutil.CreateDataDir(t, baseDir, true) // Already initialized

		service := &PgCtldService{
			logger: testLogger(),
		}

		request := &pb.InitDataDirRequest{
			DataDir: dataDir,
		}

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
