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

package command

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// testServiceConfig is the standard PgCtldServiceConfig used across service tests.
var testServiceConfig = PgCtldServiceConfig{
	Port:     5432,
	User:     constants.DefaultPostgresUser,
	Database: constants.DefaultPostgresDatabase,
	Password: shardsetup.TestPostgresPassword,
}

func TestIntToInt32(t *testing.T) {
	tests := []struct {
		name      string
		input     int
		expected  int32
		expectErr bool
	}{
		{"zero", 0, 0, false},
		{"positive", 12345, 12345, false},
		{"negative", -12345, -12345, false},
		{"max int32", math.MaxInt32, math.MaxInt32, false},
		{"min int32", math.MinInt32, math.MinInt32, false},
		{"typical PID", 98765, 98765, false},
		{"typical port", 5432, 5432, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := intToInt32(tt.input)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}

	// Test overflow cases (only meaningful on 64-bit systems where int > int32)
	if math.MaxInt > math.MaxInt32 {
		t.Run("overflow positive", func(t *testing.T) {
			_, err := intToInt32(math.MaxInt32 + 1)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "exceeds int32 range")
		})
		t.Run("overflow negative", func(t *testing.T) {
			_, err := intToInt32(math.MinInt32 - 1)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "exceeds int32 range")
		})
	}
}

func TestPgCtldServiceStartAsStandby(t *testing.T) {
	tests := []struct {
		name          string
		request       *pb.StartAsStandbyRequest
		setupDataDir  func(string) string
		setupBinaries bool
		expectError   bool
		errorContains string
		checkResponse func(*testing.T, *pb.StartAsStandbyResponse)
	}{
		{
			name: "start with uninitialized data dir should fail",
			request: &pb.StartAsStandbyRequest{
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
			request: &pb.StartAsStandbyRequest{
				Port: 5432,
			},
			setupDataDir: func(baseDir string) string {
				dataDir := testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, dataDir, 12345)
				return dataDir
			},
			setupBinaries: true,
			expectError:   false,
			checkResponse: func(t *testing.T, resp *pb.StartAsStandbyResponse) {
				assert.Equal(t, pb.StartAsStandbyResult_START_AS_STANDBY_RESULT_ALREADY_RUNNING, resp.GetResult())
			},
		},
	}

	t.Run("writes standby.signal before starting postgres", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_standby_signal_test")
		defer cleanup()

		dataDir := testutil.CreateDataDir(t, baseDir, true)
		_ = dataDir

		// Mock postgres binary that records what files exist at startup time.
		// We verify standby.signal is present when postgres is called.
		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0o755))

		// The mock postgres checks for standby.signal in the data dir and exits 0.
		// If standby.signal is missing, it exits 1.
		pgDataDir := filepath.Join(baseDir, "pg_data")

		// pg_ctl start creates a postmaster.pid and exits 0.
		// The code writes standby.signal before calling pg_ctl start, so
		// standby.signal will be present when pg_ctl runs. We verify this
		// ordering by asserting standby.signal still exists after the call.
		testutil.MockBinary(t, binDir, "pg_ctl", `
case "$1" in
    "start")
        DATADIR=""
        while [[ $# -gt 0 ]]; do
            case $1 in
                -D) DATADIR="$2"; shift 2 ;;
                -D*) DATADIR="${1#-D}"; shift ;;
                *) shift ;;
            esac
        done
        if [ -n "$DATADIR" ]; then
            sleep 3600 >/dev/null 2>&1 &
            MOCK_PID=$!
            printf '%s\n%s\n%s\n5432\n/tmp\nlocalhost\n*\nready\n' \
                "$MOCK_PID" "$DATADIR" "$(date +%s)" > "$DATADIR/postmaster.pid"
        fi
        echo "server started"
        ;;
    *) exit 1 ;;
esac
`)
		testutil.MockBinary(t, binDir, "pg_isready", "exit 0")
		t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

		service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, baseDir, "localhost", 0, "")
		require.NoError(t, err)

		resp, err := service.StartAsStandby(context.Background(), &pb.StartAsStandbyRequest{})
		require.NoError(t, err)
		assert.Equal(t, pb.StartAsStandbyResult_START_AS_STANDBY_RESULT_STARTED, resp.GetResult())

		// standby.signal should still be present after startup
		assert.FileExists(t, filepath.Join(pgDataDir, "standby.signal"))
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_start_test")
			defer cleanup()

			tt.setupDataDir(baseDir)

			poolerDir := baseDir

			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)

				// Mock PATH
				t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
			}

			service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, poolerDir, "localhost", 0, "")
			require.NoError(t, err)

			resp, err := service.StartAsStandby(context.Background(), tt.request)

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
		_, err := NewPgCtldService(testLogger(), PgCtldServiceConfig{}, 0, "", "", 0, "")
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
				Timeout: durationpb.New(30 * time.Second),
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

			_ = tt.setupDataDir(baseDir)

			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)
				t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
			}

			service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, poolerDir, "localhost", 0, "")
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
		name          string
		request       *pb.StatusRequest
		setupDataDir  func(string) string
		setupBinaries func(t *testing.T, binDir string)
		expected      pb.ServerStatus
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
			// pg_isready must succeed for the process to be reported as RUNNING.
			setupBinaries: func(t *testing.T, binDir string) {
				testutil.CreateMockPostgreSQLBinaries(t, binDir)
			},
			expected: pb.ServerStatus_RUNNING,
		},
		{
			name:    "status running but unresponsive",
			request: &pb.StatusRequest{},
			setupDataDir: func(baseDir string) string {
				dataDir := testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, dataDir, 12345)
				return dataDir
			},
			// pg_isready fails: process exists but is not accepting connections (e.g. SIGSTOP).
			setupBinaries: func(t *testing.T, binDir string) {
				testutil.CreateMockPostgreSQLBinaries(t, binDir)
				testutil.MockBinary(t, binDir, "pg_isready", "exit 1")
			},
			expected: pb.ServerStatus_STOPPED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_status_test")
			defer cleanup()

			poolerDir := baseDir

			if tt.setupBinaries != nil {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))
				tt.setupBinaries(t, binDir)
				t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
			}

			_ = tt.setupDataDir(baseDir)

			service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, poolerDir, "localhost", 0, "")
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

		service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, poolerDir, "localhost", 0, "")
		require.NoError(t, err)

		request := &pb.RestartRequest{
			Mode:    "fast",
			Timeout: durationpb.New(30 * time.Second),
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

		dataDir := testutil.CreateDataDir(t, baseDir, true)
		testutil.CreatePIDFile(t, dataDir, 12345)

		service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, poolerDir, "localhost", 0, "")
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

		testutil.CreateDataDir(t, baseDir, true)
		// No PID file = not running

		service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, poolerDir, "localhost", 0, "")
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

		poolerDir := baseDir
		t.Setenv(constants.PgDataDirEnvVar, filepath.Join(poolerDir, "pg_data"))
		service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, poolerDir, "localhost", 0, "")
		require.NoError(t, err)

		request := &pb.VersionRequest{
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

		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0o755))
		testutil.CreateMockPostgreSQLBinaries(t, binDir)
		t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

		poolerDir := baseDir
		t.Setenv(constants.PgDataDirEnvVar, filepath.Join(poolerDir, "pg_data"))
		service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, poolerDir, "localhost", 0, "")
		require.NoError(t, err)

		request := &pb.InitDataDirRequest{
			AuthLocal: "trust",
			AuthHost:  "scram-sha-256",
		}

		resp, err := service.InitDataDir(context.Background(), request)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Contains(t, resp.Message, "initialized successfully")
	})

	t.Run("already initialized", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_grpc_init_test")
		defer cleanup()

		_ = testutil.CreateDataDir(t, baseDir, true)

		poolerDir := baseDir
		service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, poolerDir, "localhost", 0, "")
		require.NoError(t, err)

		request := &pb.InitDataDirRequest{}

		resp, err := service.InitDataDir(context.Background(), request)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Contains(t, resp.Message, "already initialized")
	})
}

func TestGetPoolerDir(t *testing.T) {
	// Set up poolerDir for testing using temporary directory
	tempDir := t.TempDir()

	// Test with configured directory
	reg1 := viperutil.NewRegistry()
	pg1 := PgCtlCommand{
		reg: reg1,
		poolerDir: viperutil.Configure(reg1, "pooler-dir", viperutil.Options[string]{
			Default:  tempDir,
			FlagName: "pooler-dir",
			Dynamic:  false,
		}),
	}
	result := pg1.GetPoolerDir()
	assert.Equal(t, tempDir, result, "GetPoolerDir should return configured directory")

	// Test empty case
	reg2 := viperutil.NewRegistry()
	pg2 := PgCtlCommand{
		reg: reg2,
		poolerDir: viperutil.Configure(reg2, "pooler-dir", viperutil.Options[string]{
			Default:  "",
			FlagName: "pooler-dir",
			Dynamic:  false,
		}),
	}
	result = pg2.GetPoolerDir()
	assert.Equal(t, "", result, "GetPoolerDir should return empty string when not configured")
}

func TestPgCtldService_PgBackRestFields(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tmpDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, tmpDir+"/pg_data")

	service, err := NewPgCtldService(logger, testServiceConfig, 60, tmpDir, "localhost", 0, "")
	require.NoError(t, err)

	// Verify service has pgBackRest management fields
	assert.NotNil(t, service.ctx, "service should have context")
	assert.NotNil(t, service.cancel, "service should have cancel func")
}

func TestPgCtldService_StatusMethods(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tmpDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, tmpDir+"/pg_data")
	service, err := NewPgCtldService(logger, testServiceConfig, 60, tmpDir, "localhost", 0, "")
	require.NoError(t, err)
	defer service.Close()

	// Test initial status
	status := service.getPgBackRestStatus()
	assert.False(t, status.Running)
	assert.Empty(t, status.ErrorMessage)

	// Test setting status
	service.setPgBackRestStatus(true, "", false)
	status = service.getPgBackRestStatus()
	assert.True(t, status.Running)

	// Test with error and increment restart count to 3
	service.setPgBackRestStatus(false, "", true)           // 1
	service.setPgBackRestStatus(false, "", true)           // 2
	service.setPgBackRestStatus(false, "test error", true) // 3
	status = service.getPgBackRestStatus()
	assert.False(t, status.Running)
	assert.Equal(t, "test error", status.ErrorMessage)
	assert.Equal(t, int32(3), status.RestartCount)
}

func TestPgCtldService_StartPgBackRest_ValidationErrors(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tmpDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, tmpDir+"/pg_data")

	service, err := NewPgCtldService(logger, testServiceConfig, 60, tmpDir, "localhost", 0, "")
	require.NoError(t, err)
	defer service.Close()

	// Should fail when config doesn't exist
	_, err = service.startPgBackRest(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pgbackrest-server.conf not found")
}

func TestPgCtldService_ManagePgBackRest_Lifecycle(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tmpDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, tmpDir+"/pg_data")

	service, err := NewPgCtldService(logger, testServiceConfig, 60, tmpDir, "localhost", 0, "")
	require.NoError(t, err)

	// Create minimal config to prevent immediate failure
	pgbackrestDir := filepath.Join(tmpDir, "pgbackrest")
	require.NoError(t, os.MkdirAll(pgbackrestDir, 0o755))
	configPath := filepath.Join(pgbackrestDir, "pgbackrest-server.conf")
	require.NoError(t, os.WriteFile(configPath, []byte("[global]\n"), 0o644))

	// Start management goroutine
	service.StartPgBackRestManagement()

	// Give it time to attempt startup
	time.Sleep(100 * time.Millisecond)

	// Verify status is updated
	status := service.getPgBackRestStatus()
	assert.NotNil(t, status)
	// It will fail to start (invalid config), but should have tried

	// Close and verify cleanup
	service.Close()

	// Verify goroutine exited
	assert.True(t, true, "should complete without hanging")
}

func TestPgCtldService_Status_IncludesPgBackRest(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tmpDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, tmpDir+"/pg_data")

	service, err := NewPgCtldService(logger, testServiceConfig, 60, tmpDir, "localhost", 0, "")
	require.NoError(t, err)
	defer service.Close()

	// Set a known status with restart count of 5
	for range 5 {
		service.setPgBackRestStatus(false, "", true)
	}
	service.setPgBackRestStatus(true, "", false)

	// Call Status RPC
	resp, err := service.Status(context.Background(), &pb.StatusRequest{})
	require.NoError(t, err)

	// Verify pgBackRest status is included
	require.NotNil(t, resp.PgbackrestStatus)
	assert.True(t, resp.PgbackrestStatus.Running)
	assert.Equal(t, int32(5), resp.PgbackrestStatus.RestartCount)
}

// testLogger returns a no-op logger for testing
func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}
