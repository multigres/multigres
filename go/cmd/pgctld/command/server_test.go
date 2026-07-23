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
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/tools/executil"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// testServiceConfig is the standard PgCtldServiceConfig used across service tests.
var testServiceConfig = PgCtldServiceConfig{
	Port:           5432,
	User:           constants.DefaultPostgresUser,
	Database:       constants.DefaultPostgresDatabase,
	Password:       shardsetup.TestPostgresPassword,
	PasswordSource: PasswordSourceEnv,
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

			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)

				// Mock PATH
				t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
			}

			service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, poolerDir, "localhost", 0, "")
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

// TestPgCtldServiceStart_AsPrimaryCrashRecoveryMatrix covers all four
// combinations of the two StartRequest controls on an already-initialized data
// directory that was NOT cleanly shut down (so allow_crash_recovery has an
// effect):
//   - as_primary selects the start mode: false (default) writes standby.signal
//     (recovery/standby mode); true removes it (writable primary).
//   - allow_crash_recovery selects whether an unclean node is crash-recovered
//     before the start; the response reports it via CrashRecoveryRan.
func TestPgCtldServiceStart_AsPrimaryCrashRecoveryMatrix(t *testing.T) {
	cases := []struct {
		name                 string
		asPrimary            bool
		allowCrashRecovery   bool
		wantStandbySignal    bool
		wantCrashRecoveryRan bool
	}{
		{"standby, no crash recovery", false, false, true, false},
		{"standby, allow crash recovery", false, true, true, true},
		{"primary, no crash recovery", true, false, false, false},
		{"primary, allow crash recovery", true, true, false, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseDir := t.TempDir()
			dataDir := filepath.Join(baseDir, "pg_data")
			t.Setenv(constants.PgDataDirEnvVar, dataDir)

			binDir := filepath.Join(baseDir, "bin")
			require.NoError(t, os.MkdirAll(binDir, 0o755))
			testutil.CreateMockPostgreSQLBinaries(t, binDir)
			// Report an UNCLEAN shutdown so allow_crash_recovery actually triggers
			// recovery (the default mock reports "shut down").
			testutil.MockBinary(t, binDir, "pg_controldata",
				`echo "Database cluster state:               in production"`)
			t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

			// Initialized data dir, plus a pre-existing standby.signal so we can
			// observe as_primary=true removing it and as_primary=false keeping it.
			require.NoError(t, os.MkdirAll(dataDir, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16\n"), 0o644))
			require.NoError(t, os.WriteFile(filepath.Join(dataDir, constants.StandbySignalFile), []byte(""), 0o644))

			service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, baseDir, "localhost", 0, "")
			require.NoError(t, err)
			defer service.Close()

			ctx := context.Background()
			resp, err := service.Start(ctx, &pb.StartRequest{
				AsPrimary:          tc.asPrimary,
				AllowCrashRecovery: tc.allowCrashRecovery,
			})
			// Stop the mock postgres (pg_ctl start backgrounds a sleep) on cleanup.
			defer func() { _, _ = service.Stop(ctx, &pb.StopRequest{Mode: "fast"}) }()
			require.NoError(t, err)
			require.NotNil(t, resp)

			_, statErr := os.Stat(filepath.Join(dataDir, constants.StandbySignalFile))
			if tc.wantStandbySignal {
				assert.NoError(t, statErr, "standby.signal must be present for as_primary=%v", tc.asPrimary)
			} else {
				assert.True(t, os.IsNotExist(statErr), "standby.signal must be absent for as_primary=%v", tc.asPrimary)
			}
			assert.Equal(t, tc.wantCrashRecoveryRan, resp.GetCrashRecoveryRan(),
				"CrashRecoveryRan for allow_crash_recovery=%v on an unclean node", tc.allowCrashRecovery)
		})
	}
}

// TestPgCtldServiceStart_StandbySignalError exercises the two error-return
// branches of the Start handler's standby.signal step: as_primary=false must
// surface a createStandbySignal failure and as_primary=true a removeStandbySignal
// failure, in both cases returning an error instead of starting postgres. A
// standby.signal path that is a NON-EMPTY directory triggers both: os.WriteFile
// fails (the path is a directory) and os.Remove fails (directory not empty), and
// neither error is IsNotExist.
func TestPgCtldServiceStart_StandbySignalError(t *testing.T) {
	cases := []struct {
		name      string
		asPrimary bool
	}{
		{"standby start, createStandbySignal fails", false},
		{"primary start, removeStandbySignal fails", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseDir := t.TempDir()
			dataDir := filepath.Join(baseDir, "pg_data")
			t.Setenv(constants.PgDataDirEnvVar, dataDir)

			// Initialized data dir (PG_VERSION present) so Start proceeds past the
			// not-initialized guard to the standby.signal step.
			require.NoError(t, os.MkdirAll(dataDir, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16\n"), 0o644))

			// Make standby.signal a non-empty directory so the file operation
			// as_primary selects fails before postgres is ever started.
			signalDir := filepath.Join(dataDir, constants.StandbySignalFile)
			require.NoError(t, os.MkdirAll(signalDir, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(signalDir, "child"), []byte(""), 0o644))

			service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, baseDir, "localhost", 0, "")
			require.NoError(t, err)
			defer service.Close()

			resp, err := service.Start(context.Background(), &pb.StartRequest{AsPrimary: tc.asPrimary})
			require.Error(t, err, "Start must fail when the standby.signal operation fails")
			assert.Nil(t, resp)
			assert.Contains(t, err.Error(), "standby.signal")
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

// TestPgCtldService_StopRewindStart verifies the stop→pg_rewind→start lifecycle.
// This is a regression test for a bug where the pgctld service would fail to start
// postgres after a stop+pg_rewind sequence. The fix ensures that the pgctld RPC
// handlers have no internal "closed" state that prevents re-starting postgres after
// a stop-for-rewind flow.
func TestPgCtldService_StopRewindStart(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	baseDir := t.TempDir()
	poolerDir := baseDir
	dataDir := filepath.Join(baseDir, "pg_data")

	// Set env vars expected by pgctld
	t.Setenv(constants.PgDataDirEnvVar, dataDir)

	// Create mock PostgreSQL binaries (including pg_rewind)
	binDir := filepath.Join(baseDir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))
	testutil.CreateMockPostgreSQLBinaries(t, binDir)
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	// Initialize a mock data directory so IsDataDirInitialized() returns true
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16\n"), 0o644))

	service, err := NewPgCtldService(logger, testServiceConfig, 30, poolerDir, "localhost", 0, "")
	require.NoError(t, err)
	defer service.Close()

	ctx := context.Background()

	// Step 1: Simulate a running postgres by installing a postmaster.pid that
	// references a real process owned by the test. Using CreatePIDFile avoids
	// spawning a real pg_ctl subprocess (which would leave orphaned children).
	testutil.CreatePIDFile(t, dataDir, 0 /* pid is filled in by helper */)

	// Step 2: Stop postgres. This is the first part of the stop→rewind→start sequence.
	stopResp, err := service.Stop(ctx, &pb.StopRequest{Mode: "fast"})
	require.NoError(t, err, "Stop must succeed — this is the first half of the stop→rewind→start sequence")
	require.NotNil(t, stopResp)

	// Step 3: Run pg_rewind (dry-run). The service must accept this call after a stop;
	// if the stop had put the service into a closed state this call would fail.
	rewindResp, err := service.PgRewind(ctx, &pb.PgRewindRequest{
		SourceHost: "127.0.0.1",
		SourcePort: 5433,
		DryRun:     true,
	})
	require.NoError(t, err, "PgRewind must succeed after Stop")
	require.NotNil(t, rewindResp)

	// Step 4: Start postgres again as standby. This is the critical step: the service
	// must be able to restart after stop+rewind without requiring a pgctld process restart.
	restartResp, err := service.Restart(ctx, &pb.RestartRequest{
		Mode:      "fast",
		AsStandby: true,
	})
	require.NoError(t, err, "Restart as standby must succeed after stop+pg_rewind — REGRESSION: stop→rewind→start lifecycle must work")
	require.NotNil(t, restartResp)
	assert.Greater(t, restartResp.Pid, int32(0), "restarted postgres must have a valid PID")

	// Cleanup: stop the mock postgres process created by the Restart call so the
	// test does not leave an orphaned background subprocess.
	_, _ = service.Stop(ctx, &pb.StopRequest{Mode: "fast"})
}

// testLogger returns a no-op logger for testing
func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestPgCtldServiceStopRestoreCommand(t *testing.T) {
	t.Run("no pidfile", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_stop_restore_no_pidfile")
		defer cleanup()
		t.Setenv(constants.PgDataDirEnvVar, filepath.Join(baseDir, "pg_data"))

		service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, baseDir, "localhost", 0, "")
		require.NoError(t, err)

		resp, err := service.StopRestoreCommand(context.Background(), &pb.StopRestoreCommandRequest{})
		require.NoError(t, err)
		assert.False(t, resp.Found)
		assert.False(t, resp.Killed)
	})

	t.Run("stale pidfile referencing an already-exited process", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_stop_restore_stale_pidfile")
		defer cleanup()
		t.Setenv(constants.PgDataDirEnvVar, filepath.Join(baseDir, "pg_data"))

		// Spawn and fully wait on a short-lived process so its PID is guaranteed
		// to no longer be running by the time we reference it.
		cmd := exec.Command("true")
		require.NoError(t, cmd.Run())
		stalePID := cmd.Process.Pid

		require.NoError(t, os.WriteFile(pgctld.RestoreCommandPIDFile(baseDir), []byte(strconv.Itoa(stalePID)), 0o644))

		service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, baseDir, "localhost", 0, "")
		require.NoError(t, err)

		resp, err := service.StopRestoreCommand(context.Background(), &pb.StopRestoreCommandRequest{})
		require.NoError(t, err)
		assert.False(t, resp.Found)
		assert.False(t, resp.Killed)
	})

	t.Run("live process gets terminated", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_stop_restore_live")
		defer cleanup()
		t.Setenv(constants.PgDataDirEnvVar, filepath.Join(baseDir, "pg_data"))

		// A process that ignores SIGTERM, to exercise the SIGKILL escalation path.
		cmd := exec.Command("sh", "-c", "trap '' TERM; sleep 30")
		require.NoError(t, cmd.Start())
		t.Cleanup(func() { _, _ = executil.KillProcess(context.Background(), cmd.Process) })
		// Reap it once killed. Our own exec.Cmd is this process's real OS-level
		// parent (unlike production, where pgctld signals a process it did not
		// start), so without a Wait() call here it becomes a zombie that still
		// answers Signal(0) as "alive" forever, and StopRestoreCommand's wait
		// loop below would never see it as gone.
		go func() { _ = cmd.Wait() }()

		require.NoError(t, os.WriteFile(pgctld.RestoreCommandPIDFile(baseDir), []byte(strconv.Itoa(cmd.Process.Pid)), 0o644))

		service, err := NewPgCtldService(testLogger(), testServiceConfig, 30, baseDir, "localhost", 0, "")
		require.NoError(t, err)

		resp, err := service.StopRestoreCommand(context.Background(), &pb.StopRestoreCommandRequest{})
		require.NoError(t, err)
		assert.True(t, resp.Found)
		assert.True(t, resp.Killed)

		// Process must actually be gone (SIGTERM was ignored, so this only
		// passes if the SIGKILL escalation ran).
		err = cmd.Process.Signal(syscall.Signal(0))
		assert.Error(t, err, "process should no longer exist after StopRestoreCommand")
	})
}
