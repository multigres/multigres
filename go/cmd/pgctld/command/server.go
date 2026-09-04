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
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/tools/ctxutil"
	"github.com/multigres/multigres/go/tools/executil"
	"github.com/multigres/multigres/go/tools/retry"
	"github.com/multigres/multigres/go/tools/viperutil"

	"github.com/spf13/cobra"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// intToInt32 safely converts int to int32 for protobuf fields.
// Returns an error if value exceeds int32 range (should never happen for PIDs/ports).
func intToInt32(v int) (int32, error) {
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, fmt.Errorf("value %d exceeds int32 range", v)
	}
	return int32(v), nil
}

// PgCtldServerCmd holds the server command configuration
type PgCtldServerCmd struct {
	pgCtlCmd          *PgCtlCommand
	grpcServer        *servenv.GrpcServer
	senv              *servenv.ServEnv
	pgbackrestPort    viperutil.Value[int]
	pgbackrestCertDir viperutil.Value[string]
}

// AddServerCommand adds the server subcommand to the root command
func AddServerCommand(root *cobra.Command, pc *PgCtlCommand) {
	serverCmd := &PgCtldServerCmd{
		pgCtlCmd:   pc,
		grpcServer: servenv.NewGrpcServer(pc.reg),
		senv:       servenv.NewServEnvWithConfig(pc.reg, pc.lg, pc.vc, pc.telemetry),
		pgbackrestPort: viperutil.Configure(pc.reg, "pgbackrest-port", viperutil.Options[int]{
			Default:  0,
			FlagName: "pgbackrest-port",
			Dynamic:  false,
		}),
		pgbackrestCertDir: viperutil.Configure(pc.reg, "pgbackrest-cert-dir", viperutil.Options[string]{
			Default:  "",
			FlagName: "pgbackrest-cert-dir",
			Dynamic:  false,
		}),
	}
	serverCmd.senv.InitServiceMap("grpc", constants.ServicePgctld)
	root.AddCommand(serverCmd.createCommand())
}

// validateServerFlags validates required flags for the server command
func (s *PgCtldServerCmd) validateServerFlags(cmd *cobra.Command, args []string) error {
	if err := s.senv.CobraPreRunE(cmd); err != nil {
		return err
	}
	return s.pgCtlCmd.validateGlobalFlags(cmd, args)
}

func (s *PgCtldServerCmd) createCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "server",
		Short:   "Run pgctld as a gRPC server daemon",
		Long:    `Run pgctld as a background gRPC server daemon to handle PostgreSQL management requests.`,
		RunE:    s.runServer,
		Args:    cobra.NoArgs,
		PreRunE: s.validateServerFlags,
	}

	s.grpcServer.RegisterFlags(cmd.Flags())
	// Don't register logger and viper config flags since they're already registered
	// as persistent flags in the root command and we're sharing those instances
	s.senv.RegisterFlagsWithoutLoggerAndConfig(cmd.Flags())

	// pgBackRest TLS server flags (server command only)
	cmd.Flags().Int("pgbackrest-port", s.pgbackrestPort.Default(), "pgBackRest TLS server port")
	cmd.Flags().String("pgbackrest-cert-dir", s.pgbackrestCertDir.Default(), "Directory containing ca.crt, pgbackrest.crt, pgbackrest.key")
	viperutil.BindFlags(cmd.Flags(), s.pgbackrestPort, s.pgbackrestCertDir)

	return cmd
}

func (s *PgCtldServerCmd) runServer(cmd *cobra.Command, args []string) error {
	// TODO(dweitzman): Add ServiceInstanceID and Cell that relate to the multipooler this pgctld serves
	if err := s.senv.Init(servenv.ServiceIdentity{
		ServiceName: constants.ServicePgctld,
	}); err != nil {
		return fmt.Errorf("servenv init: %w", err)
	}

	// Get the configured logger
	logger := s.senv.GetLogger()

	// Create and register our service
	poolerDir := s.pgCtlCmd.GetPoolerDir()
	pgbackrestPort := s.pgbackrestPort.Get()
	pgbackrestCertDir := s.pgbackrestCertDir.Get()

	pgctldConfig, err := s.pgCtlCmd.buildServiceConfig()
	if err != nil {
		return err
	}

	pgctldService, err := NewPgCtldService(
		logger,
		pgctldConfig,
		s.pgCtlCmd.timeout.Get(),
		poolerDir,
		s.pgCtlCmd.pgListenAddresses.Get(),
		pgbackrestPort,
		pgbackrestCertDir,
	)
	if err != nil {
		return err
	}

	// Register /ready probe: ready iff the gRPC control plane is accepting
	// connections. Postgres/replication health is intentionally excluded — a
	// pod whose postgres is down must stay reachable and must NOT be pulled
	// from Service endpoints / DNS, so operators and the control plane can
	// still observe and drive it back to health. Postgres liveness is signalled
	// out-of-band (health stream), not via this probe.
	grpcSocketPath := s.grpcServer.SocketFile()
	grpcBindAddress := s.grpcServer.BindAddress()
	grpcPort := s.grpcServer.Port()

	s.senv.RegisterReadyCheck(func() error {
		if !grpcAccepting(grpcSocketPath, grpcBindAddress, grpcPort) {
			return errors.New("grpc not accepting")
		}
		return nil
	})

	s.senv.OnRun(func() {
		logger.Info(
			"pgctld server starting up",
			"grpc_port", s.grpcServer.Port(),
			"http_port", s.senv.GetHTTPPort(),
		)

		// Start reaping the postmaster to prevent a zombie. Only needed when
		// pgctld is PID 1 (container init): `pg_ctl start -W` forks the postmaster
		// and exits, so it is reparented to pgctld and must be wait()ed on. When
		// pgctld is NOT PID 1 (tests, CI, systemd) orphans reparent to the system
		// init instead, so no reaper is needed. See childReaper for why it tracks
		// exact PIDs rather than using Wait4(-1).
		if os.Getpid() == 1 {
			pgctldService.reaper = newChildReaper(logger)
			go pgctldService.reaper.Run()
		}

		// Start pgBackRest management
		pgctldService.StartPgBackRestManagement()

		// Register gRPC service with the global GRPCServer
		if s.grpcServer.CheckServiceMap(constants.ServicePgctld, s.senv) {
			pb.RegisterPgCtldServer(s.grpcServer.Server, pgctldService)
		}
	})

	s.senv.OnClose(func() {
		logger.Info("pgctld server shutting down")
		pgctldService.Close()
	})

	return s.senv.RunDefault(s.grpcServer)
}

// PgCtldServiceConfig holds the PostgreSQL instance identity and initialization
// parameters. These are the most commonly passed parameters and are grouped
// here to reduce argument lists.
type PgCtldServiceConfig struct {
	Port     int
	User     string
	Database string
	Password string
	// PasswordSource records where Password came from so log lines can report
	// it without leaking the value itself. Optional; defaults to none.
	PasswordSource PasswordSource
	// PasswordFile is the absolute path to the operator-supplied password file
	// when PasswordSource == PasswordSourceFile, otherwise "". initdb is handed
	// this path directly via --pwfile so the plaintext never lands in /tmp.
	PasswordFile         string
	InitdbArgs           string
	InitdbSQLFiles       []string
	InitdbSQLDirs        []string
	InitdbExtraConfFiles []string
	// InitSecretsFile is the path to a mounted JSON file of per-project day-0
	// state (role passwords/verifiers and database settings) applied during the
	// transient init phase. Empty when the feature is unused.
	InitSecretsFile string
}

// PgCtldService implements the pgctld gRPC service
type PgCtldService struct {
	pb.UnimplementedPgCtldServer
	logger     *slog.Logger
	ctldConfig PgCtldServiceConfig
	timeout    int
	poolerDir  string
	pgConfig   *pgctld.PostgresCtlConfig

	// pgBackRest management
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	pgBackRestCmd    *executil.Cmd
	pgBackRestStatus *pb.PgBackRestStatus
	statusMu         sync.RWMutex
	restartCount     int32
	metrics          *Metrics

	// reaper reaps the postmaster when pgctld runs as PID 1. Nil otherwise.
	reaper *childReaper
}

// pgbackrestServerConfigPath returns the path to the pgbackrest server config file.
func (s *PgCtldService) pgbackrestServerConfigPath() string {
	return filepath.Join(s.poolerDir, "pgbackrest", "pgbackrest-server.conf")
}

// NewPgCtldService creates a new PgCtldService with validation
func NewPgCtldService(
	logger *slog.Logger,
	cfg PgCtldServiceConfig,
	timeout int,
	poolerDir string,
	listenAddresses string,
	pgbackrestPort int,
	pgbackrestCertDir string,
) (*PgCtldService, error) {
	// Validate essential parameters for service creation
	// Note: We don't validate postgresDataDir or postgresConfigFile existence here
	// because the server should be able to start even with uninitialized data directory
	if poolerDir == "" {
		return nil, errors.New("pooler-dir needs to be set")
	}
	if cfg.Port == 0 {
		return nil, errors.New("pg-port needs to be set")
	}
	if cfg.User == "" {
		return nil, errors.New("pg-user needs to be set")
	}
	if cfg.Database == "" {
		return nil, errors.New("pg-database needs to be set")
	}
	if timeout == 0 {
		return nil, errors.New("timeout needs to be set")
	}
	if listenAddresses == "" {
		return nil, errors.New("listen-addresses needs to be set")
	}
	// cfg.Password emptiness is not re-checked here: production callers
	// (runServer) populate it via PgCtlCommand.GetPostgresPassword, which
	// returns an error when no password source is configured.

	// Write a pgpass file and set PGPASSFILE so pgbackrest (archive-push runs as a
	// postgres subprocess and inherits this process's environment) can authenticate
	// against PostgreSQL without exposing the password in the process environment.
	pgpassPath, err := backup.WritePgpassFile(poolerDir, cfg.User, cfg.Password)
	if err != nil {
		return nil, err
	}
	if err := os.Setenv("PGPASSFILE", pgpassPath); err != nil {
		return nil, fmt.Errorf("failed to set PGPASSFILE: %w", err)
	}

	// Create the PostgreSQL config once during service initialization
	pgConfig, err := pgctld.NewPostgresCtlConfig(
		cfg.Port,
		cfg.User,
		cfg.Database,
		timeout,
		pgctld.PostgresDataDir(),
		pgctld.PostgresConfigFile(),
		poolerDir,
		listenAddresses,
		pgctld.PostgresSocketDir(poolerDir),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres config: %w", err)
	}
	pgConfig.Password = cfg.Password

	// Generate pgbackrest-server.conf if pgbackrest port and cert dir provided
	if pgbackrestPort > 0 && pgbackrestCertDir != "" {
		configPath, err := backup.WriteServerConfig(backup.ServerConfigOpts{
			PoolerDir:     poolerDir,
			CertDir:       pgbackrestCertDir,
			Port:          pgbackrestPort,
			Pg1Port:       cfg.Port,
			Pg1SocketPath: pgctld.PostgresSocketDir(poolerDir),
			Pg1Path:       pgctld.PostgresDataDir(),
			Pg1User:       cfg.User,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to generate pgbackrest-server.conf: %w", err)
		}
		logger.Info("generated pgbackrest-server.conf", "path", configPath)
	}

	metrics, metricsErr := NewMetrics()
	if metricsErr != nil {
		logger.Warn("failed to register pgctld metrics", "error", metricsErr)
	}

	//nolint:gocritic // Background context for pgBackRest lifecycle management
	ctx, cancel := context.WithCancel(context.Background())

	return &PgCtldService{
		logger:     logger,
		ctldConfig: cfg,
		timeout:    timeout,
		poolerDir:  poolerDir,
		pgConfig:   pgConfig,
		ctx:        ctx,
		cancel:     cancel,
		metrics:    metrics,
		pgBackRestStatus: &pb.PgBackRestStatus{
			Running: false,
		},
	}, nil
}

// standbySignalPath returns the path to the standby.signal marker file inside
// the service's configured PostgreSQL data directory.
func (s *PgCtldService) standbySignalPath() string {
	return filepath.Join(s.pgConfig.PostgresDataDir, constants.StandbySignalFile)
}

// hasStandbySignal reports whether a standby.signal marker file is present, i.e.
// PostgreSQL is configured to start in standby mode.
func (s *PgCtldService) hasStandbySignal() bool {
	_, err := os.Stat(s.standbySignalPath())
	return err == nil
}

// createStandbySignal creates an empty standby.signal file in the configured
// data directory so that PostgreSQL comes up in recovery (standby) mode
// instead of as a writable primary. The write truncates any existing file, so
// it is idempotent.
func (s *PgCtldService) createStandbySignal() (string, error) {
	path := s.standbySignalPath()
	if err := os.WriteFile(path, []byte(""), 0o644); err != nil {
		return path, fmt.Errorf("failed to create standby.signal: %w", err)
	}
	s.logger.Info("standby.signal created successfully", "path", path)
	return path, nil
}

// removeStandbySignal removes standby.signal from the configured data
// directory so that PostgreSQL starts as a writable primary instead of
// recovering as a standby. A no-op if the file does not exist.
func (s *PgCtldService) removeStandbySignal() (string, error) {
	path := s.standbySignalPath()
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return path, nil
		}
		return path, fmt.Errorf("failed to remove standby.signal: %w", err)
	}
	s.logger.Info("standby.signal removed successfully", "path", path)
	return path, nil
}

// setPgBackRestStatus updates the pgBackRest status thread-safely and returns the current restart count
func (s *PgCtldService) setPgBackRestStatus(running bool, errorMessage string, incrementRestart bool) int32 {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()

	if incrementRestart {
		s.restartCount++
	}

	s.pgBackRestStatus.Running = running
	s.pgBackRestStatus.ErrorMessage = errorMessage
	s.pgBackRestStatus.RestartCount = s.restartCount

	if running {
		s.pgBackRestStatus.LastStarted = timestamppb.Now()
	}

	s.metrics.SetServerUp(running)
	s.metrics.SetRestartCount(s.restartCount)

	return s.restartCount
}

// getPgBackRestStatus returns a copy of the current status thread-safely
func (s *PgCtldService) getPgBackRestStatus() *pb.PgBackRestStatus {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()

	// Return a copy to avoid race conditions
	return &pb.PgBackRestStatus{
		Running:      s.pgBackRestStatus.Running,
		ErrorMessage: s.pgBackRestStatus.ErrorMessage,
		RestartCount: s.pgBackRestStatus.RestartCount,
		LastStarted:  s.pgBackRestStatus.LastStarted,
	}
}

// Close shuts down the pgctld service gracefully
func (s *PgCtldService) Close() {
	s.logger.Info("shutting down pgctld service")

	// Signal managePgBackRest goroutine to stop
	s.cancel()

	// Kill pgBackRest process if running
	if s.pgBackRestCmd != nil {
		s.logger.Info("terminating pgBackRest server")
		killCtx, killCancel := context.WithTimeout(ctxutil.Detach(s.ctx), 100*time.Millisecond)
		_, _ = s.pgBackRestCmd.Stop(killCtx)
		killCancel()
	}

	// Wait for goroutines to fully exit
	s.wg.Wait()
	s.logger.Info("pgctld service shutdown complete")
}

// startPgBackRest starts the pgBackRest TLS server process
// Returns the command on success, or error on failure
func (s *PgCtldService) startPgBackRest(ctx context.Context) (*executil.Cmd, error) {
	configPath := s.pgbackrestServerConfigPath()

	// Verify config exists
	if _, err := os.Stat(configPath); err != nil {
		return nil, fmt.Errorf("pgbackrest-server.conf not found at %s: %w", configPath, err)
	}

	// Build command: pgbackrest server
	// Note: Config is passed via PGBACKREST_CONFIG environment variable
	cmd := executil.Command(ctx, "pgbackrest", "server")
	cmd.AddEnv("PGBACKREST_CONFIG=" + configPath)

	// Start the process
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start pgbackrest server: %w", err)
	}

	logPath := filepath.Join(s.poolerDir, "pgbackrest", "log")
	s.logger.InfoContext(ctx, "pgBackRest TLS server started",
		"pid", cmd.Process.Pid,
		"config", configPath,
		"logs", logPath)
	return cmd, nil
}

// managePgBackRest manages the pgBackRest TLS server lifecycle with retry and restart logic
func (s *PgCtldService) managePgBackRest(ctx context.Context) {
	// Check if pgbackrest config exists before attempting to start
	configPath := s.pgbackrestServerConfigPath()
	if _, err := os.Stat(configPath); err != nil {
		s.logger.InfoContext(ctx, "pgBackRest config not found, skipping pgBackRest server startup", "config_path", configPath)
		s.setPgBackRestStatus(false, "config not found", false)
		return
	}

	r := retry.New(1*time.Second, 10*time.Second)

	for {
		var cmd *executil.Cmd

		// Try to start with retry policy (max 5 attempts)
		for attempt, err := range r.Attempts(ctx) {
			if err != nil {
				// Context cancelled during startup - clean shutdown
				s.setPgBackRestStatus(false, fmt.Sprintf("context cancelled: %v", err), false)
				return
			}
			if attempt >= 4 {
				s.setPgBackRestStatus(false, "failed after 5 attempts", false)
				return
			}

			var startErr error
			cmd, startErr = s.startPgBackRest(ctx)
			if startErr == nil {
				s.setPgBackRestStatus(true, "", false)
				s.pgBackRestCmd = cmd
				break // Success, exit retry loop
			}
			s.logger.WarnContext(ctx, "pgBackRest startup failed", "attempt", attempt+1, "error", startErr)
		}

		if cmd == nil {
			// Failed to start after retries
			return
		}

		// Wait for exit OR context cancellation
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait() // Ignore error - process exit is expected
			close(done)
		}()

		select {
		case <-done:
			// Process exited, restart
			currentCount := s.setPgBackRestStatus(false, "process exited, restarting", true)
			s.logger.InfoContext(ctx, "pgBackRest exited, restarting", "restart_count", currentCount)

		case <-ctx.Done():
			// Shutdown requested, stop process
			killCtx, cancel := context.WithTimeout(ctxutil.Detach(ctx), 5*time.Second)
			_, _ = cmd.Stop(killCtx) // Ignore error - process may already be dead
			cancel()
			<-done // Wait for Wait() to complete
			return
		}
	}
}

// StartPgBackRestManagement begins pgBackRest management in background
func (s *PgCtldService) StartPgBackRestManagement() {
	s.wg.Go(func() {
		s.managePgBackRest(s.ctx)
	})
}

func (s *PgCtldService) Start(ctx context.Context, req *pb.StartRequest) (*pb.StartResponse, error) {
	s.logger.InfoContext(ctx, "gRPC Start request", "port", req.Port, "as_primary", req.GetAsPrimary())

	// Check if data directory is initialized
	if !pgctld.IsDataDirInitialized() {
		dataDir := pgctld.PostgresDataDir()
		return nil, fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	// Select the start mode for this existing data directory. Default (as_primary
	// false) writes standby.signal so postgres comes up in recovery (standby) mode
	// and never as a writable primary on its own; as_primary removes it for a
	// writable start. Sequenced before crash recovery below so a written
	// standby.signal is preserved through single-user recovery (which removes and
	// recreates it), and an as_primary start clears any leftover signal.
	if req.GetAsPrimary() {
		if _, err := s.removeStandbySignal(); err != nil {
			return nil, err
		}
	} else {
		if _, err := s.createStandbySignal(); err != nil {
			return nil, err
		}
	}

	// When the caller allows it, force single-user crash recovery for a standby
	// the caller flagged as possibly diverged, so it reaches the clean-shutdown
	// state pg_rewind needs before the start below. crashRecoveryRan reports
	// whether that single-user recovery ran (matching
	// StartResponse.crash_recovery_ran); a clean follower, or an as_primary start,
	// is instead crash-recovered by the postmaster on the normal start below and
	// does not set it.
	var crashRecoveryRan bool
	if req.GetAllowCrashRecovery() && postgresMayBeRunning(s.pgConfig.PostgresDataDir) {
		// Never crash-recover a running postmaster: runCrashRecovery removes
		// standby.signal for single-user recovery, and doing that to a postmaster
		// that is up — or still starting after a Start whose success was misreported
		// — can bring it up read-write on a timeline it must not claim. A Start
		// against an already-running node is an idempotent no-op via
		// StartPostgreSQLWithResult below.
		s.logger.InfoContext(ctx, "skipping crash recovery before start: postgres is already running")
	} else if req.GetAllowCrashRecovery() {
		needed, nErr := s.crashRecoveryNeeded(ctx)
		if nErr != nil {
			s.logger.WarnContext(ctx, "could not determine clean-shutdown state before start (continuing)", "error", nErr)
		} else if needed && s.hasStandbySignal() && req.GetSuspectedDivergence() {
			// A not-cleanly-stopped standby that the caller suspects may have
			// diverged (a former primary being demoted, or a node already flagged
			// for rewind) is force-recovered in single-user mode. runCrashRecovery
			// removes standby.signal first — postgres --single refuses to run with
			// it — and recreates it afterwards; this reaches the clean-shutdown
			// state pg_rewind needs (e.g. to unwedge a node whose earlier pg_rewind
			// stamped minRecoveryPoint onto the wrong timeline).
			//
			// A clean follower is deliberately NOT sent here: single-user
			// recovery runs in primary mode and does not follow timeline-history
			// switches, so it would finalize the node on its old timeline past the
			// leader's fork and wedge the standby start ("requested timeline N is not
			// a child"). Its crash recovery — and that of a node without
			// standby.signal — is handled by the postmaster on the normal start
			// below, which in standby mode follows the timeline switch. crashRecoveryRan
			// therefore reports specifically whether single-user recovery ran.
			crashRecoveryRan = true
			if rcErr := s.runCrashRecovery(ctx); rcErr != nil {
				// Best effort: the start below may still surface a clearer error.
				s.logger.WarnContext(ctx, "standby crash recovery before start failed (continuing)", "error", rcErr)
			}
		}
	}

	// Use the pre-configured PostgreSQL config for start operation
	result, err := s.StartPostgreSQLWithResult()
	if err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	// Reap this postmaster when it exits (no-op unless pgctld is PID 1).
	s.reaper.TrackPID(result.PID)

	pid, err := intToInt32(result.PID)
	if err != nil {
		return nil, fmt.Errorf("invalid PID: %w", err)
	}

	return &pb.StartResponse{
		Pid:              pid,
		Message:          result.Message,
		CrashRecoveryRan: crashRecoveryRan,
	}, nil
}

func (s *PgCtldService) Stop(ctx context.Context, req *pb.StopRequest) (*pb.StopResponse, error) {
	s.logger.InfoContext(ctx, "gRPC Stop request", "mode", req.Mode)

	// Check if data directory is initialized
	if !pgctld.IsDataDirInitialized() {
		dataDir := pgctld.PostgresDataDir()
		return nil, fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	// Use the pre-configured PostgreSQL config for stop operation
	result, err := s.StopPostgreSQLWithResult(req.Mode)
	if err != nil {
		return nil, fmt.Errorf("failed to stop PostgreSQL: %w", err)
	}

	return &pb.StopResponse{
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) Restart(ctx context.Context, req *pb.RestartRequest) (*pb.RestartResponse, error) {
	s.logger.InfoContext(ctx, "gRPC Restart request", "mode", req.Mode, "port", req.Port, "as_standby", req.AsStandby)

	// Check if data directory is initialized
	if !pgctld.IsDataDirInitialized() {
		dataDir := pgctld.PostgresDataDir()
		return nil, fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	// Use the pre-configured PostgreSQL config for restart operation
	result, err := s.RestartPostgreSQLWithResult(req.Mode, req.AsStandby)
	if err != nil {
		return nil, fmt.Errorf("failed to restart PostgreSQL: %w", err)
	}

	// Reap the restarted postmaster when it exits (no-op unless pgctld is PID 1).
	s.reaper.TrackPID(result.PID)

	pid, err := intToInt32(result.PID)
	if err != nil {
		return nil, fmt.Errorf("invalid PID: %w", err)
	}

	return &pb.RestartResponse{
		Pid:     pid,
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) ReloadConfig(ctx context.Context, req *pb.ReloadConfigRequest) (*pb.ReloadConfigResponse, error) {
	s.logger.InfoContext(ctx, "gRPC ReloadConfig request")

	// Check if data directory is initialized
	if !pgctld.IsDataDirInitialized() {
		dataDir := pgctld.PostgresDataDir()
		return nil, fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	// Use the pre-configured PostgreSQL config for reload operation
	result, err := ReloadPostgreSQLConfigWithResult(s.logger, s.pgConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to reload PostgreSQL configuration: %w", err)
	}

	return &pb.ReloadConfigResponse{
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	s.logger.DebugContext(ctx, "gRPC Status request")

	// First check if data directory is initialized
	if !pgctld.IsDataDirInitialized() {
		port, err := intToInt32(s.ctldConfig.Port)
		if err != nil {
			return nil, fmt.Errorf("invalid port: %w", err)
		}
		return &pb.StatusResponse{
			Status:           pb.ServerStatus_NOT_INITIALIZED,
			DataDir:          pgctld.PostgresDataDir(),
			Port:             port,
			Message:          "Data directory is not initialized",
			PgbackrestStatus: s.getPgBackRestStatus(),
		}, nil
	}

	// Use the pre-configured PostgreSQL config for status operation
	result, err := GetStatusWithResult(ctx, s.logger, s.pgConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get status: %w", err)
	}

	// Convert status string to protobuf enum
	var status pb.ServerStatus
	switch result.Status {
	case statusStopped:
		status = pb.ServerStatus_STOPPED
	case statusRunning:
		status = pb.ServerStatus_RUNNING
	default:
		status = pb.ServerStatus_STOPPED
	}

	pid, err := intToInt32(result.PID)
	if err != nil {
		return nil, fmt.Errorf("invalid PID: %w", err)
	}
	port, err := intToInt32(result.Port)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %w", err)
	}

	return &pb.StatusResponse{
		Status:           status,
		Pid:              pid,
		Version:          result.Version,
		Uptime:           durationpb.New(time.Duration(result.UptimeSeconds) * time.Second),
		DataDir:          result.DataDir,
		Port:             port,
		Ready:            result.Ready,
		Message:          result.Message,
		PgbackrestStatus: s.getPgBackRestStatus(),
	}, nil
}

func (s *PgCtldService) Version(ctx context.Context, req *pb.VersionRequest) (*pb.VersionResponse, error) {
	s.logger.DebugContext(ctx, "gRPC Version request")
	result, err := GetVersionWithResult(ctx, s.pgConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}

	return &pb.VersionResponse{
		Version: result.Version,
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) InitDataDir(ctx context.Context, req *pb.InitDataDirRequest) (*pb.InitDataDirResponse, error) {
	s.logger.InfoContext(ctx, "gRPC InitDataDir request")

	// Use the shared init function with detailed result
	result, err := InitDataDirWithResult(s.logger, s.poolerDir, s.ctldConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize data directory: %w", err)
	}

	return &pb.InitDataDirResponse{
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) PgRewind(ctx context.Context, req *pb.PgRewindRequest) (*pb.PgRewindResponse, error) {
	s.logger.InfoContext(ctx, "gRPC PgRewind request",
		"source_host", req.GetSourceHost(),
		"source_port", req.GetSourcePort(),
		"dry_run", req.GetDryRun())

	// Check if postgres is cleanly stopped before pg_rewind
	// If not, try crash recovery - this is needed for rewind dry-run to work
	// This check is best effort. It's not harmful to try the pg_rewind if
	// crash recovery fails, the dry run is just unlikely to succeed in that case.
	cleanlyStopped, err := s.isPostgresCleanlyStopped(ctx)
	if err != nil {
		s.logger.WarnContext(ctx, "failed to check postgres state (continuing anyway)", "error", err)
	} else if !cleanlyStopped {
		// Try to run crash recovery.
		// It's not harmful to do this if postgres is already running.
		if err := s.runCrashRecovery(ctx); err != nil {
			s.logger.WarnContext(ctx, "crash recovery failed (continuing anyway)", "error", err)
		}
	}

	// Construct source server connection string (without password - will use PGPASSWORD env var)
	// Include application_name if provided (used for replication identification)
	// connect_timeout ensures pg_rewind fails fast if the source is not yet ready to accept
	// connections (e.g. newly-promoted primary still in crash recovery), allowing the caller
	// to retry rather than blocking for the OS TCP timeout.
	sourceServer := fmt.Sprintf("host=%s port=%d user=%s dbname=postgres connect_timeout=5",
		req.GetSourceHost(), req.GetSourcePort(), s.ctldConfig.User)
	if req.GetApplicationName() != "" {
		sourceServer = fmt.Sprintf("%s application_name=%s", sourceServer, req.GetApplicationName())
	}

	// Use the shared rewind function with detailed result, passing password separately
	result, err := PgRewindWithResult(ctx, s.logger, sourceServer, s.ctldConfig.Password, req.GetDryRun(), req.GetExtraArgs())
	if err != nil {
		s.logger.ErrorContext(ctx, "pg_rewind output", "output", result.Output)
		return nil, fmt.Errorf("failed to rewind PostgreSQL: %w", err)
	}

	return &pb.PgRewindResponse{
		Message: result.Message,
		Output:  result.Output,
	}, nil
}

// stopRestoreCommandGracePeriod gives the wrapper enough time to exhaust its
// own child-process grace period and exit cleanly before pgctld escalates.
const stopRestoreCommandGracePeriod = restoreWrapperGracePeriod + 500*time.Millisecond

// StopRestoreCommand checks whether the restore_command wrapper (see
// `pgctld restore-wrapper`) is currently running, by reading the PID it
// recorded at RestoreCommandPIDFile, and terminates it if so. Postgres itself
// cannot cancel an in-flight restore_command invocation — a config change
// only affects the next fetch decision — so this is the only way to
// confidently stop one that is already running rather than just disabling it
// for the future.
func (s *PgCtldService) StopRestoreCommand(ctx context.Context, req *pb.StopRestoreCommandRequest) (*pb.StopRestoreCommandResponse, error) {
	pidFile := pgctld.RestoreCommandPIDFile(s.pgConfig.PoolerDir)
	s.logger.InfoContext(ctx, "gRPC StopRestoreCommand request", "pidfile", pidFile)

	pidBytes, err := os.ReadFile(pidFile)
	if err != nil {
		if os.IsNotExist(err) {
			return &pb.StopRestoreCommandResponse{Found: false, Message: "no restore_command pidfile found"}, nil
		}
		return nil, fmt.Errorf("failed to read restore_command pidfile %s: %w", pidFile, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if err != nil {
		return nil, fmt.Errorf("invalid PID in %s: %w", pidFile, err)
	}

	// findErr means the process doesn't exist (only possible FindProcess
	// failure on Unix), which just means the pidfile is stale — not an RPC
	// failure, so returning nil here alongside Found:false is intentional.
	process, findErr := os.FindProcess(pid)
	if findErr != nil {
		return &pb.StopRestoreCommandResponse{Found: false, Message: fmt.Sprintf("pid %d from pidfile not found", pid)}, nil
	}
	if process.Signal(syscall.Signal(0)) != nil {
		// Already exited on its own; the pidfile is just stale.
		return &pb.StopRestoreCommandResponse{Found: false, Message: fmt.Sprintf("pid %d is not running", pid)}, nil
	}

	s.logger.InfoContext(ctx, "restore_command wrapper still running, stopping it", "pid", pid)
	stopCtx, cancel := context.WithTimeout(ctx, stopRestoreCommandGracePeriod)
	defer cancel()
	stopErr, stopped := executil.StopProcess(stopCtx, process)
	if stopErr != nil {
		return nil, fmt.Errorf("failed to stop restore_command wrapper pid %d: %w", pid, stopErr)
	}
	if !stopped {
		return nil, fmt.Errorf("failed to stop restore_command wrapper pid %d: process did not exit after SIGKILL", pid)
	}
	return &pb.StopRestoreCommandResponse{
		Found: true, Killed: true,
		Message: fmt.Sprintf("pid %d stopped", pid),
	}, nil
}
