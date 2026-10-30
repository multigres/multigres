// Copyright 2026 Supabase, Inc.
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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/services/pgbackrestd"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// initServeTelemetry starts OpenTelemetry for the long-running server so the
// pgbackrest.server.* gauges reach the collector (export is an outbound push
// configured through the standard OTEL_* environment). Only serve does this:
// creds and health run per probe or per child and are not instrumented.
func initServeTelemetry(ctx context.Context, tel *telemetry.Telemetry) (shutdown func(), err error) {
	if err := tel.InitTelemetry(ctx, serviceName); err != nil {
		return nil, fmt.Errorf("initialize OpenTelemetry: %w", err)
	}
	return func() {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := tel.ShutdownTelemetry(sctx); err != nil {
			slog.WarnContext(sctx, "shutdown OpenTelemetry", "error", err)
		}
	}, nil
}

// sandboxDefault is the --sandbox default: the environment variable when set,
// otherwise required. The flag wins over the variable. The variable is read at
// flag registration, so a value that is not one of the three modes surfaces as
// a startup error from Config.Validate rather than being ignored.
func sandboxDefault() string {
	if v := os.Getenv(constants.PgBackRestSandboxEnvVar); v != "" {
		return v
	}
	return constants.SandboxRequired
}

func newServeCommand(logger func() *slog.Logger, tel *telemetry.Telemetry) *cobra.Command {
	var cfg pgbackrestd.Config
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the pgBackRest sidecar server",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			shutdownTelemetry, err := initServeTelemetry(cmd.Context(), tel)
			if err != nil {
				return err
			}
			defer shutdownTelemetry()
			srv, err := pgbackrestd.New(cfg)
			if err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, syscall.SIGINT)
			defer stop()
			logger().Info("pgbackrestd serving", "wal_socket", cfg.WALSocket, "control_socket", cfg.ControlSocket)
			return srv.Run(ctx)
		},
	}

	f := cmd.Flags()
	f.StringVar(&cfg.WALSocket, "wal-socket", "/run/pgbackrest-wal/wal.sock", "Unix socket for the WAL archive API")
	f.StringVar(&cfg.RestorePIDFile, "restore-pidfile", "", "Path pgctld restore-wrapper writes its PID to (required)")
	f.StringVar(&cfg.ControlSocket, "control-socket", "/run/pgbackrest-control/control.sock", "Unix socket for the control API")
	f.StringVar(&cfg.StateDir, "state-dir", "/var/lib/pgbackrestd", "Private state directory")
	f.StringVar(&cfg.DataVolume, "data-volume", "", "Data volume mount point, the sandbox anchor (required)")
	f.StringVar(&cfg.PGData, "pgdata", "", "PostgreSQL data directory")
	f.StringVar(&cfg.PGSocketDir, "pg-socket-dir", "", "PostgreSQL unix socket directory")
	f.IntVar(&cfg.PGPort, "pg-port", 5432, "PostgreSQL port")
	f.StringVar(&cfg.PGUser, "pg-user", "", "PostgreSQL user")
	f.StringVar(&cfg.PGPasswordFile, "pg-password-file", "", "File holding the PostgreSQL password")
	f.StringVar(&cfg.LocationType, "location-type", "", "Repository location type: s3 or filesystem")
	f.StringVar(&cfg.S3Bucket, "s3-bucket", "", "S3 bucket")
	f.StringVar(&cfg.S3Region, "s3-region", "", "S3 region")
	f.StringVar(&cfg.S3Endpoint, "s3-endpoint", "", "S3 endpoint")
	f.StringVar(&cfg.S3KeyPrefix, "s3-key-prefix", "", "S3 key prefix")
	f.StringVar(&cfg.S3CredSource, "s3-cred-source", "", "S3 credential source: web-identity, pod-identity, static, none, or endpoint (tests only)")
	f.StringVar(&cfg.S3CredEndpoint, "s3-cred-endpoint", "", "Credential URL for --s3-cred-source=endpoint")
	f.StringVar(&cfg.S3CredCAFile, "s3-cred-ca-file", "", "PEM CA or certificate that verifies --s3-cred-endpoint (tests only)")
	f.StringVar(&cfg.S3StaticKeyDir, "s3-static-key-dir", "", "Directory holding the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY files for --s3-cred-source=static")
	f.StringVar(&cfg.CipherKeyFile, "cipher-key-file", "", "Backup cipher key file")
	f.StringVar(&cfg.FSRepoPath, "fs-repo-path", "", "Repository path for --location-type=filesystem")
	f.StringVar(&cfg.TLSCertDir, "tls-cert-dir", "", "Directory holding the pgBackRest TLS certificates")
	f.IntVar(&cfg.ServerPort, "server-port", 8432, "pgBackRest TLS server port")
	f.StringVar(&cfg.Sandbox, "sandbox", sandboxDefault(),
		"Landlock confinement of pgBackRest children: required (fail closed), auto (confine if the host supports Landlock, else run unconfined), or off; default from $"+constants.PgBackRestSandboxEnvVar)
	f.StringVar(&cfg.SandboxAnchor, "sandbox-anchor", "mount", "Data volume anchor: mount or dir")
	f.IntVar(&cfg.MaxFetches, "max-fetches", 2, "Maximum concurrent archive-get fetches")
	f.StringVar(&cfg.ArchiveGetQueueMax, "archive-get-queue-max", "256MiB", "archive-get prefetch queue size")
	return cmd
}
