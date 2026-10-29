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

package local

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/executil"
)

// maxUnixSocketPath is the smallest sun_path across the supported hosts
// (macOS allows 104 bytes including the terminating NUL, Linux 108).
const maxUnixSocketPath = 103

// PgBackRestdPaths is where one pooler's pgbackrestd keeps its state and
// sockets. It is a sibling of the pooler directory and never inside it:
// pgbackrestd's data volume is the pooler directory, and its validation
// rejects a data volume that holds the state directory or a socket directory,
// because every sandboxed child may read the data volume.
type PgBackRestdPaths struct {
	// Dir holds everything below. It is <poolerBase>/pgbackrestd-<pooler dir name>.
	Dir string
	// StateDir is --state-dir.
	StateDir string
	// WALSocket is --wal-socket, the socket postgres's archive_command and
	// restore_command dial.
	WALSocket string
	// ControlSocket is --control-socket, the socket multipooler dials.
	ControlSocket string
	// PasswordFile is the sidecar's own copy of the postgres password, kept
	// off the data volume, where every sandboxed child could read it. The
	// local provisioner still writes <poolerDir>/postgres-password for pgctld
	// and multipooler exactly as it does without the sidecar, so on this
	// local-only path the password is also on the volume; the sidecar never
	// reads that one.
	PasswordFile string
}

// NewPgBackRestdPaths derives the pgbackrestd layout for a pooler directory.
func NewPgBackRestdPaths(poolerDir string) PgBackRestdPaths {
	dir := filepath.Join(filepath.Dir(poolerDir), "pgbackrestd-"+filepath.Base(poolerDir))
	return PgBackRestdPaths{
		Dir:           dir,
		StateDir:      filepath.Join(dir, "state"),
		WALSocket:     filepath.Join(dir, "run-wal", "wal.sock"),
		ControlSocket: filepath.Join(dir, "run-control", "control.sock"),
		PasswordFile:  filepath.Join(dir, "pg-password"),
	}
}

// credsSocket is where pgbackrestd serves credentials to its S3 children. It
// is the longest socket path in the layout, so it bounds the check in Prepare.
func (p PgBackRestdPaths) credsSocket() string {
	return filepath.Join(p.StateDir, "creds", "creds.sock")
}

// Prepare creates the directories and writes the password copy. The socket
// directories are private to the host user: Landlock does not exist on every
// dev machine, so the mode is what keeps another local user off the sockets.
func (p PgBackRestdPaths) Prepare(pgPassword string) error {
	for _, sock := range []string{p.WALSocket, p.ControlSocket, p.credsSocket()} {
		if len(sock) > maxUnixSocketPath {
			return fmt.Errorf("unix socket path %q is %d bytes, over the %d byte limit: use a shorter root working directory",
				sock, len(sock), maxUnixSocketPath)
		}
	}
	for _, d := range []string{p.StateDir, filepath.Dir(p.WALSocket), filepath.Dir(p.ControlSocket)} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fmt.Errorf("create %s: %w", d, err)
		}
	}
	if err := os.WriteFile(p.PasswordFile, []byte(pgPassword), 0o600); err != nil {
		return fmt.Errorf("write postgres password copy: %w", err)
	}
	return nil
}

// PgBackRestdConfig is everything BuildArgs needs to render one pooler's
// `pgbackrestd serve` command line.
type PgBackRestdConfig struct {
	// PoolerDir is the pooler directory, which is pgbackrestd's data volume.
	PoolerDir string
	Paths     PgBackRestdPaths

	PgPort int
	PgUser string

	// Location is the backup location from topology.
	Location *clustermetadatapb.BackupLocation

	// ServerPort and TLSCertDir configure the pgBackRest TLS server the
	// sidecar runs for standbys to pull from.
	ServerPort int
	TLSCertDir string

	// CipherKeyFile is the backup cipher key file. Empty means unencrypted.
	CipherKeyFile string

	// S3CredEndpoint, when set, makes S3 credentials come from POSTing to
	// this URL (the s3mock session endpoint). S3CAFile verifies it and the
	// S3 endpoint itself. Tests only.
	S3CredEndpoint string
	S3CAFile       string
	// S3StaticKeyDir holds the files AWS_ACCESS_KEY_ID and
	// AWS_SECRET_ACCESS_KEY. Ignored when S3CredEndpoint is set.
	S3StaticKeyDir string

	// Sandbox is the --sandbox mode; empty means required.
	Sandbox string

	// LogFile receives the sidecar's log output.
	LogFile  string
	LogLevel string
}

// SandboxMode is the --sandbox mode this provisioner passes to every
// pgbackrestd it starts: $MULTIGRES_PGBACKREST_SANDBOX, or required when it is
// unset. An unknown value is an error, never a silent default.
func SandboxMode() (string, error) {
	mode := os.Getenv(constants.PgBackRestSandboxEnvVar)
	if mode == "" {
		return constants.SandboxRequired, nil
	}
	if err := constants.ValidateSandboxMode(mode); err != nil {
		return "", fmt.Errorf("%s: %w", constants.PgBackRestSandboxEnvVar, err)
	}
	return mode, nil
}

// requireSidecarHost refuses to provision, in --sandbox=required, where
// Landlock cannot exist. auto and off run on any host; whether a Linux kernel
// has Landlock is the sidecar's own start-time decision. goos is a parameter
// so the check is testable on any host.
func requireSidecarHost(goos, mode string) error {
	if goos != "linux" && mode == constants.SandboxRequired {
		return errors.New(constants.SidecarHostRequirement)
	}
	return nil
}

// sidecarPoolerArgs are the flags that point a multipooler (standalone or the
// one inside minigres) at its pooler's sidecar.
func sidecarPoolerArgs(poolerDir string) []string {
	paths := NewPgBackRestdPaths(poolerDir)
	return []string{
		"--pgbackrest-control-socket", paths.ControlSocket,
		"--pgbackrest-wal-socket", paths.WALSocket,
		// The pooler directory is not a mount point here.
		"--data-volume-root", poolerDir,
		"--data-volume-anchor", "dir",
	}
}

// BuildArgs renders the argv (after the binary name) for `pgbackrestd serve`.
func (c PgBackRestdConfig) BuildArgs() ([]string, error) {
	if c.PoolerDir == "" || c.Paths.Dir == "" {
		return nil, errors.New("pgbackrestd needs a pooler directory and a state layout")
	}
	args := []string{
		"serve",
		"--wal-socket", c.Paths.WALSocket,
		"--control-socket", c.Paths.ControlSocket,
		"--state-dir", c.Paths.StateDir,
		"--data-volume", c.PoolerDir,
		"--pgdata", filepath.Join(c.PoolerDir, "pg_data"),
		"--pg-socket-dir", constants.PostgresSocketDir(c.PoolerDir),
		"--pg-port", strconv.Itoa(c.PgPort),
		"--pg-user", c.PgUser,
		"--pg-password-file", c.Paths.PasswordFile,
		"--restore-pidfile", filepath.Join(c.PoolerDir, constants.RestoreCommandPIDFile),
	}

	locArgs, err := c.locationArgs()
	if err != nil {
		return nil, err
	}
	args = append(args, locArgs...)

	args = append(args, "--server-port", strconv.Itoa(c.ServerPort))
	if c.TLSCertDir != "" {
		args = append(args, "--tls-cert-dir", c.TLSCertDir)
	}
	if c.CipherKeyFile != "" {
		args = append(args, "--cipher-key-file", c.CipherKeyFile)
	}
	// The local pooler directory is not a mount point, which is why the anchor
	// is a directory here; production anchors on the mount.
	args = append(args, "--sandbox-anchor=dir")
	sandboxMode := c.Sandbox
	if sandboxMode == "" {
		sandboxMode = constants.SandboxRequired
	}
	args = append(args, "--sandbox="+sandboxMode)
	if c.LogFile != "" {
		args = append(args, "--log-output", c.LogFile)
	}
	if c.LogLevel != "" {
		args = append(args, "--log-level", c.LogLevel)
	}
	return args, nil
}

func (c PgBackRestdConfig) locationArgs() ([]string, error) {
	if fs := c.Location.GetFilesystem(); fs != nil {
		return []string{"--location-type", "filesystem", "--fs-repo-path", fs.GetPath()}, nil
	}
	s3 := c.Location.GetS3()
	if s3 == nil {
		return nil, errors.New("pgbackrestd needs a filesystem or s3 backup location")
	}
	args := []string{
		"--location-type", "s3",
		"--s3-bucket", s3.GetBucket(),
		"--s3-region", s3.GetRegion(),
	}
	if s3.GetEndpoint() != "" {
		args = append(args, "--s3-endpoint", s3.GetEndpoint())
	}
	if s3.GetKeyPrefix() != "" {
		args = append(args, "--s3-key-prefix", s3.GetKeyPrefix())
	}
	switch {
	case c.S3CredEndpoint != "":
		args = append(args, "--s3-cred-source", "endpoint", "--s3-cred-endpoint", c.S3CredEndpoint)
	case c.S3StaticKeyDir != "":
		args = append(args, "--s3-cred-source", "static", "--s3-static-key-dir", c.S3StaticKeyDir)
	default:
		args = append(args, "--s3-cred-source", "none")
	}
	if c.S3CAFile != "" {
		args = append(args, "--s3-cred-ca-file", c.S3CAFile)
	}
	return args, nil
}

// WaitForPgBackRestdSockets polls until both of the sidecar's API sockets
// accept connections. exited, when non-nil, is closed by the caller if the
// process dies, so a crash at startup fails fast instead of timing out.
func WaitForPgBackRestdSockets(ctx context.Context, paths PgBackRestdPaths, exited <-chan struct{}, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if socketAccepts(paths.WALSocket) && socketAccepts(paths.ControlSocket) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-exited:
			return errors.New("pgbackrestd exited before its sockets came up")
		case <-deadline.C:
			return fmt.Errorf("pgbackrestd sockets %s and %s not accepting after %v", paths.WALSocket, paths.ControlSocket, timeout)
		case <-tick.C:
		}
	}
}

func socketAccepts(path string) bool {
	conn, err := net.DialTimeout("unix", path, 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// s3KeyDir materializes the AWS credentials from this process's environment
// into a directory pgbackrestd can read with --s3-static-key-dir. It lives
// beside the sidecar's state, never in the pooler directory.
func (p PgBackRestdPaths) s3KeyDir() string { return filepath.Join(p.Dir, "s3-keys") }

func (p PgBackRestdPaths) writeS3KeysFromEnv() (string, error) {
	keyID, secret := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	if keyID == "" || secret == "" {
		return "", errors.New("use-env-credentials is set but AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY is empty")
	}
	dir := p.s3KeyDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create %s: %w", dir, err)
	}
	for name, value := range map[string]string{"AWS_ACCESS_KEY_ID": keyID, "AWS_SECRET_ACCESS_KEY": secret} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
			return "", fmt.Errorf("write %s: %w", name, err)
		}
	}
	return dir, nil
}

// startPgBackRestd starts the sidecar for one pooler and waits for its API
// sockets. It runs before pgctld so postgres's archive_command never dials a
// socket that does not exist yet.
func (p *localProvisioner) startPgBackRestd(ctx context.Context, binary, poolerDir, pgUser, pgPassword, logFile string, pgPort, serverPort int) (int, error) {
	mode, err := SandboxMode()
	if err != nil {
		return 0, err
	}
	if err := requireSidecarHost(runtime.GOOS, mode); err != nil {
		return 0, err
	}
	location, err := p.buildBackupLocation()
	if err != nil {
		return 0, fmt.Errorf("failed to build backup location: %w", err)
	}

	paths := NewPgBackRestdPaths(poolerDir)
	if err := os.MkdirAll(poolerDir, 0o755); err != nil {
		return 0, fmt.Errorf("create pooler directory %s: %w", poolerDir, err)
	}
	if err := paths.Prepare(pgPassword); err != nil {
		return 0, err
	}

	cfg := PgBackRestdConfig{
		PoolerDir:     poolerDir,
		Paths:         paths,
		PgPort:        pgPort,
		PgUser:        pgUser,
		Location:      location,
		ServerPort:    serverPort,
		TLSCertDir:    p.certDir(),
		CipherKeyFile: p.cipherKeyFilePath,
		Sandbox:       mode,
		LogFile:       logFile,
		LogLevel:      "info",
	}
	if p.config.Backup.S3 != nil && p.config.Backup.S3.UseEnvCredentials {
		if cfg.S3StaticKeyDir, err = paths.writeS3KeysFromEnv(); err != nil {
			return 0, err
		}
	}
	args, err := cfg.BuildArgs()
	if err != nil {
		return 0, err
	}

	cmd := executil.Command(ctx, binary, args...)
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("failed to start pgbackrestd: %w", err)
	}
	pid := cmd.Process.Pid
	if err := p.validateProcessRunning(pid); err != nil {
		return 0, fmt.Errorf("pgbackrestd process validation failed: %w", err)
	}
	if err := WaitForPgBackRestdSockets(ctx, paths, nil, 30*time.Second); err != nil {
		_ = p.stopProcessByPID(ctx, "pgbackrestd", pid)
		return 0, fmt.Errorf("%w\n\nLast 20 lines from pgbackrestd logs:\n%s", err, p.readServiceLogs(logFile, 20))
	}
	return pid, nil
}
