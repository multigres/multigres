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
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/multigres/multigres/go/common/constants"
)

// pgInstance represents a transiently-running PostgreSQL server started for
// one-off setup tasks such as creating a database after initdb.  The server
// listens only on a private temporary Unix socket directory so it is invisible
// to multipooler's background goroutines.
//
// # Why a private temporary socket directory?
//
// setupDatabase is called while the multipooler process that issued the
// InitDataDir RPC is still alive with its background goroutines running.
// Those goroutines (heartbeat writer, status poller) continuously poll
// pgctld.PostgresSocketDir(poolerDir) — the same path the permanent
// PostgreSQL server will use.  If we started the transient server on that
// path, multipooler could connect to it, find it alive, and misreport the
// pooler as a functioning primary before the multigres schema has been
// created.  It would also suffer an abrupt connection teardown when we stop
// the transient server.
//
// By using an os.MkdirTemp directory as the socket path, the transient server
// is completely invisible to multipooler.  The real socket only appears once
// pgctld processes the subsequent Start RPC.
//
// Create with newPgInstance; always call stop() when done (typically via defer).
type pgInstance struct {
	dataDir   string
	socketDir string // private temp dir owned by this instance
	port      int
	user      string
	logger    *slog.Logger
}

// newPgInstance starts a transient PostgreSQL server on a private temporary
// socket directory and blocks until it is ready to accept connections.
// The caller must call stop() when done (typically via defer pg.stop()).
func newPgInstance(logger *slog.Logger, dataDir, configFile string, port int, user string) (*pgInstance, error) {
	socketDir, err := os.MkdirTemp("", "pgctld-setup-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary socket directory: %w", err)
	}

	pg := &pgInstance{
		dataDir:   dataDir,
		socketDir: socketDir,
		port:      port,
		user:      user,
		logger:    logger,
	}

	if err := pg.start(configFile); err != nil {
		os.RemoveAll(socketDir)
		return nil, err
	}
	return pg, nil
}

// start launches the PostgreSQL server via pg_ctl and waits for readiness.
func (p *pgInstance) start(configFile string) error {
	logFile := filepath.Join(p.dataDir, "setup.log")

	// No TCP (listen_addresses=), private socket, config file already written by
	// GeneratePostgresServerConfig.  -W tells pg_ctl not to wait; we poll below
	// with pg_isready so we can target the exact socket path.
	postgresOpts := fmt.Sprintf(
		"-c config_file=%s -c port=%d -c listen_addresses= -c unix_socket_directories=%s",
		configFile, p.port, p.socketDir,
	)
	if out, err := exec.Command("pg_ctl",
		"start", "-D", p.dataDir, "-o", postgresOpts, "-l", logFile, "-W",
	).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to start transient PostgreSQL: %w\nOutput: %s", err, out)
	}

	return p.waitReady()
}

// waitReady polls pg_isready until PostgreSQL accepts connections or the
// attempt limit is reached.
func (p *pgInstance) waitReady() error {
	const maxAttempts = 30
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := exec.Command("pg_isready",
			"-h", p.socketDir,
			"-p", strconv.Itoa(p.port),
			"-d", constants.DefaultPostgresDatabase,
		).Run(); err == nil {
			return nil
		}
		if attempt == maxAttempts {
			return fmt.Errorf("transient PostgreSQL did not become ready after %d seconds", maxAttempts)
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

// stop shuts down the transient PostgreSQL server and removes the private
// socket directory.  Errors are logged as warnings because stop is typically
// called from a defer and must not shadow the caller's primary error.
func (p *pgInstance) stop() {
	// pg_ctl stop targets the data directory directly and reads the PID from
	// postmaster.pid, so it works regardless of the socket path in use.
	if out, err := exec.Command("pg_ctl",
		"stop", "-D", p.dataDir, "-m", "fast",
	).CombinedOutput(); err != nil {
		p.logger.Warn("Failed to stop transient PostgreSQL",
			"error", err, "output", string(out))
	}
	os.RemoveAll(p.socketDir)
}

// psql runs a psql command against this instance connected to database,
// appending args after the standard -h/-p/-U/-d connection flags.
// Returns combined stdout+stderr and any error.
func (p *pgInstance) psql(database string, args ...string) ([]byte, error) {
	baseArgs := []string{
		"-h", p.socketDir,
		"-p", strconv.Itoa(p.port),
		"-U", p.user,
		"-d", database,
	}
	return exec.Command("psql", append(baseArgs, args...)...).CombinedOutput()
}
