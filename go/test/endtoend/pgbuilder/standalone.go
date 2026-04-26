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

package pgbuilder

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/tools/executil"
)

// Standalone is a single PostgreSQL server process initialized from a
// Builder's installed binaries and running on a dynamically chosen TCP port.
//
// It is intentionally independent of shardsetup, pgctld, and multigateway:
// callers (e.g. the sqllogictest differential harness) need a clean baseline
// PostgreSQL to diff Multigres behaviour against.
type Standalone struct {
	// DataDir is the data directory used by initdb/postgres.
	DataDir string
	// LogPath is the path to the captured server log.
	LogPath string
	// Port is the TCP port the server listens on.
	Port int
	// User is the superuser created by initdb (always "postgres").
	User string
	// Password is the superuser password.
	Password string
	// Database is the default database available on startup.
	Database string

	binDir string
	cmd    *executil.Cmd
}

// StartStandalone runs initdb into a fresh data directory under builder.OutputDir
// and launches a postgres server on a free port. The returned Standalone
// exposes connection parameters and a Stop method.
//
// Callers should defer Stop. The server logs are written to LogPath and left
// on disk after Stop to aid debugging.
func StartStandalone(t *testing.T, ctx context.Context, builder *Builder, password string) (*Standalone, error) {
	t.Helper()

	if password == "" {
		password = "pgbuilder"
	}

	port, err := pickFreePort()
	if err != nil {
		return nil, fmt.Errorf("pick free port: %w", err)
	}

	rootDir := filepath.Join(builder.OutputDir, "standalone")
	dataDir := filepath.Join(rootDir, "data")
	logPath := filepath.Join(rootDir, "postgres.log")
	pwFile := filepath.Join(rootDir, "pwfile")

	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, fmt.Errorf("create standalone root: %w", err)
	}
	if err := os.WriteFile(pwFile, []byte(password), 0o600); err != nil {
		return nil, fmt.Errorf("write password file: %w", err)
	}

	si := &Standalone{
		DataDir:  dataDir,
		LogPath:  logPath,
		Port:     port,
		User:     "postgres",
		Password: password,
		Database: "postgres",
		binDir:   builder.BinDir(),
	}

	if err := si.runInitdb(ctx, pwFile); err != nil {
		return nil, err
	}

	// Loopback-only, md5 auth so psql/pgx connecting with PGPASSWORD works and
	// nothing outside the host can reach the server.
	hba := "host all all 127.0.0.1/32 md5\nhost all all ::1/128 md5\n"
	if err := os.WriteFile(filepath.Join(dataDir, "pg_hba.conf"), []byte(hba), 0o600); err != nil {
		return nil, fmt.Errorf("write pg_hba.conf: %w", err)
	}

	if err := si.launch(); err != nil {
		return nil, err
	}

	if err := si.waitReady(ctx); err != nil {
		_ = si.Stop()
		return nil, fmt.Errorf("standalone postgres not ready: %w", err)
	}

	t.Logf("Standalone PostgreSQL ready at localhost:%d (data=%s, log=%s)", si.Port, si.DataDir, si.LogPath)
	return si, nil
}

func (s *Standalone) runInitdb(ctx context.Context, pwFile string) error {
	cmd := executil.Command(ctx, filepath.Join(s.binDir, "initdb"),
		"-D", s.DataDir,
		"-U", s.User,
		"--pwfile="+pwFile,
		"--encoding=UTF8",
		"--locale=C",
		"--auth-local=trust",
		"--auth-host=md5",
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("initdb failed: %w\n%s", err, out.String())
	}
	return nil
}

func (s *Standalone) launch() error {
	logFile, err := os.Create(s.LogPath)
	if err != nil {
		return fmt.Errorf("create log file: %w", err)
	}

	s.cmd = executil.Command(context.Background(), filepath.Join(s.binDir, "postgres"),
		"-D", s.DataDir,
		"-p", strconv.Itoa(s.Port),
		"-c", "listen_addresses=127.0.0.1",
		"-c", "unix_socket_directories=",
		"-c", "fsync=off",
		"-c", "synchronous_commit=off",
		"-c", "full_page_writes=off",
	).WithProcessGroup()
	s.cmd.Stdout = logFile
	s.cmd.Stderr = logFile

	if err := s.cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("launch postgres: %w", err)
	}
	return nil
}

// waitReady polls pg_isready until postgres accepts connections or the deadline
// expires. pg_isready ships with every PG install so this avoids pulling in a
// Go database driver just for the healthcheck.
func (s *Standalone) waitReady(ctx context.Context) error {
	pgIsReady := filepath.Join(s.binDir, "pg_isready")
	deadline := time.Now().Add(30 * time.Second)

	var lastOut []byte
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		cmd := executil.Command(ctx, pgIsReady,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(s.Port),
			"-U", s.User,
			"-d", s.Database,
			"-q",
		)
		out, err := cmd.CombinedOutput()
		if err == nil {
			return nil
		}
		lastOut = out
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for postgres: %s", strings.TrimSpace(string(lastOut)))
}

// Stop terminates the running postgres process. Safe to call multiple times.
func (s *Standalone) Stop() error {
	if s.cmd == nil {
		return nil
	}
	_, _ = s.cmd.Stop(context.Background())
	s.cmd = nil
	return nil
}

// pickFreePort asks the kernel for an unused TCP port by binding :0, then
// closes the listener and returns the port.
func pickFreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
