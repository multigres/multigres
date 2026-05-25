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

package pgregresstest

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/multigres/multigres/go/tools/executil"
)

// PinnedPgbouncerVersion is the pgbouncer release the comparison targets run
// against. Pinning ensures reproducible fixture output across CI runs and
// developer machines: a pgbouncer minor bump can shift which regression tests
// pass under transaction pooling, and the report would no longer be
// comparable to previous runs.
//
// Bump deliberately when the project decides to track a newer pgbouncer; the
// current value matches what PGDG ships for Ubuntu 24.04 (noble) as of the
// install date.
const PinnedPgbouncerVersion = "1.25.2"

//go:embed testdata/pgbouncer/session.ini.tmpl testdata/pgbouncer/tx.ini.tmpl
var pgbouncerTemplates embed.FS

// PgbouncerMode selects the pool_mode pgbouncer is configured with.
type PgbouncerMode string

const (
	// PgbouncerModeSession runs pgbouncer with pool_mode = session.
	PgbouncerModeSession PgbouncerMode = "session"
	// PgbouncerModeTransaction runs pgbouncer with pool_mode = transaction.
	PgbouncerModeTransaction PgbouncerMode = "transaction"
)

// Pgbouncer represents a running pgbouncer process fronting a backend
// PostgreSQL instance. Callers obtain one via StartPgbouncer and must invoke
// Stop when done (typically via t.Cleanup).
type Pgbouncer struct {
	// Mode is the configured pool_mode.
	Mode PgbouncerMode
	// ListenAddr is the loopback address pgbouncer accepts connections on.
	ListenAddr string
	// ListenPort is the TCP port pgbouncer listens on. Clients should use this.
	ListenPort int
	// BackendPort is the PostgreSQL port pgbouncer forwards to.
	BackendPort int
	// Password is the password pgbouncer uses to authenticate to the backend.
	Password string
	// ConfigPath is the rendered pgbouncer.ini path.
	ConfigPath string
	// LogPath is the pgbouncer process log path.
	LogPath string
	// DataDir holds the config + log + auxiliary files.
	DataDir string

	binary string
	cmd    *executil.Cmd
}

// RequirePgbouncer locates the pgbouncer binary in PATH and verifies it
// matches PinnedPgbouncerVersion. Hard fails the test if the binary is
// missing or the version differs. Returns the absolute path to the binary on
// success.
//
// Hard-failing (rather than skipping the target) is intentional: a CI job
// that asks for a pgbouncer target but silently runs only multigateway would
// hide pooler-targeted regressions for weeks at a time.
func RequirePgbouncer(t *testing.T) string {
	t.Helper()
	bin, err := exec.LookPath("pgbouncer")
	if err != nil {
		t.Fatalf("pgbouncer binary not found in PATH; install pinned version %s before running pgbouncer targets (see README): %v",
			PinnedPgbouncerVersion, err)
	}
	out, vErr := exec.Command(bin, "--version").CombinedOutput()
	if vErr != nil {
		t.Fatalf("pgbouncer --version failed (binary=%s): %v\n%s", bin, vErr, out)
	}
	v := parsePgbouncerVersion(string(out))
	if v == "" {
		t.Fatalf("could not parse pgbouncer version from output: %q", strings.TrimSpace(string(out)))
	}
	if v != PinnedPgbouncerVersion {
		t.Fatalf("pgbouncer version mismatch: have %s, want pinned %s (binary=%s)", v, PinnedPgbouncerVersion, bin)
	}
	return bin
}

// pgbouncerVersionRe captures the first SemVer-looking token in the
// "pgbouncer --version" output (e.g. "PgBouncer 1.24.1").
var pgbouncerVersionRe = regexp.MustCompile(`(\d+\.\d+\.\d+)`)

func parsePgbouncerVersion(output string) string {
	m := pgbouncerVersionRe.FindStringSubmatch(output)
	if len(m) < 2 {
		return ""
	}
	return m[1]
}

// pgbouncerTemplateData is the substitution context exposed to the embedded
// .ini.tmpl files. Field names are referenced directly in the templates.
type pgbouncerTemplateData struct {
	BackendPort int
	ListenPort  int
	Password    string
	LogPath     string
}

// StartPgbouncer renders the per-mode template, launches pgbouncer against
// the supplied backend, and waits for the listener to accept connections.
//
// dataDir is the per-target scratch directory the config and logs live in.
// backendPort is the PostgreSQL port pgbouncer forwards to. password is the
// superuser password configured on that backend.
//
// The caller owns lifecycle: defer p.Stop() or register a t.Cleanup. The data
// directory is left on disk so a failing CI job can upload the log.
func StartPgbouncer(t *testing.T, ctx context.Context, mode PgbouncerMode, dataDir string, backendPort int, password string) (*Pgbouncer, error) {
	t.Helper()
	bin := RequirePgbouncer(t)

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir pgbouncer data dir: %w", err)
	}
	port, err := pickFreePortPgbouncer()
	if err != nil {
		return nil, fmt.Errorf("pick pgbouncer listen port: %w", err)
	}

	p := &Pgbouncer{
		Mode:        mode,
		ListenAddr:  "127.0.0.1",
		ListenPort:  port,
		BackendPort: backendPort,
		Password:    password,
		DataDir:     dataDir,
		ConfigPath:  filepath.Join(dataDir, "pgbouncer.ini"),
		LogPath:     filepath.Join(dataDir, "pgbouncer.log"),
		binary:      bin,
	}
	if err := p.renderConfig(); err != nil {
		return nil, err
	}
	if err := p.launch(); err != nil {
		return nil, err
	}
	if err := p.waitReady(ctx); err != nil {
		_ = p.Stop()
		return nil, fmt.Errorf("pgbouncer (%s) not ready: %w", p.Mode, err)
	}
	t.Logf("Pgbouncer (%s) listening on %s:%d -> backend :%d (config=%s, log=%s)",
		p.Mode, p.ListenAddr, p.ListenPort, p.BackendPort, p.ConfigPath, p.LogPath)
	return p, nil
}

// renderConfig writes the per-mode pgbouncer.ini into DataDir.
func (p *Pgbouncer) renderConfig() error {
	var tmplPath string
	switch p.Mode {
	case PgbouncerModeSession:
		tmplPath = "testdata/pgbouncer/session.ini.tmpl"
	case PgbouncerModeTransaction:
		tmplPath = "testdata/pgbouncer/tx.ini.tmpl"
	default:
		return fmt.Errorf("unknown pgbouncer mode %q", p.Mode)
	}
	raw, err := pgbouncerTemplates.ReadFile(tmplPath)
	if err != nil {
		return fmt.Errorf("read embedded template %s: %w", tmplPath, err)
	}
	tmpl, err := template.New(filepath.Base(tmplPath)).Parse(string(raw))
	if err != nil {
		return fmt.Errorf("parse template %s: %w", tmplPath, err)
	}
	f, err := os.Create(p.ConfigPath)
	if err != nil {
		return fmt.Errorf("create pgbouncer config: %w", err)
	}
	defer f.Close()
	return tmpl.Execute(f, pgbouncerTemplateData{
		BackendPort: p.BackendPort,
		ListenPort:  p.ListenPort,
		Password:    p.Password,
		LogPath:     p.LogPath,
	})
}

// launch starts pgbouncer in the background. pgbouncer writes its own
// structured log to LogPath (see "logfile =" in the templates); we still
// capture stdout/stderr to LogPath + ".stderr" in case it dies before
// initialising the configured log.
func (p *Pgbouncer) launch() error {
	stderrLog, err := os.Create(p.LogPath + ".stderr")
	if err != nil {
		return fmt.Errorf("create pgbouncer stderr log: %w", err)
	}
	// Use context.Background so the pgbouncer process is not killed when the
	// caller's setup ctx (typically time-boxed) expires. Stop() handles shutdown.
	p.cmd = executil.Command(context.Background(), p.binary, p.ConfigPath).WithProcessGroup()
	p.cmd.Stdout = stderrLog
	p.cmd.Stderr = stderrLog
	if err := p.cmd.Start(); err != nil {
		_ = stderrLog.Close()
		return fmt.Errorf("start pgbouncer: %w", err)
	}
	return nil
}

// waitReady polls the listen port until a TCP connection succeeds or the
// caller's context deadline expires. A 15-second cap protects against
// silently-broken configs without depending on the caller passing a tight
// timeout.
func (p *Pgbouncer) waitReady(ctx context.Context) error {
	deadline := time.Now().Add(15 * time.Second)
	addr := net.JoinHostPort(p.ListenAddr, strconv.Itoa(p.ListenPort))
	var lastErr error
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = errors.New("deadline exceeded")
	}
	return fmt.Errorf("pgbouncer not accepting connections on %s: %w", addr, lastErr)
}

// Stop terminates the pgbouncer process. Safe to call multiple times and on
// a half-initialised instance.
func (p *Pgbouncer) Stop() error {
	if p == nil || p.cmd == nil {
		return nil
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = p.cmd.Terminate(stopCtx)
	p.cmd = nil
	return nil
}

// pickFreePortPgbouncer asks the kernel for an unused TCP port. Mirrors the
// helper in pgbuilder/standalone.go; duplicated to avoid widening that
// package's exported API for a single test-only consumer.
func pickFreePortPgbouncer() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
