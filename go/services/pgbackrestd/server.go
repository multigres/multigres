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

package pgbackrestd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/common/safefs"
	"github.com/multigres/multigres/go/common/sandbox"
)

const shutdownGrace = 10 * time.Second

// Server is the pgbackrestd process.
type Server struct {
	serverState

	cfg    Config
	self   string
	token  string
	snap   atomic.Pointer[RepoSnapshot]
	gate   *Gate
	anchor *os.File
	// decision is made once at start and never revisited.
	decision sandbox.Decision
	logger   *slog.Logger
	// bin is the pgBackRest executable; tests substitute a probe.
	bin string
}

// New validates cfg, decides once whether children are confined, anchors the
// data volume and prepares the private state directory.
func New(cfg Config) (*Server, error) {
	return newWithProbe(cfg, sandbox.Probe)
}

// newWithProbe is New with the kernel probe injectable, so the decision is
// testable on any host.
func newWithProbe(cfg Config, probe sandbox.ProbeFunc) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	d, err := sandbox.Decide(cfg.sandboxMode(), probe)
	if err != nil {
		return nil, err
	}
	logDecision(slog.Default(), d)
	return newServer(cfg, d)
}

// logDecision is the whole visibility story of the sandbox mode: one line at
// start, at WARN when children run unconfined.
func logDecision(logger *slog.Logger, d sandbox.Decision) {
	level := slog.LevelInfo
	if !d.Enforced() {
		level = slog.LevelWarn
	}
	//nolint:gocritic // start-up entry point: there is no request context yet
	logger.Log(context.Background(), level, "pgbackrestd sandbox decision",
		"requested", string(d.Requested),
		"capability", d.CapabilityText(),
		"effective", string(d.Effective))
}

// command builds the child for one invocation. Under an enforced decision
// every child is wrapped in sandbox-exec, which fails closed; the decision is
// never revisited per child.
func (s *Server) command(ctx context.Context, command string, argv []string) *exec.Cmd {
	if s.decision.Enforced() {
		return sandbox.Command(ctx, s.self, policyFor(command, s.cfg, s.self), argv)
	}
	s.logger.WarnContext(ctx, "running pgBackRest child unconfined", "command", command, "sandbox", string(s.decision.Requested))
	//nolint:gocritic,gosec // argv is fixed words plus validated config, and a cancel must SIGKILL the whole group
	return exec.CommandContext(ctx, argv[0], argv[1:]...)
}

// newServer builds the server for a decision already made. Tests that run fake
// children without Landlock pass an unconfined one.
func newServer(cfg Config, d sandbox.Decision) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	// Children are re-executions of this binary, so the path must be the one
	// it actually runs from; it differs under the local provisioner.
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate own executable: %w", err)
	}
	self, err := filepath.EvalSymlinks(exe)
	if err != nil {
		return nil, fmt.Errorf("resolve own executable: %w", err)
	}
	anchor, err := sandbox.OpenAnchor(cfg.DataVolume, cfg.anchorMode())
	if err != nil {
		// Without Landlock there may be no anchor to open (not Linux); an
		// unconfined server has nothing the anchor protects.
		if d.Enforced() || !errors.Is(err, sandbox.ErrUnsupported) {
			return nil, err
		}
		anchor = nil
	}
	closeAnchor := func() {
		if anchor != nil {
			anchor.Close()
		}
	}
	token, err := newCredsToken()
	if err != nil {
		closeAnchor()
		return nil, err
	}
	if err := prepareState(cfg); err != nil {
		closeAnchor()
		return nil, err
	}

	s := &Server{
		cfg:      cfg,
		self:     self,
		token:    token,
		gate:     NewGate(),
		anchor:   anchor,
		decision: d,
		logger:   slog.Default(),
		bin:      "pgbackrest",
	}
	s.walSegBytes.Store(defaultWALSegmentBytes)
	// Fail at startup on an unreadable key file or static key directory. The
	// snapshot stays empty until Configure names the repositories.
	if _, err := s.buildSnapshot(nil); err != nil {
		closeAnchor()
		return nil, err
	}
	s.setRepoSnapshot(RepoSnapshot{})
	if err := s.loadState(); err != nil {
		closeAnchor()
		return nil, err
	}
	return s, nil
}

// binPath returns the pgBackRest executable as a path. A bare name would
// become the child's argv[0], and the asynchronous archive worker pgBackRest
// forks re-executes argv[0] on the child's fixed PATH. That finds
// /usr/bin/pgbackrest in a pod but not one installed elsewhere (Homebrew on a
// dev machine), so the worker would never start and archive-push would wait for
// it until its timeout. The lookup uses PID 1's PATH, at spawn time so a test
// that substitutes a probe on PATH still takes effect.
func (s *Server) binPath() string {
	if strings.ContainsRune(s.bin, filepath.Separator) {
		return s.bin
	}
	p, err := exec.LookPath(s.bin)
	if err != nil {
		return s.bin
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return s.bin
	}
	return abs
}

// Close releases the data-volume anchor.
func (s *Server) Close() error {
	if s.anchor == nil {
		return nil
	}
	return s.anchor.Close()
}

// Run serves until ctx is cancelled. There is no TCP listener here: the only
// network port is the pgBackRest server's, which pgBackRest itself opens.
func (s *Server) Run(ctx context.Context) error {
	defer s.Close()

	// The supervisor and the start retry run on this context and are awaited
	// before Run returns, so no pgBackRest child outlives it.
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		s.stopBackground()
	}()

	s.serveCtx.Store(&ctx)
	// Runs before the cancel and the wait above, so nothing is left writing
	// when PID 1 returns.
	defer s.killChildren()

	if err := s.runStartupDuties(ctx); err != nil {
		return fmt.Errorf("startup: %w", err)
	}

	walMux, controlMux := http.NewServeMux(), http.NewServeMux()
	s.registerWAL(walMux)
	s.registerControl(controlMux)

	walLn, err := listenUnix(s.cfg.WALSocket)
	if err != nil {
		return err
	}
	controlLn, err := listenUnix(s.cfg.ControlSocket)
	if err != nil {
		walLn.Close()
		return err
	}

	if err := s.startCreds(ctx); err != nil {
		walLn.Close()
		controlLn.Close()
		return fmt.Errorf("start creds: %w", err)
	}
	// Every socket is bound now: listenUnix's umask window is over, so
	// forking the version probe cannot inherit it.
	s.pgbackrestVersion()
	if err := s.startPgServer(ctx); err != nil {
		walLn.Close()
		controlLn.Close()
		return fmt.Errorf("start pgbackrest server: %w", err)
	}

	servers := []*http.Server{
		{Handler: walMux, ReadHeaderTimeout: 10 * time.Second},
		{Handler: controlMux, ReadHeaderTimeout: 10 * time.Second},
	}
	errCh := make(chan error, len(servers))
	for i, ln := range []net.Listener{walLn, controlLn} {
		go func() { errCh <- servers[i].Serve(ln) }()
	}

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errCh:
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
	defer cancel()
	for _, srv := range servers {
		if err := srv.Shutdown(shutdownCtx); err != nil {
			s.logger.WarnContext(ctx, "http shutdown", "error", err)
		}
	}
	if errors.Is(runErr, http.ErrServerClosed) {
		runErr = nil
	}
	return runErr
}

// socketUmask makes the socket come out 0700 at creation, so there is no
// window in which it has another mode and no path-based chmod a peer could
// redirect. The owner's x bit is kept on purpose: the umask is process-wide,
// and anything else created while it is set (a directory, a forked child's
// file) must stay traversable by its owner.
const socketUmask = 0o077

// umaskMu serializes listenUnix against itself only. It does not make the
// umask change safe against other goroutines, so listenUnix must be called
// only at startup, before anything else creates files or forks children.
var umaskMu sync.Mutex

// listenUnix replaces whatever is at path with a fresh socket. The directory
// is shared with a peer that can plant entries in it, so the old entry is
// removed relative to a descriptor on the directory (links are never
// followed, directories are removed recursively but bounded) and the socket
// is bound under a umask that sets its final mode. Startup only.
func listenUnix(path string) (net.Listener, error) {
	dir, base := filepath.Split(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create socket dir: %w", err)
	}
	if err := removeEntry(dir, base); err != nil {
		return nil, err
	}

	umaskMu.Lock()
	old := syscall.Umask(socketUmask)
	ln, err := net.Listen("unix", path)
	syscall.Umask(old)
	umaskMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", path, err)
	}
	return ln, nil
}

// removeEntry removes dir/base, whatever it is. A peer that can write the
// directory could plant a non-empty directory there, which must not stop the
// sidecar from starting.
func removeEntry(dir, base string) error {
	root, err := safefs.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("open socket dir %q: %w", dir, err)
	}
	defer root.Close()
	if err := root.RemoveAll(base); err != nil {
		return fmt.Errorf("remove stale %s: %w", filepath.Join(dir, base), err)
	}
	return nil
}
