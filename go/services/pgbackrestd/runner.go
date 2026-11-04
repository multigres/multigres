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
	"io"
	"os"
	"runtime"
	"slices"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// Kind distinguishes the two environments a child can have.
type Kind int

const (
	// KindS3Command is a short-lived command that may need credentials.
	KindS3Command Kind = iota
	// KindServer is the 8432 TLS server, which never holds credentials.
	KindServer
	// KindLocalCommand is a short-lived command that never contacts the
	// repository (stop, start, version). It is only an environment class:
	// runChild still takes KindS3Command for it and picks this class itself.
	KindLocalCommand
)

const (
	serverConfName = "pgbackrest-server.conf"

	// killWaitDelay is the default allowance for draining foreground output
	// before response sinks detach from pipes retained by descendants.
	killWaitDelay = 2 * time.Second
)

// Timing a test shortens.
var (
	// serverWorkersWait bounds how long a command that takes pgBackRest's
	// backup lock waits for the 8432 server's connection workers to finish.
	serverWorkersWait = 10 * time.Second

	// cancelledEntryWait bounds how long one admission waits for one cancelled
	// child to finish dying. It is above what a healthy kill takes
	// (the supervisor TERM/KILL escalation and output drain), so it fires only for a child that
	// cannot be reaped at all, such as a leader stuck in uninterruptible sleep.
	cancelledEntryWait = 15 * time.Second
)

// settleHook runs after cancelled supervised execution returns and before its
// admission entry is released. Tests stretch this bookkeeping window.
var settleHook func(command string)

// liveChild is a running S3-command child as admission sees it, or (handler)
// a Run request that has been accepted and has not yet finished reaping its
// child. The second kind exists for Status.active_commands only: it covers the
// moments before the child is forked, so a caller that polls Status never sees
// a gap between "accepted" and "running". It is not waited for by admission,
// because a run's own child would then wait for the run itself.
type liveChild struct {
	ctx     context.Context
	handler bool
	api     pb.Command // set for handler entries
	done    chan struct{}
}

// awaitCancelled blocks until no S3-command child whose request has already
// been cancelled is still being killed. The client of a cancelled Run returns
// at once, so the next Run (a second backup after a lost lease) can reach the
// sidecar while the first child's group is still dying and holding
// pgBackRest's lock; admitting it then fails with [050]. A child whose context
// is cancelled counts from the instant of the cancel, before its kill has been
// issued, so there is no window between "cancelled" and "dying". Children that
// are merely running are not waited for: pgBackRest's own lock must still
// refuse a genuinely concurrent command.
//
// Each entry is waited for at most cancelledEntryWait. A cancelled child
// normally leaves after supervisor quiescence and output settlement. A worker
// stuck in uninterruptible sleep can prevent quiescence, and an unbounded wait
// would then hold every S3 admission, WAL push and fetch included, until its
// own context ends. An entry given up on is logged and not waited for again by
// this call.
func (s *Server) awaitCancelled(ctx context.Context) error {
	gaveUp := map[*liveChild]bool{}
	for {
		var dying *liveChild
		s.liveMu.Lock()
		for lc := range s.live {
			if !lc.handler && lc.ctx.Err() != nil && !gaveUp[lc] {
				dying = lc
				break
			}
		}
		s.liveMu.Unlock()
		if dying == nil {
			return nil
		}
		timer := time.NewTimer(cancelledEntryWait)
		select {
		case <-dying.done:
			timer.Stop()
		case <-timer.C:
			gaveUp[dying] = true
			s.logger.ErrorContext(ctx, "a cancelled pgbackrest child has not exited; admitting a command without waiting for it", "waited", cancelledEntryWait)
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

// awaitServerWorkers waits, bounded, for the 8432 server's per-connection
// workers to exit before a command that takes the backup lock starts. A backup
// run on a standby by another pooler reaches this sidecar through the server,
// and the worker it spawns here holds the same lock; when that backup is
// cancelled its connection drops and the worker exits a moment later, and a
// backup started on this pooler in that moment gets [050]. Nothing on this
// side can tell the dying worker from a live concurrent backup, so the wait is
// bounded: a genuinely concurrent backup still ends in pgBackRest's own [050],
// ten seconds later.
func (s *Server) awaitServerWorkers(ctx context.Context) error {
	pid := int(s.serverPID.Load())
	if pid == 0 {
		return nil
	}
	began := time.Now()
	deadline := began.Add(serverWorkersWait)
	waited := false
	for descendantsAlive(pid) {
		waited = true
		if time.Now().After(deadline) {
			s.logger.WarnContext(ctx, "pgbackrest server workers still running; starting the command anyway",
				"pid", pid, "waited", time.Since(began))
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	if waited {
		s.logger.InfoContext(ctx, "pgbackrest server workers exited; starting the command", "pid", pid, "waited", time.Since(began))
	}
	return nil
}

func (s *Server) trackChild(ctx context.Context) *liveChild {
	return s.track(&liveChild{ctx: ctx, done: make(chan struct{})})
}

// trackRun registers an accepted Run request; see liveChild.
func (s *Server) trackRun(ctx context.Context, cmd pb.Command) *liveChild {
	return s.track(&liveChild{ctx: ctx, handler: true, api: cmd, done: make(chan struct{})})
}

func (s *Server) track(lc *liveChild) *liveChild {
	s.liveMu.Lock()
	if s.live == nil {
		s.live = map[*liveChild]struct{}{}
	}
	s.live[lc] = struct{}{}
	s.liveMu.Unlock()
	return lc
}

// activeCommands lists the API commands that are running or still being
// reaped, sorted, one entry per run. Only handler entries count: WAL and
// housekeeping children (archive-push, start, stop) are not API commands, and
// a run's own child would list it twice.
func (s *Server) activeCommands() []pb.Command {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	var out []pb.Command
	for lc := range s.live {
		if lc.handler {
			out = append(out, lc.api)
		}
	}
	slices.Sort(out)
	return out
}

func (s *Server) untrackChild(lc *liveChild) {
	s.liveMu.Lock()
	delete(s.live, lc)
	s.liveMu.Unlock()
	close(lc.done)
}

// runChild runs one allowlisted invocation under a lifetime supervisor. Linux
// subreaping owns daemonized descendants after the foreground exits; cancellation
// requests termination and waits for positive quiescence. The actual pgBackRest
// child still receives its per-command sandbox and sanitized environment.
func (s *Server) runChild(ctx context.Context, kind Kind, command string, args []string, stdout, stderr io.Writer) (exitCode int, err error) {
	switch kind {
	case KindServer:
		command = cmdServer
		if err := s.checkCertDir(); err != nil {
			return -1, err
		}
	case KindS3Command:
		if !slices.Contains(s3Commands, command) {
			return -1, fmt.Errorf("refusing to run %q as an S3-command child", command)
		}
	default:
		return -1, fmt.Errorf("unknown child kind %d", kind)
	}
	argv := s.childArgv(kind, command, args)

	if kind == KindS3Command {
		if err := s.awaitCancelled(ctx); err != nil {
			return -1, err
		}
		if command == cmdBackup || command == cmdExpire {
			if err := s.awaitServerWorkers(ctx); err != nil {
				return -1, err
			}
		}
		lc := s.trackChild(ctx)
		defer s.untrackChild(lc)
	}

	cmd := s.command(ctx, command, argv)
	cmd.Env = envFor(envKind(kind, command), s.cfg, s.secretsNow())
	code, err := s.runOwned(ctx, cmd, stdout, stderr, kind, command)
	if ctx.Err() != nil && settleHook != nil {
		settleHook(command)
	}
	return code, err
}

// checkCertDir fails closed for the server: without the certificate, key and
// CA files its configuration names, readable, it must not start rather than
// start without TLS material. An empty or partial directory fails too.
func (s *Server) checkCertDir() error {
	if s.cfg.TLSCertDir == "" {
		return errors.New("no --tls-cert-dir configured for the pgBackRest server")
	}
	entries, err := os.ReadDir(s.cfg.TLSCertDir)
	if err != nil {
		return fmt.Errorf("pgBackRest server certificate directory: %w", err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("pgBackRest server certificate directory %q is empty", s.cfg.TLSCertDir)
	}
	cert, key, ca := constants.PgBackRestCertFiles(s.cfg.TLSCertDir)
	for _, p := range []string{cert, key, ca} {
		f, err := os.Open(p)
		if err != nil {
			return fmt.Errorf("pgBackRest server TLS file: %w", err)
		}
		fi, err := f.Stat()
		f.Close()
		if err != nil {
			return fmt.Errorf("pgBackRest server TLS file: %w", err)
		}
		if !fi.Mode().IsRegular() {
			return fmt.Errorf("pgBackRest server TLS file %q is not a regular file", p)
		}
	}
	return nil
}

func (s *Server) childArgv(kind Kind, command string, args []string) []string {
	argv := []string{s.binPath()}
	switch {
	case kind == KindServer:
		// The server command rejects --stanza (error 031).
		argv = append(argv, "--config="+s.cfg.stateSub(dirConf+"/"+serverConfName), cmdServer)
	case command == cmdVersion:
		// version rejects --config, --stanza and the log options.
		argv = append(argv, cmdVersion)
	default:
		// The lock path and log level are pinned here because stop and start
		// run before the first Configure has rendered a config, and the
		// defaults (/tmp/pgbackrest, a log file) are outside the policy.
		argv = append(argv,
			"--stanza="+Stanza,
			"--config="+s.cfg.stateSub(fileCommandConf),
		)
		// check and verify take no lock, so pgBackRest rejects the option
		// (error 031) on them. Their config file still carries lock-path.
		if command != cmdCheck && command != cmdVerify {
			argv = append(argv, "--lock-path="+s.cfg.stateSub(dirLock))
		}
		argv = append(argv, "--log-level-file=off", command)
		if command == cmdRestore {
			argv = append(argv, "--spool-path="+s.cfg.stateSub(restoreSpoolRel))
		}
	}
	if runtime.GOOS != "linux" && (command == cmdArchivePush || command == cmdArchiveGet) {
		// Only the archive-async path calls fork/setsid/exec for a detached daemon.
		argv = append(argv[:len(argv)-1], "--no-archive-async", argv[len(argv)-1])
	}
	return append(argv, args...)
}

// killChildren closes admission and all lifetime channels, then waits bounded
// for supervisors to acknowledge quiescence. Failed ACKs stay latched, and their
// durable markers prevent a restarted sidecar from claiming the data is idle.
func (s *Server) killChildren() {
	s.workersMu.Lock()
	s.workersClosing = true
	s.workersMu.Unlock()
	//nolint:gocritic // lifecycle teardown has no request context and must settle workers after serve cancellation
	ctx, cancel := context.WithTimeout(context.Background(), workerStopWait)
	defer cancel()
	if err := s.quiesceWorkers(ctx); err != nil {
		s.logger.Error("shutdown worker quiescence unproven", "error", err)
	}
}
