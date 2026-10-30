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
	"os/exec"
	"runtime"
	"slices"
	"syscall"
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

	// killWaitDelay bounds how long Wait lingers for output pipes held open
	// by a grandchild after the group has been killed.
	killWaitDelay = 2 * time.Second
)

// Timing a test shortens.
var (
	// killGroupWait bounds how long a cancelled run waits for every process of
	// the child's group to be gone before it reports completion.
	killGroupWait = 10 * time.Second

	// serverWorkersWait bounds how long a command that takes pgBackRest's
	// backup lock waits for the 8432 server's connection workers to finish.
	serverWorkersWait = 10 * time.Second

	// cancelledEntryWait bounds how long one admission waits for one cancelled
	// child to finish dying. It is above what a healthy kill takes
	// (killWaitDelay plus killGroupWait), so it fires only for a child that
	// cannot be reaped at all, such as a leader stuck in uninterruptible sleep.
	cancelledEntryWait = 15 * time.Second
)

// settleHook runs in a cancelled run after its child has been reaped and
// before its group is confirmed gone. A test uses it to stretch that window.
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
// normally leaves within killWaitDelay plus killGroupWait, but cmd.Wait does
// not return for a leader stuck in uninterruptible sleep, and an unbounded wait
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

// reapGroup returns once no process of the group remains, so the locks the
// children held are released. The group has already been sent SIGKILL by the
// run's cancel, and cmd.Wait has returned because the leader was reaped. A
// SIGKILLed worker can still be inside its exit, holding the flock until its
// files are closed, so this waits for the group to empty; it repeats the signal
// for a member forked after the first one was delivered. It gives up after
// killGroupWait and says so: the entry must still be removed or every later
// run would wait.
func (s *Server) reapGroup(pgid int, command string) {
	deadline := time.Now().Add(killGroupWait)
	for !groupGone(pgid) {
		if time.Now().After(deadline) {
			s.logger.Error("pgbackrest process group survived SIGKILL", "command", command, "pgid", pgid)
			return
		}
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		time.Sleep(5 * time.Millisecond)
	}
}

// runChild runs one pgBackRest invocation as a child, confined when the sandbox decision enforces, in its own
// process group and returns its exit code. A cancelled ctx kills the whole
// group, because pgBackRest forks helpers that must not outlive the request.
// It never runs a shell.
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
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.SysProcAttr = childSysProcAttr()
	cmd.Cancel = func() error {
		// Setpgid makes the child's pid the group id.
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = killWaitDelay

	// Pdeathsig is tied to the forking thread, so Start and Wait stay on one.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	err = cmd.Start()
	if err == nil {
		s.children.Store(cmd.Process.Pid, struct{}{})
		if kind == KindServer {
			s.serverPID.Store(int64(cmd.Process.Pid))
			defer s.serverPID.Store(0)
		}
		err = cmd.Wait()
		if ctx.Err() != nil {
			// Wait returns when the leader is reaped; its workers may still
			// hold the lock, and a caller that sees this return may start the
			// next command.
			if settleHook != nil {
				settleHook(command)
			}
			s.reapGroup(cmd.Process.Pid, command)
		}
		s.children.Delete(cmd.Process.Pid)
	}
	if err == nil {
		return 0, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return -1, ctxErr
	}
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		return exitErr.ExitCode(), nil
	}
	return -1, fmt.Errorf("run pgbackrest %s: %w", command, err)
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
	return append(argv, args...)
}

// killChildren SIGKILLs the process group of every child still running. It is
// the clean-shutdown half of "no child outlives PID 1": a restore child runs on
// a context that outlives its request, so nothing else would stop it.
func (s *Server) killChildren() {
	s.children.Range(func(k, _ any) bool {
		if pid, ok := k.(int); ok {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}
		return true
	})
}
