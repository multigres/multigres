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

package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// SidecarRunner runs pgBackRest through the pgbackrestd sidecar. The engine
// execs nothing locally.
type SidecarRunner interface {
	Run(ctx context.Context, cmd pb.Command, args []string, stdout, stderr io.Writer) (exitCode int, err error)
	Status(ctx context.Context) (*pb.StatusResponse, error)
}

// ErrOutputTruncated is returned with the exit code of a command whose output
// the sidecar had to drop. Only restore drops output; its result does not depend
// on what it printed.
var ErrOutputTruncated = errors.New("pgbackrestd dropped part of the command output")

// ErrRunNotDelivered means the Run request never reached the sidecar (it could
// not be dialed), so no restore was started by it and there is nothing to wait
// for.
var ErrRunNotDelivered = errors.New("the pgbackrestd Run request was not delivered")

// sidecarExitError is a command that ran and exited non-zero. Only this kind of
// failure carries output that says anything about the repository.
type sidecarExitError struct{ code int }

func (e *sidecarExitError) Error() string { return fmt.Sprintf("exit status %d", e.code) }

// ErrRestoreUnsettled means a restore failed on this side but the sidecar may
// still be writing the data directory, so nothing may be deleted or started.
var ErrRestoreUnsettled = errors.New("a sidecar restore may still be running")

// Timing a test shortens.
var (
	// restoreDrainAllowance and restoreMargin cover the sidecar's own drain and
	// its start/stop children, which run before and after the restore child.
	restoreDrainAllowance = time.Minute
	restoreMargin         = 2 * time.Minute
	// restoreStatusCallTimeout bounds each Status call while settling.
	restoreStatusCallTimeout = 5 * time.Second
	restoreSettlePoll        = time.Second
)

// sidecarRestoreDeadline is how long a restore through the sidecar may take
// before this side gives up on it: longer than the sidecar's own deadline.
func sidecarRestoreDeadline() time.Duration {
	if restoreDeadlineOverride > 0 {
		return restoreDeadlineOverride
	}
	return commonbackup.RestoreTimeout + restoreDrainAllowance + restoreMargin
}

var restoreDeadlineOverride time.Duration

// SidecarRestoreDeadline is how long a restore through the sidecar may take,
// for callers that bound an action around it.
func SidecarRestoreDeadline() time.Duration { return sidecarRestoreDeadline() }

// OverrideSidecarRestoreTimingForTest shortens the restore deadline and the
// settle poll so a test need not wait out minutes. It returns the undo.
func OverrideSidecarRestoreTimingForTest(deadline, poll time.Duration) (undo func()) {
	oldD, oldP := restoreDeadlineOverride, restoreSettlePoll
	restoreDeadlineOverride, restoreSettlePoll = deadline, poll
	return func() { restoreDeadlineOverride, restoreSettlePoll = oldD, oldP }
}

// restoreStartWait bounds how long Restore waits for the sidecar to clear the
// stop file it leaves behind a restore. A variable so a test need not wait it out.
var (
	restoreStartWait = 2 * time.Minute
	restoreStartPoll = time.Second
)

// pgCmd is one pgBackRest invocation, to be sent to the sidecar through Run.
type pgCmd struct {
	// args has --stanza removed, because the sidecar adds it; the API cannot
	// name a stanza or a config file.
	command pb.Command
	args    []string
	err     error
}

var sidecarCommands = map[string]pb.Command{
	"backup":        pb.Command_COMMAND_BACKUP,
	"verify":        pb.Command_COMMAND_VERIFY,
	"info":          pb.Command_COMMAND_INFO,
	"check":         pb.Command_COMMAND_CHECK,
	"expire":        pb.Command_COMMAND_EXPIRE,
	"stanza-create": pb.Command_COMMAND_STANZA_CREATE,
	"restore":       pb.Command_COMMAND_RESTORE,
}

// newSidecarCmd splits an argv into the sidecar's command enum and its options.
func newSidecarCmd(args []string) *pgCmd {
	c := &pgCmd{}
	var subcommand string
	for _, a := range args {
		switch {
		case strings.HasPrefix(a, "--stanza="), strings.HasPrefix(a, "--config="):
		case strings.HasPrefix(a, "--"):
			c.args = append(c.args, a)
		case subcommand == "":
			subcommand = a
		default:
			c.err = fmt.Errorf("unexpected positional argument %q", a)
		}
	}
	cmd, ok := sidecarCommands[subcommand]
	if !ok && c.err == nil {
		c.err = fmt.Errorf("pgbackrest command %q is not available through the sidecar", subcommand)
	}
	c.command = cmd
	return c
}

// runSidecar runs c and returns stdout and stderr separately. A non-zero exit
// is an error, worded like an exec failure.
func (e *Engine) runSidecar(ctx context.Context, c *pgCmd) (stdout, stderr []byte, err error) {
	if c.err != nil {
		return nil, nil, c.err
	}
	var so, se bytes.Buffer
	exit, err := e.settings.Sidecar.Run(ctx, c.command, c.args, &so, &se)
	if errors.Is(err, ErrOutputTruncated) {
		if c.command != pb.Command_COMMAND_RESTORE {
			// Callers parse this output (the info JSON, the verify error scan),
			// and a partial copy of it could read as success.
			return so.Bytes(), se.Bytes(), fmt.Errorf("pgbackrest %s: %w", c.command, err)
		}
		e.logger.WarnContext(ctx, "pgbackrestd dropped part of the restore output; the result is its exit code", "exit_code", exit)
		err = nil
	}
	if err != nil {
		return so.Bytes(), se.Bytes(), fmt.Errorf("pgbackrestd run: %w", err)
	}
	if exit != 0 {
		return so.Bytes(), se.Bytes(), &sidecarExitError{code: exit}
	}
	return so.Bytes(), se.Bytes(), nil
}

// execute runs c with progress logging and returns its combined output.
func (e *Engine) execute(ctx context.Context, c *pgCmd, operationName string) ([]byte, error) {
	return RunWithProgress(ctx, e.logger, operationName, func() ([]byte, error) {
		so, se, err := e.runSidecar(ctx, c)
		return append(so, se...), err
	})
}

// output runs c and returns what a caller parses. That is
// stdout only on success: the sidecar renders log-level-stderr=info, so stderr
// carries log lines that would corrupt `info --output=json`. On failure both
// streams come back, since the error text may be on either.
func (e *Engine) output(ctx context.Context, c *pgCmd) (string, error) {
	so, se, err := e.runSidecar(ctx, c)
	if err != nil {
		out := string(so) + string(se)
		// Callers read an empty output, or one that says "does not exist", from
		// a failed info as "no stanza yet". Only a command that ran and exited
		// non-zero can say that: a truncated, cancelled or undeliverable call
		// may hold a partial copy of anything, so its error text stands in.
		var exitErr *sidecarExitError
		if !errors.As(err, &exitErr) || out == "" {
			out = err.Error()
		}
		return out, err
	}
	return string(so), nil
}

// RunWithProgress runs fn, logging every 10 seconds that it is still going and
// once more when it finishes.
func RunWithProgress(ctx context.Context, logger *slog.Logger, operationName string, fn func() ([]byte, error)) ([]byte, error) {
	logger.InfoContext(ctx, "starting command", "operation", operationName)

	startTime := time.Now()
	logCtx, cancelLog := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-logCtx.Done():
				return
			case <-ticker.C:
				logger.InfoContext(ctx, "command still in progress",
					"operation", operationName,
					"elapsed_seconds", int(time.Since(startTime).Seconds()))
			}
		}
	}()

	output, err := fn()
	cancelLog()

	elapsed := int(time.Since(startTime).Seconds())
	if err != nil {
		logger.ErrorContext(ctx, "command failed",
			"operation", operationName,
			"elapsed_seconds", elapsed,
			"error", err)
	} else {
		logger.InfoContext(ctx, "command completed",
			"operation", operationName,
			"elapsed_seconds", elapsed)
	}
	return output, err
}

// awaitRestoreStart waits for the sidecar to finish clearing the stop file a
// restore leaves behind. A restore that succeeded but whose start keeps
// failing exits 0 with restore_start_pending set, so the exit code alone says
// nothing about whether archiving can resume. The wait is bounded and a status
// failure ends it: the restored data is good either way, and blocking the
// restore flow on a sidecar that cannot answer would stall recovery.
func (e *Engine) awaitRestoreStart(ctx context.Context) {
	deadline := time.Now().Add(restoreStartWait)
	for {
		st, err := e.settings.Sidecar.Status(ctx)
		if err != nil {
			e.logger.WarnContext(ctx, "cannot confirm the sidecar cleared its stop file after restore", "error", err)
			return
		}
		if !st.GetRestoreStartPending() {
			return
		}
		if time.Now().After(deadline) {
			e.logger.ErrorContext(ctx, "restore succeeded but the sidecar is still retrying pgbackrest start; WAL archiving stays stopped until it succeeds",
				"waited", restoreStartWait)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(restoreStartPoll):
		}
	}
}

// settleSidecarRestore runs after a restore call failed. It waits, bounded, for
// the sidecar to report that no restore is running, with each Status call
// bounded and independent of the caller's context (which may be the very thing
// that ended). Once the restore has ended it returns nil when the sidecar says
// the restore succeeded and PG_VERSION is in place, else the original error. If
// the restore cannot be shown to have ended it returns ErrRestoreUnsettled.
func (e *Engine) settleSidecarRestore(ctx context.Context, runErr error, output string) error {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sidecarRestoreDeadline())
	defer cancel()
	var shutdown <-chan struct{}
	if e.settings.Shutdown != nil {
		shutdown = e.settings.Shutdown.Done()
	}
	var st *pb.StatusResponse
	for {
		callCtx, callCancel := context.WithTimeout(sctx, restoreStatusCallTimeout)
		got, err := e.settings.Sidecar.Status(callCtx)
		callCancel()
		if err == nil && !got.GetRestoreInProgress() {
			st = got
			break
		}
		select {
		case <-shutdown:
			// Leaving is safe: the caller's marker keeps the directory untrusted.
			return fmt.Errorf("%w: shutting down while waiting for the restore to end (restore call failed: %w)", ErrRestoreUnsettled, runErr)
		case <-sctx.Done():
			return fmt.Errorf("%w: restore call failed (%w) and the sidecar still reports it running or cannot be reached", ErrRestoreUnsettled, runErr)
		case <-time.After(restoreSettlePoll):
		}
	}
	if st.GetLastRestoreSucceeded() && e.dataDirHasPGVersion() {
		e.logger.WarnContext(ctx, "restore call failed but the sidecar finished the restore successfully; treating it as done", "error", runErr, "output", output)
		return nil
	}
	return runErr
}

// dataDirHasPGVersion reports whether PG_VERSION exists, through the same
// anchored root as postgresql.auto.conf.
func (e *Engine) dataDirHasPGVersion() bool {
	root, rel, err := e.openAutoConf()
	if err != nil {
		return false
	}
	defer root.Close()
	_, err = root.Stat(filepath.Join(filepath.Dir(rel), "PG_VERSION"))
	return err == nil
}
