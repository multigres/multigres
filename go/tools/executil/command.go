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

// Package executil provides safe subprocess execution with graceful termination,
// explicit environment variable handling, and OpenTelemetry trace propagation.
//
// # Why Use executil Instead of exec.Command?
//
// Go's standard library exec package has several issues for production infrastructure:
//
//  1. Ungraceful termination: exec.CommandContext immediately kills subprocesses
//     on context cancellation, preventing log flushing and telemetry export
//  2. No trace propagation: Manual TRACEPARENT handling is error-prone
//  3. Environment footguns: exec.Cmd.Env has confusing nil semantics
//     (nil = inherit, non-nil = replace entirely)
//  4. Implicit termination: No control over grace periods
//
// By using executil consistently across the codebase, we ensure:
//
//   - Graceful termination (SIGTERM → SIGKILL) reduces telemetry data loss
//   - Automatic trace context propagation to all subprocesses
//   - Explicit environment variable handling (AddEnv/SetEnv)
//   - Explicit grace period control (automatic or explicit)
//
// # Graceful Termination
//
// Commands are terminated gracefully with SIGTERM first, escalating to SIGKILL
// if needed. This allows subprocesses to flush logs, send telemetry, and clean
// up properly.
//
// Termination can happen in two ways:
//   - Automatic: When the parent context passed to Command() is cancelled,
//     the process is terminated with the default grace period (10s).
//   - Explicit: Call Stop(ctx) where ctx's timeout controls the grace period,
//     or call Terminate(ctx)/Kill(ctx) for lower-level control.
//
// # Environment Variables
//
// Environment variables are handled explicitly via AddEnv() and SetEnv() methods,
// avoiding the subtle pitfalls of exec.Cmd.Env (where nil means "inherit" but
// non-nil means "replace entirely").
//
// # Trace Propagation
//
// Trace context is automatically propagated to subprocesses via the TRACEPARENT
// environment variable if the context contains a valid span. This ensures
// distributed tracing works correctly across process boundaries.
//
// # Example
//
//	ctx := context.Background()
//	cmd := executil.Command(ctx, "postgres", "-D", dataDir).
//	    AddEnv("PGPORT=5432").
//	    SetDir(workDir)
//
//	if err := cmd.Start(); err != nil {
//	    return err
//	}
//
//	// Process will be gracefully terminated when ctx is cancelled
//	// or you can explicitly control termination:
//	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//	exitErr, stopped := cmd.Stop(stopCtx)
package executil

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/multigres/multigres/go/tools/ctxutil"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// DefaultGracePeriod is the time to wait after SIGTERM before escalating to SIGKILL.
// This is used when the parent context is cancelled.
const DefaultGracePeriod = 10 * time.Second

// DefaultKillTimeout is the time to wait for a process to exit after SIGKILL.
// SIGKILL should be nearly instant, but zombies or uninterruptible sleep can delay.
const DefaultKillTimeout = 5 * time.Second

const tracingServiceName = "multigres"

var tracer = otel.Tracer(tracingServiceName)

// Cmd wraps exec.Cmd with a builder pattern for safe configuration.
// Create with Command() or CommandWithGracePeriod().
type Cmd struct {
	*exec.Cmd
	parentCtx          context.Context
	defaultGracePeriod time.Duration
	extraEnv           []string
	clientSpan         bool

	// Termination coordination
	terminateOnce sync.Once
	terminated    chan struct{} // Closed when Terminate() is called
	waitDone      chan struct{} // Closed when Wait() completes
	waitErr       error         // Result of Wait() (valid after waitDone closed)
	waitOnce      sync.Once
}

// Command creates a new Cmd with graceful termination support.
//
// If the parent context is cancelled, the process receives SIGTERM and is given
// DefaultGracePeriod to exit before SIGKILL. For explicit termination with a
// different grace period, call Terminate(ctx) where ctx's timeout controls the wait.
//
// By default, the command inherits the parent process environment.
// Use AddEnv() to add variables, or SetEnv() to replace the entire environment.
func Command(ctx context.Context, name string, args ...string) *Cmd {
	return CommandWithGracePeriod(ctx, DefaultGracePeriod, name, args...)
}

// CommandWithGracePeriod creates a Cmd with a custom default grace period.
//
// The grace period controls how long to wait after SIGTERM before sending SIGKILL
// when the parent context is cancelled. For explicit Terminate() calls, the caller
// controls the grace period via the context timeout instead.
//
// Use a shorter grace period for commands that should terminate quickly
// (e.g., 100ms for simple queries).
func CommandWithGracePeriod(ctx context.Context, gracePeriod time.Duration, name string, args ...string) *Cmd {
	return &Cmd{
		Cmd:                exec.Command(name, args...),
		parentCtx:          ctx,
		defaultGracePeriod: gracePeriod,
		terminated:         make(chan struct{}),
		waitDone:           make(chan struct{}),
	}
}

// AddEnv adds environment variables to the command. Variables are specified
// as "KEY=value" strings. Safe to call multiple times - variables accumulate.
//
// Variables are added on top of the inherited environment (or the explicit
// base if SetEnv was called). The actual environment is finalized when
// Start/Run/Output/CombinedOutput is called.
func (c *Cmd) AddEnv(keyvals ...string) *Cmd {
	c.extraEnv = append(c.extraEnv, keyvals...)
	return c
}

// SetEnv replaces the entire environment with the provided variables.
// The command will NOT inherit any environment from the parent process.
//
// Call AddEnv() after SetEnv() to add additional variables on top of
// this explicit base.
func (c *Cmd) SetEnv(env []string) *Cmd {
	c.Cmd.Env = env
	return c
}

// SetDir sets the working directory for the command.
func (c *Cmd) SetDir(dir string) *Cmd {
	c.Cmd.Dir = dir
	return c
}

// SetStdin sets the stdin for the command.
func (c *Cmd) SetStdin(r *os.File) *Cmd {
	c.Cmd.Stdin = r
	return c
}

// SetStdout sets the stdout for the command.
func (c *Cmd) SetStdout(w *os.File) *Cmd {
	c.Cmd.Stdout = w
	return c
}

// SetStderr sets the stderr for the command.
func (c *Cmd) SetStderr(w *os.File) *Cmd {
	c.Cmd.Stderr = w
	return c
}

// WithClientSpan enables creating an OpenTelemetry client span around
// the command execution. The span is started when Start/Run is called
// and ended when the command completes.
func (c *Cmd) WithClientSpan() *Cmd {
	c.clientSpan = true
	return c
}

// finalizeEnv prepares cmd.Env before execution, including trace propagation.
func (c *Cmd) finalizeEnv() {
	// Add TRACEPARENT if context has a valid span
	if envVar := telemetry.TraceparentEnvVar(c.parentCtx); envVar != "" {
		c.extraEnv = append(c.extraEnv, envVar)
	}

	if len(c.extraEnv) == 0 {
		return
	}

	if c.Cmd.Env == nil {
		c.Cmd.Env = os.Environ()
	}
	c.Cmd.Env = append(c.Cmd.Env, c.extraEnv...)
}

// Start starts the command without waiting for it to complete.
//
// If the parent context is cancelled, the process will be terminated with
// SIGTERM followed by SIGKILL after the default grace period.
//
// Note: WithClientSpan() has no effect on Start() since the span cannot be
// ended until Wait() is called. Use Run() for client span support.
func (c *Cmd) Start() error {
	c.finalizeEnv()
	if err := c.Cmd.Start(); err != nil {
		return err
	}

	// Watch for parent context cancellation
	go func() {
		select {
		case <-c.parentCtx.Done():
			// Parent context cancelled - terminate with default grace period
			// Fresh context needed; parent context is cancelled
			termCtx, termCancel := context.WithTimeout(ctxutil.Detach(c.parentCtx), c.defaultGracePeriod)
			_, exited := c.Terminate(termCtx)
			termCancel()
			if !exited {
				// Fresh context needed for kill timeout
				killCtx, killCancel := context.WithTimeout(ctxutil.Detach(c.parentCtx), DefaultKillTimeout)
				_, _ = c.Kill(killCtx)
				killCancel()
			}
		case <-c.terminated:
			// Already terminated explicitly, nothing to do
		case <-c.waitDone:
			// Process exited naturally, nothing to do
		}
	}()

	return nil
}

// Wait waits for the command to exit and returns its exit status.
// Wait must be called after Start() to release resources.
// Safe to call multiple times or concurrently - returns cached result.
func (c *Cmd) Wait() error {
	// Ensure we only call the underlying Wait() once.
	// sync.Once.Do blocks all callers until the first call completes.
	c.waitOnce.Do(func() {
		c.waitErr = c.Cmd.Wait()
		// Channel close provides happens-before guarantee for waitErr read
		close(c.waitDone)
	})

	// Block until Wait() completes (no-op if already done), then return cached result
	<-c.waitDone
	return c.waitErr
}

// Terminate sends SIGTERM to the process and waits for it to exit gracefully.
//
// Returns (exitErr, true) if the process exited before ctx expired.
// Returns (nil, false) if ctx expired before the process exited.
//
// If false is returned, the process is still running. Call Kill() to force termination.
//
// Safe to call multiple times or concurrently - only the first call sends SIGTERM,
// subsequent calls just wait for the process to exit.
//
// Panics if called on a Cmd not created via Command() or CommandWithGracePeriod().
func (c *Cmd) Terminate(ctx context.Context) (error, bool) {
	if c.terminated == nil {
		panic("executil: Terminate called on Cmd not created via Command()")
	}

	// Send SIGTERM only once
	c.terminateOnce.Do(func() {
		close(c.terminated)
		if c.Process != nil {
			_ = c.Process.Signal(syscall.SIGTERM)
		}
	})

	// Ensure Wait() is running in background.
	// Result is stored in c.waitErr and signaled via c.waitDone.
	go func() { _ = c.Wait() }()

	// Wait for process exit or context timeout
	select {
	case <-c.waitDone:
		return c.waitErr, true
	case <-ctx.Done():
		return nil, false
	}
}

// Kill sends SIGKILL to the process and waits for it to exit.
//
// The context controls how long to wait for the process to actually exit.
// SIGKILL should be nearly instant, but zombies or uninterruptible sleep can delay.
//
// Returns (exitErr, true) if the process exited before ctx expired.
// Returns (ctx.Err(), false) if the wait timed out.
//
// Safe to call after Terminate() times out - reuses the same Wait() call.
func (c *Cmd) Kill(ctx context.Context) (error, bool) {
	// Send SIGKILL
	if c.Process != nil {
		_ = c.Process.Kill()
	}

	// Ensure Wait() is running in background.
	// Result is stored in c.waitErr and signaled via c.waitDone.
	go func() { _ = c.Wait() }()

	// Wait for process exit or context timeout
	select {
	case <-c.waitDone:
		return c.waitErr, true
	case <-ctx.Done():
		return ctx.Err(), false
	}
}

// Stop gracefully stops the process: SIGTERM first, then SIGKILL if needed.
//
// The context controls how long to wait for graceful shutdown (SIGTERM phase).
// If the process doesn't exit before ctx expires, SIGKILL is sent with a short
// fixed timeout (100ms). SIGKILL should be nearly instant - if it times out,
// the process is likely a zombie or in uninterruptible sleep (rare system issue).
//
// Returns (exitErr, true) if the process stopped.
// Returns (nil, false) if SIGKILL timed out (very rare - indicates system issue).
//
// This is the recommended way to stop a process - always try graceful termination first.
//
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//	exitErr, stopped := cmd.Stop(ctx)
//	// Tries SIGTERM for up to 10s, then SIGKILL with 100ms timeout
func (c *Cmd) Stop(ctx context.Context) (error, bool) {
	exitErr, exited := c.Terminate(ctx)

	if exited {
		return exitErr, true
	}

	killCtx, killCancel := context.WithTimeout(ctxutil.Detach(ctx), 100*time.Millisecond)
	exitErr, killed := c.Kill(killCtx)
	killCancel()

	return exitErr, killed
}

// Run starts the command and waits for it to complete.
// If WithClientSpan() was called, an OpenTelemetry span is created around
// the command execution.
func (c *Cmd) Run() error {
	if c.clientSpan {
		_, span := tracer.Start(c.parentCtx, c.Cmd.Path)
		defer span.End()
	}
	c.finalizeEnv()
	return c.Cmd.Run()
}

// Output runs the command and returns its stdout.
// If WithClientSpan() was called, an OpenTelemetry span is created around
// the command execution.
func (c *Cmd) Output() ([]byte, error) {
	if c.clientSpan {
		_, span := tracer.Start(c.parentCtx, c.Cmd.Path)
		defer span.End()
	}
	c.finalizeEnv()
	return c.Cmd.Output()
}

// CombinedOutput runs the command and returns its combined stdout and stderr.
// If WithClientSpan() was called, an OpenTelemetry span is created around
// the command execution.
func (c *Cmd) CombinedOutput() ([]byte, error) {
	if c.clientSpan {
		_, span := tracer.Start(c.parentCtx, c.Cmd.Path)
		defer span.End()
	}
	c.finalizeEnv()
	return c.Cmd.CombinedOutput()
}
