//go:build !windows

/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Modifications Copyright 2025 Supabase, Inc.
*/

package servenv

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/tools/netutil"
)

// Init is the first phase of the server startup.
func (sv *ServEnv) Init(serviceName string) {
	sv.mu.Lock()
	sv.initStartTime = time.Now()
	sv.mu.Unlock()
	sv.lg.SetupLogging()

	// Initialize OpenTelemetry
	if err := sv.telemetry.InitTelemetry(context.Background(), serviceName); err != nil {
		slog.Error("Failed to initialize OpenTelemetry", "error", err)
		// Continue without telemetry rather than crashing
	}

	// Ignore SIGPIPE if specified
	// The Go runtime catches SIGPIPE for us on all fds except stdout/stderr
	// See https://golang.org/pkg/os/signal/#hdr-SIGPIPE
	if sv.catchSigpipe {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGPIPE)
		go func() {
			<-sigChan
			slog.Warn("Caught SIGPIPE (ignoring all future SIGPIPEs)")
			signal.Ignore(syscall.SIGPIPE)
		}()
	}

	// Add version tag to every info log
	sv.mu.Lock()
	if sv.inited {
		sv.mu.Unlock()
		log.Fatal("servenv.Init called second time")
	}
	sv.inited = true
	sv.mu.Unlock()

	// Once you run as root, you pretty much destroy the chances of a
	// non-privileged user starting the program correctly.
	if uid := os.Getuid(); uid == 0 {
		slog.Error("servenv.Init: running this as root makes no sense")
		os.Exit(1)
	}

	// We used to set this limit directly, but you pretty much have to
	// use a root account to allow increasing a limit reliably. Dropping
	// privileges is also tricky. The best strategy is to make a shell
	// script set up the limits as root and switch users before starting
	// the server.
	fdLimit := &syscall.Rlimit{}
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, fdLimit); err != nil {
		slog.Error("max-open-fds failed", "err", err)
	}

	// Limit the stack size. We don't need huge stacks and smaller limits mean
	// any infinite recursion fires earlier and on low memory systems avoids
	// out of memory issues in favor of a stack overflow error.
	debug.SetMaxStack(sv.maxStackSize)

	// Get hostname upfront so we can crash early if it fails.
	sv.populateHostname()

	sv.onInitHooks.Fire()
	sv.registerPidFile()
	sv.RegisterCommonHTTPEndpoints()
	sv.HTTPRegisterPprofProfile()
	sv.pprofInit()
	sv.updateServiceMap()
	sv.startOrphanDetection()
}

func (sv *ServEnv) populateHostname() {
	// If hostname was explicitly set via --hostname flag, use that
	if sv.hostname.Get() != "" {
		slog.Info("Using explicitly configured hostname for service URL", "hostname", sv.hostname.Get())
		return
	}

	// Otherwise, auto-detect hostname
	host, err := netutil.FullyQualifiedHostname()
	if err != nil {
		slog.Warn("Failed to get fully qualified hostname, falling back to simple hostname",
			"error", err,
			"note", "This may indicate DNS configuration issues but service will continue normally")
		host, err = os.Hostname()
		if err != nil {
			slog.Error("os.Hostname() failed", "err", err)
			os.Exit(1)
		}
		slog.Info("Using simple hostname for service URL", "hostname", host)
	} else {
		slog.Info("Using fully qualified hostname for service URL", "hostname", host)
	}
	sv.hostname.Set(host)
}

// startOrphanDetection starts a goroutine that monitors for orphan conditions.
// It checks:
// 1. If MULTIGRES_TESTDATA_DIR is set and the directory is deleted
// 2. If MULTIGRES_TEST_PARENT_PID is set and that process no longer exists
// If either condition is true, initiates graceful shutdown.
// This is used in integration tests to ensure child processes don't become
// orphans if the test runner is killed.
func (sv *ServEnv) startOrphanDetection() {
	// Only run if orphan detection environment variables are set
	if !IsTestOrphanDetectionEnabled() {
		return
	}

	testDataDir := os.Getenv("MULTIGRES_TESTDATA_DIR")
	testParentPIDStr := os.Getenv("MULTIGRES_TEST_PARENT_PID")

	var testParentPID int
	if testParentPIDStr != "" {
		var err error
		testParentPID, err = strconv.Atoi(testParentPIDStr)
		if err != nil {
			slog.Warn("Invalid MULTIGRES_TEST_PARENT_PID", "value", testParentPIDStr)
			testParentPID = 0
		}
	}

	slog.Info("Starting orphan detection",
		"testdata_dir", testDataDir,
		"test_parent_pid", testParentPID)

	// Channel to signal when close hooks have completed
	closeComplete := make(chan struct{})
	sv.OnClose(func() {
		close(closeComplete)
	})

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				shouldShutdown := false
				reason := ""

				// Check if testdata directory was deleted
				if testDataDir != "" {
					if _, err := os.Stat(testDataDir); os.IsNotExist(err) {
						shouldShutdown = true
						reason = "testdata directory deleted"
					}
				}

				// Check if test parent process died
				if testParentPID > 0 && !shouldShutdown {
					process, err := os.FindProcess(testParentPID)
					if err != nil || process.Signal(syscall.Signal(0)) != nil {
						shouldShutdown = true
						reason = "test parent process died"
					}
				}

				if shouldShutdown {
					slog.Warn("Orphan condition detected, initiating graceful shutdown",
						"reason", reason,
						"testdata_dir", testDataDir,
						"test_parent_pid", testParentPID)

					// Trigger graceful shutdown
					sv.exitChan <- syscall.SIGTERM

					// Wait for close hooks to complete or 10 second timeout
					select {
					case <-closeComplete:
						return
					case <-time.After(10 * time.Second):
						slog.Error("Graceful shutdown timed out after orphan detection, force killing")
						os.Exit(1)
					}
				}
			case <-closeComplete:
				// Normal shutdown - stop orphan detection
				return
			}
		}
	}()
}
