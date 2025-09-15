// Copyright 2019 The Vitess Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Modifications Copyright 2025 The Multigres Authors.

package servenv

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/event"
)

var (
	onCloseHooks event.Hooks
	// ExitChan waits for a signal that tells the process to terminate
	ExitChan chan os.Signal
)

// Run starts listening for RPC and HTTP requests,
// and blocks until it the process gets a signal.
func Run(bindAddress string, port int) {
	populateListeningURL(int32(port))
	createGRPCServer()
	onRunHooks.Fire()
	serveGRPC()
	// Do we want this?
	// serveSocketFile()

	l, err := net.Listen("tcp", net.JoinHostPort(bindAddress, strconv.Itoa(port)))
	if err != nil {
		slog.Error("failed to listen", "err", err)
		os.Exit(1)
	}
	go func() {
		err := HTTPServe(l)
		if err != nil {
			slog.Error("http serve returned unexpected error", "err", err)
		}
	}()

	ExitChan = make(chan os.Signal, 1)
	signal.Notify(ExitChan, syscall.SIGTERM, syscall.SIGINT)
	slog.Info("service successfully started", "port", port)
	// Wait for signal
	<-ExitChan

	startTime := time.Now()
	slog.Info("entering lameduck mode", "period", timeouts.LameduckPeriod)
	slog.Info("firing asynchronous OnTerm hooks")
	go onTermHooks.Fire()

	fireOnTermSyncHooks(timeouts.OnTermTimeout)
	if remain := timeouts.LameduckPeriod - time.Since(startTime); remain > 0 {
		slog.Info(fmt.Sprintf("sleeping an extra %v after OnTermSync to finish lameduck period", remain))
		time.Sleep(remain)
	}
	_ = l.Close()

	slog.Info("shutting down gracefully")
	fireOnCloseHooks(timeouts.OnCloseTimeout)
	ListeningURL = url.URL{}
}

// OnClose registers f to be run at the end of the app lifecycle.
// This happens after the lameduck period just before the program exits.
// All hooks are run in parallel.
func OnClose(f func()) {
	onCloseHooks.Add(f)
}
