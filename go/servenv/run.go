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
// Modifications Copyright 2025 Supabase, Inc.

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
)

// Run starts listening for RPC and HTTP requests,
// and blocks until it the process gets a signal.
func (sv *ServEnv) Run(bindAddress string, port int, grpcServer *GrpcServer) {
	sv.PopulateListeningURL(int32(port))
	grpcServer.Create()
	sv.FireRunHooks()
	grpcServer.Serve(sv)

	l, err := net.Listen("tcp", net.JoinHostPort(bindAddress, strconv.Itoa(port)))
	if err != nil {
		slog.Error("failed to listen", "err", err)
		os.Exit(1)
	}

	// If port was 0, log the actual allocated port
	if port == 0 {
		if addr, ok := l.Addr().(*net.TCPAddr); ok {
			actualPort := addr.Port
			slog.Info("HTTP port was dynamically allocated", "requested_port", port, "actual_port", actualPort)
			// Update the ListeningURL with the actual port
			sv.PopulateListeningURL(int32(actualPort))
		}
	}
	go func() {
		err := sv.HTTPServe(l)
		if err != nil {
			slog.Error("http serve returned unexpected error", "err", err)
		}
	}()

	sv.exitChan = make(chan os.Signal, 1)
	signal.Notify(sv.exitChan, syscall.SIGTERM, syscall.SIGINT)
	slog.Info("service successfully started", "port", port)
	// Wait for signal
	<-sv.exitChan

	startTime := time.Now()
	slog.Info("entering lameduck mode", "period", sv.Timeouts.LameduckPeriod)
	slog.Info("firing asynchronous OnTerm hooks")
	go sv.OnTermHooks.Fire()

	sv.fireOnTermSyncHooks(sv.Timeouts.OnTermTimeout)
	if remain := sv.Timeouts.LameduckPeriod - time.Since(startTime); remain > 0 {
		slog.Info(fmt.Sprintf("sleeping an extra %v after OnTermSync to finish lameduck period", remain))
		time.Sleep(remain)
	}
	_ = l.Close()

	slog.Info("shutting down gracefully")
	sv.fireOnCloseHooks(sv.Timeouts.OnCloseTimeout)
	sv.ListeningURL = url.URL{}
}
