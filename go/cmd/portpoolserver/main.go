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

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/multigres/multigres/go/test/utils/poolserver"
)

// main for the portpoolserver
//
// Portpoolserver is a cross-process port allocation service for integration
// tests.
//
// It holds an open TCP listener on every port it allocates, preventing
// parallel test binaries from obtaining the same port from the OS. Test
// processes connect over a Unix-domain socket to allocate ports and to release
// the server's listener just before starting a child process.
//
// Usage in CI:
//
//	./bin/portpoolserver --socket /tmp/multigres-port-pool.sock &
//	export MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock
//	go test ./go/test/endtoend/...
//	kill %1
//
// The socket path is printed to stdout on startup, so no extra flag is needed.
func main() {
	socketPath := flag.String("socket", "/tmp/multigres-port-pool.sock",
		"Unix socket path to listen on. Set MULTIGRES_PORT_POOL_ADDR to this value in test processes.")
	flag.Parse()

	srv, err := poolserver.NewServer(*socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "portpoolserver: %v\n", err)
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}

	// Print socket path so CI scripts can capture it.
	fmt.Println(srv.SocketPath())

	// Run server in background; block until signal.
	done := make(chan struct{})
	go func() {
		srv.Serve()
		close(done)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sigCh:
	case <-done:
	}
	srv.Stop()
}
