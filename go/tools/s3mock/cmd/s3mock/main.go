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

// go/tools/s3mock/cmd/s3mock/main.go
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/multigres/multigres/go/tools/s3mock"
)

func main() {
	// Parse command-line flags
	port := flag.Int("port", 9000, "Port to listen on (default: 9000)")
	flag.Parse()

	// Remaining args are bucket names
	bucketNames := flag.Args()

	// Create and start server
	srv, err := s3mock.NewServer(*port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start S3 mock server: %v\n", err)
		os.Exit(1) //nolint:forbidigo // os.Exit is appropriate in main()
	}

	// Create buckets from command line args
	for _, bucketName := range bucketNames {
		if bucketName == "" {
			continue
		}
		if err := srv.CreateBucket(bucketName); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create bucket %q: %v\n", bucketName, err)
			_ = srv.Stop()
			os.Exit(1) //nolint:forbidigo // os.Exit is appropriate in main()
		}
		fmt.Printf("Created bucket: %s\n", bucketName)
	}

	// Print server info
	fmt.Printf("S3 Mock Server started\n")
	fmt.Printf("Endpoint: %s\n", srv.Endpoint())
	fmt.Printf("Press Ctrl+C to stop\n")

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	if err := srv.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "Error during shutdown: %v\n", err)
		os.Exit(1) //nolint:forbidigo // os.Exit is appropriate in main()
	}
	fmt.Println("Server stopped")
}
