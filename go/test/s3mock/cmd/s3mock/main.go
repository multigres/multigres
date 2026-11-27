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

// go/test/s3mock/cmd/s3mock/main.go

// Command s3mock runs the S3 mock server.
//
// With --cert-out the PEM certificate is written (temp file plus rename, mode
// 0644 since it is public) before the "Endpoint:" line is printed, so that
// line is the readiness signal: once a caller sees it, the certificate file is
// complete and will never be observed partially written.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/multigres/multigres/go/test/s3mock"
)

// writeFileAtomic writes via a temp file in the destination directory and
// renames it into place, so a reader sees either no file or the whole file.
func writeFileAtomic(path string, data []byte, perm os.FileMode) (err error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	if _, err = tmp.Write(data); err != nil {
		return err
	}
	// CreateTemp makes the file 0600; the certificate is public.
	if err = tmp.Chmod(perm); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

func main() {
	// Parse command-line flags
	port := flag.Int("port", 9000, "Port to listen on (default: 9000)")
	requireSessions := flag.Bool("require-sessions", false, "Reject requests lacking a live X-Amz-Security-Token; mint tokens with POST /_session?ttl=<duration>")
	certOut := flag.String("cert-out", "", "Write the server's PEM certificate to this path (atomically, mode 0644) before the Endpoint line is printed, so that line means the file is ready")
	flag.Parse()

	// Remaining args are bucket names
	bucketNames := flag.Args()

	// Create and start server
	srv, err := s3mock.NewServer(*port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start S3 mock server: %v\n", err)
		os.Exit(1) //nolint:forbidigo // os.Exit is appropriate in main()
	}

	if *requireSessions {
		srv.RequireSessions()
	}

	if *certOut != "" {
		if err := writeFileAtomic(*certOut, srv.CertPEM(), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write certificate to %q: %v\n", *certOut, err)
			_ = srv.Stop()
			os.Exit(1) //nolint:forbidigo // os.Exit is appropriate in main()
		}
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
