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

// go/tools/s3mock/server.go
package s3mock

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// Server is an S3-compatible mock server
type Server struct {
	storage  *Storage
	server   *http.Server
	listener net.Listener
	endpoint string
}

// NewServer creates and starts a new S3 mock server on the specified port
func NewServer(port int) (*Server, error) {
	storage := NewStorage()

	// Generate TLS config
	tlsConfig, err := generateTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to generate TLS config: %w", err)
	}

	// Create listener on specified port
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	listener, err := tls.Listen("tcp", addr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create listener: %w", err)
	}

	// Extract actual port from listener (important when port=0)
	actualAddr := listener.Addr().(*net.TCPAddr)
	actualPort := actualAddr.Port

	// Build endpoint URL with actual port
	endpoint := fmt.Sprintf("https://127.0.0.1:%d", actualPort)

	// Create server
	s := &Server{
		storage:  storage,
		listener: listener,
		endpoint: endpoint,
	}

	// Create HTTP server with router
	s.server = &http.Server{
		Handler:      s.router(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// Start server in background
	go func() { _ = s.server.Serve(listener) }()

	return s, nil
}

// loggingMiddleware wraps a handler with request logging
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[S3] %s %s%s\n", r.Method, r.URL.Path, func() string {
			if r.URL.RawQuery != "" {
				return "?" + r.URL.RawQuery
			}
			return ""
		}())
		next.ServeHTTP(w, r)
	})
}

// isLoggingEnabled checks if S3mock logging should be enabled based on environment variable
func isLoggingEnabled() bool {
	val := os.Getenv("MULTIGRES_TEST_LOG_S3MOCK")
	if val == "" {
		return false
	}
	// Accept: 1, true, True, TRUE, yes, Yes, YES
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

// router creates the HTTP request router
func (s *Server) router() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, key := parseBucketAndKey(r.URL.Path)

		// Route based on method and query params
		switch r.Method {
		case "HEAD":
			if key == "" {
				handleHeadBucket(w, r, s.storage)
			} else {
				handleHeadObject(w, r, s.storage)
			}
		case "GET":
			if key == "" && r.URL.Query().Get("list-type") == "2" {
				handleListObjectsV2(w, r, s.storage)
			} else if key != "" {
				handleGetObject(w, r, s.storage)
			} else {
				writeS3Error(w, "MethodNotAllowed", "Operation not implemented", http.StatusMethodNotAllowed)
			}
		case "POST":
			// Check for multipart upload operations
			if r.URL.Query().Has("delete") {
				// DeleteObjects (batch delete)
				handleDeleteObjects(w, r, s.storage)
			} else if r.URL.Query().Has("uploads") {
				// CreateMultipartUpload
				handleCreateMultipartUpload(w, r, s.storage)
			} else if r.URL.Query().Has("uploadId") {
				// CompleteMultipartUpload
				handleCompleteMultipartUpload(w, r, s.storage)
			} else {
				writeS3Error(w, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
			}
		case "PUT":
			if key == "" {
				// Create bucket
				writeS3Error(w, "MethodNotAllowed", "Bucket creation not supported via HTTP", http.StatusMethodNotAllowed)
			} else if r.URL.Query().Has("uploadId") {
				// UploadPart
				handleUploadPart(w, r, s.storage)
			} else {
				// PutObject
				handlePutObject(w, r, s.storage)
			}
		case "DELETE":
			if r.URL.Query().Has("uploadId") {
				// AbortMultipartUpload
				handleAbortMultipartUpload(w, r, s.storage)
			} else if key != "" {
				// DeleteObject
				handleDeleteObject(w, r, s.storage)
			} else {
				writeS3Error(w, "MethodNotAllowed", "Bucket deletion not supported", http.StatusMethodNotAllowed)
			}
		default:
			writeS3Error(w, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// Only apply logging middleware if enabled
	if isLoggingEnabled() {
		return loggingMiddleware(mux)
	}
	return mux
}

// Endpoint returns the HTTPS endpoint URL
func (s *Server) Endpoint() string {
	return s.endpoint
}

// CreateBucket creates a bucket in the mock storage
func (s *Server) CreateBucket(name string) error {
	return s.storage.CreateBucket(name)
}

// Stop gracefully stops the server and cleans up storage
func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	// Shutdown HTTP server
	if err := s.server.Shutdown(ctx); err != nil {
		return err
	}

	// Clean up storage
	return s.storage.Close()
}
