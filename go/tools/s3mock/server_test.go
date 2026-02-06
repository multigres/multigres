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

// go/tools/s3mock/server_test.go
package s3mock

import (
	"bytes"
	"crypto/tls"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestNewServer(t *testing.T) {
	srv, err := NewServer(0) // Port 0 = random port for tests
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	if srv.Endpoint() == "" {
		t.Fatal("expected non-empty endpoint")
	}

	if !strings.HasPrefix(srv.Endpoint(), "https://127.0.0.1:") {
		t.Fatalf("expected https://127.0.0.1:port, got %s", srv.Endpoint())
	}
}

func TestServerHTTPSEndpoint(t *testing.T) {
	srv, err := NewServer(0) // Port 0 = random port for tests
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	// Create HTTP client that skips TLS verification
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // #nosec G402 - test code using self-signed certificates
			},
		},
		Timeout: 5 * time.Second,
	}

	// Test HEAD on non-existent bucket (should return 404)
	resp, err := client.Head(srv.Endpoint() + "/nonexistent")
	if err != nil {
		t.Fatalf("HEAD request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestServerCreateBucket(t *testing.T) {
	srv, err := NewServer(0) // Port 0 = random port for tests
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	err = srv.CreateBucket("test-bucket")
	if err != nil {
		t.Fatalf("CreateBucket() failed: %v", err)
	}

	// Verify bucket exists via HTTP
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // #nosec G402 - test code using self-signed certificates
			},
		},
	}

	resp, err := client.Head(srv.Endpoint() + "/test-bucket")
	if err != nil {
		t.Fatalf("HEAD request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestServerPutGetObject(t *testing.T) {
	srv, err := NewServer(0) // Port 0 = random port for tests
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	_ = srv.CreateBucket("test-bucket")

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // #nosec G402 - test code using self-signed certificates
			},
		},
	}

	// PUT object
	data := []byte("hello world")
	req, _ := http.NewRequest("PUT", srv.Endpoint()+"/test-bucket/test-key", bytes.NewReader(data))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT request failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// GET object
	resp, err = client.Get(srv.Endpoint() + "/test-bucket/test-key")
	if err != nil {
		t.Fatalf("GET request failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(body, data) {
		t.Fatalf("expected data %q, got %q", data, body)
	}
}
