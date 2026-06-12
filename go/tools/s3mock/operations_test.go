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

// go/tools/s3mock/operations_test.go
package s3mock

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleHeadBucket(t *testing.T) {
	storage := NewStorage()
	defer func() { _ = storage.Close() }()
	_ = storage.CreateBucket("test-bucket")

	req := httptest.NewRequest("HEAD", "/test-bucket", nil)
	w := httptest.NewRecorder()

	handleHeadBucket(w, req, storage)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}
}

func TestHandleHeadBucketNotFound(t *testing.T) {
	storage := NewStorage()
	defer func() { _ = storage.Close() }()

	req := httptest.NewRequest("HEAD", "/nonexistent", nil)
	w := httptest.NewRecorder()

	handleHeadBucket(w, req, storage)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", w.Code)
	}
}

func TestHandlePutObject(t *testing.T) {
	storage := NewStorage()
	defer func() { _ = storage.Close() }()
	_ = storage.CreateBucket("test-bucket")

	req := httptest.NewRequest("PUT", "/test-bucket/test-key", strings.NewReader("hello world"))
	w := httptest.NewRecorder()

	handlePutObject(w, req, storage)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	// Verify ETag header is present
	etag := w.Header().Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag header")
	}

	// Verify object was stored
	obj, err := storage.GetObject("test-bucket", "test-key")
	if err != nil {
		t.Fatalf("object not found after PUT: %v", err)
	}
	data, err := obj.ReadData()
	if err != nil {
		t.Fatalf("failed to read object data: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("expected data 'hello world', got %q", data)
	}
}

func TestHandleGetObject(t *testing.T) {
	storage := NewStorage()
	defer func() { _ = storage.Close() }()
	_ = storage.CreateBucket("test-bucket")
	_ = storage.PutObject("test-bucket", "test-key", []byte("hello world"))

	req := httptest.NewRequest("GET", "/test-bucket/test-key", nil)
	w := httptest.NewRecorder()

	handleGetObject(w, req, storage)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	body := w.Body.String()
	if body != "hello world" {
		t.Fatalf("expected body 'hello world', got %q", body)
	}

	// Verify headers
	if w.Header().Get("Content-Length") != "11" {
		t.Fatalf("expected Content-Length 11, got %s", w.Header().Get("Content-Length"))
	}
	if w.Header().Get("ETag") == "" {
		t.Fatal("expected ETag header")
	}
	if w.Header().Get("Last-Modified") == "" {
		t.Fatal("expected Last-Modified header")
	}
}

func TestHandleGetObjectNotFound(t *testing.T) {
	storage := NewStorage()
	defer func() { _ = storage.Close() }()
	_ = storage.CreateBucket("test-bucket")

	req := httptest.NewRequest("GET", "/test-bucket/nonexistent", nil)
	w := httptest.NewRecorder()

	handleGetObject(w, req, storage)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", w.Code)
	}
}

func TestHandleHeadObject(t *testing.T) {
	storage := NewStorage()
	defer func() { _ = storage.Close() }()
	_ = storage.CreateBucket("test-bucket")
	_ = storage.PutObject("test-bucket", "test-key", []byte("hello world"))

	req := httptest.NewRequest("HEAD", "/test-bucket/test-key", nil)
	w := httptest.NewRecorder()

	handleHeadObject(w, req, storage)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	// Verify headers but no body
	if w.Header().Get("Content-Length") != "11" {
		t.Fatalf("expected Content-Length 11, got %s", w.Header().Get("Content-Length"))
	}
	if w.Body.Len() != 0 {
		t.Fatal("expected empty body for HEAD request")
	}
}

func TestHandleDeleteObject(t *testing.T) {
	storage := NewStorage()
	defer func() { _ = storage.Close() }()
	_ = storage.CreateBucket("test-bucket")
	_ = storage.PutObject("test-bucket", "test-key", []byte("data"))

	req := httptest.NewRequest("DELETE", "/test-bucket/test-key", nil)
	w := httptest.NewRecorder()

	handleDeleteObject(w, req, storage)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected status 204, got %d", w.Code)
	}

	// Verify object was deleted
	_, err := storage.GetObject("test-bucket", "test-key")
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatal("expected object to be deleted")
	}
}

func TestHandleListObjectsV2(t *testing.T) {
	storage := NewStorage()
	defer func() { _ = storage.Close() }()
	_ = storage.CreateBucket("test-bucket")
	_ = storage.PutObject("test-bucket", "file1.txt", []byte("data1"))
	_ = storage.PutObject("test-bucket", "file2.txt", []byte("data2"))

	req := httptest.NewRequest("GET", "/test-bucket?list-type=2", nil)
	w := httptest.NewRecorder()

	handleListObjectsV2(w, req, storage)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	// Parse XML response
	body := w.Body.String()
	if !strings.Contains(body, "file1.txt") {
		t.Fatal("expected file1.txt in response")
	}
	if !strings.Contains(body, "file2.txt") {
		t.Fatal("expected file2.txt in response")
	}
	if !strings.Contains(body, "<KeyCount>2</KeyCount>") {
		t.Fatal("expected KeyCount=2 in response")
	}
}
