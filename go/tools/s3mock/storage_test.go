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

// go/tools/s3mock/storage_test.go
package s3mock

import (
	"bytes"
	"os"
	"testing"
)

func TestStorageCreateBucket(t *testing.T) {
	s := NewStorage()
	defer func() { _ = s.Close() }()

	err := s.CreateBucket("test-bucket")
	if err != nil {
		t.Fatalf("CreateBucket() failed: %v", err)
	}

	// Creating same bucket again should fail
	err = s.CreateBucket("test-bucket")
	if err == nil {
		t.Fatal("expected error when creating duplicate bucket")
	}
}

func TestStoragePutGetObject(t *testing.T) {
	s := NewStorage()
	defer func() { _ = s.Close() }()
	_ = s.CreateBucket("test-bucket")

	data := []byte("hello world")
	err := s.PutObject("test-bucket", "test-key", data)
	if err != nil {
		t.Fatalf("PutObject() failed: %v", err)
	}

	obj, err := s.GetObject("test-bucket", "test-key")
	if err != nil {
		t.Fatalf("GetObject() failed: %v", err)
	}

	// Verify data is read from disk
	objData, err := obj.ReadData()
	if err != nil {
		t.Fatalf("ReadData() failed: %v", err)
	}
	if !bytes.Equal(objData, data) {
		t.Fatalf("expected data %q, got %q", data, objData)
	}
	if obj.ETag == "" {
		t.Fatal("expected non-empty ETag")
	}
	if obj.LastModified.IsZero() {
		t.Fatal("expected non-zero LastModified")
	}
}

func TestStorageDeleteObject(t *testing.T) {
	s := NewStorage()
	defer func() { _ = s.Close() }()
	_ = s.CreateBucket("test-bucket")
	_ = s.PutObject("test-bucket", "test-key", []byte("data"))

	// Get file path before deletion
	obj, _ := s.GetObject("test-bucket", "test-key")
	filePath := obj.filePath

	// Verify file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatalf("file %q should exist before deletion", filePath)
	}

	err := s.DeleteObject("test-bucket", "test-key")
	if err != nil {
		t.Fatalf("DeleteObject() failed: %v", err)
	}

	// Getting deleted object should fail
	_, err = s.GetObject("test-bucket", "test-key")
	if err == nil {
		t.Fatal("expected error when getting deleted object")
	}

	// File should be removed from disk
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Fatalf("file %q should be removed from disk", filePath)
	}
}

func TestStorageListObjects(t *testing.T) {
	s := NewStorage()
	defer func() { _ = s.Close() }()
	_ = s.CreateBucket("test-bucket")
	_ = s.PutObject("test-bucket", "file1.txt", []byte("data1"))
	_ = s.PutObject("test-bucket", "file2.txt", []byte("data2"))
	_ = s.PutObject("test-bucket", "dir/file3.txt", []byte("data3"))

	objects := s.ListObjects("test-bucket", "", "")
	if len(objects) != 3 {
		t.Fatalf("expected 3 objects, got %d", len(objects))
	}
}

func TestStorageListObjectsWithPrefix(t *testing.T) {
	s := NewStorage()
	defer func() { _ = s.Close() }()
	_ = s.CreateBucket("test-bucket")
	_ = s.PutObject("test-bucket", "file1.txt", []byte("data1"))
	_ = s.PutObject("test-bucket", "dir/file2.txt", []byte("data2"))

	objects := s.ListObjects("test-bucket", "dir/", "")
	if len(objects) != 1 {
		t.Fatalf("expected 1 object with prefix 'dir/', got %d", len(objects))
	}
	if objects[0].Key != "dir/file2.txt" {
		t.Fatalf("expected key 'dir/file2.txt', got %q", objects[0].Key)
	}
}

func TestStorageConcurrentAccess(t *testing.T) {
	s := NewStorage()
	defer func() { _ = s.Close() }()
	_ = s.CreateBucket("test-bucket")

	// Concurrent writes and reads
	done := make(chan bool)
	for i := range 10 {
		go func(n int) {
			key := string(rune('a' + n))
			_ = s.PutObject("test-bucket", key, []byte("data"))
			_, _ = s.GetObject("test-bucket", key)
			done <- true
		}(i)
	}

	for range 10 {
		<-done
	}
}

func TestStorageTempDirectory(t *testing.T) {
	s := NewStorage()
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close() failed: %v", err)
		}
	}()

	// Temp directory should exist
	if s.tempDir == "" {
		t.Fatal("expected non-empty tempDir")
	}
	if _, err := os.Stat(s.tempDir); os.IsNotExist(err) {
		t.Fatalf("temp directory %q does not exist", s.tempDir)
	}

	tempDir := s.tempDir

	// After Close(), temp directory should be removed
	if err := s.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}
	if _, err := os.Stat(tempDir); !os.IsNotExist(err) {
		t.Fatalf("temp directory %q should be removed after Close()", tempDir)
	}
}

func TestStorageDiskBasedObject(t *testing.T) {
	s := NewStorage()
	defer func() { _ = s.Close() }()

	_ = s.CreateBucket("test-bucket")

	data := []byte("hello from disk")
	err := s.PutObject("test-bucket", "test-key", data)
	if err != nil {
		t.Fatalf("PutObject() failed: %v", err)
	}

	// Verify file exists on disk
	obj, err := s.GetObject("test-bucket", "test-key")
	if err != nil {
		t.Fatalf("GetObject() failed: %v", err)
	}

	if obj.filePath == "" {
		t.Fatal("expected non-empty filePath")
	}

	// Verify file exists
	if _, err := os.Stat(obj.filePath); os.IsNotExist(err) {
		t.Fatalf("file %q should exist on disk", obj.filePath)
	}

	// Verify we can read the data
	fileData, err := os.ReadFile(obj.filePath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	if !bytes.Equal(fileData, data) {
		t.Fatalf("expected data %q, got %q", data, fileData)
	}
}

func TestStorageCreateMultipartUpload(t *testing.T) {
	s := NewStorage()
	defer func() { _ = s.Close() }()
	_ = s.CreateBucket("test-bucket")

	uploadID, err := s.CreateMultipartUpload("test-bucket", "test-key")
	if err != nil {
		t.Fatalf("CreateMultipartUpload() failed: %v", err)
	}

	if uploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}

	// Upload ID should be unique
	uploadID2, _ := s.CreateMultipartUpload("test-bucket", "test-key2")
	if uploadID == uploadID2 {
		t.Fatal("expected unique upload IDs")
	}
}

func TestStorageUploadPart(t *testing.T) {
	s := NewStorage()
	defer func() { _ = s.Close() }()
	_ = s.CreateBucket("test-bucket")

	uploadID, _ := s.CreateMultipartUpload("test-bucket", "test-key")

	// Upload part 1
	part1Data := []byte("part one data")
	etag1, err := s.UploadPart("test-bucket", "test-key", uploadID, 1, part1Data)
	if err != nil {
		t.Fatalf("UploadPart(1) failed: %v", err)
	}
	if etag1 == "" {
		t.Fatal("expected non-empty ETag for part 1")
	}

	// Upload part 2
	part2Data := []byte("part two data")
	etag2, err := s.UploadPart("test-bucket", "test-key", uploadID, 2, part2Data)
	if err != nil {
		t.Fatalf("UploadPart(2) failed: %v", err)
	}
	if etag2 == "" {
		t.Fatal("expected non-empty ETag for part 2")
	}

	// ETags should be different for different data
	if etag1 == etag2 {
		t.Fatal("expected different ETags for different parts")
	}

	// Uploading with invalid upload ID should fail
	_, err = s.UploadPart("test-bucket", "test-key", "invalid-id", 1, part1Data)
	if err == nil {
		t.Fatal("expected error for invalid upload ID")
	}
}

func TestStorageCompleteMultipartUpload(t *testing.T) {
	s := NewStorage()
	defer func() { _ = s.Close() }()
	_ = s.CreateBucket("test-bucket")

	uploadID, _ := s.CreateMultipartUpload("test-bucket", "test-key")

	// Upload parts out of order
	part1Data := []byte("aaaaa")
	part2Data := []byte("bbbbb")
	part3Data := []byte("ccccc")

	etag1, _ := s.UploadPart("test-bucket", "test-key", uploadID, 1, part1Data)
	etag3, _ := s.UploadPart("test-bucket", "test-key", uploadID, 3, part3Data)
	etag2, _ := s.UploadPart("test-bucket", "test-key", uploadID, 2, part2Data)

	// Complete the upload with parts in order
	parts := []CompletedPart{
		{PartNumber: 1, ETag: etag1},
		{PartNumber: 2, ETag: etag2},
		{PartNumber: 3, ETag: etag3},
	}

	etag, err := s.CompleteMultipartUpload("test-bucket", "test-key", uploadID, parts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload() failed: %v", err)
	}
	if etag == "" {
		t.Fatal("expected non-empty ETag")
	}

	// Verify final object exists and has correct data
	obj, err := s.GetObject("test-bucket", "test-key")
	if err != nil {
		t.Fatalf("GetObject() failed: %v", err)
	}

	expectedData := append(append(part1Data, part2Data...), part3Data...)
	objData, _ := obj.ReadData()
	if !bytes.Equal(objData, expectedData) {
		t.Fatalf("expected data %q, got %q", expectedData, objData)
	}

	// Upload should be removed after completion
	s.mu.RLock()
	_, exists := s.multipartUploads[uploadID]
	s.mu.RUnlock()
	if exists {
		t.Fatal("expected upload to be removed after completion")
	}
}

func TestStorageAbortMultipartUpload(t *testing.T) {
	s := NewStorage()
	defer func() { _ = s.Close() }()
	_ = s.CreateBucket("test-bucket")

	uploadID, _ := s.CreateMultipartUpload("test-bucket", "test-key")
	_, _ = s.UploadPart("test-bucket", "test-key", uploadID, 1, []byte("data"))

	err := s.AbortMultipartUpload("test-bucket", "test-key", uploadID)
	if err != nil {
		t.Fatalf("AbortMultipartUpload() failed: %v", err)
	}

	// Upload should be removed
	s.mu.RLock()
	_, exists := s.multipartUploads[uploadID]
	s.mu.RUnlock()
	if exists {
		t.Fatal("expected upload to be removed after abort")
	}

	// Object should not exist
	_, err = s.GetObject("test-bucket", "test-key")
	if err == nil {
		t.Fatal("expected object to not exist after abort")
	}
}
