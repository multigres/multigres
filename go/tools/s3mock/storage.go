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

// go/tools/s3mock/storage.go
package s3mock

import (
	"crypto/md5" // #nosec G501 - MD5 used for S3 ETag calculation (non-cryptographic use)
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrObjectNotFound      = errors.New("object not found")
	ErrUploadNotFound      = errors.New("upload not found")
)

// MultipartUpload tracks an in-progress multipart upload
type MultipartUpload struct {
	uploadID string
	bucket   string
	key      string
	parts    map[int]*UploadPart
	mu       sync.Mutex
}

// UploadPart represents one part of a multipart upload
type UploadPart struct {
	partNumber int
	etag       string
	size       int64
	data       []byte
}

// CompletedPart represents a part in CompleteMultipartUpload request
type CompletedPart struct {
	PartNumber int
	ETag       string
}

// Object represents an S3 object in storage
type Object struct {
	Key          string
	filePath     string // path to file on disk
	ETag         string
	LastModified time.Time
	Size         int64
}

// ReadData reads the object's data from disk
func (o *Object) ReadData() ([]byte, error) {
	return os.ReadFile(o.filePath)
}

// Bucket represents an S3 bucket
type Bucket struct {
	mu      sync.RWMutex
	objects map[string]*Object
}

// Storage is the disk-based storage backend
type Storage struct {
	mu               sync.RWMutex
	buckets          map[string]*Bucket
	tempDir          string
	multipartUploads map[string]*MultipartUpload
}

// NewStorage creates a new disk-based storage backend
func NewStorage() *Storage {
	tempDir, err := os.MkdirTemp("", "s3mock-*")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp directory: %v", err))
	}
	return &Storage{
		buckets:          make(map[string]*Bucket),
		tempDir:          tempDir,
		multipartUploads: make(map[string]*MultipartUpload),
	}
}

// Close cleans up the storage, removing all temporary files
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tempDir == "" {
		return nil
	}

	err := os.RemoveAll(s.tempDir)
	s.tempDir = ""
	return err
}

// CreateBucket creates a new bucket
func (s *Storage) CreateBucket(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.buckets[name]; exists {
		return ErrBucketAlreadyExists
	}

	s.buckets[name] = &Bucket{
		objects: make(map[string]*Object),
	}
	return nil
}

// BucketExists checks if a bucket exists
func (s *Storage) BucketExists(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.buckets[name]
	return exists
}

// PutObject stores an object on disk
func (s *Storage) PutObject(bucket, key string, data []byte) error {
	s.mu.RLock()
	b, exists := s.buckets[bucket]
	tempDir := s.tempDir
	s.mu.RUnlock()

	if !exists {
		return ErrBucketNotFound
	}

	// Calculate ETag (MD5 of data)
	hash := md5.Sum(data) // #nosec G401 - MD5 used for S3 ETag calculation (non-cryptographic use)
	etag := hex.EncodeToString(hash[:])

	// Write data to temp file
	tempFile, err := os.CreateTemp(tempDir, "obj-*")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	if _, err := tempFile.Write(data); err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("failed to write data: %w", err)
	}

	obj := &Object{
		Key:          key,
		filePath:     tempFile.Name(),
		ETag:         etag,
		LastModified: time.Now(),
		Size:         int64(len(data)),
	}

	b.mu.Lock()
	// Delete old file if object already exists
	if oldObj, exists := b.objects[key]; exists && oldObj.filePath != "" {
		os.Remove(oldObj.filePath)
	}
	b.objects[key] = obj
	b.mu.Unlock()

	return nil
}

// GetObject retrieves an object
func (s *Storage) GetObject(bucket, key string) (*Object, error) {
	s.mu.RLock()
	b, exists := s.buckets[bucket]
	s.mu.RUnlock()

	if !exists {
		return nil, ErrBucketNotFound
	}

	b.mu.RLock()
	obj, exists := b.objects[key]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrObjectNotFound
	}

	return obj, nil
}

// DeleteObject deletes an object
func (s *Storage) DeleteObject(bucket, key string) error {
	s.mu.RLock()
	b, exists := s.buckets[bucket]
	s.mu.RUnlock()

	if !exists {
		return ErrBucketNotFound
	}

	b.mu.Lock()
	obj, exists := b.objects[key]
	if exists && obj.filePath != "" {
		os.Remove(obj.filePath)
	}
	delete(b.objects, key)
	b.mu.Unlock()

	return nil
}

// ListObjects returns objects in a bucket with optional prefix and start-after filtering
func (s *Storage) ListObjects(bucket, prefix, startAfter string) []*Object {
	s.mu.RLock()
	b, exists := s.buckets[bucket]
	s.mu.RUnlock()

	if !exists {
		return nil
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []*Object
	for _, obj := range b.objects {
		// Filter by prefix
		if prefix != "" && !strings.HasPrefix(obj.Key, prefix) {
			continue
		}

		// Filter by start-after (lexicographically greater than)
		if startAfter != "" && obj.Key <= startAfter {
			continue
		}

		result = append(result, obj)
	}

	// Sort by key for consistent ordering
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}

// CreateMultipartUpload initiates a multipart upload
func (s *Storage) CreateMultipartUpload(bucket, key string) (string, error) {
	s.mu.RLock()
	_, exists := s.buckets[bucket]
	s.mu.RUnlock()

	if !exists {
		return "", ErrBucketNotFound
	}

	// Generate unique upload ID (16 random bytes as hex)
	uploadIDBytes := make([]byte, 16)
	if _, err := rand.Read(uploadIDBytes); err != nil {
		return "", fmt.Errorf("failed to generate upload ID: %w", err)
	}
	uploadID := hex.EncodeToString(uploadIDBytes)

	upload := &MultipartUpload{
		uploadID: uploadID,
		bucket:   bucket,
		key:      key,
		parts:    make(map[int]*UploadPart),
	}

	s.mu.Lock()
	s.multipartUploads[uploadID] = upload
	s.mu.Unlock()

	return uploadID, nil
}

// UploadPart uploads a part of a multipart upload
func (s *Storage) UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (string, error) {
	s.mu.RLock()
	upload, exists := s.multipartUploads[uploadID]
	s.mu.RUnlock()

	if !exists {
		return "", ErrUploadNotFound
	}

	// Verify bucket and key match
	if upload.bucket != bucket || upload.key != key {
		return "", ErrUploadNotFound
	}

	// Calculate ETag for this part (MD5)
	hash := md5.Sum(data) // #nosec G401 - MD5 used for S3 ETag calculation
	etag := hex.EncodeToString(hash[:])

	part := &UploadPart{
		partNumber: partNumber,
		etag:       etag,
		size:       int64(len(data)),
		data:       data,
	}

	upload.mu.Lock()
	upload.parts[partNumber] = part
	upload.mu.Unlock()

	return etag, nil
}

// CompleteMultipartUpload completes a multipart upload by assembling parts
func (s *Storage) CompleteMultipartUpload(bucket, key, uploadID string, parts []CompletedPart) (string, error) {
	s.mu.RLock()
	upload, exists := s.multipartUploads[uploadID]
	tempDir := s.tempDir
	s.mu.RUnlock()

	if !exists {
		return "", ErrUploadNotFound
	}

	// Verify bucket and key match
	if upload.bucket != bucket || upload.key != key {
		return "", ErrUploadNotFound
	}

	// Assemble parts in order
	upload.mu.Lock()
	var assembledData []byte
	for _, completedPart := range parts {
		part, exists := upload.parts[completedPart.PartNumber]
		if !exists {
			upload.mu.Unlock()
			return "", fmt.Errorf("part %d not found", completedPart.PartNumber)
		}
		// Verify ETag matches
		if part.etag != completedPart.ETag {
			upload.mu.Unlock()
			return "", fmt.Errorf("part %d ETag mismatch", completedPart.PartNumber)
		}
		assembledData = append(assembledData, part.data...)
	}
	upload.mu.Unlock()

	// Calculate final ETag (MD5 of assembled data)
	hash := md5.Sum(assembledData) // #nosec G401 - MD5 used for S3 ETag calculation
	etag := hex.EncodeToString(hash[:])

	// Write assembled data to disk
	tempFile, err := os.CreateTemp(tempDir, "obj-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	if _, err := tempFile.Write(assembledData); err != nil {
		os.Remove(tempFile.Name())
		return "", fmt.Errorf("failed to write data: %w", err)
	}

	obj := &Object{
		Key:          key,
		filePath:     tempFile.Name(),
		ETag:         etag,
		LastModified: time.Now(),
		Size:         int64(len(assembledData)),
	}

	// Store object
	s.mu.RLock()
	b := s.buckets[bucket]
	s.mu.RUnlock()

	b.mu.Lock()
	// Delete old file if object already exists
	if oldObj, exists := b.objects[key]; exists && oldObj.filePath != "" {
		os.Remove(oldObj.filePath)
	}
	b.objects[key] = obj
	b.mu.Unlock()

	// Remove multipart upload
	s.mu.Lock()
	delete(s.multipartUploads, uploadID)
	s.mu.Unlock()

	return etag, nil
}

// AbortMultipartUpload aborts a multipart upload
func (s *Storage) AbortMultipartUpload(bucket, key, uploadID string) error {
	s.mu.RLock()
	upload, exists := s.multipartUploads[uploadID]
	s.mu.RUnlock()

	if !exists {
		return ErrUploadNotFound
	}

	// Verify bucket and key match
	if upload.bucket != bucket || upload.key != key {
		return ErrUploadNotFound
	}

	// Remove multipart upload
	s.mu.Lock()
	delete(s.multipartUploads, uploadID)
	s.mu.Unlock()

	return nil
}
