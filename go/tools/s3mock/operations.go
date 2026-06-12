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

// go/tools/s3mock/operations.go
package s3mock

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// S3Error represents an S3 error response
type S3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	RequestId string   `xml:"RequestId"`
}

// writeS3Error writes an S3-compliant error response
func writeS3Error(w http.ResponseWriter, code string, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)

	errResp := S3Error{
		Code:      code,
		Message:   message,
		RequestId: fmt.Sprintf("mock-%d", time.Now().Unix()),
	}

	_ = xml.NewEncoder(w).Encode(errResp)
}

// parseRange parses HTTP Range header and returns start, end offsets
// Supports "bytes=start-end" and "bytes=-suffix" formats
// Returns start=-1, end=-1 if no range or invalid range
func parseRange(rangeHeader string, contentLength int64) (start int64, end int64) {
	// No range header
	if rangeHeader == "" {
		return -1, -1
	}

	// Must start with "bytes="
	const prefix = "bytes="
	if !strings.HasPrefix(rangeHeader, prefix) {
		return -1, -1
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, prefix)

	// Handle suffix range (bytes=-N means last N bytes)
	if strings.HasPrefix(rangeSpec, "-") {
		suffixLen, err := strconv.ParseInt(rangeSpec[1:], 10, 64)
		if err != nil || suffixLen <= 0 || suffixLen > contentLength {
			return -1, -1
		}
		return contentLength - suffixLen, contentLength - 1
	}

	// Handle normal range (bytes=start-end)
	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		return -1, -1
	}

	// Parse start
	startVal, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || startVal < 0 {
		return -1, -1
	}

	// Parse end (can be empty for "bytes=start-")
	var endVal int64
	if parts[1] == "" {
		endVal = contentLength - 1
	} else {
		endVal, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil || endVal < startVal {
			return -1, -1
		}
	}

	// Clamp to content length
	if startVal >= contentLength {
		return -1, -1
	}
	if endVal >= contentLength {
		endVal = contentLength - 1
	}

	return startVal, endVal
}

// parseBucketAndKey extracts bucket name and object key from URL path
func parseBucketAndKey(urlPath string) (bucket, key string) {
	// Remove leading slash
	urlPath = strings.TrimPrefix(urlPath, "/")

	// Split into bucket and key
	parts := strings.SplitN(urlPath, "/", 2)
	bucket = parts[0]
	if len(parts) > 1 {
		key = parts[1]
	}
	return bucket, key
}

// handleHeadBucket handles HEAD /{bucket} requests
func handleHeadBucket(w http.ResponseWriter, r *http.Request, storage *Storage) {
	bucket, _ := parseBucketAndKey(r.URL.Path)

	if !storage.BucketExists(bucket) {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handlePutObject handles PUT /{bucket}/{key} requests
func handlePutObject(w http.ResponseWriter, r *http.Request, storage *Storage) {
	bucket, key := parseBucketAndKey(r.URL.Path)

	if !storage.BucketExists(bucket) {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	// Read request body
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, "InternalError", "Failed to read request body", http.StatusInternalServerError)
		return
	}

	// Store object
	err = storage.PutObject(bucket, key, data)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	// Get object to retrieve ETag
	obj, _ := storage.GetObject(bucket, key)

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, obj.ETag))
	w.WriteHeader(http.StatusOK)
}

// handleGetObject handles GET /{bucket}/{key} requests
func handleGetObject(w http.ResponseWriter, r *http.Request, storage *Storage) {
	bucket, key := parseBucketAndKey(r.URL.Path)

	if !storage.BucketExists(bucket) {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	obj, err := storage.GetObject(bucket, key)
	if errors.Is(err, ErrObjectNotFound) {
		writeS3Error(w, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
		return
	}
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	// Read full object data
	data, err := obj.ReadData()
	if err != nil {
		writeS3Error(w, "InternalError", "Failed to read object data", http.StatusInternalServerError)
		return
	}

	// Check for Range header
	rangeHeader := r.Header.Get("Range")
	start, end := parseRange(rangeHeader, obj.Size)

	// If range is valid, return partial content (206)
	if start >= 0 && end >= 0 {
		// Extract range from data
		rangeData := data[start : end+1]

		// Set headers for partial content
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(int64(len(rangeData)), 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, obj.Size))
		w.Header().Set("ETag", fmt.Sprintf(`"%s"`, obj.ETag))
		w.Header().Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))
		w.Header().Set("Accept-Ranges", "bytes")

		w.WriteHeader(http.StatusPartialContent) // 206
		_, _ = w.Write(rangeData)
		return
	}

	// No range or invalid range - return full object (200)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, obj.ETag))
	w.Header().Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// handleHeadObject handles HEAD /{bucket}/{key} requests
func handleHeadObject(w http.ResponseWriter, r *http.Request, storage *Storage) {
	bucket, key := parseBucketAndKey(r.URL.Path)

	if !storage.BucketExists(bucket) {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	obj, err := storage.GetObject(bucket, key)
	if errors.Is(err, ErrObjectNotFound) {
		writeS3Error(w, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
		return
	}
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	// Set headers but no body
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, obj.ETag))
	w.Header().Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))

	w.WriteHeader(http.StatusOK)
}

// handleDeleteObject handles DELETE /{bucket}/{key} requests
func handleDeleteObject(w http.ResponseWriter, r *http.Request, storage *Storage) {
	bucket, key := parseBucketAndKey(r.URL.Path)

	if !storage.BucketExists(bucket) {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	// S3 returns 204 even if object doesn't exist
	_ = storage.DeleteObject(bucket, key)
	w.WriteHeader(http.StatusNoContent)
}

// ListBucketResult represents the XML response for ListObjectsV2
type ListBucketResult struct {
	XMLName               xml.Name       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	KeyCount              int            `xml:"KeyCount"`
	IsTruncated           bool           `xml:"IsTruncated"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	Contents              []ObjectResult `xml:"Contents"`
	CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes,omitempty"`
}

type ObjectResult struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// InitiateMultipartUploadResult represents XML response for CreateMultipartUpload
type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

// CompleteMultipartUploadResult represents XML response for CompleteMultipartUpload
type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// CompleteMultipartUploadRequest represents XML request for CompleteMultipartUpload
type CompleteMultipartUploadRequest struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []CompletePart `xml:"Part"`
}

// CompletePart represents a part in CompleteMultipartUpload XML request
type CompletePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// DeleteRequest represents the XML request for DeleteObjects
type DeleteRequest struct {
	XMLName xml.Name         `xml:"Delete"`
	Objects []ObjectToDelete `xml:"Object"`
}

// ObjectToDelete represents an object to delete
type ObjectToDelete struct {
	Key string `xml:"Key"`
}

// DeleteResult represents the XML response for DeleteObjects
type DeleteResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Deleted []DeletedObject `xml:"Deleted"`
}

// DeletedObject represents a successfully deleted object
type DeletedObject struct {
	Key string `xml:"Key"`
}

// handleListObjectsV2 handles GET /{bucket}?list-type=2 requests
func handleListObjectsV2(w http.ResponseWriter, r *http.Request, storage *Storage) {
	bucket, _ := parseBucketAndKey(r.URL.Path)

	if !storage.BucketExists(bucket) {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	// Parse query parameters
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	maxKeysStr := r.URL.Query().Get("max-keys")
	continuationToken := r.URL.Query().Get("continuation-token")
	startAfter := r.URL.Query().Get("start-after")

	// Parse max-keys (default 1000, AWS limit)
	maxKeys := 1000
	if maxKeysStr != "" {
		if parsed, err := strconv.Atoi(maxKeysStr); err == nil && parsed > 0 {
			maxKeys = min(parsed, 1000)
		}
	}

	// If continuation token is present, it takes precedence over start-after
	if continuationToken != "" {
		startAfter = continuationToken
	}

	// Get all matching objects
	objects := storage.ListObjects(bucket, prefix, startAfter)

	var contents []ObjectResult
	var commonPrefixes []CommonPrefix
	prefixMap := make(map[string]bool)
	var nextContinuationToken string
	isTruncated := false

	// Process objects with delimiter
	if delimiter != "" {
		objectCount := 0
		var lastKey string
		for _, obj := range objects {
			if objectCount >= maxKeys {
				isTruncated = true
				nextContinuationToken = lastKey
				break
			}

			// Find the next delimiter after the prefix
			remainder := strings.TrimPrefix(obj.Key, prefix)
			delimIndex := strings.Index(remainder, delimiter)

			if delimIndex >= 0 {
				// This object is in a "subdirectory" - add to common prefixes
				commonPrefix := prefix + remainder[:delimIndex+len(delimiter)]
				if !prefixMap[commonPrefix] {
					prefixMap[commonPrefix] = true
					commonPrefixes = append(commonPrefixes, CommonPrefix{Prefix: commonPrefix})
					objectCount++
					lastKey = obj.Key
				}
			} else {
				// This object is at the current "level" - add to contents
				contents = append(contents, ObjectResult{
					Key:          obj.Key,
					LastModified: obj.LastModified.UTC().Format(time.RFC3339),
					ETag:         fmt.Sprintf(`"%s"`, obj.ETag),
					Size:         obj.Size,
				})
				objectCount++
				lastKey = obj.Key
			}
		}
	} else {
		// No delimiter - return objects up to maxKeys
		for i, obj := range objects {
			if i >= maxKeys {
				isTruncated = true
				// Use the last key we included as the continuation token
				nextContinuationToken = contents[len(contents)-1].Key
				break
			}

			contents = append(contents, ObjectResult{
				Key:          obj.Key,
				LastModified: obj.LastModified.UTC().Format(time.RFC3339),
				ETag:         fmt.Sprintf(`"%s"`, obj.ETag),
				Size:         obj.Size,
			})
		}
	}

	// Build response
	result := ListBucketResult{
		Name:           bucket,
		Prefix:         prefix,
		Delimiter:      delimiter,
		MaxKeys:        maxKeys,
		KeyCount:       len(contents) + len(commonPrefixes),
		IsTruncated:    isTruncated,
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}

	// Include continuation tokens if paginating
	if continuationToken != "" {
		result.ContinuationToken = continuationToken
	}
	if startAfter != "" && continuationToken == "" {
		result.StartAfter = startAfter
	}
	if isTruncated {
		result.NextContinuationToken = nextContinuationToken
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(result)
}

// handleCreateMultipartUpload handles POST /{bucket}/{key}?uploads requests
func handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, storage *Storage) {
	bucket, key := parseBucketAndKey(r.URL.Path)

	if !storage.BucketExists(bucket) {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	uploadID, err := storage.CreateMultipartUpload(bucket, key)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	resp := InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

// handleUploadPart handles PUT /{bucket}/{key}?partNumber=N&uploadId=ID requests
func handleUploadPart(w http.ResponseWriter, r *http.Request, storage *Storage) {
	bucket, key := parseBucketAndKey(r.URL.Path)

	uploadID := r.URL.Query().Get("uploadId")
	partNumberStr := r.URL.Query().Get("partNumber")

	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil {
		writeS3Error(w, "InvalidArgument", "Invalid part number", http.StatusBadRequest)
		return
	}

	// Read part data
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, "InternalError", "Failed to read request body", http.StatusInternalServerError)
		return
	}

	etag, err := storage.UploadPart(bucket, key, uploadID, partNumber, data)
	if errors.Is(err, ErrUploadNotFound) {
		writeS3Error(w, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
		return
	}
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, etag))
	w.WriteHeader(http.StatusOK)
}

// handleCompleteMultipartUpload handles POST /{bucket}/{key}?uploadId=ID requests
func handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, storage *Storage) {
	bucket, key := parseBucketAndKey(r.URL.Path)
	uploadID := r.URL.Query().Get("uploadId")

	// Parse XML request body
	var req CompleteMultipartUploadRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeS3Error(w, "MalformedXML", "The XML was malformed", http.StatusBadRequest)
		return
	}

	// Convert XML parts to storage CompletedPart
	parts := make([]CompletedPart, len(req.Parts))
	for i, p := range req.Parts {
		// Remove quotes from ETag if present
		etag := strings.Trim(p.ETag, `"`)
		parts[i] = CompletedPart{
			PartNumber: p.PartNumber,
			ETag:       etag,
		}
	}

	etag, err := storage.CompleteMultipartUpload(bucket, key, uploadID, parts)
	if errors.Is(err, ErrUploadNotFound) {
		writeS3Error(w, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
		return
	}
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	resp := CompleteMultipartUploadResult{
		Location: fmt.Sprintf("/%s/%s", bucket, key),
		Bucket:   bucket,
		Key:      key,
		ETag:     fmt.Sprintf(`"%s"`, etag),
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

// handleAbortMultipartUpload handles DELETE /{bucket}/{key}?uploadId=ID requests
func handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, storage *Storage) {
	bucket, key := parseBucketAndKey(r.URL.Path)
	uploadID := r.URL.Query().Get("uploadId")

	err := storage.AbortMultipartUpload(bucket, key, uploadID)
	if errors.Is(err, ErrUploadNotFound) {
		writeS3Error(w, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
		return
	}
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleDeleteObjects handles POST /{bucket}?delete requests
func handleDeleteObjects(w http.ResponseWriter, r *http.Request, storage *Storage) {
	bucket, _ := parseBucketAndKey(r.URL.Path)

	if !storage.BucketExists(bucket) {
		writeS3Error(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	// Parse XML request
	var req DeleteRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeS3Error(w, "MalformedXML", "The XML was malformed", http.StatusBadRequest)
		return
	}

	// Delete each object
	deleted := make([]DeletedObject, 0, len(req.Objects))
	for _, obj := range req.Objects {
		// S3 doesn't return errors for missing objects
		_ = storage.DeleteObject(bucket, obj.Key)
		deleted = append(deleted, DeletedObject(obj))
	}

	resp := DeleteResult{
		Deleted: deleted,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}
