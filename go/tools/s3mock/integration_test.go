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

// go/tools/s3mock/integration_test.go
package s3mock

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestIntegrationAWSSDK(t *testing.T) {
	// Start mock server
	srv, err := NewServer(0) // Port 0 = random port for tests
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	_ = srv.CreateBucket("test-bucket")

	// Create AWS SDK client pointing to mock
	cfg := aws.Config{
		Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(
			"test-key",
			"test-secret",
			"",
		),
		BaseEndpoint: aws.String(srv.Endpoint()),
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, // #nosec G402 - test code using self-signed certificates
				},
			},
		},
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	ctx := context.Background()

	// Test HeadBucket
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("test-bucket"),
	})
	if err != nil {
		t.Fatalf("HeadBucket failed: %v", err)
	}

	// Test PutObject
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test-key"),
		Body:   bytes.NewReader([]byte("hello world")),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Test GetObject
	getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test-key"),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer getResp.Body.Close()

	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(getResp.Body)
	if buf.String() != "hello world" {
		t.Fatalf("expected 'hello world', got %q", buf.String())
	}

	// Test HeadObject
	headResp, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test-key"),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	if *headResp.ContentLength != 11 {
		t.Fatalf("expected ContentLength 11, got %d", *headResp.ContentLength)
	}

	// Test ListObjectsV2
	listResp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("test-bucket"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}
	if *listResp.KeyCount != 1 {
		t.Fatalf("expected KeyCount 1, got %d", *listResp.KeyCount)
	}
	if *listResp.Contents[0].Key != "test-key" {
		t.Fatalf("expected key 'test-key', got %q", *listResp.Contents[0].Key)
	}

	// Test DeleteObject
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test-key"),
	})
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Verify object deleted
	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test-key"),
	})
	if err == nil {
		t.Fatal("expected error getting deleted object")
	}
}

func TestServerCleanup(t *testing.T) {
	srv, err := NewServer(0) // Port 0 = random port for tests
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}

	_ = srv.CreateBucket("test-bucket")
	_ = srv.storage.PutObject("test-bucket", "test-key", []byte("test data"))

	// Get temp directory before shutdown
	tempDir := srv.storage.tempDir

	// Verify temp directory exists
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		t.Fatalf("temp directory %q should exist", tempDir)
	}

	// Stop server
	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}

	// Verify temp directory is removed
	if _, err := os.Stat(tempDir); !os.IsNotExist(err) {
		t.Fatalf("temp directory %q should be removed after Stop()", tempDir)
	}
}

func TestIntegrationLargeFile(t *testing.T) {
	srv, err := NewServer(0) // Port 0 = random port for tests
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	_ = srv.CreateBucket("test-bucket")

	// Create AWS SDK client
	cfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
		BaseEndpoint: aws.String(srv.Endpoint()),
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, // #nosec G402 - test code
				},
			},
		},
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	ctx := context.Background()

	// Create 10MB file
	largeData := make([]byte, 10*1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	// Upload large file
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("large-file"),
		Body:   bytes.NewReader(largeData),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Download and verify
	getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("large-file"),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer getResp.Body.Close()

	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(getResp.Body)
	if !bytes.Equal(buf.Bytes(), largeData) {
		t.Fatal("downloaded data does not match uploaded data")
	}

	// Cleanup should remove the file
}

func TestIntegrationMultipartUpload(t *testing.T) {
	srv, err := NewServer(0) // Port 0 = random port for tests
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	_ = srv.CreateBucket("test-bucket")

	// Create AWS SDK client
	cfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
		BaseEndpoint: aws.String(srv.Endpoint()),
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, // #nosec G402 - test code
				},
			},
		},
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	ctx := context.Background()

	// Initiate multipart upload
	createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("multipart-key"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	uploadID := createResp.UploadId
	if uploadID == nil || *uploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}

	// Upload parts
	part1Data := []byte("part one content")
	part2Data := []byte("part two content")

	uploadPart1Resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("test-bucket"),
		Key:        aws.String("multipart-key"),
		UploadId:   uploadID,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part1Data),
	})
	if err != nil {
		t.Fatalf("UploadPart(1) failed: %v", err)
	}

	uploadPart2Resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("test-bucket"),
		Key:        aws.String("multipart-key"),
		UploadId:   uploadID,
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader(part2Data),
	})
	if err != nil {
		t.Fatalf("UploadPart(2) failed: %v", err)
	}

	// Complete multipart upload
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("test-bucket"),
		Key:      aws.String("multipart-key"),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: uploadPart1Resp.ETag},
				{PartNumber: aws.Int32(2), ETag: uploadPart2Resp.ETag},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	// Verify object exists with correct data
	getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("multipart-key"),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer getResp.Body.Close()

	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(getResp.Body)
	expectedData := append(part1Data, part2Data...)
	if !bytes.Equal(buf.Bytes(), expectedData) {
		t.Fatalf("expected data %q, got %q", expectedData, buf.Bytes())
	}
}

func TestIntegrationDeleteObjects(t *testing.T) {
	srv, err := NewServer(0) // Port 0 = random port for tests
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	_ = srv.CreateBucket("test-bucket")

	cfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
		BaseEndpoint: aws.String(srv.Endpoint()),
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, // #nosec G402 - test code
				},
			},
		},
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	ctx := context.Background()

	// Upload multiple objects
	for i := 1; i <= 5; i++ {
		data := fmt.Appendf(nil, "data%d", i)
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("test-bucket"),
			Key:    aws.String(fmt.Sprintf("key%d", i)),
			Body:   bytes.NewReader(data),
		})
		if err != nil {
			t.Fatalf("PutObject(key%d) failed: %v", i, err)
		}
	}

	// Delete multiple objects in one request
	deleteResp, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String("test-bucket"),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String("key1")},
				{Key: aws.String("key3")},
				{Key: aws.String("key5")},
			},
		},
	})
	if err != nil {
		t.Fatalf("DeleteObjects failed: %v", err)
	}

	if len(deleteResp.Deleted) != 3 {
		t.Fatalf("expected 3 deleted objects, got %d", len(deleteResp.Deleted))
	}

	// Verify key1, key3, key5 are deleted
	for _, key := range []string{"key1", "key3", "key5"} {
		_, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("test-bucket"),
			Key:    aws.String(key),
		})
		if err == nil {
			t.Fatalf("expected %s to be deleted", key)
		}
	}

	// Verify key2, key4 still exist
	for _, key := range []string{"key2", "key4"} {
		_, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("test-bucket"),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("GetObject(%s) failed: %v", key, err)
		}
	}
}

func TestIntegrationListObjectsV2Pagination(t *testing.T) {
	srv, err := NewServer(0) // Port 0 = random port for tests
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	_ = srv.CreateBucket("test-bucket")

	// Create AWS SDK client
	cfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
		BaseEndpoint: aws.String(srv.Endpoint()),
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, // #nosec G402 - test code
				},
			},
		},
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	ctx := context.Background()

	// Upload 10 objects
	for i := 1; i <= 10; i++ {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("test-bucket"),
			Key:    aws.String(fmt.Sprintf("key-%02d", i)),
			Body:   bytes.NewReader(fmt.Appendf(nil, "data%d", i)),
		})
		if err != nil {
			t.Fatalf("PutObject(key-%02d) failed: %v", i, err)
		}
	}

	// List with MaxKeys=3 to test pagination
	var allKeys []string
	var continuationToken *string

	for {
		listResp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String("test-bucket"),
			MaxKeys:           aws.Int32(3),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		// Collect keys from this page
		for _, obj := range listResp.Contents {
			allKeys = append(allKeys, *obj.Key)
		}

		// Check pagination fields
		if len(listResp.Contents) > 3 {
			t.Fatalf("expected at most 3 objects per page, got %d", len(listResp.Contents))
		}

		// If not truncated, we're done
		if listResp.IsTruncated == nil || !*listResp.IsTruncated {
			break
		}

		// Verify NextContinuationToken is present when truncated
		if listResp.NextContinuationToken == nil {
			t.Fatal("expected NextContinuationToken when IsTruncated=true")
		}

		continuationToken = listResp.NextContinuationToken
	}

	// Verify we got all 10 keys
	if len(allKeys) != 10 {
		t.Fatalf("expected 10 keys total, got %d", len(allKeys))
	}

	// Verify keys are in order
	for i := 1; i <= 10; i++ {
		expectedKey := fmt.Sprintf("key-%02d", i)
		if allKeys[i-1] != expectedKey {
			t.Fatalf("expected key[%d]=%s, got %s", i-1, expectedKey, allKeys[i-1])
		}
	}
}

func TestIntegrationGetObjectRange(t *testing.T) {
	srv, err := NewServer(0) // Port 0 = random port for tests
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	_ = srv.CreateBucket("test-bucket")

	// Create AWS SDK client
	cfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
		BaseEndpoint: aws.String(srv.Endpoint()),
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, // #nosec G402 - test code
				},
			},
		},
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	ctx := context.Background()

	// Upload object with known content
	fullData := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test-file"),
		Body:   bytes.NewReader(fullData),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Test: Get bytes 0-9 (first 10 bytes)
	getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test-file"),
		Range:  aws.String("bytes=0-9"),
	})
	if err != nil {
		t.Fatalf("GetObject with Range failed: %v", err)
	}
	defer getResp.Body.Close()

	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(getResp.Body)
	expectedData := fullData[0:10]
	if !bytes.Equal(buf.Bytes(), expectedData) {
		t.Fatalf("expected data %q, got %q", expectedData, buf.Bytes())
	}

	// Verify Content-Range header
	if getResp.ContentRange == nil {
		t.Fatal("expected Content-Range header")
	}
	expectedContentRange := "bytes 0-9/36"
	if *getResp.ContentRange != expectedContentRange {
		t.Fatalf("expected Content-Range %q, got %q", expectedContentRange, *getResp.ContentRange)
	}

	// Verify Content-Length matches range length
	if getResp.ContentLength == nil || *getResp.ContentLength != 10 {
		t.Fatalf("expected Content-Length 10, got %v", getResp.ContentLength)
	}

	// Test: Get bytes 10-19
	getResp2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test-file"),
		Range:  aws.String("bytes=10-19"),
	})
	if err != nil {
		t.Fatalf("GetObject with Range 10-19 failed: %v", err)
	}
	defer getResp2.Body.Close()

	buf2 := new(bytes.Buffer)
	_, _ = buf2.ReadFrom(getResp2.Body)
	expectedData2 := fullData[10:20]
	if !bytes.Equal(buf2.Bytes(), expectedData2) {
		t.Fatalf("expected data %q, got %q", expectedData2, buf2.Bytes())
	}

	// Test: Get last 10 bytes using suffix range
	getResp3, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test-file"),
		Range:  aws.String("bytes=-10"),
	})
	if err != nil {
		t.Fatalf("GetObject with suffix range failed: %v", err)
	}
	defer getResp3.Body.Close()

	buf3 := new(bytes.Buffer)
	_, _ = buf3.ReadFrom(getResp3.Body)
	expectedData3 := fullData[len(fullData)-10:]
	if !bytes.Equal(buf3.Bytes(), expectedData3) {
		t.Fatalf("expected data %q, got %q", expectedData3, buf3.Bytes())
	}
}

func TestIntegrationListLargeNumberOfObjects(t *testing.T) {
	srv, err := NewServer(0) // Port 0 = random port for tests
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	_ = srv.CreateBucket("test-bucket")

	// Create AWS SDK client
	cfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
		BaseEndpoint: aws.String(srv.Endpoint()),
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, // #nosec G402 - test code
				},
			},
		},
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	ctx := context.Background()

	// Upload 2000 small objects to test pagination beyond default max-keys
	t.Log("Uploading 2000 objects...")
	for i := 1; i <= 2000; i++ {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("test-bucket"),
			Key:    aws.String(fmt.Sprintf("obj-%04d", i)),
			Body:   bytes.NewReader([]byte("x")),
		})
		if err != nil {
			t.Fatalf("PutObject(obj-%04d) failed: %v", i, err)
		}
	}

	// List all objects using pagination (should take 2 requests with default max-keys=1000)
	t.Log("Listing all objects with pagination...")
	var allKeys []string
	var continuationToken *string
	pageCount := 0

	for {
		listResp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String("test-bucket"),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		pageCount++
		t.Logf("Page %d: received %d objects, truncated=%v", pageCount, len(listResp.Contents), *listResp.IsTruncated)

		// Collect keys
		for _, obj := range listResp.Contents {
			allKeys = append(allKeys, *obj.Key)
		}

		// Check if more pages exist
		if listResp.IsTruncated == nil || !*listResp.IsTruncated {
			break
		}

		continuationToken = listResp.NextContinuationToken
	}

	// Verify we got all 2000 objects
	if len(allKeys) != 2000 {
		t.Fatalf("expected 2000 objects, got %d", len(allKeys))
	}

	// Verify we used multiple pages
	if pageCount < 2 {
		t.Fatalf("expected at least 2 pages for 2000 objects, got %d", pageCount)
	}

	// Verify keys are in order
	for i := 1; i <= 2000; i++ {
		expectedKey := fmt.Sprintf("obj-%04d", i)
		if allKeys[i-1] != expectedKey {
			t.Fatalf("expected key[%d]=%s, got %s", i-1, expectedKey, allKeys[i-1])
		}
	}

	t.Logf("Successfully listed 2000 objects in %d pages", pageCount)
}
