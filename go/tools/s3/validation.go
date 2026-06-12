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

package s3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ValidationConfig contains the configuration for S3 validation
type ValidationConfig struct {
	Bucket       string
	Region       string
	Endpoint     string // Optional: for S3-compatible storage (e.g., s3mock)
	KeyPrefix    string // Optional: prefix for test object
	AccessKey    string
	SecretKey    string
	SessionToken string // Optional: for temporary credentials (AWS STS)
}

// isLocalhostEndpoint checks if an endpoint URL points to localhost
func isLocalhostEndpoint(endpoint string) bool {
	return strings.Contains(endpoint, "localhost") ||
		strings.Contains(endpoint, "127.0.0.1") ||
		strings.Contains(endpoint, "[::1]")
}

// ValidateBucketName validates S3 bucket naming rules
//
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
func ValidateBucketName(bucket string) error {
	if len(bucket) < 3 || len(bucket) > 63 {
		return errors.New("bucket name must be between 3 and 63 characters")
	}
	// Basic validation - lowercase, numbers, hyphens, dots
	matched, _ := regexp.MatchString("^[a-z0-9][a-z0-9.-]*[a-z0-9]$", bucket)
	if !matched {
		return errors.New("bucket name must contain only lowercase letters, numbers, hyphens, and dots")
	}
	return nil
}

// ValidateAccess tests S3 bucket access by writing and deleting a test object
func ValidateAccess(ctx context.Context, cfg ValidationConfig) error {
	if cfg.Bucket == "" {
		return errors.New("bucket is required")
	}
	if err := ValidateBucketName(cfg.Bucket); err != nil {
		return fmt.Errorf("invalid bucket name: %w", err)
	}
	if cfg.Region == "" {
		return errors.New("region is required")
	}
	if cfg.AccessKey == "" {
		return errors.New("access key is required")
	}
	if cfg.SecretKey == "" {
		return errors.New("secret key is required")
	}

	// Create AWS config with explicit credentials
	awsCfg := aws.Config{
		Region: cfg.Region,
		Credentials: credentials.NewStaticCredentialsProvider(
			cfg.AccessKey,
			cfg.SecretKey,
			cfg.SessionToken, // session token for temporary credentials (AWS STS)
		),
	}

	// Set custom endpoint if specified (for s3mock, etc.)
	if cfg.Endpoint != "" {
		awsCfg.BaseEndpoint = aws.String(cfg.Endpoint)

		// For localhost endpoints only, skip TLS verification for self-signed certs
		// This is needed for local development with s3mock
		transport := &http.Transport{
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: 5 * time.Second,
		}

		if isLocalhostEndpoint(cfg.Endpoint) {
			transport.TLSClientConfig = &tls.Config{
				InsecureSkipVerify: true, //nolint:gosec // Required for local s3mock with self-signed certs
			}
		}

		awsCfg.HTTPClient = &http.Client{
			Transport: transport,
		}
	}

	// Create S3 client
	// When using custom endpoints (s3mock, etc.), use path-style addressing
	// This is required for S3-compatible storage that doesn't support virtual-hosted-style
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.UsePathStyle = true
		}
	})

	// 1. HeadBucket - verify bucket exists and we have access
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	if err != nil {
		return fmt.Errorf("bucket '%s' not accessible: %w", cfg.Bucket, err)
	}

	// 2. PutObject - write test file to verify write permissions
	testKey := fmt.Sprintf(".multigres-cluster-init-%d", time.Now().Unix())
	if cfg.KeyPrefix != "" {
		testKey = strings.TrimSuffix(cfg.KeyPrefix, "/") + "/" + testKey
	}

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(testKey),
		Body:   strings.NewReader("multigres cluster init test"),
	})
	if err != nil {
		return fmt.Errorf("failed to write test object to bucket '%s': %w", cfg.Bucket, err)
	}

	// 3. DeleteObject - clean up test file
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(testKey),
	})
	if err != nil {
		// Don't fail if delete fails - write succeeded which is what matters
		return fmt.Errorf("test object written successfully but failed to clean up (bucket: %s, key: %s): %w", cfg.Bucket, testKey, err)
	}

	return nil
}
