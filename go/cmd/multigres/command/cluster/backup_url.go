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

package cluster

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/multigres/multigres/go/tools/s3"
)

// parseBackupURL parses an S3 backup URL and returns bucket name and key prefix.
// Format: s3://bucket-name/optional/key/prefix/
// Returns error if URL is invalid or doesn't follow required format.
func parseBackupURL(urlStr string) (bucket, keyPrefix string, err error) {
	// Parse URL
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", "", fmt.Errorf("invalid URL: %w", err)
	}

	// Validate scheme
	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("invalid backup URL scheme %q\nMust use s3:// scheme (example: s3://my-bucket/backups/)", u.Scheme)
	}

	// Check for unsupported features
	if u.RawQuery != "" {
		return "", "", fmt.Errorf("query parameters not supported in --backup-url\nFound: %s\nUse --region flag instead: --backup-url=%s://%s%s --region=us-east-1", urlStr, u.Scheme, u.Host, u.Path)
	}
	if u.Fragment != "" {
		return "", "", fmt.Errorf("fragment not supported in --backup-url: %s", urlStr)
	}
	if u.User != nil {
		return "", "", errors.New("credentials not supported in --backup-url\nUse environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
	}

	// Extract bucket (host component)
	bucket = u.Host
	if bucket == "" {
		return "", "", errors.New("bucket name cannot be empty")
	}

	// Validate bucket name
	if err := s3.ValidateBucketName(bucket); err != nil {
		return "", "", fmt.Errorf("invalid S3 bucket name %q\n%w", bucket, err)
	}

	// Extract key prefix (path component)
	keyPrefix = strings.TrimPrefix(u.Path, "/")
	// Ensure trailing slash if prefix is not empty
	if keyPrefix != "" && !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}

	return bucket, keyPrefix, nil
}
