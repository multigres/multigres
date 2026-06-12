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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBackupURL(t *testing.T) {
	tests := []struct {
		name       string
		url        string
		wantBucket string
		wantPrefix string
		wantErr    string
	}{
		{
			name:       "bucket only",
			url:        "s3://my-bucket",
			wantBucket: "my-bucket",
			wantPrefix: "",
		},
		{
			name:       "bucket with trailing slash",
			url:        "s3://my-bucket/",
			wantBucket: "my-bucket",
			wantPrefix: "",
		},
		{
			name:       "bucket with prefix",
			url:        "s3://my-bucket/backups/",
			wantBucket: "my-bucket",
			wantPrefix: "backups/",
		},
		{
			name:       "bucket with nested prefix",
			url:        "s3://my-bucket/backups/prod/",
			wantBucket: "my-bucket",
			wantPrefix: "backups/prod/",
		},
		{
			name:       "bucket with prefix no trailing slash",
			url:        "s3://my-bucket/backups",
			wantBucket: "my-bucket",
			wantPrefix: "backups/",
		},
		{
			name:    "invalid scheme http",
			url:     "http://my-bucket/backups/",
			wantErr: "invalid backup URL scheme \"http\"",
		},
		{
			name:    "invalid scheme https",
			url:     "https://my-bucket/backups/",
			wantErr: "invalid backup URL scheme \"https\"",
		},
		{
			name:    "query parameters not allowed",
			url:     "s3://bucket/prefix?region=us-east-1",
			wantErr: "query parameters not supported in --backup-url",
		},
		{
			name:    "fragment not allowed",
			url:     "s3://bucket/prefix#anchor",
			wantErr: "fragment not supported in --backup-url",
		},
		{
			name:    "credentials not allowed",
			url:     "s3://user:pass@bucket/prefix",
			wantErr: "credentials not supported in --backup-url",
		},
		{
			name:    "empty bucket",
			url:     "s3:///prefix",
			wantErr: "bucket name cannot be empty",
		},
		{
			name:    "invalid bucket uppercase",
			url:     "s3://My-Bucket/prefix",
			wantErr: "bucket name must contain only lowercase letters, numbers, hyphens, and dots",
		},
		{
			name:    "invalid bucket too short",
			url:     "s3://ab/prefix",
			wantErr: "bucket name must be between 3 and 63 characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, prefix, err := parseBackupURL(tt.url)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantBucket, bucket)
				assert.Equal(t, tt.wantPrefix, prefix)
			}
		})
	}
}
