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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateS3BucketName(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		expectError bool
	}{
		{
			name:        "valid bucket name",
			bucket:      "my-backup-bucket",
			expectError: false,
		},
		{
			name:        "valid with dots",
			bucket:      "my.backup.bucket",
			expectError: false,
		},
		{
			name:        "valid with numbers",
			bucket:      "backup123",
			expectError: false,
		},
		{
			name:        "too short",
			bucket:      "ab",
			expectError: true,
		},
		{
			name:        "too long",
			bucket:      "this-is-a-very-long-bucket-name-that-exceeds-the-sixty-three-character-limit",
			expectError: true,
		},
		{
			name:        "uppercase letters",
			bucket:      "MyBucket",
			expectError: true,
		},
		{
			name:        "underscores",
			bucket:      "my_bucket",
			expectError: true,
		},
		{
			name:        "starts with hyphen",
			bucket:      "-mybucket",
			expectError: true,
		},
		{
			name:        "ends with hyphen",
			bucket:      "mybucket-",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBucketName(tt.bucket)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
