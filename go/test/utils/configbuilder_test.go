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

package utils_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

func TestFilesystemBackupLocation(t *testing.T) {
	loc := utils.FilesystemBackupLocation("/var/backups")

	require.NotNil(t, loc)
	require.NotNil(t, loc.GetFilesystem())
	assert.Equal(t, "/var/backups", loc.GetFilesystem().GetPath())
}

func TestS3BackupLocation_Basic(t *testing.T) {
	loc := utils.S3BackupLocation("my-bucket", "us-east-1")

	require.NotNil(t, loc)
	require.NotNil(t, loc.GetS3())
	assert.Equal(t, "my-bucket", loc.GetS3().GetBucket())
	assert.Equal(t, "us-east-1", loc.GetS3().GetRegion())
	assert.Equal(t, "", loc.GetS3().GetKeyPrefix())
	assert.Equal(t, "", loc.GetS3().GetEndpoint())
	assert.False(t, loc.GetS3().GetUseEnvCredentials())
}

func TestS3BackupLocation_WithKeyPrefix(t *testing.T) {
	loc := utils.S3BackupLocation("my-bucket", "us-east-1",
		utils.WithS3KeyPrefix("prod/"))

	require.NotNil(t, loc.GetS3())
	assert.Equal(t, "prod/", loc.GetS3().GetKeyPrefix())
}

func TestS3BackupLocation_WithEndpoint(t *testing.T) {
	loc := utils.S3BackupLocation("my-bucket", "us-east-1",
		utils.WithS3Endpoint("https://s3.example.com"))

	require.NotNil(t, loc.GetS3())
	assert.Equal(t, "https://s3.example.com", loc.GetS3().GetEndpoint())
}

func TestS3BackupLocation_WithEnvCredentials(t *testing.T) {
	loc := utils.S3BackupLocation("my-bucket", "us-east-1",
		utils.WithS3EnvCredentials())

	require.NotNil(t, loc.GetS3())
	assert.True(t, loc.GetS3().GetUseEnvCredentials())
}

func TestS3BackupLocation_MultipleOptions(t *testing.T) {
	loc := utils.S3BackupLocation("my-bucket", "us-east-1",
		utils.WithS3KeyPrefix("prod/"),
		utils.WithS3Endpoint("https://s3.example.com"),
		utils.WithS3EnvCredentials())

	require.NotNil(t, loc.GetS3())
	assert.Equal(t, "prod/", loc.GetS3().GetKeyPrefix())
	assert.Equal(t, "https://s3.example.com", loc.GetS3().GetEndpoint())
	assert.True(t, loc.GetS3().GetUseEnvCredentials())
}
