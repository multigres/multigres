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

package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// The hashes are compared between a sidecar and a multipooler that may be
// built from different commits during a rollout, so their values are pinned:
// a change here is a wire change.
func TestLocationFingerprint_PinnedValues(t *testing.T) {
	s3 := LocationFingerprint{LocationType: LocationTypeS3, S3Bucket: "b", S3Region: "r", S3Endpoint: "https://e", S3KeyPrefix: "p/"}
	assert.Equal(t, "3fd332ada31a0d7a11c515a65ea8edd6e030aad106be71742c70cc82dd777c58", s3.Hash())
	fs := LocationFingerprint{LocationType: LocationTypeFilesystem, FSRepoPath: "/data/backups"}
	assert.Equal(t, "ac9d5d4edc31242dd8ebdb27151e528dc39568c30066b8104248bea9cf6d1a19", fs.Hash())
}

func TestLocationFingerprintOf(t *testing.T) {
	s3 := &clustermetadatapb.BackupLocation{Location: &clustermetadatapb.BackupLocation_S3{S3: &clustermetadatapb.S3Backup{
		Bucket: "b", Region: "r", Endpoint: "https://e", KeyPrefix: "p/",
	}}}
	assert.Equal(t, LocationFingerprint{LocationType: LocationTypeS3, S3Bucket: "b", S3Region: "r", S3Endpoint: "https://e", S3KeyPrefix: "p/"}, LocationFingerprintOf(s3))
	fs := &clustermetadatapb.BackupLocation{Location: &clustermetadatapb.BackupLocation_Filesystem{Filesystem: &clustermetadatapb.FilesystemBackup{Path: "/data/backups"}}}
	assert.Equal(t, LocationFingerprint{LocationType: LocationTypeFilesystem, FSRepoPath: "/data/backups"}, LocationFingerprintOf(fs))
	// No location in the topology means the default filesystem repository.
	assert.Equal(t, LocationFingerprint{LocationType: LocationTypeFilesystem, FSRepoPath: DefaultFilesystemRepoPath}, LocationFingerprintOf(nil))
	assert.Equal(t, LocationFingerprintOf(nil), LocationFingerprintOf(&clustermetadatapb.BackupLocation{}))
}
