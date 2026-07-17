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
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestInitialPgBackRestRepo(t *testing.T) {
	tests := []struct {
		name string
		keys CipherKeys
		want PgBackRestRepo
	}{
		{
			name: "no keys is an unencrypted repo",
			keys: nil,
			want: PgBackRestRepo{Generation: 1, RepoNumber: 1, State: PgBackRestRepoStateActive, Authoritative: true, Version: 1},
		},
		{
			name: "declared-unencrypted generation is an unencrypted repo",
			keys: CipherKeys{1: ""},
			want: PgBackRestRepo{Generation: 1, RepoNumber: 1, State: PgBackRestRepoStateActive, Authoritative: true, Version: 1},
		},
		{
			name: "mounted key is an encrypted repo with its fingerprint",
			keys: CipherKeys{1: "some-passphrase"},
			want: PgBackRestRepo{
				Generation:     1,
				RepoNumber:     1,
				Encrypted:      true,
				KeyFingerprint: CipherKeyFingerprint("some-passphrase"),
				State:          PgBackRestRepoStateActive,
				Authoritative:  true,
				Version:        1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, InitialPgBackRestRepo(tt.keys))
		})
	}
}

func TestRepoStorageConfigGenerationPaths(t *testing.T) {
	fsCfg, err := NewConfig(&clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_Filesystem{
			Filesystem: &clustermetadatapb.FilesystemBackup{Path: "/backups"},
		},
	})
	require.NoError(t, err)
	s3Cfg, err := NewConfig(&clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_S3{
			S3: &clustermetadatapb.S3Backup{Bucket: "bucket", Region: "us-east-1", KeyPrefix: "prod/"},
		},
	})
	require.NoError(t, err)

	// Generation 1 is the base path (byte-identical to the pre-generation
	// layout); later generations get a distinct gen-<N> component.
	cfg, err := fsCfg.PgBackRestConfig(1, 1, "multigres")
	require.NoError(t, err)
	assert.Equal(t, "/backups", cfg["repo1-path"])

	cfg, err = fsCfg.PgBackRestConfig(2, 3, "multigres")
	require.NoError(t, err)
	assert.Equal(t, "/backups/gen-3", cfg["repo2-path"])
	assert.Equal(t, "posix", cfg["repo2-type"])

	cfg, err = s3Cfg.PgBackRestConfig(1, 1, "multigres")
	require.NoError(t, err)
	assert.Equal(t, "/prod/multigres", cfg["repo1-path"])

	cfg, err = s3Cfg.PgBackRestConfig(2, 2, "multigres")
	require.NoError(t, err)
	assert.Equal(t, "/prod/multigres/gen-2", cfg["repo2-path"])
	assert.Equal(t, "bucket", cfg["repo2-s3-bucket"])
}

func TestRepoRetentionConfig(t *testing.T) {
	// repo1 carries the default retention values.
	cfg := repoRetentionConfig(1)
	assert.Equal(t, RetentionDifferential, cfg["repo1-retention-diff"])
	assert.Equal(t, RetentionFull, cfg["repo1-retention-full"])
	assert.Equal(t, RetentionFullType, cfg["repo1-retention-full-type"])
	assert.Equal(t, RetentionHistory, cfg["repo1-retention-history"])

	// A later repo index prefixes every key with its own repoN-.
	cfg = repoRetentionConfig(2)
	assert.Equal(t, RetentionDifferential, cfg["repo2-retention-diff"])
	assert.Equal(t, RetentionFull, cfg["repo2-retention-full"])
	assert.Equal(t, RetentionFullType, cfg["repo2-retention-full-type"])
	assert.Equal(t, RetentionHistory, cfg["repo2-retention-history"])
}
