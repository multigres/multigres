// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A running sidecar belongs to the repository its scope flags named. Starting
// "the same sidecar" for another shard is a different configuration and is not
// adopted, so a pooler cannot be handed another shard's sidecar.
func TestSidecarForAnotherShardIsNotAdopted(t *testing.T) {
	paths := ownerTestPaths(t)
	binary, err := os.Executable()
	require.NoError(t, err)
	t.Setenv("MULTIGRES_PGBACKREST_SANDBOX", "off")
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_SESSION_TOKEN", "token")
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: filepath.Dir(paths.Dir), Backup: BackupConfig{Type: "s3", S3: &S3Backup{Bucket: "bucket", Region: "region", UseEnvCredentials: true}}}}
	pooler := filepath.Join(filepath.Dir(paths.Dir), "pooler")

	_, err = p.startPgBackRestd(t.Context(), binary, pooler, "postgres", "pw", "", "default", "0-inf", 5432, 8432, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, paths.stopOwned(context.Background())) })

	_, err = p.startPgBackRestd(t.Context(), binary, pooler, "postgres", "pw", "", "commerce", "0-80", 5432, 8432, 0)
	require.ErrorContains(t, err, "configuration differs")
}

// Credentials are fixed for the life of a sidecar: asking for the same sidecar
// with another postgres password is refused instead of silently keeping the old
// one.
func TestLiveSidecarPasswordChangeIsRefused(t *testing.T) {
	paths := ownerTestPaths(t)
	binary, err := os.Executable()
	require.NoError(t, err)
	t.Setenv("MULTIGRES_PGBACKREST_SANDBOX", "off")
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_SESSION_TOKEN", "token")
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: filepath.Dir(paths.Dir), Backup: BackupConfig{Type: "s3", S3: &S3Backup{Bucket: "bucket", Region: "region", UseEnvCredentials: true}}}}
	pooler := filepath.Join(filepath.Dir(paths.Dir), "pooler")

	_, err = p.startPgBackRestd(t.Context(), binary, pooler, "postgres", "pw", "", "default", "0-inf", 5432, 8432, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, paths.stopOwned(context.Background())) })

	_, err = p.startPgBackRestd(t.Context(), binary, pooler, "postgres", "another-password", "", "default", "0-inf", 5432, 8432, 0)
	require.ErrorContains(t, err, "password differs")
}
