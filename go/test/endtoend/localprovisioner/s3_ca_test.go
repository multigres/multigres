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

package localprovisioner

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/test/s3mock"
	"github.com/multigres/multigres/go/test/utils"
)

// withS3Backup makes the cluster back up to an S3 endpoint whose certificate
// is verified with caFile, using the keys in the launcher's environment.
func withS3Backup(endpoint, caFile string) clusterOption {
	return func(c *local.LocalProvisionerConfig) {
		c.Backup = local.BackupConfig{
			Type: "s3",
			S3: &local.S3Backup{
				Bucket:            "multigres",
				Region:            "us-east-1",
				Endpoint:          endpoint,
				KeyPrefix:         "backups/",
				UseEnvCredentials: true,
				CAFile:            caFile,
			},
		}
	}
}

// TestClusterBacksUpToSelfHostedS3 starts a cluster with `multigres cluster
// start` against a self-hosted S3 endpoint: an s3mock whose certificate is
// self-signed, named in the cluster config as the endpoint's CA file. The
// cluster only bootstraps if every pooler's sidecar creates the stanza, takes
// the first backup and archives WAL there, and pgBackRest verifies the
// endpoint's certificate against that file to do it. Verification is on: no
// sidecar may have been started with it switched off.
func TestClusterBacksUpToSelfHostedS3(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping self-hosted S3 test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}
	if _, err := exec.LookPath("etcd"); err != nil {
		t.Skip("etcd binary not found in PATH")
	}

	s3Server, err := s3mock.NewServer(0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s3Server.Stop() })
	require.NoError(t, s3Server.CreateBucket("multigres"))
	// Outside the cluster directory: the sidecar refuses a CA file on the data
	// volume, which postgres can write.
	caDir, err := os.MkdirTemp("/tmp", "mlt-ca-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(caDir) })
	caFile := filepath.Join(caDir, "s3-ca.pem")
	require.NoError(t, os.WriteFile(caFile, s3Server.CertPEM(), 0o644))

	clusterSetup := setupTestCluster(t, withS3Backup(s3Server.Endpoint(), caFile))

	// The first backup and its WAL are in the bucket.
	require.Eventually(t, func() bool {
		backup, wal := false, false
		for _, key := range s3Server.ListKeys("multigres", "backups/") {
			backup = backup || strings.Contains(key, "/backup/")
			wal = wal || strings.Contains(key, "/archive/")
		}
		return backup && wal
	}, 60*time.Second, time.Second, "the bucket must hold the first backup and archived WAL: %v", s3Server.ListKeys("multigres", ""))

	// Every sidecar of this cluster verifies with the CA file.
	out, err := exec.Command("ps", "-axww", "-o", "command=").Output()
	require.NoError(t, err)
	sidecars := 0
	for line := range strings.SplitSeq(string(out), "\n") {
		if !strings.Contains(line, "pgbackrestd serve") && !strings.Contains(line, "pgbackrestd  serve") {
			continue
		}
		if !strings.Contains(line, clusterSetup.TempDir) {
			continue
		}
		sidecars++
		assert.Contains(t, line, "--s3-ca-file "+caFile, "sidecar command line")
		assert.NotContains(t, line, "--s3-skip-tls-verify", "the local provisioner must never turn verification off for an endpoint with a CA")
	}
	assert.Positive(t, sidecars, "no pgbackrestd of this cluster was found")
}
