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

package pgbackrestsidecar

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/s3mock"
)

// tlsVerifyOffWarning is what a sidecar logs when it talks to the S3 endpoint
// without verifying its certificate.
const tlsVerifyOffWarning = "TLS certificate verification of the S3 endpoint is OFF"

// newDirectCluster boots a shard whose repository is an s3mock the sidecars
// reach directly. The s3mock's certificate is a self-signed leaf: it is not a
// CA and no system trust store holds it, so pgBackRest reaches the repository
// only by verifying against the file the backup location names, or by not
// verifying at all. caPEM is the content of that file; nil means the s3mock's
// own certificate.
func newDirectCluster(t *testing.T, poolers int, caPEM func(s3 *s3mock.Server) []byte, opts ...shardsetup.SetupOption) *sidecarCluster {
	t.Helper()
	requireSidecarHost(t)
	t.Parallel()
	clusterSlots <- struct{}{}
	t.Cleanup(func() { <-clusterSlots })

	s3Server, err := s3mock.NewServer(0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s3Server.Stop() })
	require.NoError(t, s3Server.CreateBucket(bucket))
	s3Server.RequireSessions()

	ca := s3Server.CertPEM()
	if caPEM != nil {
		ca = caPEM(s3Server)
	}
	all := append([]shardsetup.SetupOption{
		shardsetup.WithMultipoolerCount(poolers),
		shardsetup.WithS3Backup(bucket, "us-east-1", s3Server.Endpoint()),
		shardsetup.WithS3CAPEM(ca),
	}, opts...)
	setup, cleanup := shardsetup.NewIsolated(t, all...)
	t.Cleanup(cleanup)
	return &sidecarCluster{setup: setup, s3: s3Server}
}

// otherCertPEM is a self-signed CA certificate that vouches for no endpoint a
// test uses. Its subject differs from the s3mock certificate's: OpenSSL finds
// a trust anchor by subject, and of two self-signed certificates with one
// subject and no key identifiers it only tries the first in the file.
func otherCertPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "unrelated test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// clientConf is the pgBackRest configuration a pooler's sidecar rendered for
// its repository commands.
func clientConf(t *testing.T, inst *shardsetup.MultipoolerInstance) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(inst.PgBackRestd.Paths.StateDir, "conf", "pgbackrest.conf"))
	require.NoError(t, err)
	return string(raw)
}

// TestSidecar_S3EndpointVerifiedWithCAFile archives, backs up and restores
// against an S3 endpoint whose certificate only the backup location's CA file
// vouches for. The file is the endpoint's self-signed certificate used as its
// own CA in one case, and a bundle that holds it after an unrelated
// certificate in the other.
//
// It fails if the CA file does not reach pgBackRest: the certificate is in no
// trust store, so the stanza is never created and the cluster does not boot.
func TestSidecar_S3EndpointVerifiedWithCAFile(t *testing.T) {
	cases := []struct {
		name string
		ca   func(t *testing.T) func(*s3mock.Server) []byte
	}{
		{"self-signed certificate as its own CA", func(*testing.T) func(*s3mock.Server) []byte { return nil }},
		{"bundle of several certificates", func(t *testing.T) func(*s3mock.Server) []byte {
			other := otherCertPEM(t)
			return func(s3 *s3mock.Server) []byte { return append(append([]byte{}, other...), s3.CertPEM()...) }
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			marks := sidecarLogs{}
			c := newDirectCluster(t, 2, tc.ca(t))
			primary := c.setup.GetPrimary(t)

			// Backup: bootstrap stored one. Archive: every seeded row is in its
			// own archived segment.
			backups := 0
			for _, key := range c.s3.ListKeys(bucket, "") {
				if strings.Contains(key, "/backup/") && strings.HasSuffix(key, "backup.manifest") {
					backups++
				}
			}
			require.Positive(t, backups, "the bootstrap backup should be in the repository")
			want := seedArchive(t, c, 2)

			// Restore: a pooler with no data directory gets every row from the
			// backup and the archive.
			inst := startRestoringPooler(t, c, "pooler-3")
			require.Eventually(t, rowCountIs(openSocketDB(t, inst), want), 5*time.Minute, time.Second,
				"the restored pooler should replay every archived row")

			caFile := filepath.Join(c.setup.TempDir, "s3-ca.pem")
			for _, p := range []*shardsetup.MultipoolerInstance{primary, inst} {
				conf := clientConf(t, p)
				assert.Contains(t, conf, "repo1-storage-ca-file="+caFile, "%s", p.Name)
				assert.NotContains(t, conf, "storage-verify-tls", "%s", p.Name)
				assert.False(t, sidecarStatus(t, p).S3TlsVerifyDisabled, "%s", p.Name)
			}
			for _, p := range c.setup.Multipoolers {
				marks[p.PgBackRestd.LogFile] = 0
			}
			assert.NotContains(t, marks.added(t), tlsVerifyOffWarning, "a verifying sidecar must not warn")
		})
	}
}

// TestSidecar_S3WrongCAFailsClosed replaces the content of the CA file with a
// certificate that does not vouch for the endpoint. pgBackRest reads the file
// for every command, so the next archive push fails on the certificate:
// nothing reaches the repository and postgres keeps the segment. With the
// right certificate back in the file the same segment is archived.
//
// It fails if a certificate that does not verify is accepted (verification
// off, or a fallback to no verification when the CA does not match).
func TestSidecar_S3WrongCAFailsClosed(t *testing.T) {
	c := newDirectCluster(t, 2, nil)
	primary := c.setup.GetPrimary(t)
	db := openSocketDB(t, primary)
	waitArchived(t, c, primary, writeAndSwitch(t, db, "before"), 2*time.Minute)

	caFile := filepath.Join(c.setup.TempDir, "s3-ca.pem")
	right, err := os.ReadFile(caFile)
	require.NoError(t, err)
	var failedBefore int64
	require.NoError(t, db.QueryRow("SELECT failed_count FROM pg_stat_archiver").Scan(&failedBefore))
	require.NoError(t, os.WriteFile(caFile, otherCertPEM(t), 0o644))

	seg := writeAndSwitch(t, db, "wrong-ca")
	require.Eventually(t, func() bool {
		var failed int64
		return db.QueryRow("SELECT failed_count FROM pg_stat_archiver").Scan(&failed) == nil && failed > failedBefore
	}, 3*time.Minute, time.Second, "archive_command should fail while the CA file does not vouch for the endpoint")
	require.False(t, repoSegments(c.s3)[seg], "%s cannot be in the repository: its push could not verify the endpoint", seg)
	require.FileExists(t, filepath.Join(primary.Pgctld.PoolerDir, "pg_data", "pg_wal", "archive_status", seg+".ready"),
		"%s must stay queued", seg)
	// The failure is pgBackRest's refusal of the certificate, which the WAL
	// endpoint returns to archive_command and postgres logs.
	pgLog, err := os.ReadFile(filepath.Join(primary.Pgctld.PoolerDir, "pg_data", "postgresql.log"))
	require.NoError(t, err)
	assert.Contains(t, string(pgLog), "unable to verify certificate presented by",
		"archive_command should have failed on the endpoint's certificate")

	require.NoError(t, os.WriteFile(caFile, right, 0o644))
	waitArchived(t, c, primary, seg, 3*time.Minute)
}

// TestSidecar_S3VerifyOffIsLoud boots a cluster whose backup location turns
// verification of the S3 endpoint off. The endpoint's certificate is in no
// trust store and no CA file is rendered, so archiving and the bootstrap
// backup work only because pgBackRest does not verify. Every sidecar says so:
// a warning when it starts, a warning for every Configure it applies, and a
// field in Status.
//
// It fails if the warning is not logged at either point, or if Status does not
// report the setting.
func TestSidecar_S3VerifyOffIsLoud(t *testing.T) {
	c := newDirectCluster(t, 2, nil, shardsetup.WithS3SkipTLSVerify())
	primary := c.setup.GetPrimary(t)
	waitArchived(t, c, primary, writeAndSwitch(t, openSocketDB(t, primary), "verify-off"), 2*time.Minute)

	for _, p := range c.setup.Multipoolers {
		conf := clientConf(t, p)
		assert.Contains(t, conf, "repo1-storage-verify-tls=n", "%s", p.Name)
		assert.NotContains(t, conf, "storage-ca-file", "%s", p.Name)
		assert.True(t, sidecarStatus(t, p).S3TlsVerifyDisabled, "%s: Status must report that verification is off", p.Name)
		// A standby's multipooler configures its sidecar once it has joined.
		require.Eventually(t, func() bool { return sidecarStatus(t, p).Revision > 0 }, 2*time.Minute, time.Second,
			"%s: a Configure should have been applied", p.Name)

		raw, err := os.ReadFile(p.PgBackRestd.LogFile)
		require.NoError(t, err)
		var atStart, atConfigure int
		for line := range strings.SplitSeq(string(raw), "\n") {
			if !strings.Contains(line, tlsVerifyOffWarning) {
				continue
			}
			assert.Contains(t, line, "WARN", "%s: %s", p.Name, line)
			if strings.Contains(line, "when=start") || strings.Contains(line, `"when":"start"`) {
				atStart++
			}
			if strings.Contains(line, "when=configure") || strings.Contains(line, `"when":"configure"`) {
				atConfigure++
			}
		}
		assert.Positive(t, atStart, "%s: no warning at sidecar start", p.Name)
		assert.Positive(t, atConfigure, "%s: no warning for an applied Configure", p.Name)
	}
}
