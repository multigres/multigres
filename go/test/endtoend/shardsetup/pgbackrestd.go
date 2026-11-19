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

package shardsetup

import (
	"crypto/tls"
	"encoding/pem"
	"net/url"
	"os/exec"
	"testing"

	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
)

// PgBackRestdInstance is the pgbackrestd process serving one pooler.
type PgBackRestdInstance = clustersetup.PgBackRestdInstance

// trustS3Endpoint reads the certificate of an s3mock endpoint named in the
// setup so the sidecar can verify it: the suites that use this path build their
// own s3mock and hand shardsetup only its URL. The certificate is only taken
// from the local test endpoint named in the setup, and the sidecar then
// verifies against it like any other CA file.
func trustS3Endpoint(t *testing.T, config *SetupConfig) {
	t.Helper()
	if config.S3BackupEndpoint == "" || len(config.S3CAPEM) > 0 {
		return
	}
	u, err := url.Parse(config.S3BackupEndpoint)
	if err != nil {
		t.Fatalf("parse S3 endpoint %q: %v", config.S3BackupEndpoint, err)
	}
	conn, err := tls.Dial("tcp", u.Host, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // harvests the local test mock's self-signed certificate; see above
	if err != nil {
		t.Fatalf("read the S3 endpoint's certificate from %s: %v", u.Host, err)
	}
	defer conn.Close()
	config.S3CAPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: conn.ConnectionState().PeerCertificates[0].Raw})
}

// requirePgBackrestVersion fails the test when the pgbackrest on PATH is older
// than the sidecar supports. The setup does not go through the local
// provisioner's own binary checks, so it makes the version one here.
func requirePgBackrestVersion(t *testing.T) {
	t.Helper()
	path, err := exec.LookPath("pgbackrest")
	if err != nil {
		t.Fatalf("the pgbackrestd sidecar needs pgbackrest on PATH: %v", err)
	}
	if err := local.CheckPgBackrestVersion(path); err != nil {
		t.Fatalf("pgbackrest: %v", err)
	}
}
