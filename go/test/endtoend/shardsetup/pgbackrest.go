// Copyright 2025 Supabase, Inc.
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
	"path/filepath"
	"testing"

	"github.com/multigres/multigres/go/provisioner/local"
)

// generatePgBackRestCerts creates TLS certificates for pgBackRest server.
// Returns the certificate paths.
// Uses the public function from go/provisioner/local package.
func (s *ShardSetup) generatePgBackRestCerts(t *testing.T) *local.PgBackRestCertPaths {
	t.Helper()

	certDir := filepath.Join(s.TempDir, "certs")
	certPaths, err := local.GeneratePgBackRestCerts(certDir)
	if err != nil {
		t.Fatalf("failed to generate pgBackRest certificates: %v", err)
	}

	t.Logf("Generated pgBackRest certificates in %s", certDir)

	return certPaths
}
