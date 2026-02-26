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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/provisioner/local"
)

// generatePgBackRestCerts creates TLS certificates for pgBackRest and PostgreSQL.
// Mirrors what the production provisioner does in generatePgBackRestCertsOnce:
// both GeneratePgBackRestCerts and GeneratePgCerts are called so the shared CA
// covers pgBackRest, the PostgreSQL server, and the pgctld client role.
func (s *ShardSetup) generatePgCerts(t *testing.T) *local.PgCertPaths {
	t.Helper()

	certDir := filepath.Join(s.TempDir, "certs")

	// Generate PostgreSQL server SSL certs (server.crt/key) and the pgctld client
	// cert into the same directory so PostgreSQL can start with ssl=on.
	certPaths, err := local.GeneratePgCerts(certDir, constants.DefaultMultigresUser)
	if err != nil {
		t.Fatalf("failed to generate PostgreSQL SSL certificates: %v", err)
	}

	t.Logf("Generated pgBackRest and PostgreSQL SSL certificates in %s", certDir)

	return certPaths
}
