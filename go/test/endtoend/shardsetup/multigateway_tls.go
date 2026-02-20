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
	"path/filepath"
	"testing"

	"github.com/multigres/multigres/go/provisioner/local"
)

// MultigatewayTLSCertPaths holds the paths to the generated multigateway TLS certificates.
type MultigatewayTLSCertPaths struct {
	CACertFile     string // CA certificate file (for client verify-ca / verify-full)
	ServerCertFile string // Server certificate file
	ServerKeyFile  string // Server private key file
}

// generateMultigatewayTLSCerts creates TLS certificates for the multigateway PostgreSQL listener.
// Generates a CA and server cert with CN=localhost, SANs=[localhost] for test connections.
func (s *ShardSetup) generateMultigatewayTLSCerts(t *testing.T) {
	t.Helper()

	certDir := filepath.Join(s.TempDir, "multigateway-tls")

	caCertFile := filepath.Join(certDir, "ca.crt")
	caKeyFile := filepath.Join(certDir, "ca.key")
	if err := local.GenerateCA(caCertFile, caKeyFile); err != nil {
		t.Fatalf("failed to generate CA for multigateway TLS: %v", err)
	}

	certFile := filepath.Join(certDir, "server.crt")
	keyFile := filepath.Join(certDir, "server.key")
	if err := local.GenerateCert(caCertFile, caKeyFile, certFile, keyFile, "localhost", []string{"localhost"}); err != nil {
		t.Fatalf("failed to generate certificate for multigateway TLS: %v", err)
	}

	s.MultigatewayTLSCertPaths = &MultigatewayTLSCertPaths{
		CACertFile:     caCertFile,
		ServerCertFile: certFile,
		ServerKeyFile:  keyFile,
	}

	t.Logf("Generated multigateway TLS certificates in %s", certDir)
}
