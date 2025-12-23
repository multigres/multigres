// Copyright 2025 Supabase, Inc.
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

package pgbackrest

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateCA(t *testing.T) {
	tmpDir := t.TempDir()
	caKeyPath := filepath.Join(tmpDir, "ca.key")
	caCertPath := filepath.Join(tmpDir, "ca.crt")

	err := GenerateCA(caCertPath, caKeyPath)
	require.NoError(t, err)

	// Verify CA key exists and is valid PEM
	keyData, err := os.ReadFile(caKeyPath)
	require.NoError(t, err)
	block, _ := pem.Decode(keyData)
	require.NotNil(t, block)
	assert.Equal(t, "EC PRIVATE KEY", block.Type)

	// Verify CA cert exists and is valid
	certData, err := os.ReadFile(caCertPath)
	require.NoError(t, err)
	block, _ = pem.Decode(certData)
	require.NotNil(t, block)
	assert.Equal(t, "CERTIFICATE", block.Type)

	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	assert.True(t, cert.IsCA)
	assert.Equal(t, "multigres-pgbackrest-ca", cert.Subject.CommonName)
}

func TestGenerateServerCert(t *testing.T) {
	tmpDir := t.TempDir()

	// First generate CA
	caKeyPath := filepath.Join(tmpDir, "ca.key")
	caCertPath := filepath.Join(tmpDir, "ca.crt")
	err := GenerateCA(caCertPath, caKeyPath)
	require.NoError(t, err)

	// Generate server cert
	serverKeyPath := filepath.Join(tmpDir, "server.key")
	serverCertPath := filepath.Join(tmpDir, "server.crt")
	err = GenerateServerCert(caCertPath, caKeyPath, serverCertPath, serverKeyPath, "node1")
	require.NoError(t, err)

	// Verify server key exists
	keyData, err := os.ReadFile(serverKeyPath)
	require.NoError(t, err)
	block, _ := pem.Decode(keyData)
	require.NotNil(t, block)
	assert.Equal(t, "EC PRIVATE KEY", block.Type)

	// Verify server cert exists and is signed by CA
	certData, err := os.ReadFile(serverCertPath)
	require.NoError(t, err)
	block, _ = pem.Decode(certData)
	require.NotNil(t, block)

	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	assert.False(t, cert.IsCA)
	assert.Equal(t, "node1", cert.Subject.CommonName)

	// Verify cert is valid for server auth
	assert.Contains(t, cert.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
	assert.Contains(t, cert.ExtKeyUsage, x509.ExtKeyUsageClientAuth)

	// Verify DNS names include localhost
	assert.Contains(t, cert.DNSNames, "localhost")
}
