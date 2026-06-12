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

package local

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGeneratePgBackRestCerts(t *testing.T) {
	// Create temp directory for certificates
	tempDir := t.TempDir()

	// Generate certificates
	certPaths, err := GeneratePgBackRestCerts(tempDir)
	require.NoError(t, err, "failed to generate certificates")
	require.NotNil(t, certPaths)

	// Verify all certificate files were created
	assert.FileExists(t, certPaths.CACertFile, "CA certificate file should exist")
	assert.FileExists(t, certPaths.ServerCertFile, "server certificate file should exist")
	assert.FileExists(t, certPaths.ServerKeyFile, "server key file should exist")

	// Test CA certificate properties
	t.Run("CA certificate", func(t *testing.T) {
		// Read and parse CA certificate
		caCertPEM, err := os.ReadFile(certPaths.CACertFile)
		require.NoError(t, err, "failed to read CA certificate")

		block, _ := pem.Decode(caCertPEM)
		require.NotNil(t, block, "failed to decode CA certificate PEM")
		assert.Equal(t, "CERTIFICATE", block.Type)

		caCert, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err, "failed to parse CA certificate")

		// Verify it's RSA (not ECDSA)
		caPublicKey, ok := caCert.PublicKey.(*rsa.PublicKey)
		require.True(t, ok, "CA certificate should use RSA key, not ECDSA")

		// Verify CA key size is 4096 bits
		assert.Equal(t, 4096, caPublicKey.N.BitLen(), "CA RSA key should be 4096 bits")

		// Verify CA properties
		assert.True(t, caCert.IsCA, "CA certificate should be marked as CA")
		assert.Equal(t, "Multigres Root CA", caCert.Subject.CommonName)
		assert.Contains(t, caCert.Subject.Organization, "Multigres")

		// Verify CA key usage
		assert.Equal(t, x509.KeyUsageCertSign|x509.KeyUsageCRLSign, caCert.KeyUsage)
	})

	// Test CA private key
	t.Run("CA private key", func(t *testing.T) {
		caKeyPath := certPaths.CACertFile[:len(certPaths.CACertFile)-4] + ".key" // Replace .crt with .key
		caKeyPEM, err := os.ReadFile(caKeyPath)
		require.NoError(t, err, "failed to read CA private key")

		block, _ := pem.Decode(caKeyPEM)
		require.NotNil(t, block, "failed to decode CA key PEM")
		assert.Equal(t, "RSA PRIVATE KEY", block.Type, "CA key should be RSA PRIVATE KEY, not EC PRIVATE KEY")

		caKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		require.NoError(t, err, "failed to parse CA private key as PKCS1 RSA key")

		// Verify key size
		assert.Equal(t, 4096, caKey.N.BitLen(), "CA private key should be 4096 bits")
	})

	// Test server certificate properties
	t.Run("server certificate", func(t *testing.T) {
		// Read and parse server certificate
		serverCertPEM, err := os.ReadFile(certPaths.ServerCertFile)
		require.NoError(t, err, "failed to read server certificate")

		block, _ := pem.Decode(serverCertPEM)
		require.NotNil(t, block, "failed to decode server certificate PEM")
		assert.Equal(t, "CERTIFICATE", block.Type)

		serverCert, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err, "failed to parse server certificate")

		// Verify it's RSA (not ECDSA)
		serverPublicKey, ok := serverCert.PublicKey.(*rsa.PublicKey)
		require.True(t, ok, "server certificate should use RSA key, not ECDSA")

		// Verify server key size is 2048 bits
		assert.Equal(t, 2048, serverPublicKey.N.BitLen(), "server RSA key should be 2048 bits")

		// Verify server certificate properties
		assert.False(t, serverCert.IsCA, "server certificate should not be marked as CA")
		assert.Equal(t, "pgbackrest", serverCert.Subject.CommonName)

		// Verify SANs (Subject Alternative Names)
		assert.Contains(t, serverCert.DNSNames, "localhost", "server cert should have localhost in SANs")
		assert.Contains(t, serverCert.DNSNames, "pgbackrest", "server cert should have pgbackrest in SANs")

		// Verify key usage
		assert.Equal(t, x509.KeyUsageDigitalSignature|x509.KeyUsageKeyEncipherment, serverCert.KeyUsage)
		assert.Contains(t, serverCert.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
	})

	// Test server private key
	t.Run("server private key", func(t *testing.T) {
		serverKeyPEM, err := os.ReadFile(certPaths.ServerKeyFile)
		require.NoError(t, err, "failed to read server private key")

		block, _ := pem.Decode(serverKeyPEM)
		require.NotNil(t, block, "failed to decode server key PEM")
		assert.Equal(t, "RSA PRIVATE KEY", block.Type, "server key should be RSA PRIVATE KEY, not EC PRIVATE KEY")

		serverKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		require.NoError(t, err, "failed to parse server private key as PKCS1 RSA key")

		// Verify key size
		assert.Equal(t, 2048, serverKey.N.BitLen(), "server private key should be 2048 bits")
	})

	// Test certificate chain validation
	t.Run("certificate chain validation", func(t *testing.T) {
		// Read CA certificate
		caCertPEM, err := os.ReadFile(certPaths.CACertFile)
		require.NoError(t, err)
		block, _ := pem.Decode(caCertPEM)
		caCert, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)

		// Read server certificate
		serverCertPEM, err := os.ReadFile(certPaths.ServerCertFile)
		require.NoError(t, err)
		block, _ = pem.Decode(serverCertPEM)
		serverCert, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)

		// Create cert pool with CA
		roots := x509.NewCertPool()
		roots.AddCert(caCert)

		// Verify server certificate was signed by CA
		opts := x509.VerifyOptions{
			Roots:     roots,
			DNSName:   "localhost",
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}

		_, err = serverCert.Verify(opts)
		assert.NoError(t, err, "server certificate should be verifiable with CA certificate")
	})
}
