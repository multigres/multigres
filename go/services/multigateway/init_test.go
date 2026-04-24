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

package multigateway

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
)

func TestBuildPGTLSConfig(t *testing.T) {
	none := server.CertAuthModeNone
	verifyFull := server.CertAuthModeVerifyFull

	t.Run("all empty returns nil", func(t *testing.T) {
		config, err := buildPGTLSConfig("", "", "", none)
		require.NoError(t, err)
		assert.Nil(t, config)
	})

	t.Run("cert without key returns error", func(t *testing.T) {
		_, err := buildPGTLSConfig("/some/cert.pem", "", "", none)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--pg-tls-cert-file requires --pg-tls-key-file")
	})

	t.Run("key without cert returns error", func(t *testing.T) {
		_, err := buildPGTLSConfig("", "/some/key.pem", "", none)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--pg-tls-key-file requires --pg-tls-cert-file")
	})

	t.Run("CA without cert/key returns error", func(t *testing.T) {
		_, err := buildPGTLSConfig("", "", "/some/ca.pem", none)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--pg-tls-ca-file requires --pg-tls-cert-file")
	})

	t.Run("verify-full without any TLS files returns error", func(t *testing.T) {
		_, err := buildPGTLSConfig("", "", "", verifyFull)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--pg-tls-client-auth-mode=verify-full requires")
	})

	t.Run("verify-full without CA returns error", func(t *testing.T) {
		certFile, keyFile := generateTestCertAndKey(t)
		_, err := buildPGTLSConfig(certFile, keyFile, "", verifyFull)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires --pg-tls-ca-file")
	})

	t.Run("CA set but mode is none returns error", func(t *testing.T) {
		certFile, keyFile := generateTestCertAndKey(t)
		// Reuse the self-signed cert file as a stand-in CA bundle; the content
		// parser only needs PEM-decodable certificates, which this provides.
		_, err := buildPGTLSConfig(certFile, keyFile, certFile, none)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--pg-tls-ca-file set but --pg-tls-client-auth-mode is none")
	})

	t.Run("cert + key only produces TLS config with no client auth", func(t *testing.T) {
		certFile, keyFile := generateTestCertAndKey(t)
		config, err := buildPGTLSConfig(certFile, keyFile, "", none)
		require.NoError(t, err)
		require.NotNil(t, config)
		assert.Equal(t, uint16(tls.VersionTLS12), config.MinVersion)
		assert.Len(t, config.Certificates, 1)
		assert.Equal(t, tls.NoClientCert, config.ClientAuth)
	})

	t.Run("verify-full with CA builds mTLS config", func(t *testing.T) {
		certFile, keyFile := generateTestCertAndKey(t)
		// The self-signed cert is its own issuer, so it's a valid CA bundle.
		config, err := buildPGTLSConfig(certFile, keyFile, certFile, verifyFull)
		require.NoError(t, err)
		require.NotNil(t, config)
		assert.Equal(t, tls.RequireAndVerifyClientCert, config.ClientAuth)
		require.NotNil(t, config.ClientCAs)
	})

	t.Run("invalid cert file wraps underlying error", func(t *testing.T) {
		dir := t.TempDir()
		certFile := filepath.Join(dir, "bad.crt")
		keyFile := filepath.Join(dir, "bad.key")
		require.NoError(t, os.WriteFile(certFile, []byte("not a cert"), 0o600))
		require.NoError(t, os.WriteFile(keyFile, []byte("not a key"), 0o600))

		_, err := buildPGTLSConfig(certFile, keyFile, "", none)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to build PG TLS config")
	})
}

// generateTestCertAndKey creates a self-signed certificate and key in a temp directory.
func generateTestCertAndKey(t *testing.T) (certFile, keyFile string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	dir := t.TempDir()
	certFile = filepath.Join(dir, "server.crt")
	keyFile = filepath.Join(dir, "server.key")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

	return certFile, keyFile
}
