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

package servenv

import (
	"crypto/rand"
	"crypto/rsa"
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

	"github.com/multigres/multigres/go/tools/viperutil"
)

// writeTestCertKey writes a self-signed cert/key pair usable as both server
// material and a client CA, and returns their paths.
func writeTestCertKey(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	dir := t.TempDir()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-server"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certPath,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath,
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600))
	return certPath, keyPath
}

func TestValidateHTTPTLS(t *testing.T) {
	cert, key := writeTestCertKey(t)

	t.Run("nothing configured is valid (plaintext, unchanged default)", func(t *testing.T) {
		se := NewServEnv(viperutil.NewRegistry())
		require.NoError(t, se.validateHTTPTLS())
	})

	t.Run("valid cert and key without client-cert auth", func(t *testing.T) {
		se := NewServEnv(viperutil.NewRegistry())
		se.httpCert.Set(cert)
		se.httpKey.Set(key)
		require.NoError(t, se.validateHTTPTLS())
	})

	t.Run("unloadable cert fails at Init rather than in the serving goroutine", func(t *testing.T) {
		se := NewServEnv(viperutil.NewRegistry())
		se.httpCert.Set(filepath.Join(t.TempDir(), "does-not-exist.pem"))
		se.httpKey.Set(key)
		err := se.validateHTTPTLS()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "http tls config")
	})

	t.Run("client-cert auth without any TLS material is rejected", func(t *testing.T) {
		se := NewServEnv(viperutil.NewRegistry())
		se.RequireHTTPClientCert()
		err := se.validateHTTPTLS()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--http-cert")
	})

	// Without --http-ca the handshake never requests a client certificate, so
	// every non-exempt request would 401 while /live and /ready keep passing.
	t.Run("client-cert auth without a client CA is rejected", func(t *testing.T) {
		se := NewServEnv(viperutil.NewRegistry())
		se.httpCert.Set(cert)
		se.httpKey.Set(key)
		se.httpAuthMtlsAllowedSubstrings.Set("CN=gateway,")
		se.RequireHTTPClientCert()
		err := se.validateHTTPTLS()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--http-ca")
	})

	t.Run("client-cert auth with full TLS material is valid", func(t *testing.T) {
		se := NewServEnv(viperutil.NewRegistry())
		se.httpCert.Set(cert)
		se.httpKey.Set(key)
		se.httpCA.Set(cert)
		se.httpAuthMtlsAllowedSubstrings.Set("CN=gateway,")
		se.RequireHTTPClientCert()
		require.NoError(t, se.validateHTTPTLS())
		assert.Equal(t, []string{"CN=gateway,"}, se.httpClientCertSubstrings,
			"validateHTTPTLS parses the allow-list once at startup")
	})

	// The allow-list is its own flag, not gRPC's: an operator who set only
	// --grpc-auth-mtls-allowed-substrings must be told, not silently given
	// either an empty (rejects everyone) or a borrowed allow-list.
	t.Run("client-cert auth without an allow-list is rejected", func(t *testing.T) {
		se := NewServEnv(viperutil.NewRegistry())
		se.httpCert.Set(cert)
		se.httpKey.Set(key)
		se.httpCA.Set(cert)
		se.RequireHTTPClientCert()
		err := se.validateHTTPTLS()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--http-auth-mtls-allowed-substrings")
	})

	t.Run("client-cert auth with a malformed allow-list is rejected", func(t *testing.T) {
		se := NewServEnv(viperutil.NewRegistry())
		se.httpCert.Set(cert)
		se.httpKey.Set(key)
		se.httpCA.Set(cert)
		se.httpAuthMtlsAllowedSubstrings.Set("CN=gateway,::CN=other,")
		se.RequireHTTPClientCert()
		err := se.validateHTTPTLS()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--http-auth-mtls-allowed-substrings")
	})

	// The gRPC allow-list must not leak into the HTTP listener.
	t.Run("gRPC allow-list does not satisfy the HTTP one", func(t *testing.T) {
		prev := clientCertSubstrings
		clientCertSubstrings = "CN=grpc-caller,"
		t.Cleanup(func() { clientCertSubstrings = prev })

		se := NewServEnv(viperutil.NewRegistry())
		se.httpCert.Set(cert)
		se.httpKey.Set(key)
		se.httpCA.Set(cert)
		se.RequireHTTPClientCert()
		err := se.validateHTTPTLS()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--http-auth-mtls-allowed-substrings")
	})
}
