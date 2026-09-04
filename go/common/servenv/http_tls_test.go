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
		se.RequireHTTPClientCert([]string{"CN=envoy,"})
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
		se.RequireHTTPClientCert([]string{"CN=envoy,"})
		err := se.validateHTTPTLS()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--http-ca")
	})

	t.Run("client-cert auth with full TLS material is valid", func(t *testing.T) {
		se := NewServEnv(viperutil.NewRegistry())
		se.httpCert.Set(cert)
		se.httpKey.Set(key)
		se.httpCA.Set(cert)
		se.RequireHTTPClientCert([]string{"CN=envoy,"})
		require.NoError(t, se.validateHTTPTLS())
	})
}

// TestCertSubjectMatches_Anchoring characterizes the substring semantics
// documented on certSubjectMatches, which callers relying on the allow-list
// as a tenant boundary have to configure around.
func TestCertSubjectMatches_Anchoring(t *testing.T) {
	t.Run("unanchored entry admits an extended subject", func(t *testing.T) {
		tenant := generateTestPeerCert(t, "ns-team-a")
		neighbour := generateTestPeerCert(t, "ns-team-a-evil")
		allow := []string{"CN=ns-team-a"}

		assert.True(t, certSubjectMatches([]*x509.Certificate{tenant}, allow))
		assert.True(t, certSubjectMatches([]*x509.Certificate{neighbour}, allow),
			"documented hazard: an extended subject also matches")
	})

	t.Run("anchoring on the next RDN's delimiter excludes an extended subject", func(t *testing.T) {
		tenant := generateTestPeerCertWithOrg(t, "ns-team-a", "supabase")
		neighbour := generateTestPeerCertWithOrg(t, "ns-team-a-evil", "supabase")
		allow := []string{"CN=ns-team-a,"}

		assert.True(t, certSubjectMatches([]*x509.Certificate{tenant}, allow))
		assert.False(t, certSubjectMatches([]*x509.Certificate{neighbour}, allow),
			"anchored entry must exclude an extended subject")
	})

	t.Run("a comma in the subject cannot forge an RDN boundary", func(t *testing.T) {
		// pkix escapes commas inside attribute values, so a caller cannot
		// smuggle "CN=ns-team-a," out of its own CN.
		forger := generateTestPeerCertWithOrg(t, "ns-team-a,O=supabase", "evil")
		allow := []string{"CN=ns-team-a,"}

		assert.NotContains(t, forger.Subject.String(), "CN=ns-team-a,O=supabase,")
		assert.False(t, certSubjectMatches([]*x509.Certificate{forger}, allow),
			"escaped comma must not satisfy an anchored entry")
	})

	t.Run("a bare CN cannot be anchored at all", func(t *testing.T) {
		tenant := generateTestPeerCert(t, "ns-team-a")
		allow := []string{"CN=ns-team-a,"}

		assert.False(t, certSubjectMatches([]*x509.Certificate{tenant}, allow),
			"no trailing delimiter exists to anchor against, so the legitimate holder is rejected too")
	})
}
