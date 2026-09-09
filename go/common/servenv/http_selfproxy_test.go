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
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/viperutil"
)

// writeLoopbackCertKey writes a self-signed cert valid for 127.0.0.1, usable as
// both the listener's server certificate and, for the self-proxy, its client
// certificate.
func writeLoopbackCertKey(t *testing.T, cn string) (certPath, keyPath string) {
	t.Helper()
	dir := t.TempDir()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath,
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600))
	return certPath, keyPath
}

func TestHTTPTLSEnabled(t *testing.T) {
	cert, key := writeLoopbackCertKey(t, "multiadmin")

	se := NewServEnv(viperutil.NewRegistry())
	assert.False(t, se.HTTPTLSEnabled(), "plaintext by default")

	se.httpCert.Set(cert)
	se.httpKey.Set(key)
	assert.True(t, se.HTTPTLSEnabled())
}

// TestHTTPSelfClientTLSConfig_RoundTrip is the regression test for the
// self-proxy hop: with the listener on TLS, dialing it with the config this
// returns must succeed, and a client that assumes plaintext must not.
func TestHTTPSelfClientTLSConfig_RoundTrip(t *testing.T) {
	cert, key := writeLoopbackCertKey(t, "multiadmin")

	newServEnv := func(enforce bool) *ServEnv {
		se := NewServEnv(viperutil.NewRegistry())
		se.httpCert.Set(cert)
		se.httpKey.Set(key)
		se.httpCA.Set(cert)
		if enforce {
			se.RequireHTTPClientCert()
		}
		return se
	}

	t.Run("plaintext listener needs no config", func(t *testing.T) {
		se := NewServEnv(viperutil.NewRegistry())
		cfg, err := se.HTTPSelfClientTLSConfig()
		require.NoError(t, err)
		assert.Nil(t, cfg)
	})

	t.Run("dials its own TLS listener", func(t *testing.T) {
		se := newServEnv(false)
		cfg, err := se.HTTPSelfClientTLSConfig()
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Empty(t, cfg.Certificates, "no client certificate unless enforcement is on")

		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		pair, err := tls.LoadX509KeyPair(cert, key)
		require.NoError(t, err)
		srv.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
		srv.StartTLS()
		defer srv.Close()

		client := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
		resp, err := client.Get(srv.URL)
		require.NoError(t, err, "self-proxy must trust its own listener")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// A plaintext request to a TLS listener does not fail at the transport
		// level; it comes back as a 400, so a caller that picks the wrong
		// scheme sees a puzzling status rather than a connection error.
		plain, err := (&http.Client{}).Get("http://" + srv.Listener.Addr().String())
		require.NoError(t, err)
		defer plain.Body.Close()
		assert.Equal(t, http.StatusBadRequest, plain.StatusCode,
			"plaintext against a TLS listener yields 400, not a transport error")
	})

	t.Run("presents a client certificate when enforcement is on", func(t *testing.T) {
		se := newServEnv(true)
		cfg, err := se.HTTPSelfClientTLSConfig()
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Len(t, cfg.Certificates, 1, "must present a client certificate")

		leaf, err := x509.ParseCertificate(cfg.Certificates[0].Certificate[0])
		require.NoError(t, err)
		assert.Equal(t, "multiadmin", leaf.Subject.CommonName,
			"the subject operators must allow-list for the self-proxy hop")

		// And it satisfies an allow-list naming that subject.
		allowed, err := parseCertSubjects([]string{"CN=multiadmin"})
		require.NoError(t, err)
		assert.True(t, certSubjectEquals([]*x509.Certificate{leaf}, allowed))
	})
}
