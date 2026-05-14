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

package server

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// newTestCA generates an ephemeral CA. Returned together so signLeafCert
// can issue multiple distinct leaves (for SNI dispatch tests) without
// re-creating trust roots.
func newTestCA(t *testing.T) (caCert *x509.Certificate, caKey *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "MUL-434 Test CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	parsed, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return parsed, key
}

// signLeafCert issues a leaf cert under ca for the given DNS name. Each
// leaf carries a fresh serial so two leaves under the same CA produce
// distinguishable channel-binding material.
func signLeafCert(t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, dnsName string, serial int64) tls.Certificate {
	t.Helper()
	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: dnsName},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(1 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{dnsName},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, ca, &leafKey.PublicKey, caKey)
	require.NoError(t, err)
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  leafKey,
	}
}

// runSCRAMPlusServerOnce accepts one connection on ln, drives the SCRAM-
// SHA-256-PLUS server side via newTestConn + tlsConfig, and forwards the
// startup result to errCh. Mirrors the pattern used by ssl_scram_plus_test.
func runSCRAMPlusServerOnce(t *testing.T, ln net.Listener, tlsConfig *tls.Config, errCh chan<- error) {
	t.Helper()
	go func() {
		netConn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		c := newTestConn(t, netConn, tlsConfig)
		errCh <- c.handleStartup()
	}()
}

// TestSSL_SCRAMPlus_GetCertificatePath verifies that when the listener's
// tls.Config uses GetCertificate (no static Certificates), the wrapper
// captures the chosen leaf and SCRAM-SHA-256-PLUS advertises and completes
// end-to-end. Regression target for MUL-434: prior to the wrapper,
// c.tlsServerCert stayed nil and PLUS silently fell back to base SCRAM.
func TestSSL_SCRAMPlus_GetCertificatePath(t *testing.T) {
	ca, caKey := newTestCA(t)
	leaf := signLeafCert(t, ca, caKey, "localhost", 2)
	caPool := x509.NewCertPool()
	caPool.AddCert(ca)

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
			return &leaf, nil
		},
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	errCh := make(chan error, 1)
	runSCRAMPlusServerOnce(t, ln, tlsConfig, errCh)

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer clientConn.Close()

	writeSSLRequest(t, clientConn)
	require.Equal(t, byte('S'), readSingleByte(t, clientConn))

	tlsClientConn := tls.Client(clientConn, &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	})
	require.NoError(t, tlsClientConn.Handshake())

	writeStartupPacketToPipe(t, tlsClientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "tlsuser",
		"database": "tlsdb",
	})
	scramPlusClientHelper(t, tlsClientConn, "tlsuser", "postgres")

	require.NoError(t, <-errCh)
}

// TestSSL_SCRAMPlus_GetConfigForClientPath covers the dynamic-config
// branch: the listener's tls.Config has GetConfigForClient returning a
// per-handshake *tls.Config that itself only carries static Certificates.
// crypto/tls runs cert selection against the inner config, so the outer
// wrapper alone wouldn't see the chosen cert — this exercises
// wrapInnerCfgForCertCapture.
func TestSSL_SCRAMPlus_GetConfigForClientPath(t *testing.T) {
	ca, caKey := newTestCA(t)
	leaf := signLeafCert(t, ca, caKey, "localhost", 3)
	caPool := x509.NewCertPool()
	caPool.AddCert(ca)

	innerCfg := &tls.Config{
		Certificates: []tls.Certificate{leaf},
		MinVersion:   tls.VersionTLS12,
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetConfigForClient: func(_ *tls.ClientHelloInfo) (*tls.Config, error) {
			return innerCfg, nil
		},
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	errCh := make(chan error, 1)
	runSCRAMPlusServerOnce(t, ln, tlsConfig, errCh)

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer clientConn.Close()

	writeSSLRequest(t, clientConn)
	require.Equal(t, byte('S'), readSingleByte(t, clientConn))

	tlsClientConn := tls.Client(clientConn, &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	})
	require.NoError(t, tlsClientConn.Handshake())

	writeStartupPacketToPipe(t, tlsClientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "tlsuser",
		"database": "tlsdb",
	})
	scramPlusClientHelper(t, tlsClientConn, "tlsuser", "postgres")

	require.NoError(t, <-errCh)
}

// TestSSL_SCRAMPlus_SNIDispatch_DistinctCBindPerConn drives two sequential
// connections at the same listener with different SNI names; each conn
// must receive the cert matching its SNI, and the SCRAM-PLUS channel
// binding hash therefore differs per conn. Verifies that
// captureTLSServerCert records the right leaf per handshake rather than
// pinning to a single listener-wide cert.
func TestSSL_SCRAMPlus_SNIDispatch_DistinctCBindPerConn(t *testing.T) {
	ca, caKey := newTestCA(t)
	certA := signLeafCert(t, ca, caKey, "tenant-a.localhost", 10)
	certB := signLeafCert(t, ca, caKey, "tenant-b.localhost", 11)
	caPool := x509.NewCertPool()
	caPool.AddCert(ca)

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(chi *tls.ClientHelloInfo) (*tls.Certificate, error) {
			switch chi.ServerName {
			case "tenant-a.localhost":
				return &certA, nil
			case "tenant-b.localhost":
				return &certB, nil
			default:
				return &certA, nil
			}
		},
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	serials := make(map[string]*big.Int)
	for _, sni := range []string{"tenant-a.localhost", "tenant-b.localhost"} {
		errCh := make(chan error, 1)
		runSCRAMPlusServerOnce(t, ln, tlsConfig, errCh)

		clientConn, err := net.Dial("tcp", ln.Addr().String())
		require.NoError(t, err)

		writeSSLRequest(t, clientConn)
		require.Equal(t, byte('S'), readSingleByte(t, clientConn))

		tlsClientConn := tls.Client(clientConn, &tls.Config{
			RootCAs:    caPool,
			ServerName: sni,
			MinVersion: tls.VersionTLS12,
		})
		require.NoError(t, tlsClientConn.Handshake())

		// Capture the cert SNI selected. SCRAM-PLUS will use this same
		// cert's tls-server-end-point hash; scramPlusClientHelper below
		// would fail if the server-side capture wired up a different leaf.
		peerCerts := tlsClientConn.ConnectionState().PeerCertificates
		require.NotEmpty(t, peerCerts)
		serials[sni] = peerCerts[0].SerialNumber

		writeStartupPacketToPipe(t, tlsClientConn, protocol.ProtocolVersionNumber, map[string]string{
			"user":     "tlsuser",
			"database": "tlsdb",
		})
		scramPlusClientHelper(t, tlsClientConn, "tlsuser", "postgres")
		require.NoError(t, <-errCh)
		_ = clientConn.Close()
	}

	require.Len(t, serials, 2)
	require.Equal(t, big.NewInt(10), serials["tenant-a.localhost"])
	require.Equal(t, big.NewInt(11), serials["tenant-b.localhost"])
	assert.NotEqual(t, serials["tenant-a.localhost"].String(), serials["tenant-b.localhost"].String(),
		"each SNI must surface a distinct leaf cert")
}

// TestTLSConfigYieldsServerCert covers the listener init warning logic.
// Each path that ought to yield a cert is recognized; the empty case
// (which would silently disable SCRAM-SHA-256-PLUS) is flagged.
func TestTLSConfigYieldsServerCert(t *testing.T) {
	tests := []struct {
		name string
		cfg  *tls.Config
		want bool
	}{
		{name: "nil", cfg: nil, want: false},
		{name: "empty", cfg: &tls.Config{MinVersion: tls.VersionTLS12}, want: false},
		{
			name: "static_certs",
			cfg:  &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{}}},
			want: true,
		},
		{
			name: "get_certificate",
			cfg: &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
				return nil, nil
			}},
			want: true,
		},
		{
			name: "get_config_for_client",
			cfg: &tls.Config{MinVersion: tls.VersionTLS12, GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
				return nil, nil
			}},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tlsConfigYieldsServerCert(tt.cfg))
		})
	}
}

// TestWrapTLSConfigForCertCapture_StaticPathBypasses asserts the helper
// returns the same *tls.Config (not a clone) when no dynamic getter is
// set. The static path is handled by handleSSLRequest's post-handshake
// Certificates[0] fallback; cloning would be wasted work on the hot path.
func TestWrapTLSConfigForCertCapture_StaticPathBypasses(t *testing.T) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{}}}
	got := wrapTLSConfigForCertCapture(cfg, func(*tls.Certificate) {})
	assert.Same(t, cfg, got, "static-only config must not be cloned")
}

// TestWrapTLSConfigForCertCapture_NilInputs guards the early returns:
// nil base or nil capture must be no-op regardless.
func TestWrapTLSConfigForCertCapture_NilInputs(t *testing.T) {
	assert.Nil(t, wrapTLSConfigForCertCapture(nil, func(*tls.Certificate) {}))
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		return nil, nil
	}}
	assert.Same(t, cfg, wrapTLSConfigForCertCapture(cfg, nil),
		"nil capture must return base unchanged")
}
