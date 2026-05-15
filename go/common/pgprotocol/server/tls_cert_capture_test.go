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
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"log/slog"
	"math/big"
	"net"
	"strings"
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

// minimalCaptureConn returns a Conn populated only with what
// captureTLSServerCert touches (logger + the tlsServerCert writeback).
// Avoids spinning a listener for pure-helper unit coverage.
func minimalCaptureConn(t *testing.T) *Conn {
	t.Helper()
	return &Conn{logger: testLogger(t)}
}

// TestCaptureTLSServerCert_NilArgIsNoop ensures the helper tolerates the
// crypto/tls case where selection returned (nil, nil); we must not panic
// nor store anything.
func TestCaptureTLSServerCert_NilArgIsNoop(t *testing.T) {
	c := minimalCaptureConn(t)
	c.captureTLSServerCert(nil)
	assert.Nil(t, c.tlsServerCert)
}

// TestCaptureTLSServerCert_PrefersLeafField verifies the fast path:
// callers that pre-parse and set tls.Certificate.Leaf skip x509 parsing.
func TestCaptureTLSServerCert_PrefersLeafField(t *testing.T) {
	ca, caKey := newTestCA(t)
	leaf := signLeafCert(t, ca, caKey, "localhost", 42)
	parsed, err := x509.ParseCertificate(leaf.Certificate[0])
	require.NoError(t, err)
	leaf.Leaf = parsed
	// Clobber DER so a parse-fallback would fail; this proves Leaf is used.
	leaf.Certificate = [][]byte{{0x00}}

	c := minimalCaptureConn(t)
	c.captureTLSServerCert(&leaf)
	require.NotNil(t, c.tlsServerCert)
	assert.Equal(t, big.NewInt(42), c.tlsServerCert.SerialNumber)
}

// TestCaptureTLSServerCert_EmptyCertificateBytes covers the guard before
// x509.ParseCertificate: an empty Certificate slice with nil Leaf must
// leave tlsServerCert nil rather than indexing into an empty slice.
func TestCaptureTLSServerCert_EmptyCertificateBytes(t *testing.T) {
	c := minimalCaptureConn(t)
	c.captureTLSServerCert(&tls.Certificate{})
	assert.Nil(t, c.tlsServerCert)
}

// TestCaptureTLSServerCert_ParseErrorIsNonFatal covers the warn-and-leave-nil
// branch: garbage DER must not crash and must not be stored. SCRAM falls
// back to non-PLUS in this case.
func TestCaptureTLSServerCert_ParseErrorIsNonFatal(t *testing.T) {
	c := minimalCaptureConn(t)
	c.captureTLSServerCert(&tls.Certificate{Certificate: [][]byte{{0x00, 0x01, 0x02}}})
	assert.Nil(t, c.tlsServerCert)
}

// TestWrapTLSConfigForCertCapture_GetConfigForClientError forwards the
// user-supplied getter's error verbatim without invoking capture.
func TestWrapTLSConfigForCertCapture_GetConfigForClientError(t *testing.T) {
	sentinel := errors.New("boom")
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return nil, sentinel
		},
	}
	called := false
	wrapped := wrapTLSConfigForCertCapture(cfg, func(*tls.Certificate) { called = true })
	inner, err := wrapped.GetConfigForClient(&tls.ClientHelloInfo{})
	assert.Nil(t, inner)
	assert.ErrorIs(t, err, sentinel)
	assert.False(t, called, "capture must not run when getter errors")
}

// TestWrapTLSConfigForCertCapture_GetConfigForClientNilInner covers the
// crypto/tls "fall back to outer config" contract: returning nil from
// GetConfigForClient must propagate untouched (outer config is already
// wrapped, so re-wrapping would double-invoke capture).
func TestWrapTLSConfigForCertCapture_GetConfigForClientNilInner(t *testing.T) {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return nil, nil
		},
	}
	wrapped := wrapTLSConfigForCertCapture(cfg, func(*tls.Certificate) {})
	inner, err := wrapped.GetConfigForClient(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	assert.Nil(t, inner)
}

// TestWrapInnerCfgForCertCapture_NoGetterNoCertsBypasses asserts the
// short-circuit: an inner config with neither GetCertificate nor any
// Certificates is returned unchanged (no clone). crypto/tls falls back
// to the outer config in this case, which is already wrapped by
// wrapTLSConfigForCertCapture.
func TestWrapInnerCfgForCertCapture_NoGetterNoCertsBypasses(t *testing.T) {
	inner := &tls.Config{MinVersion: tls.VersionTLS12}
	got := wrapInnerCfgForCertCapture(inner, func(*tls.Certificate) {})
	assert.Same(t, inner, got, "empty inner config must not be cloned")
}

// TestWrapInnerCfgForCertCapture_InnerHasGetCertificate covers the inner
// dynamic-getter branch: when GetConfigForClient returns a config that
// itself has GetCertificate, that getter is the one crypto/tls uses, so
// it must be wrapped to invoke capture.
func TestWrapInnerCfgForCertCapture_InnerHasGetCertificate(t *testing.T) {
	ca, caKey := newTestCA(t)
	leaf := signLeafCert(t, ca, caKey, "localhost", 100)

	var captured *tls.Certificate
	inner := &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return &leaf, nil
		},
	}

	wrapped := wrapInnerCfgForCertCapture(inner, func(c *tls.Certificate) { captured = c })
	require.NotSame(t, inner, wrapped, "inner must be cloned before mutating GetCertificate")
	got, err := wrapped.GetCertificate(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Same(t, &leaf, captured, "capture must receive the cert that crypto/tls would present")
}

// TestWrapInnerCfgForCertCapture_InnerGetCertificateError surfaces the
// user-supplied getter's error without capturing.
func TestWrapInnerCfgForCertCapture_InnerGetCertificateError(t *testing.T) {
	sentinel := errors.New("inner-boom")
	inner := &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return nil, sentinel
		},
	}
	called := false
	wrapped := wrapInnerCfgForCertCapture(inner, func(*tls.Certificate) { called = true })
	got, err := wrapped.GetCertificate(&tls.ClientHelloInfo{})
	assert.Nil(t, got)
	assert.ErrorIs(t, err, sentinel)
	assert.False(t, called, "capture must not run when getter errors")
}

// TestSelectCertificate_EmptyList covers the early return for a config
// with no Certificates — should be nil, not a panic from indexing.
func TestSelectCertificate_EmptyList(t *testing.T) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	assert.Nil(t, selectCertificate(cfg, &tls.ClientHelloInfo{}))
}

// TestSelectCertificate_FallsBackToFirst covers the final return when
// SupportsCertificate matches no entry. Mirrors crypto/tls's behavior of
// using Certificates[0] as a last resort. The ClientHelloInfo here names
// an SNI neither leaf supports, so both SupportsCertificate calls fail.
func TestSelectCertificate_FallsBackToFirst(t *testing.T) {
	ca, caKey := newTestCA(t)
	leafA := signLeafCert(t, ca, caKey, "tenant-a.localhost", 50)
	leafB := signLeafCert(t, ca, caKey, "tenant-b.localhost", 51)
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{leafA, leafB},
	}

	chi := &tls.ClientHelloInfo{
		ServerName:        "mismatch.localhost",
		CipherSuites:      []uint16{tls.TLS_AES_128_GCM_SHA256},
		SupportedVersions: []uint16{tls.VersionTLS13},
	}
	got := selectCertificate(cfg, chi)
	require.NotNil(t, got)
	assert.Same(t, &cfg.Certificates[0], got, "fallback must be Certificates[0]")
}

// TestNewListener_WarnsWhenTLSConfigYieldsNoCert exercises the listener-
// init warn path: a non-nil TLSConfig with no cert source would silently
// disable SCRAM-SHA-256-PLUS and break handshakes at runtime, so init
// must emit a Warn record. Captures the slog text output and asserts on
// the marker phrase.
func TestNewListener_WarnsWhenTLSConfigYieldsNoCert(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	listener, err := NewListener(ListenerConfig{
		Address:            "localhost:0",
		Handler:            &mockHandler{},
		CredentialProvider: newMockCredentialProvider("postgres"),
		Logger:             logger,
		TLSConfig:          &tls.Config{MinVersion: tls.VersionTLS12}, // no Certificates, no getters
	})
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	out := buf.String()
	assert.True(t, strings.Contains(out, "SCRAM-SHA-256-PLUS will not be advertised"),
		"expected warn about disabled SCRAM-PLUS; got: %s", out)
}
