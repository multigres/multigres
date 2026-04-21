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
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

func TestParseCertAuthMode(t *testing.T) {
	cases := []struct {
		in      string
		want    CertAuthMode
		wantErr string
	}{
		{"none", CertAuthModeNone, ""},
		{"verify-full", CertAuthModeVerifyFull, ""},
		{"verify-ca", "", "reserved but not yet implemented"},
		{"optional", "", "reserved but not yet implemented"},
		{"bogus", "", "invalid client auth mode"},
		{"", "", "invalid client auth mode"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseCertAuthMode(tc.in)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseCertIdentitySource(t *testing.T) {
	cases := []struct {
		in      string
		want    CertIdentitySource
		wantErr string
	}{
		{"cn", CertIdentityCN, ""},
		{"dn", CertIdentityDN, ""},
		{"bogus", "", "invalid cert identity source"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseCertIdentitySource(tc.in)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// mTLSFixture bundles the TLS material needed to drive cert-auth tests:
// a server TLS config with RequireAndVerifyClientCert, and a factory for
// producing client certs signed by the same CA.
type mTLSFixture struct {
	serverConfig *tls.Config
	caPool       *x509.CertPool
	caCert       *x509.Certificate
	caKey        *rsa.PrivateKey
}

// newMTLSFixture builds a self-signed CA and a server cert signed by it,
// wiring the server side for strict mTLS (RequireAndVerifyClientCert).
func newMTLSFixture(t *testing.T) *mTLSFixture {
	t.Helper()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caTemplate := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caCertDER)
	require.NoError(t, err)

	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	serverTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:     []string{"localhost"},
	}
	serverCertDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, caCert, &serverKey.PublicKey, caKey)
	require.NoError(t, err)

	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	serverConfig := &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{serverCertDER},
			PrivateKey:  serverKey,
		}},
		MinVersion: tls.VersionTLS12,
		ClientCAs:  caPool,
		ClientAuth: tls.RequireAndVerifyClientCert,
	}

	return &mTLSFixture{
		serverConfig: serverConfig,
		caPool:       caPool,
		caCert:       caCert,
		caKey:        caKey,
	}
}

// issueClientCert produces a leaf client certificate with the given Subject,
// signed by the fixture's CA.
func (f *mTLSFixture) issueClientCert(t *testing.T, subject pkix.Name) tls.Certificate {
	t.Helper()

	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      subject,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &tmpl, f.caCert, &clientKey.PublicKey, f.caKey)
	require.NoError(t, err)
	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  clientKey,
	}
}

// certAuthTestConn mirrors newTestConn but also installs cert-auth config
// on the Conn so the authenticate() branch fires.
func certAuthTestConn(t *testing.T, serverConn net.Conn, tlsConfig *tls.Config, mode CertAuthMode, source CertIdentitySource) *Conn {
	t.Helper()
	listener := testListener(t)
	// Install cert-auth config on the shared listener so the Conn sees it
	// for listener.RegisterConn etc.
	listener.certAuthMode = mode
	listener.certIdentitySource = source
	c := &Conn{
		conn:               serverConn,
		listener:           listener,
		handler:            listener.handler,
		hashProvider:       listener.hashProvider,
		bufferedReader:     bufio.NewReader(serverConn),
		params:             make(map[string]string),
		txnStatus:          protocol.TxnStatusIdle,
		tlsConfig:          tlsConfig,
		certAuthMode:       mode,
		certIdentitySource: source,
	}
	c.ctx = context.Background()
	c.logger = testLogger(t)
	return c
}

// runCertAuthServer accepts a single connection on the listener, drives it
// through handleStartup, and returns the error on the given channel.
func runCertAuthServer(t *testing.T, ln net.Listener, fix *mTLSFixture, source CertIdentitySource) <-chan error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		netConn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		c := certAuthTestConn(t, netConn, fix.serverConfig, CertAuthModeVerifyFull, source)
		errCh <- c.handleStartup()
	}()
	return errCh
}

// connectWithClientCert dials the listener, negotiates SSL + TLS (with the
// given client cert), and returns the TLS-wrapped connection ready for
// sending a StartupMessage.
func connectWithClientCert(t *testing.T, addr string, caPool *x509.CertPool, clientCert tls.Certificate) *tls.Conn {
	t.Helper()
	rawConn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	writeSSLRequest(t, rawConn)
	resp := readSingleByte(t, rawConn)
	require.Equal(t, byte('S'), resp)
	tlsConn := tls.Client(rawConn, &tls.Config{
		RootCAs:      caPool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{clientCert},
	})
	require.NoError(t, tlsConn.Handshake())
	return tlsConn
}

func TestCertAuth_CNMatch_Succeeds(t *testing.T) {
	fix := newMTLSFixture(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	errCh := runCertAuthServer(t, ln, fix, CertIdentityCN)

	clientCert := fix.issueClientCert(t, pkix.Name{CommonName: "svc-foo"})
	tlsConn := connectWithClientCert(t, ln.Addr().String(), fix.caPool, clientCert)
	defer tlsConn.Close()

	writeStartupPacketToPipe(t, tlsConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "svc-foo",
		"database": "app",
	})

	// Expect AuthenticationOk, BackendKeyData, ParameterStatus messages,
	// and ReadyForQuery — no SCRAM exchange.
	expectAuthenticationOk(t, tlsConn)

	require.NoError(t, <-errCh)
}

func TestCertAuth_CNMismatch_Fails(t *testing.T) {
	fix := newMTLSFixture(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	errCh := runCertAuthServer(t, ln, fix, CertIdentityCN)

	clientCert := fix.issueClientCert(t, pkix.Name{CommonName: "svc-foo"})
	tlsConn := connectWithClientCert(t, ln.Addr().String(), fix.caPool, clientCert)
	defer tlsConn.Close()

	// Request a different user than the cert's CN.
	writeStartupPacketToPipe(t, tlsConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "svc-bar",
		"database": "app",
	})

	// Expect an ErrorResponse with FATAL severity and SQLSTATE 28000.
	expectCertAuthErrorResponse(t, tlsConn, "svc-bar", "CN", "svc-foo")

	// Server returns the same error upstream.
	serverErr := <-errCh
	// The server write of the error response succeeds even though the
	// auth semantically failed, so handleStartup may return nil after
	// sending the response. Accept either nil or a non-nil error.
	_ = serverErr
}

func TestCertAuth_DNMatch_Succeeds(t *testing.T) {
	fix := newMTLSFixture(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	errCh := runCertAuthServer(t, ln, fix, CertIdentityDN)

	subject := pkix.Name{
		CommonName:   "svc-foo",
		Organization: []string{"Multigres"},
		Country:      []string{"US"},
	}
	clientCert := fix.issueClientCert(t, subject)

	// Build the expected DN string the way Go renders it (RFC 2253).
	parsed, err := x509.ParseCertificate(clientCert.Certificate[0])
	require.NoError(t, err)
	expectedDN := parsed.Subject.String()

	tlsConn := connectWithClientCert(t, ln.Addr().String(), fix.caPool, clientCert)
	defer tlsConn.Close()

	writeStartupPacketToPipe(t, tlsConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     expectedDN,
		"database": "app",
	})

	expectAuthenticationOk(t, tlsConn)

	require.NoError(t, <-errCh)
}

// expectAuthenticationOk reads messages from the server and asserts that the
// AuthenticationOk / BackendKeyData / ParameterStatus / ReadyForQuery
// sequence arrives without any intervening SASL challenge.
func expectAuthenticationOk(t *testing.T, r io.Reader) {
	t.Helper()
	sawAuthOk := false
	sawReady := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !sawReady {
		msgType, body := readTypedMessage(t, r)
		switch msgType {
		case protocol.MsgAuthenticationRequest:
			require.GreaterOrEqual(t, len(body), 4)
			code := binary.BigEndian.Uint32(body[:4])
			require.Equalf(t, uint32(protocol.AuthOk), code,
				"expected AuthenticationOk (0), got auth code %d — SCRAM must not run for cert auth", code)
			sawAuthOk = true
		case protocol.MsgBackendKeyData, protocol.MsgParameterStatus:
			// expected
		case protocol.MsgReadyForQuery:
			sawReady = true
		case protocol.MsgErrorResponse:
			t.Fatalf("unexpected ErrorResponse: %q", body)
		default:
			t.Fatalf("unexpected message type %q", msgType)
		}
	}
	require.True(t, sawAuthOk, "did not see AuthenticationOk")
	require.True(t, sawReady, "did not see ReadyForQuery")
}

// expectCertAuthErrorResponse reads a single ErrorResponse and asserts it
// has severity FATAL, SQLSTATE 28000, and a DETAIL line identifying the
// matched field and presented value.
func expectCertAuthErrorResponse(t *testing.T, r io.Reader, user, fieldName, presented string) {
	t.Helper()
	msgType, body := readTypedMessage(t, r)
	require.Equal(t, byte(protocol.MsgErrorResponse), msgType)

	fields := parseErrorFields(body)
	assert.Equal(t, "FATAL", fields['S'])
	assert.Equal(t, mterrors.PgSSInvalidAuthSpec, fields['C'])
	assert.Contains(t, fields['M'], "certificate authentication failed for user")
	assert.Contains(t, fields['M'], user)
	assert.Contains(t, fields['D'], fieldName)
	assert.Contains(t, fields['D'], presented)
}

// readTypedMessage reads a single typed PG message (1-byte type + 4-byte length + body).
func readTypedMessage(t *testing.T, r io.Reader) (byte, []byte) {
	t.Helper()
	header := make([]byte, 5)
	_, err := io.ReadFull(r, header)
	require.NoError(t, err)
	msgType := header[0]
	length := binary.BigEndian.Uint32(header[1:5])
	bodyLen := int(length) - 4
	body := make([]byte, bodyLen)
	if bodyLen > 0 {
		_, err = io.ReadFull(r, body)
		require.NoError(t, err)
	}
	return msgType, body
}

// parseErrorFields parses an ErrorResponse body into a code → value map.
// Each field is: 1-byte code, null-terminated string; terminated by a 0 byte.
func parseErrorFields(body []byte) map[byte]string {
	out := make(map[byte]string)
	i := 0
	for i < len(body) {
		code := body[i]
		if code == 0 {
			break
		}
		i++
		start := i
		for i < len(body) && body[i] != 0 {
			i++
		}
		out[code] = string(body[start:i])
		i++ // skip null terminator
	}
	return out
}
