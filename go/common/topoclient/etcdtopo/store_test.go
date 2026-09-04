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

package etcdtopo

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
)

func TestNewTLSConfigFromPEM(t *testing.T) {
	certPEM, keyPEM, caPEM := newTestTLSMaterial(t)

	tlsConfig, err := newTLSConfigFromPEM(certPEM, keyPEM, caPEM)
	require.NoError(t, err)
	require.NotNil(t, tlsConfig)
	assert.Len(t, tlsConfig.Certificates, 1)
	assert.NotNil(t, tlsConfig.RootCAs)
	assert.Equal(t, uint16(tls.VersionTLS12), tlsConfig.MinVersion)
	assert.False(t, tlsConfig.InsecureSkipVerify)

	clientConfig := newEtcdClientConfig([]string{"https://etcd.example"}, tlsConfig)
	assert.Same(t, tlsConfig, clientConfig.TLS)
}

func TestNewTLSConfigFromPEMWithoutMaterialUsesPlaintext(t *testing.T) {
	tlsConfig, err := newTLSConfigFromPEM(nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, tlsConfig)

	clientConfig := newEtcdClientConfig([]string{"http://etcd.example"}, tlsConfig)
	assert.Nil(t, clientConfig.TLS)
}

func TestNewServerWithTLSRequiresCertificateAndKey(t *testing.T) {
	certPEM, keyPEM, _ := newTestTLSMaterial(t)

	for _, testCase := range []struct {
		name    string
		certPEM []byte
		keyPEM  []byte
	}{
		{name: "certificate without key", certPEM: certPEM},
		{name: "key without certificate", keyPEM: keyPEM},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := NewServerWithTLS([]string{"http://etcd.example"}, "/topology", &topoclient.TLSOptions{
				CertPEM: testCase.certPEM,
				KeyPEM:  testCase.keyPEM,
			})
			require.ErrorContains(t, err, "both client certificate and key PEM must be provided")
		})
	}
}

func TestNewServerWithTLSRejectsCAWithoutClientCredentials(t *testing.T) {
	_, _, caPEM := newTestTLSMaterial(t)

	_, err := NewServerWithTLS([]string{"http://etcd.example"}, "/topology", &topoclient.TLSOptions{CAPEM: caPEM})
	require.ErrorContains(t, err, "client certificate and key PEM are required when CA PEM is provided")
}

func TestNewServerWithTLSRejectsHTTPEndpoint(t *testing.T) {
	certPEM, keyPEM, _ := newTestTLSMaterial(t)

	_, err := NewServerWithTLS([]string{"http://etcd.example"}, "/topology", &topoclient.TLSOptions{
		CertPEM: certPEM,
		KeyPEM:  keyPEM,
	})
	require.ErrorContains(t, err, "TLS cannot use HTTP endpoint")
}

func TestValidateTLSEndpoints(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		endpoint  string
		tlsConfig *tls.Config
		wantErr   string
	}{
		{name: "plaintext HTTP", endpoint: "http://etcd.example"},
		{name: "TLS HTTP", endpoint: "http://etcd.example", tlsConfig: &tls.Config{}, wantErr: "TLS cannot use HTTP endpoint"},
		{name: "TLS HTTPS", endpoint: "https://etcd.example", tlsConfig: &tls.Config{}},
		{name: "TLS bare address", endpoint: "etcd.example:2379", tlsConfig: &tls.Config{}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateTLSEndpoints([]string{testCase.endpoint}, testCase.tlsConfig)
			if testCase.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, testCase.wantErr)
		})
	}
}

func TestNewTLSConfigFromOptionsUsesPrebuiltConfig(t *testing.T) {
	insecureSkipVerify := true
	source := &tls.Config{
		MinVersion: tls.VersionTLS10,
		// #nosec G402 - NewServerWithTLS clears this test-only input.
		InsecureSkipVerify: insecureSkipVerify,
	}

	tlsConfig, err := newTLSConfigFromOptions(&topoclient.TLSOptions{Config: source})
	require.NoError(t, err)
	require.NotNil(t, tlsConfig)
	assert.NotSame(t, source, tlsConfig)
	assert.Equal(t, uint16(tls.VersionTLS12), tlsConfig.MinVersion)
	assert.False(t, tlsConfig.InsecureSkipVerify)
	assert.Equal(t, uint16(tls.VersionTLS10), source.MinVersion)
	assert.True(t, source.InsecureSkipVerify)
}

func TestNewTLSConfigFromOptionsRejectsMixedSources(t *testing.T) {
	_, err := newTLSConfigFromOptions(&topoclient.TLSOptions{
		CertPEM: []byte("certificate"),
		Config:  &tls.Config{},
	})
	require.ErrorContains(t, err, "TLS config cannot be combined with TLS PEM material")
}

func newTestTLSMaterial(t *testing.T) (certPEM, keyPEM, caPEM []byte) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTemplate := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, caKey.Public(), caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	clientTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test client"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, &clientTemplate, caCert, clientKey.Public(), caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(clientKey)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
}
