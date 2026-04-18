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

package grpccommon

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// generateTestCA creates a self-signed CA cert and key in the given directory.
func generateTestCA(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()
	certPath = filepath.Join(dir, "ca.crt")
	keyPath = filepath.Join(dir, "ca.key")

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	tmpl := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "Test CA", Organization: []string{"Test"}},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, key.Public(), key)
	require.NoError(t, err)

	writePEM(t, certPath, "CERTIFICATE", certDER)
	writePEM(t, keyPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
	return certPath, keyPath
}

// generateTestCert creates a certificate signed by the given CA.
func generateTestCert(t *testing.T, dir, cn string, caCertPath, caKeyPath string) (certPath, keyPath string) {
	t.Helper()
	certPath = filepath.Join(dir, cn+".crt")
	keyPath = filepath.Join(dir, cn+".key")

	// Load CA.
	caCertPEM, err := os.ReadFile(caCertPath)
	require.NoError(t, err)
	block, _ := pem.Decode(caCertPEM)
	require.NotNil(t, block)
	caCert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	caKeyPEM, err := os.ReadFile(caKeyPath)
	require.NoError(t, err)
	block, _ = pem.Decode(caKeyPEM)
	require.NotNil(t, block)
	caKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	require.NoError(t, err)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: cn, Organization: []string{"Test"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &tmpl, caCert, key.Public(), caKey)
	require.NoError(t, err)

	writePEM(t, certPath, "CERTIFICATE", certDER)
	writePEM(t, keyPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
	return certPath, keyPath
}

func writePEM(t *testing.T, path, blockType string, data []byte) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, pem.Encode(f, &pem.Block{Type: blockType, Bytes: data}))
}

// helper that generates a CA + signed cert in a temp dir.
func generateTestCerts(t *testing.T, cn string) (caCert, caKey, cert, key string) {
	t.Helper()
	dir := t.TempDir()
	caCert, caKey = generateTestCA(t, dir)
	cert, key = generateTestCert(t, dir, cn, caCert, caKey)
	return caCert, caKey, cert, key
}

// --- Server TLS Config tests ---

func TestBuildServerTLSConfig_NoTLS(t *testing.T) {
	cfg, err := BuildServerTLSConfig("", "", "", "")
	require.NoError(t, err)
	assert.Nil(t, cfg)
}

func TestBuildServerTLSConfig_KeyWithoutCert(t *testing.T) {
	_, err := BuildServerTLSConfig("", "some-key.pem", "", "")
	assert.ErrorContains(t, err, "server cert is required when server key is set")
}

func TestBuildServerTLSConfig_CertWithoutKey(t *testing.T) {
	_, err := BuildServerTLSConfig("some-cert.pem", "", "", "")
	assert.ErrorContains(t, err, "server key is required when server cert is set")
}

func TestBuildServerTLSConfig_CAWithoutCertAndKey(t *testing.T) {
	_, err := BuildServerTLSConfig("", "", "some-ca.pem", "")
	assert.ErrorContains(t, err, "server CA configured without server cert and key")
}

func TestBuildServerTLSConfig_ServerCAWithoutCertAndKey(t *testing.T) {
	_, err := BuildServerTLSConfig("", "", "", "some-server-ca.pem")
	assert.ErrorContains(t, err, "server CA configured without server cert and key")
}

func TestBuildServerTLSConfig_TLS(t *testing.T) {
	_, _, cert, key := generateTestCerts(t, "server")

	cfg, err := BuildServerTLSConfig(cert, key, "", "")
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Len(t, cfg.Certificates, 1)
	assert.Equal(t, tls.NoClientCert, cfg.ClientAuth)
	assert.Nil(t, cfg.ClientCAs)
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
}

func TestBuildServerTLSConfig_MTLS(t *testing.T) {
	caCert, _, cert, key := generateTestCerts(t, "server")

	cfg, err := BuildServerTLSConfig(cert, key, caCert, "")
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, tls.RequireAndVerifyClientCert, cfg.ClientAuth)
	assert.NotNil(t, cfg.ClientCAs)
}

func TestBuildServerTLSConfig_WithServerCA(t *testing.T) {
	caCert, _, cert, key := generateTestCerts(t, "server")

	cfg, err := BuildServerTLSConfig(cert, key, "", caCert)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	// Certificate chain: server cert + intermediate CA.
	assert.Len(t, cfg.Certificates[0].Certificate, 2)
}

// TestBuildServerTLSConfig_ServerCAIgnoresNonCertBlocks ensures non-CERTIFICATE
// PEM blocks (e.g., a stray PRIVATE KEY in a mixed PEM file) are skipped rather
// than appended to the chain.
func TestBuildServerTLSConfig_ServerCAIgnoresNonCertBlocks(t *testing.T) {
	caCert, caKey, cert, key := generateTestCerts(t, "server")

	// Build a mixed PEM file: the CA cert followed by the CA key.
	caCertPEM, err := os.ReadFile(caCert)
	require.NoError(t, err)
	caKeyPEM, err := os.ReadFile(caKey)
	require.NoError(t, err)

	dir := t.TempDir()
	mixedPath := filepath.Join(dir, "mixed.pem")
	require.NoError(t, os.WriteFile(mixedPath, append(caCertPEM, caKeyPEM...), 0o600))

	cfg, err := BuildServerTLSConfig(cert, key, "", mixedPath)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	// Only the server cert and the one intermediate CERTIFICATE should be in
	// the chain — the private key block must be skipped.
	assert.Len(t, cfg.Certificates[0].Certificate, 2)
}

func TestBuildServerTLSConfig_InvalidCert(t *testing.T) {
	_, err := BuildServerTLSConfig("/nonexistent/cert.pem", "/nonexistent/key.pem", "", "")
	assert.ErrorContains(t, err, "failed to load gRPC server certificate")
}

func TestBuildServerTLSConfig_InvalidCA(t *testing.T) {
	_, _, cert, key := generateTestCerts(t, "server")
	_, err := BuildServerTLSConfig(cert, key, "/nonexistent/ca.pem", "")
	assert.ErrorContains(t, err, "failed to read CA file")
}

func TestBuildServerTLSConfig_InvalidServerCA(t *testing.T) {
	_, _, cert, key := generateTestCerts(t, "server")
	_, err := BuildServerTLSConfig(cert, key, "", "/nonexistent/server-ca.pem")
	assert.ErrorContains(t, err, "failed to read server CA file")
}

// --- Client TLS Config tests ---

func TestBuildClientTLSConfig_NoTLS(t *testing.T) {
	cfg, err := BuildClientTLSConfig("", "", "", "")
	require.NoError(t, err)
	assert.Nil(t, cfg)
}

func TestBuildClientTLSConfig_CAOnly(t *testing.T) {
	caCert, _, _, _ := generateTestCerts(t, "server")

	cfg, err := BuildClientTLSConfig("", "", caCert, "")
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.NotNil(t, cfg.RootCAs)
	assert.Empty(t, cfg.Certificates)
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
}

func TestBuildClientTLSConfig_MTLS(t *testing.T) {
	caCert, _, cert, key := generateTestCerts(t, "client")

	cfg, err := BuildClientTLSConfig(cert, key, caCert, "")
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.NotNil(t, cfg.RootCAs)
	assert.Len(t, cfg.Certificates, 1)
}

func TestBuildClientTLSConfig_ServerNameWithCA(t *testing.T) {
	caCert, _, _, _ := generateTestCerts(t, "server")

	cfg, err := BuildClientTLSConfig("", "", caCert, "my-server.example.com")
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, "my-server.example.com", cfg.ServerName)
}

func TestBuildClientTLSConfig_ServerNameOnly(t *testing.T) {
	_, err := BuildClientTLSConfig("", "", "", "my-server.example.com")
	assert.ErrorContains(t, err, "client server name configured without CA or client cert+key")
}

func TestBuildClientTLSConfig_CertWithoutKey(t *testing.T) {
	_, err := BuildClientTLSConfig("some-cert.pem", "", "", "")
	assert.ErrorContains(t, err, "both client cert and key must be provided")
}

func TestBuildClientTLSConfig_KeyWithoutCert(t *testing.T) {
	_, err := BuildClientTLSConfig("", "some-key.pem", "", "")
	assert.ErrorContains(t, err, "both client cert and key must be provided")
}

func TestBuildClientTLSConfig_InvalidCA(t *testing.T) {
	_, err := BuildClientTLSConfig("", "", "/nonexistent/ca.pem", "")
	assert.ErrorContains(t, err, "failed to read CA file")
}

func TestBuildClientTLSConfig_InvalidCert(t *testing.T) {
	caCert, _, _, _ := generateTestCerts(t, "server")
	_, err := BuildClientTLSConfig("/nonexistent/cert.pem", "/nonexistent/key.pem", caCert, "")
	assert.ErrorContains(t, err, "failed to load gRPC client certificate")
}
