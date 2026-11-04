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

// go/test/s3mock/tls_test.go
package s3mock

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"testing"
	"time"
)

func TestGenerateTLSConfig(t *testing.T) {
	cfg, err := generateTLSConfig()
	if err != nil {
		t.Fatalf("generateTLSConfig() failed: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(cfg.Certificates))
	}

	// Verify certificate has private key
	cert := cfg.Certificates[0]
	if cert.PrivateKey == nil {
		t.Fatal("certificate missing private key")
	}
	if len(cert.Certificate) == 0 {
		t.Fatal("certificate missing cert data")
	}
}

func TestGenerateTLSConfig_Validity(t *testing.T) {
	cfg, err := generateTLSConfig()
	if err != nil {
		t.Fatalf("generateTLSConfig() failed: %v", err)
	}
	cert, err := x509.ParseCertificate(cfg.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if left := time.Until(cert.NotAfter); left < 24*time.Hour {
		t.Fatalf("certificate valid for only %v, want at least 24h", left)
	}
	if !cert.NotBefore.Before(time.Now()) {
		t.Fatalf("NotBefore %v is not in the past", cert.NotBefore)
	}
	for _, host := range []string{"127.0.0.1", "::1", "localhost"} {
		if err := cert.VerifyHostname(host); err != nil {
			t.Errorf("certificate does not cover %s: %v", host, err)
		}
	}
}

func TestTLSConfigHandshake(t *testing.T) {
	cfg, err := generateTLSConfig()
	if err != nil {
		t.Fatalf("generateTLSConfig() failed: %v", err)
	}

	// Test that config can be used for TLS handshake
	listener, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("tls.Listen() failed: %v", err)
	}
	defer listener.Close()

	// If we get here, config is valid for TLS server
}

func TestCertPEM_ClientTrust(t *testing.T) {
	srv, err := NewServer(0)
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(srv.CertPEM()) {
		t.Fatal("CertPEM() is not a parseable PEM certificate")
	}
	trusting := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	resp, err := trusting.Get(srv.Endpoint() + "/")
	if err != nil {
		t.Fatalf("client trusting CertPEM failed: %v", err)
	}
	resp.Body.Close()

	plain := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}}
	if resp, err := plain.Get(srv.Endpoint() + "/"); err == nil {
		resp.Body.Close()
		t.Fatal("client without the cert must fail verification")
	}
}

func TestCertPEM_ReturnsCopy(t *testing.T) {
	srv, err := NewServer(0)
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	first := srv.CertPEM()
	first[0] ^= 0xff
	if srv.CertPEM()[0] == first[0] {
		t.Fatal("CertPEM() must not expose internal storage")
	}
}
