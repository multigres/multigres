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
	before := time.Now().Truncate(time.Second)
	cfg, err := generateTLSConfig()
	after := time.Now().Truncate(time.Second)
	if err != nil {
		t.Fatalf("generateTLSConfig() failed: %v", err)
	}
	cert, err := x509.ParseCertificate(cfg.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	// Bound the issuance time, including RSA generation, at X.509's second
	// precision. No assertion depends on how long this test takes to run.
	if cert.NotAfter.Before(before.Add(48*time.Hour)) || cert.NotAfter.After(after.Add(48*time.Hour)) {
		t.Errorf("NotAfter %v is not 48h after issuance in [%v, %v]", cert.NotAfter, before, after)
	}
	if got := cert.NotAfter.Sub(cert.NotBefore); got != 48*time.Hour+5*time.Minute {
		t.Errorf("validity window = %v, want 48h5m", got)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	issuedAt := cert.NotAfter.Add(-48 * time.Hour)
	for _, host := range []string{"127.0.0.1", "::1", "localhost"} {
		for _, offset := range []time.Duration{-4 * time.Minute, 24 * time.Hour} {
			if _, err := cert.Verify(x509.VerifyOptions{
				Roots: roots, DNSName: host, CurrentTime: issuedAt.Add(offset),
			}); err != nil {
				t.Errorf("verify %s at issuance %+v: %v", host, offset, err)
			}
		}
	}
	for _, outside := range []time.Time{cert.NotBefore.Add(-time.Second), cert.NotAfter.Add(time.Second)} {
		if _, err := cert.Verify(x509.VerifyOptions{
			Roots: roots, DNSName: "localhost", CurrentTime: outside,
		}); err == nil {
			t.Errorf("verification unexpectedly succeeded outside validity window at %v", outside)
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
