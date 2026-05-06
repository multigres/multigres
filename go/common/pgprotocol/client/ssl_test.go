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

package client

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
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
)

func TestParseSSLMode(t *testing.T) {
	cases := []struct {
		in      string
		want    SSLMode
		wantErr bool
	}{
		{"", SSLModePrefer, false},
		{"disable", SSLModeDisable, false},
		{"DISABLE", SSLModeDisable, false},
		{"  prefer  ", SSLModePrefer, false},
		{"allow", "", true}, // not implemented; rejected to avoid silent disable-equivalent behavior
		{"require", SSLModeRequire, false},
		{"verify-ca", SSLModeVerifyCA, false},
		{"verify-full", SSLModeVerifyFull, false},
		{"yes", "", true},
		{"true", "", true},
	}
	for _, tc := range cases {
		got, err := ParseSSLMode(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseSSLMode(%q): want error, got %q", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseSSLMode(%q): unexpected error %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseSSLMode(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSSLModeFlags(t *testing.T) {
	tlsAttempts := map[SSLMode]bool{
		SSLModeDisable:    false,
		SSLModePrefer:     true,
		SSLModeRequire:    true,
		SSLModeVerifyCA:   true,
		SSLModeVerifyFull: true,
	}
	tlsRequired := map[SSLMode]bool{
		SSLModeDisable:    false,
		SSLModePrefer:     false,
		SSLModeRequire:    true,
		SSLModeVerifyCA:   true,
		SSLModeVerifyFull: true,
	}
	for mode, want := range tlsAttempts {
		if got := mode.AttemptsTLS(); got != want {
			t.Errorf("%s.AttemptsTLS() = %v, want %v", mode, got, want)
		}
	}
	for mode, want := range tlsRequired {
		if got := mode.RequiresTLS(); got != want {
			t.Errorf("%s.RequiresTLS() = %v, want %v", mode, got, want)
		}
	}
}

func TestBuildTLSConfig_Disable(t *testing.T) {
	got, err := BuildTLSConfig(SSLModeDisable, "", "")
	if err != nil {
		t.Fatalf("BuildTLSConfig(disable): unexpected error %v", err)
	}
	if got != nil {
		t.Errorf("BuildTLSConfig(disable) = %+v, want nil", got)
	}
}

func TestBuildTLSConfig_AllowRejected(t *testing.T) {
	if _, err := BuildTLSConfig(SSLModeAllow, "", ""); err == nil {
		t.Error("BuildTLSConfig(allow): want error (mode not supported), got nil")
	}
}

func TestBuildTLSConfig_VerifyFullRequiresHost(t *testing.T) {
	caPath, _, _ := writeTestCA(t, "host.example")
	if _, err := BuildTLSConfig(SSLModeVerifyFull, caPath, ""); err == nil {
		t.Error("BuildTLSConfig(verify-full, host=\"\"): want error, got nil")
	}
}

func TestBuildTLSConfig_RequireAndPrefer(t *testing.T) {
	for _, mode := range []SSLMode{SSLModePrefer, SSLModeRequire} {
		cfg, err := BuildTLSConfig(mode, "", "ignored")
		if err != nil {
			t.Fatalf("BuildTLSConfig(%s): unexpected error %v", mode, err)
		}
		if cfg == nil {
			t.Fatalf("BuildTLSConfig(%s) = nil, want non-nil", mode)
		}
		if !cfg.InsecureSkipVerify {
			t.Errorf("BuildTLSConfig(%s).InsecureSkipVerify = false, want true (libpq parity)", mode)
		}
		if cfg.RootCAs != nil {
			t.Errorf("BuildTLSConfig(%s).RootCAs = %v, want nil", mode, cfg.RootCAs)
		}
		if cfg.MinVersion < tls.VersionTLS12 {
			t.Errorf("BuildTLSConfig(%s).MinVersion = %x, want >= TLS 1.2", mode, cfg.MinVersion)
		}
	}
}

func TestBuildTLSConfig_VerifyRequiresRootCert(t *testing.T) {
	for _, mode := range []SSLMode{SSLModeVerifyCA, SSLModeVerifyFull} {
		_, err := BuildTLSConfig(mode, "", "host.example")
		if err == nil {
			t.Errorf("BuildTLSConfig(%s, %q): want error, got nil", mode, "")
		}
	}
}

func TestBuildTLSConfig_VerifyMissingFile(t *testing.T) {
	_, err := BuildTLSConfig(SSLModeVerifyFull, "/no/such/path.pem", "host.example")
	if err == nil {
		t.Fatal("want error for missing rootcert path")
	}
}

func TestBuildTLSConfig_VerifyEmptyPEM(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.pem")
	if err := os.WriteFile(path, []byte("not a pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := BuildTLSConfig(SSLModeVerifyFull, path, "host.example")
	if err == nil {
		t.Fatal("want error for non-PEM rootcert")
	}
}

func TestBuildTLSConfig_VerifyFullSetsServerName(t *testing.T) {
	caPath, _, _ := writeTestCA(t, "host.example")
	cfg, err := BuildTLSConfig(SSLModeVerifyFull, caPath, "host.example")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "host.example" {
		t.Errorf("ServerName = %q, want host.example", cfg.ServerName)
	}
	if cfg.RootCAs == nil {
		t.Error("RootCAs nil, want pool")
	}
}

func TestBuildTLSConfig_VerifyCASkipsHostname(t *testing.T) {
	// verify-ca should validate chain but ignore hostname mismatch.
	caPath, certPEM, keyPEM := writeTestCA(t, "other.example")
	cfg, err := BuildTLSConfig(SSLModeVerifyCA, caPath, "host.example")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "" {
		t.Errorf("ServerName = %q, want empty", cfg.ServerName)
	}
	if !cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify = false, want true (custom verifier handles chain)")
	}
	// Run verifier directly: chain valid, hostname mismatched, must accept.
	state := tlsStateFromPEM(t, certPEM, keyPEM)
	if err := cfg.VerifyConnection(state); err != nil {
		t.Errorf("verify-ca rejected valid chain on hostname mismatch: %v", err)
	}
}

func TestBuildTLSConfig_VerifyFullRejectsBadHost(t *testing.T) {
	caPath, certPEM, keyPEM := writeTestCA(t, "other.example")
	cfg, err := BuildTLSConfig(SSLModeVerifyFull, caPath, "host.example")
	if err != nil {
		t.Fatal(err)
	}
	state := tlsStateFromPEM(t, certPEM, keyPEM)
	if err := cfg.VerifyConnection(state); err == nil {
		t.Error("verify-full accepted hostname mismatch, want error")
	}
}

func TestBuildTLSConfig_VerifyFullCNFallback(t *testing.T) {
	// Cert with no SANs and CN=host.example must be accepted.
	caPath, certPEM, keyPEM := writeTestCertCNOnly(t, "host.example")
	cfg, err := BuildTLSConfig(SSLModeVerifyFull, caPath, "host.example")
	if err != nil {
		t.Fatal(err)
	}
	state := tlsStateFromPEM(t, certPEM, keyPEM)
	if err := cfg.VerifyConnection(state); err != nil {
		t.Errorf("verify-full rejected CN-only cert with matching CN: %v", err)
	}
}

func TestBuildTLSConfig_VerifyFullCNNotFallbackWhenSANsExist(t *testing.T) {
	// Cert has SANs (other.example) and CN=host.example. host.example match
	// against CN must NOT succeed because SANs are present (RFC 6125 / libpq).
	caPath, certPEM, keyPEM := writeTestCertWithSANAndCN(t, "host.example", []string{"other.example"})
	cfg, err := BuildTLSConfig(SSLModeVerifyFull, caPath, "host.example")
	if err != nil {
		t.Fatal(err)
	}
	state := tlsStateFromPEM(t, certPEM, keyPEM)
	if err := cfg.VerifyConnection(state); err == nil {
		t.Error("verify-full fell back to CN even though SANs were present")
	}
}

// --- test helpers ---

func writeTestCA(t *testing.T, sanHost string) (caPath string, certPEM, keyPEM []byte) {
	t.Helper()
	return writeTestCertWithSANAndCN(t, sanHost, []string{sanHost})
}

func writeTestCertWithSANAndCN(t *testing.T, cn string, sans []string) (caPath string, certPEM, keyPEM []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	for _, s := range sans {
		if ip := net.ParseIP(s); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, s)
		}
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	caPath = filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return caPath, certPEM, keyPEM
}

func writeTestCertCNOnly(t *testing.T, cn string) (caPath string, certPEM, keyPEM []byte) {
	t.Helper()
	return writeTestCertWithSANAndCN(t, cn, nil)
}

func tlsStateFromPEM(t *testing.T, certPEM, keyPEM []byte) tls.ConnectionState {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("failed to decode cert PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	_ = keyPEM // unused in verification path; key is only needed for live TLS
	return tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
}
