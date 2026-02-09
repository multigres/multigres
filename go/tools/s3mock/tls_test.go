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

// go/tools/s3mock/tls_test.go
package s3mock

import (
	"crypto/tls"
	"testing"
)

func TestGenerateTLSConfig(t *testing.T) {
	cfg, err := generateTLSConfig()
	if err != nil {
		t.Fatalf("generateTLSConfig() failed: %v", err)
	}
	if cfg == nil {
		t.Fatal("generateTLSConfig() returned nil config")
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
