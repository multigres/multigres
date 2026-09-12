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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// HTTPTLSEnabled reports whether this ServEnv's HTTP listener serves TLS, so
// callers building a URL back to it pick the matching scheme.
func (sv *ServEnv) HTTPTLSEnabled() bool {
	return sv.tlsCert.Get() != "" && sv.tlsKey.Get() != ""
}

// HTTPSelfClientTLSConfig returns the TLS config for dialing this process's own
// HTTP listener, or nil when that listener is plaintext.
//
// The trust root is this listener's own certificate plus --http-ca, which
// covers both a self-signed server certificate and one issued by the internal
// CA. It deliberately does not fall back to the system pool: the only intended
// peer is this very process.
//
// When client-certificate enforcement is on, the same --http-cert/--http-key is
// presented as the client certificate, so its subject has to appear in
// --http-auth-mtls-allowed-subjects for the hop to be authorized.
func (sv *ServEnv) HTTPSelfClientTLSConfig() (*tls.Config, error) {
	if !sv.HTTPTLSEnabled() {
		return nil, nil
	}
	roots := x509.NewCertPool()
	for _, path := range []string{sv.tlsCert.Get(), sv.tlsCA.Get()} {
		if path == "" {
			continue
		}
		pem, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", path, err)
		}
		roots.AppendCertsFromPEM(pem)
	}
	cfg := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	if sv.httpClientCertRequired {
		pair, err := tls.LoadX509KeyPair(sv.tlsCert.Get(), sv.tlsKey.Get())
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}
