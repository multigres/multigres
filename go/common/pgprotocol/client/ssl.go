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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
)

// SSLMode mirrors libpq's sslmode connection parameter.
// Reference: https://www.postgresql.org/docs/17/libpq-ssl.html
type SSLMode string

const (
	// SSLModeDisable never negotiates SSL.
	SSLModeDisable SSLMode = "disable"

	// SSLModeAllow first tries plaintext; falls back to SSL only if the server
	// rejects the plaintext attempt. libpq's least-preferred fallback path.
	SSLModeAllow SSLMode = "allow"

	// SSLModePrefer tries SSL first; on server refusal ('N'), continues plaintext.
	// libpq default.
	SSLModePrefer SSLMode = "prefer"

	// SSLModeRequire negotiates SSL but performs no certificate verification.
	// Encryption only.
	SSLModeRequire SSLMode = "require"

	// SSLModeVerifyCA verifies the server certificate chain against the root CA
	// but does not match the hostname.
	SSLModeVerifyCA SSLMode = "verify-ca"

	// SSLModeVerifyFull verifies the server certificate chain and matches the
	// hostname against SAN (with CN fallback) — full libpq verification.
	SSLModeVerifyFull SSLMode = "verify-full"
)

// ParseSSLMode parses the string form of an sslmode value.
// Empty input returns SSLModePrefer (libpq default).
func ParseSSLMode(s string) (SSLMode, error) {
	switch SSLMode(strings.ToLower(strings.TrimSpace(s))) {
	case "":
		return SSLModePrefer, nil
	case SSLModeDisable:
		return SSLModeDisable, nil
	case SSLModeAllow:
		return SSLModeAllow, nil
	case SSLModePrefer:
		return SSLModePrefer, nil
	case SSLModeRequire:
		return SSLModeRequire, nil
	case SSLModeVerifyCA:
		return SSLModeVerifyCA, nil
	case SSLModeVerifyFull:
		return SSLModeVerifyFull, nil
	default:
		return "", fmt.Errorf("invalid sslmode %q (want disable|allow|prefer|require|verify-ca|verify-full)", s)
	}
}

// AttemptsTLS reports whether the mode wants the SSLRequest negotiation step.
// allow is omitted: it only attempts TLS after a plaintext rejection, which is
// handled by the higher-level dial loop, not the SSLRequest path.
func (m SSLMode) AttemptsTLS() bool {
	switch m {
	case SSLModePrefer, SSLModeRequire, SSLModeVerifyCA, SSLModeVerifyFull:
		return true
	default:
		return false
	}
}

// RequiresTLS reports whether the mode must error if the server declines SSL.
// prefer/allow are tolerant; the rest are not.
func (m SSLMode) RequiresTLS() bool {
	switch m {
	case SSLModeRequire, SSLModeVerifyCA, SSLModeVerifyFull:
		return true
	default:
		return false
	}
}

// BuildTLSConfig constructs a *tls.Config matching libpq's sslmode semantics.
//
//   - disable/allow → returns (nil, nil); the dial path won't negotiate TLS for
//     these modes (allow's plaintext-then-TLS retry is the dial-loop's job).
//   - prefer/require → encryption only, no certificate verification.
//   - verify-ca → chain validated against rootCertPath; hostname not checked.
//   - verify-full → chain validated and ServerName=host (SAN match, with CN
//     fallback for libpq parity since Go's verifier removed CN matching in 1.17).
//
// rootCertPath is required for verify-ca / verify-full. host is the target
// server hostname, used as ServerName under verify-full.
func BuildTLSConfig(mode SSLMode, rootCertPath, host string) (*tls.Config, error) {
	switch mode {
	case SSLModeDisable, SSLModeAllow:
		return nil, nil
	case SSLModePrefer, SSLModeRequire:
		// libpq parity: require/prefer perform no cert verification — encryption only.
		return &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true, //nolint:gosec // libpq parity: require/prefer perform no cert verification
		}, nil
	case SSLModeVerifyCA, SSLModeVerifyFull:
		pool, err := loadCertPool(rootCertPath)
		if err != nil {
			return nil, err
		}
		cfg := &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    pool,
		}
		// verify-ca skips hostname match; verify-full adds libpq's CN fallback
		// that Go's default SAN-only verifier won't perform. Both rely on
		// VerifyConnection for the actual chain check, so InsecureSkipVerify
		// is set to bypass Go's stricter verifier.
		cfg.InsecureSkipVerify = true
		if mode == SSLModeVerifyCA {
			cfg.VerifyConnection = makeVerifyChain(pool)
		} else {
			cfg.ServerName = host
			cfg.VerifyConnection = makeVerifyFull(pool, host)
		}
		return cfg, nil
	default:
		return nil, fmt.Errorf("invalid sslmode %q", mode)
	}
}

func loadCertPool(path string) (*x509.CertPool, error) {
	if path == "" {
		return nil, errors.New("sslrootcert is required for verify-ca and verify-full")
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read sslrootcert %q: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("sslrootcert %q contains no PEM certificates", path)
	}
	return pool, nil
}

// makeVerifyChain returns a VerifyConnection function that validates the peer
// certificate chain against the supplied root pool but performs no hostname
// match — verify-ca semantics.
func makeVerifyChain(pool *x509.CertPool) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return errors.New("server presented no certificate")
		}
		opts := x509.VerifyOptions{
			Roots:         pool,
			Intermediates: x509.NewCertPool(),
		}
		for _, cert := range cs.PeerCertificates[1:] {
			opts.Intermediates.AddCert(cert)
		}
		_, err := cs.PeerCertificates[0].Verify(opts)
		return err
	}
}

// makeVerifyFull validates the chain and matches the hostname against SAN
// entries first, then falls back to the certificate's Common Name. The CN
// fallback is required for libpq parity; Go's stdlib verifier removed it in 1.17.
func makeVerifyFull(pool *x509.CertPool, host string) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return errors.New("server presented no certificate")
		}
		leaf := cs.PeerCertificates[0]
		opts := x509.VerifyOptions{
			Roots:         pool,
			Intermediates: x509.NewCertPool(),
		}
		for _, cert := range cs.PeerCertificates[1:] {
			opts.Intermediates.AddCert(cert)
		}
		if _, err := leaf.Verify(opts); err != nil {
			return err
		}
		// Try Go's SAN-based hostname match first.
		if err := leaf.VerifyHostname(host); err == nil {
			return nil
		}
		// libpq accepts a match against the certificate Common Name when no SANs
		// match. Only fall back to CN if the cert has no SANs (matches libpq's
		// behavior — RFC 6125 deprecates CN when SANs are present).
		if len(leaf.DNSNames) == 0 && leaf.IPAddresses == nil && leaf.URIs == nil && leaf.EmailAddresses == nil {
			if strings.EqualFold(leaf.Subject.CommonName, host) {
				return nil
			}
		}
		return fmt.Errorf("server certificate does not match hostname %q", host)
	}
}
