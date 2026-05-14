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
	"crypto/tls"
	"crypto/x509"
)

// captureTLSServerCert records the leaf certificate that crypto/tls
// selected for this connection's handshake. Invoked by the GetCertificate
// / GetConfigForClient wrappers installed by wrapTLSConfigForCertCapture.
//
// Prefers the pre-parsed Leaf to avoid re-parsing on hot paths; falls back
// to parsing Certificate[0] for callers that don't populate Leaf. A parse
// failure is logged at warn and leaves tlsServerCert nil — auth then falls
// back to SCRAM-SHA-256 without channel binding rather than failing the
// handshake.
//
// Called synchronously from the handshake goroutine before tls.Handshake
// returns; the subsequent reader (handleSSLRequest / SCRAM advertisement)
// runs on the same goroutine, so no synchronization is needed.
func (c *Conn) captureTLSServerCert(tlsCert *tls.Certificate) {
	if tlsCert == nil {
		return
	}
	if tlsCert.Leaf != nil {
		c.tlsServerCert = tlsCert.Leaf
		return
	}
	if len(tlsCert.Certificate) == 0 {
		return
	}
	parsed, err := x509.ParseCertificate(tlsCert.Certificate[0])
	if err != nil {
		c.logger.Warn("failed to parse selected TLS leaf cert for channel binding", "err", err)
		return
	}
	c.tlsServerCert = parsed
}

// wrapTLSConfigForCertCapture returns a clone of base whose certificate
// selection paths (GetCertificate and GetConfigForClient) are instrumented
// to invoke capture with the *tls.Certificate that crypto/tls actually
// presented to the peer. The clone may safely be passed to tls.Server.
//
// Why: crypto/tls does not expose the server-presented leaf via
// (*tls.Conn).ConnectionState(), so a deployment using dynamic cert
// selection (typical for SNI multi-tenant TLS) cannot recover the cert
// post-handshake. SCRAM-SHA-256-PLUS channel binding needs that cert
// (RFC 5929 tls-server-end-point). Capturing during selection is the
// only stable way to retrieve it.
//
// The static Certificates[0] path is intentionally NOT routed through
// capture here — the caller continues to handle it after the handshake
// to keep behavior unchanged for the common single-cert deployment.
//
// capture is invoked synchronously on the handshake goroutine; the caller
// must serialize subsequent reads (in this package, capture writes to the
// owning *Conn which is only read after Handshake returns on the same
// goroutine).
func wrapTLSConfigForCertCapture(base *tls.Config, capture func(*tls.Certificate)) *tls.Config {
	if base == nil || capture == nil {
		return base
	}
	if base.GetCertificate == nil && base.GetConfigForClient == nil {
		// Pure static deployment — caller's post-handshake fallback
		// already recovers Certificates[0]. Avoid an unnecessary clone.
		return base
	}

	cfg := base.Clone()

	if cfg.GetCertificate != nil {
		orig := cfg.GetCertificate
		cfg.GetCertificate = func(chi *tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert, err := orig(chi)
			if err == nil && cert != nil {
				capture(cert)
			}
			return cert, err
		}
	}

	if cfg.GetConfigForClient != nil {
		orig := cfg.GetConfigForClient
		cfg.GetConfigForClient = func(chi *tls.ClientHelloInfo) (*tls.Config, error) {
			inner, err := orig(chi)
			if err != nil {
				return nil, err
			}
			if inner == nil {
				// crypto/tls falls back to the outer config — already wrapped above.
				return nil, nil
			}
			return wrapInnerCfgForCertCapture(inner, capture), nil
		}
	}

	return cfg
}

// wrapInnerCfgForCertCapture handles the per-handshake *tls.Config returned
// by a user-supplied GetConfigForClient. crypto/tls uses this returned
// config for cert selection, so the outer wrapping in
// wrapTLSConfigForCertCapture does not see the eventual cert — we must
// wrap the inner config as well.
func wrapInnerCfgForCertCapture(inner *tls.Config, capture func(*tls.Certificate)) *tls.Config {
	out := inner.Clone()

	if out.GetCertificate != nil {
		orig := out.GetCertificate
		out.GetCertificate = func(chi *tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert, err := orig(chi)
			if err == nil && cert != nil {
				capture(cert)
			}
			return cert, err
		}
		return out
	}

	// Inner config has no dynamic getter — crypto/tls will select from
	// inner.Certificates. Synthesize a GetCertificate that captures the
	// chosen cert. We replicate crypto/tls's selection rules at a minimum:
	// honor NameToCertificate if populated, otherwise the first cert that
	// SupportsCertificate matches, otherwise Certificates[0].
	if len(out.Certificates) > 0 {
		out.GetCertificate = func(chi *tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert := selectCertificate(out, chi)
			if cert != nil {
				capture(cert)
			}
			return cert, nil
		}
	}

	return out
}

// selectCertificate mirrors the subset of crypto/tls's internal server cert
// selection we need for inner configs whose user didn't supply a getter.
// Iterates Certificates picking the first SupportsCertificate match; falls
// back to Certificates[0] when no match is found, matching crypto/tls
// behavior. NameToCertificate is intentionally skipped (deprecated in
// crypto/tls; SupportsCertificate covers the same SNI matching).
func selectCertificate(cfg *tls.Config, chi *tls.ClientHelloInfo) *tls.Certificate {
	if len(cfg.Certificates) == 0 {
		return nil
	}
	for i := range cfg.Certificates {
		cert := &cfg.Certificates[i]
		if err := chi.SupportsCertificate(cert); err == nil {
			return cert
		}
	}
	return &cfg.Certificates[0]
}

// tlsConfigYieldsServerCert reports whether a TLS config will provide a
// server certificate at handshake time through any supported path:
// static Certificates, GetCertificate, or GetConfigForClient. Listener
// init logs a warning when this returns false on a non-nil config so the
// operator notices the silent SCRAM-SHA-256-PLUS-off state.
func tlsConfigYieldsServerCert(cfg *tls.Config) bool {
	if cfg == nil {
		return false
	}
	return len(cfg.Certificates) > 0 || cfg.GetCertificate != nil || cfg.GetConfigForClient != nil
}
