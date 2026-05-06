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
	"errors"
	"fmt"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// CertAuthMode controls client certificate authentication behavior.
// See docs/query_serving/client_cert_auth_design.md.
type CertAuthMode string

const (
	// CertAuthModeNone disables client certificate authentication. Client
	// certificates are not requested, and SCRAM (or trust in tests) is used.
	CertAuthModeNone CertAuthMode = "none"

	// CertAuthModeVerifyFull requires a valid client certificate whose CN or
	// DN matches the requested DB user, bypassing password authentication.
	// Equivalent to PostgreSQL's `cert` auth method.
	CertAuthModeVerifyFull CertAuthMode = "verify-full"

	// CertAuthModeVerifyCA is reserved — client certificate required and
	// verified against the CA, but identity is not bound and SCRAM still runs.
	// Not implemented in v1.
	CertAuthModeVerifyCA CertAuthMode = "verify-ca"

	// CertAuthModeOptional is reserved — client certificate used for auth if
	// presented, SCRAM otherwise. Not implemented in v1.
	CertAuthModeOptional CertAuthMode = "optional"
)

// ParseCertAuthMode parses a CertAuthMode string from a flag value.
// Reserved modes (verify-ca, optional) are rejected with a "not implemented"
// error so the flag contract is stable for future releases.
func ParseCertAuthMode(s string) (CertAuthMode, error) {
	switch CertAuthMode(s) {
	case CertAuthModeNone, CertAuthModeVerifyFull:
		return CertAuthMode(s), nil
	case CertAuthModeVerifyCA, CertAuthModeOptional:
		return "", fmt.Errorf("client auth mode %q is reserved but not yet implemented", s)
	default:
		return "", fmt.Errorf("invalid client auth mode %q (valid: none, verify-full)", s)
	}
}

// CertIdentitySource selects which field of the peer certificate is compared
// to the requested DB user during verify-full cert authentication.
type CertIdentitySource string

const (
	// CertIdentityCN compares the certificate's Subject Common Name.
	// Matches PostgreSQL's default behavior (clientcertname=CN).
	CertIdentityCN CertIdentitySource = "cn"

	// CertIdentityDN compares the full RFC 2253 Subject DN.
	// Matches PostgreSQL's clientcertname=DN.
	CertIdentityDN CertIdentitySource = "dn"
)

// ParseCertIdentitySource parses a CertIdentitySource string from a flag value.
func ParseCertIdentitySource(s string) (CertIdentitySource, error) {
	switch CertIdentitySource(s) {
	case CertIdentityCN, CertIdentityDN:
		return CertIdentitySource(s), nil
	default:
		return "", fmt.Errorf("invalid cert identity source %q (valid: cn, dn)", s)
	}
}

// authenticateCertificate performs PostgreSQL-compatible `cert` authentication
// (trust + clientcert=verify-full). Must be called only when:
//   - the connection is TLS (c.conn is *tls.Conn), and
//   - the TLS layer was configured with RequireAndVerifyClientCert, so a
//     verified peer certificate is guaranteed to be present.
//
// Compares the configured identity field (CN or DN) of PeerCertificates[0]
// against the requested DB user, case-sensitive. On mismatch, sends a FATAL
// ErrorResponse with SQLSTATE 28000 and a DETAIL line identifying the matched
// field, then returns the error (caller closes the connection). On match,
// records the full peer DN as the authenticated identity via a log line and
// proceeds with the post-auth message sequence.
func (c *Conn) authenticateCertificate() error {
	state, ok := c.TLSConnectionState()
	if !ok {
		// Shouldn't happen: verify-full requires TLS, and the caller gated on
		// the connection being TLS. Treat defensively as an internal error.
		return errors.New("cert auth invoked on non-TLS connection")
	}
	if len(state.PeerCertificates) == 0 {
		// Shouldn't happen: ClientAuth=RequireAndVerifyClientCert makes the
		// handshake fail when no cert is presented. Treat defensively.
		return errors.New("cert auth invoked without peer certificate")
	}
	cert := state.PeerCertificates[0]

	var presented, fieldName string
	switch c.certIdentitySource {
	case CertIdentityDN:
		// Go's pkix.Name.String() produces RFC 2253 output (reversed RDN
		// order, short attribute names, standard escaping) — equivalent to
		// PostgreSQL's X509_NAME_print_ex(..., XN_FLAG_RFC2253) for typical
		// certificate subjects.
		presented = cert.Subject.String()
		fieldName = "DN"
	default: // CertIdentityCN (also the zero-value fallback)
		presented = cert.Subject.CommonName
		fieldName = "CN"
	}

	if presented != c.user {
		c.logger.Warn("cert auth failed: identity mismatch",
			"user", c.user,
			"field", fieldName,
			"presented", presented,
			"peer_dn", cert.Subject.String(),
		)
		return c.sendCertAuthError(fieldName, presented)
	}

	// Log the full DN as the authenticated identity, matching PostgreSQL's
	// set_authn_id(peer_dn) behavior even when CN was used for authorization.
	c.logger.Info("authentication complete",
		"user", c.user,
		"method", "cert",
		"field", fieldName,
		"peer_dn", cert.Subject.String(),
		"tls_version", tlsVersionName(state.Version),
		"cipher_suite", tls.CipherSuiteName(state.CipherSuite),
	)

	if err := c.sendAuthenticationOk(); err != nil {
		return fmt.Errorf("failed to send AuthenticationOk: %w", err)
	}
	if err := c.sendBackendKeyData(); err != nil {
		return fmt.Errorf("failed to send BackendKeyData: %w", err)
	}
	c.listener.RegisterConn(c)
	if err := c.sendParameterStatuses(); err != nil {
		return fmt.Errorf("failed to send ParameterStatus messages: %w", err)
	}
	if err := c.sendReadyForQuery(); err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}
	return nil
}

// sendCertAuthError emits a PostgreSQL-format FATAL ErrorResponse for a cert
// identity mismatch. Message matches PG's wording; DETAIL names the matched
// field and the presented value for debuggability.
func (c *Conn) sendCertAuthError(fieldName, presented string) error {
	message := fmt.Sprintf("certificate authentication failed for user %q", c.user)
	detail := fmt.Sprintf("Client certificate %s %q does not match requested user %q",
		fieldName, presented, c.user)
	diag := mterrors.NewPgError("FATAL", mterrors.PgSSInvalidAuthSpec, message, detail)
	if err := c.writePgDiagnosticResponse(protocol.MsgErrorResponse, diag); err != nil {
		return err
	}
	return c.flush()
}

// tlsVersionName returns a human-readable TLS version string for logging.
func tlsVersionName(v uint16) string {
	switch v {
	case 0x0301:
		return "TLS1.0"
	case 0x0302:
		return "TLS1.1"
	case 0x0303:
		return "TLS1.2"
	case 0x0304:
		return "TLS1.3"
	default:
		return fmt.Sprintf("0x%04x", v)
	}
}
