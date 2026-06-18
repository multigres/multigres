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

package scram

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"errors"
	"fmt"
	"hash"
)

// ChannelBindingTypeTLSServerEndPoint is the channel binding type defined in
// RFC 5929 §4: a hash of the TLS server's certificate. This is the type
// PostgreSQL advertises for SCRAM-SHA-256-PLUS.
const ChannelBindingTypeTLSServerEndPoint = "tls-server-end-point"

// ChannelBinding carries TLS channel binding context into the SCRAM
// authenticator. When non-nil and TLSServerEndPointHash is populated, the
// authenticator advertises SCRAM-SHA-256-PLUS in addition to SCRAM-SHA-256.
type ChannelBinding struct {
	// TLSServerEndPointHash is the hash of the server's TLS certificate, as
	// defined for the tls-server-end-point channel binding type (RFC 5929).
	// Compute it with ComputeTLSServerEndPointHash.
	TLSServerEndPointHash []byte
}

// ComputeTLSServerEndPointHash computes the tls-server-end-point channel
// binding data for a TLS server certificate per RFC 5929 §4: the hash of the
// DER-encoded certificate, using the certificate signature algorithm's hash
// function — with one PostgreSQL-compatible quirk: certificates signed with
// MD5 or SHA-1 are hashed with SHA-256 instead (mirroring PG's
// be_tls_get_certificate_hash). This is what libpq expects, so any client
// computing the binding the same way will interoperate.
func ComputeTLSServerEndPointHash(cert *x509.Certificate) ([]byte, error) {
	if cert == nil {
		return nil, errors.New("scram: TLS certificate is nil")
	}
	h, err := tlsServerEndPointHasher(cert.SignatureAlgorithm)
	if err != nil {
		return nil, err
	}
	h.Write(cert.Raw)
	return h.Sum(nil), nil
}

// tlsServerEndPointHasher returns the hash function to use for a given cert
// signature algorithm. MD5- and SHA-1-signed certs map to SHA-256 to match
// PostgreSQL (be_tls_get_certificate_hash). Ed25519 is intentionally NOT
// listed: PG's OpenSSL-based path cannot derive a digest for it and refuses
// to advertise PLUS, so accepting Ed25519 here would diverge from libpq's
// expected hash and break the cbind check.
func tlsServerEndPointHasher(sigAlgo x509.SignatureAlgorithm) (hash.Hash, error) {
	switch sigAlgo {
	case x509.MD2WithRSA,
		x509.MD5WithRSA,
		x509.SHA1WithRSA,
		x509.DSAWithSHA1,
		x509.ECDSAWithSHA1,
		x509.SHA256WithRSA,
		x509.DSAWithSHA256,
		x509.ECDSAWithSHA256,
		x509.SHA256WithRSAPSS:
		return sha256.New(), nil
	case x509.SHA384WithRSA,
		x509.ECDSAWithSHA384,
		x509.SHA384WithRSAPSS:
		return sha512.New384(), nil
	case x509.SHA512WithRSA,
		x509.ECDSAWithSHA512,
		x509.SHA512WithRSAPSS:
		return sha512.New(), nil
	default:
		return nil, fmt.Errorf("scram: unsupported certificate signature algorithm %s for tls-server-end-point binding", sigAlgo)
	}
}
