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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// generateTestCert produces a self-signed certificate using a specified
// signature algorithm. Returns the parsed certificate (with Raw populated).
func generateTestCert(t *testing.T, sigAlgo x509.SignatureAlgorithm) *x509.Certificate {
	t.Helper()

	template := &x509.Certificate{
		SerialNumber:       big.NewInt(1),
		Subject:            pkix.Name{CommonName: "scram-cbind-test"},
		NotBefore:          time.Now().Add(-time.Hour),
		NotAfter:           time.Now().Add(time.Hour),
		SignatureAlgorithm: sigAlgo,
	}

	var (
		certDER []byte
		err     error
	)

	switch sigAlgo {
	case x509.SHA256WithRSA, x509.SHA384WithRSA, x509.SHA512WithRSA:
		key, kerr := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, kerr)
		certDER, err = x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	case x509.ECDSAWithSHA256:
		key, kerr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, kerr)
		certDER, err = x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	case x509.ECDSAWithSHA384:
		key, kerr := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		require.NoError(t, kerr)
		certDER, err = x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	case x509.ECDSAWithSHA512:
		key, kerr := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
		require.NoError(t, kerr)
		certDER, err = x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	default:
		t.Fatalf("unsupported sigAlgo for test: %s", sigAlgo)
	}
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(certDER)
	require.NoError(t, err)
	return cert
}

func TestComputeTLSServerEndPointHash(t *testing.T) {
	t.Run("nil cert is rejected", func(t *testing.T) {
		_, err := ComputeTLSServerEndPointHash(nil)
		require.Error(t, err)
	})

	t.Run("RSA-SHA256 hashes whole cert with SHA-256", func(t *testing.T) {
		cert := generateTestCert(t, x509.SHA256WithRSA)
		got, err := ComputeTLSServerEndPointHash(cert)
		require.NoError(t, err)
		want := sha256.Sum256(cert.Raw)
		assert.Equal(t, want[:], got)
	})

	t.Run("RSA-SHA384 → SHA-384 hash", func(t *testing.T) {
		cert := generateTestCert(t, x509.SHA384WithRSA)
		got, err := ComputeTLSServerEndPointHash(cert)
		require.NoError(t, err)
		want := sha512.Sum384(cert.Raw)
		assert.Equal(t, want[:], got)
	})

	t.Run("RSA-SHA512 → SHA-512 hash", func(t *testing.T) {
		cert := generateTestCert(t, x509.SHA512WithRSA)
		got, err := ComputeTLSServerEndPointHash(cert)
		require.NoError(t, err)
		want := sha512.Sum512(cert.Raw)
		assert.Equal(t, want[:], got)
	})

	t.Run("ECDSA variants use matching hash", func(t *testing.T) {
		cases := []struct {
			algo x509.SignatureAlgorithm
			size int
		}{
			{x509.ECDSAWithSHA256, sha256.Size},
			{x509.ECDSAWithSHA384, sha512.Size384},
			{x509.ECDSAWithSHA512, sha512.Size},
		}
		for _, tc := range cases {
			cert := generateTestCert(t, tc.algo)
			got, err := ComputeTLSServerEndPointHash(cert)
			require.NoError(t, err)
			assert.Len(t, got, tc.size, "algo=%s", tc.algo)
		}
	})

	t.Run("unsupported sig algo is rejected", func(t *testing.T) {
		cert := generateTestCert(t, x509.SHA256WithRSA)
		cert.SignatureAlgorithm = x509.UnknownSignatureAlgorithm
		_, err := ComputeTLSServerEndPointHash(cert)
		require.Error(t, err)
	})

	// MD5- and SHA-1-signed certs are obsolete (modern Go won't even sign
	// with them) but PG explicitly upgrades them to SHA-256 for cbind. We
	// mirror that quirk. Test by parsing a normal cert and then poking the
	// SignatureAlgorithm enum to MD5/SHA-1 — sufficient because our hasher
	// only consults the enum.
	t.Run("PG quirk: MD5/SHA-1 signed certs hash with SHA-256", func(t *testing.T) {
		cases := []x509.SignatureAlgorithm{
			x509.MD2WithRSA,
			x509.MD5WithRSA,
			x509.SHA1WithRSA,
			x509.DSAWithSHA1,
			x509.ECDSAWithSHA1,
		}
		for _, algo := range cases {
			cert := generateTestCert(t, x509.SHA256WithRSA)
			cert.SignatureAlgorithm = algo
			got, err := ComputeTLSServerEndPointHash(cert)
			require.NoError(t, err, "algo=%s", algo)
			want := sha256.Sum256(cert.Raw)
			require.Equal(t, want[:], got, "algo=%s should hash with SHA-256", algo)
		}
	})

	t.Run("Ed25519 is rejected (PG cannot derive digest)", func(t *testing.T) {
		// PG's be_tls_get_certificate_hash returns NULL for Ed25519, so PG
		// won't advertise PLUS for Ed25519 certs. Our code must reject too
		// to avoid client/server hash divergence.
		cert := generateTestCert(t, x509.SHA256WithRSA)
		cert.SignatureAlgorithm = x509.PureEd25519
		_, err := ComputeTLSServerEndPointHash(cert)
		require.Error(t, err)
	})
}
