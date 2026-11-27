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

package s3

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateS3BucketName(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		expectError bool
	}{
		{
			name:        "valid bucket name",
			bucket:      "my-backup-bucket",
			expectError: false,
		},
		{
			name:        "valid with dots",
			bucket:      "my.backup.bucket",
			expectError: false,
		},
		{
			name:        "valid with numbers",
			bucket:      "backup123",
			expectError: false,
		},
		{
			name:        "too short",
			bucket:      "ab",
			expectError: true,
		},
		{
			name:        "too long",
			bucket:      "this-is-a-very-long-bucket-name-that-exceeds-the-sixty-three-character-limit",
			expectError: true,
		},
		{
			name:        "uppercase letters",
			bucket:      "MyBucket",
			expectError: true,
		},
		{
			name:        "underscores",
			bucket:      "my_bucket",
			expectError: true,
		},
		{
			name:        "starts with hyphen",
			bucket:      "-mybucket",
			expectError: true,
		},
		{
			name:        "ends with hyphen",
			bucket:      "mybucket-",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBucketName(tt.bucket)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// fakeS3 is a TLS server that accepts every request, which is all
// ValidateAccess needs of a bucket: HEAD, PUT and DELETE answered with 200. Its
// certificate is self-signed, so only a client that was given it trusts it.
func fakeS3(t *testing.T) (endpoint, caFile string) {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	caFile = filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600))
	return srv.URL, caFile
}

// How ValidateAccess verifies the endpoint is how the sidecar will: with the
// CA file it was given, or not at all only when told so. An endpoint on this
// machine is no exception.
func TestValidateAccess_EndpointVerification(t *testing.T) {
	endpoint, caFile := fakeS3(t)
	// Every httptest server uses the same certificate, so another CA has to be
	// made here.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "another CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	otherCA := filepath.Join(t.TempDir(), "other-ca.pem")
	require.NoError(t, os.WriteFile(otherCA, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	notPEM := filepath.Join(t.TempDir(), "not.pem")
	require.NoError(t, os.WriteFile(notPEM, []byte("not a certificate"), 0o600))
	base := ValidationConfig{Bucket: "bucket", Region: "us-east-1", Endpoint: endpoint, AccessKey: "k", SecretKey: "s"}

	for name, tc := range map[string]struct {
		ca      string
		skip    bool
		wantErr string
	}{
		"the endpoint's own certificate as CA": {ca: caFile},
		"verification off":                     {skip: true},
		"no CA: a local endpoint is verified":  {wantErr: "certificate"},
		"another CA":                           {ca: otherCA, wantErr: "certificate"},
		"a CA file without a certificate":      {ca: notPEM, wantErr: "holds no PEM certificate"},
		"a missing CA file":                    {ca: filepath.Join(t.TempDir(), "absent.pem"), wantErr: "read CA file"},
		"a CA with verification off":           {ca: caFile, skip: true, wantErr: "cannot be combined"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base
			cfg.CAFile, cfg.SkipTLSVerify = tc.ca, tc.skip
			err := ValidateAccess(t.Context(), cfg)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// An explicit endpoint must be https. A plaintext one is refused before any
// request: the requests are signed and carry the session token, and neither a
// CA file nor skipping verification protects a connection with no TLS.
func TestValidateAccess_PlaintextEndpointGetsNoRequest(t *testing.T) {
	var requests atomic.Int64
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(plain.Close)

	for name, mutate := range map[string]func(*ValidationConfig){
		"verification on":  func(*ValidationConfig) {},
		"verification off": func(c *ValidationConfig) { c.SkipTLSVerify = true },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := ValidationConfig{
				Bucket: "bucket", Region: "us-east-1", Endpoint: plain.URL,
				AccessKey: "k", SecretKey: "s", SessionToken: "session-token",
			}
			mutate(&cfg)
			err := ValidateAccess(t.Context(), cfg)
			require.ErrorContains(t, err, "must be an https URL")
			assert.Zero(t, requests.Load(), "a plaintext endpoint was sent a request")
		})
	}

	for _, endpoint := range []string{"s3.internal.example:9000", "ftp://s3.internal.example", "https://"} {
		require.Error(t, ValidateEndpoint(endpoint), endpoint)
	}
	require.NoError(t, ValidateEndpoint(""))
	require.NoError(t, ValidateEndpoint("https://s3.internal.example:9000"))
}

// A redirect from the https endpoint to a plaintext address is not followed.
func TestValidateAccess_DoesNotFollowARedirectToPlaintext(t *testing.T) {
	var plainRequests atomic.Int64
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		plainRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(plain.Close)
	redirecting := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirecting.Close)
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: redirecting.Certificate().Raw}), 0o600))

	err := ValidateAccess(t.Context(), ValidationConfig{
		Bucket: "bucket", Region: "us-east-1", Endpoint: redirecting.URL, CAFile: caFile,
		AccessKey: "k", SecretKey: "s", SessionToken: "session-token",
	})
	require.Error(t, err)
	assert.Zero(t, plainRequests.Load(), "the signed request was repeated over plaintext")
}
