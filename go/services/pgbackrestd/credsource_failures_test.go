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

package pgbackrestd

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeToken(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

// A credential response that cannot be trusted is an error, never a partial
// credential handed to pgBackRest.
func TestDecodeCredResponse_RefusesUntrustworthyResponses(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr string
	}{
		{"a non-200 status", http.StatusForbidden, `{"AccessKeyId":"A","SecretAccessKey":"S-token-material"}`, "answered 403"},
		{"a body that is not JSON", http.StatusOK, "<html>", "decode credential response"},
		{"no access key", http.StatusOK, `{"SecretAccessKey":"s","Expiration":"2030-01-01T00:00:00Z"}`, "lacks AccessKeyId"},
		{"no secret", http.StatusOK, `{"AccessKeyId":"a","Expiration":"2030-01-01T00:00:00Z"}`, "lacks AccessKeyId"},
		{"an unparseable expiration", http.StatusOK, `{"AccessKeyId":"a","SecretAccessKey":"s","Expiration":"tomorrow"}`, "Expiration"},
		{"a body over the cap", http.StatusOK, strings.Repeat("x", maxCredResponse+1), "exceeds"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeCredResponse(&http.Response{
				StatusCode: tc.status,
				Status:     fmt.Sprintf("%d %s", tc.status, http.StatusText(tc.status)),
				Body:       io.NopCloser(strings.NewReader(tc.body)),
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.NotContains(t, err.Error(), "token-material", "a refused body is never echoed")
		})
	}
}

func TestPodIdentitySource_FailsWithoutReachingTheAgent(t *testing.T) {
	t.Run("an unreadable token file", func(t *testing.T) {
		src, err := newPodIdentitySource("http://127.0.0.1:1/creds", filepath.Join(t.TempDir(), "absent"))
		require.NoError(t, err)
		_, err = src.Get(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pod identity token")
	})

	t.Run("an agent that is not listening", func(t *testing.T) {
		ts := httptest.NewServer(http.NotFoundHandler())
		url := ts.URL
		ts.Close()
		src, err := newPodIdentitySource(url, writeToken(t, "tok"))
		require.NoError(t, err)
		_, err = src.Get(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pod identity request")
	})

	t.Run("an agent that refuses the token", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) }))
		t.Cleanup(ts.Close)
		src, err := newPodIdentitySource(ts.URL, writeToken(t, "tok"))
		require.NoError(t, err)
		_, err = src.Get(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "403")
	})

	t.Run("a cancelled context", func(t *testing.T) {
		src, err := newPodIdentitySource("http://127.0.0.1:1/creds", writeToken(t, "tok"))
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = src.Get(ctx)
		require.Error(t, err)
	})
}

func TestEndpointSource_FailsWhenTheEndpointDoes(t *testing.T) {
	t.Run("an endpoint that is not listening", func(t *testing.T) {
		ts := httptest.NewServer(http.NotFoundHandler())
		url := ts.URL
		ts.Close()
		src, err := newEndpointSource(url, "")
		require.NoError(t, err)
		_, err = src.Get(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "credential endpoint request")
	})

	t.Run("an endpoint that answers an error", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }))
		t.Cleanup(ts.Close)
		src, err := newEndpointSource(ts.URL, "")
		require.NoError(t, err)
		_, err = src.Get(context.Background())
		require.Error(t, err)
	})

	t.Run("an unreadable CA file", func(t *testing.T) {
		_, err := newEndpointSource("https://127.0.0.1:1", filepath.Join(t.TempDir(), "absent.pem"))
		require.Error(t, err)
	})
}

func TestWebIdentitySource_FailsClosed(t *testing.T) {
	arn := "arn:aws:iam::1:role/r"
	client := func(url string) *sts.Client {
		return sts.New(sts.Options{Region: "us-east-1", BaseEndpoint: aws.String(url), Credentials: aws.AnonymousCredentials{}, RetryMaxAttempts: 1})
	}

	t.Run("an unreadable token file", func(t *testing.T) {
		src, err := newWebIdentitySource("us-east-1", filepath.Join(t.TempDir(), "absent"), arn, nil)
		require.NoError(t, err)
		_, err = src.Get(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "web identity token")
	})

	t.Run("STS rejects the token", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/xml")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Type>Sender</Type><Code>InvalidIdentityToken</Code><Message>no</Message></Error></ErrorResponse>`)
		}))
		t.Cleanup(ts.Close)
		src, err := newWebIdentitySource("us-east-1", writeToken(t, "jwt"), arn, client(ts.URL))
		require.NoError(t, err)
		_, err = src.Get(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "assume role with web identity")
	})

	t.Run("STS answers without a session token", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/xml")
			_, _ = io.WriteString(w, `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>A</AccessKeyId><SecretAccessKey>s</SecretAccessKey><Expiration>2030-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`)
		}))
		t.Cleanup(ts.Close)
		src, err := newWebIdentitySource("us-east-1", writeToken(t, "jwt"), arn, client(ts.URL))
		require.NoError(t, err)
		_, err = src.Get(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "incomplete")
	})
}

func TestValidateCredURL_RefusesAnUnparseableURL(t *testing.T) {
	err := validateCredURL("http://[::1", "--s3-cred-endpoint", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a URL")
}
