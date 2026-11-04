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

// go/test/s3mock/auth_test.go
package s3mock

import (
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func sessionTestServer(t *testing.T) (*Server, *http.Client) {
	t.Helper()
	srv, err := NewServer(0)
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	if err := srv.CreateBucket("b"); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // #nosec G402 - test code using self-signed certificates
	}}
	return srv, client
}

func headBucket(t *testing.T, srv *Server, c *http.Client, key, token string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.Endpoint()+"/b/k", nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("X-Amz-Security-Token", token)
	}
	if key != "" {
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+key+"/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00")
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var e S3Error
	_ = xml.NewDecoder(resp.Body).Decode(&e)
	return resp.StatusCode, e.Code
}

func TestSessions_RejectExpired(t *testing.T) {
	srv, c := sessionTestServer(t)
	sess := srv.IssueSession(-time.Minute)
	status, code := headBucket(t, srv, c, sess.AccessKeyID, sess.SessionToken)
	if status != http.StatusForbidden || code != "ExpiredToken" {
		t.Fatalf("got %d %q, want 403 ExpiredToken", status, code)
	}
}

func TestSessions_RejectUnknown(t *testing.T) {
	srv, c := sessionTestServer(t)
	srv.IssueSession(time.Hour)
	for _, tok := range []string{"", "bogus"} {
		status, code := headBucket(t, srv, c, "ASIAwhatever", tok)
		if status != http.StatusForbidden || code != "InvalidToken" {
			t.Fatalf("token %q: got %d %q, want 403 InvalidToken", tok, status, code)
		}
	}
}

func TestSessions_AcceptValid(t *testing.T) {
	srv, c := sessionTestServer(t)
	a := srv.IssueSession(time.Hour)
	b := srv.IssueSession(time.Hour)
	if a.SessionToken == b.SessionToken || a.AccessKeyID == b.AccessKeyID || a.SecretAccessKey == b.SecretAccessKey {
		t.Fatal("IssueSession must mint new random values per call")
	}
	// 404 (no such key) proves the request got past auth.
	for _, s := range []Session{a, b} {
		if status, _ := headBucket(t, srv, c, s.AccessKeyID, s.SessionToken); status != http.StatusNotFound {
			t.Fatalf("got %d, want 404 past auth", status)
		}
	}
}

func TestSessions_TokenBoundToKey(t *testing.T) {
	srv, c := sessionTestServer(t)
	a := srv.IssueSession(time.Hour)
	b := srv.IssueSession(time.Hour)
	if status, _ := headBucket(t, srv, c, a.AccessKeyID, a.SessionToken); status != http.StatusNotFound {
		t.Fatalf("matching pair: got %d, want 404 past auth", status)
	}
	for name, key := range map[string]string{"other session's key": b.AccessKeyID, "unknown key": "ASIAnope", "no key": ""} {
		status, code := headBucket(t, srv, c, key, a.SessionToken)
		if status != http.StatusForbidden || code != "InvalidToken" {
			t.Fatalf("%s: got %d %q, want 403 InvalidToken", name, status, code)
		}
	}
}

func TestSessions_PresignedCredentialBinding(t *testing.T) {
	srv, c := sessionTestServer(t)
	a := srv.IssueSession(time.Hour)
	b := srv.IssueSession(time.Hour)
	// A real presigned URL carries both the credential and the token in the
	// query, never in headers.
	presigned := func(key, token string) int {
		q := url.Values{}
		q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
		q.Set("X-Amz-Credential", key+"/20260101/us-east-1/s3/aws4_request")
		q.Set("X-Amz-Security-Token", token)
		q.Set("X-Amz-Signature", "00")
		resp, err := c.Get(srv.Endpoint() + "/b/k?" + q.Encode())
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if got := presigned(a.AccessKeyID, a.SessionToken); got != http.StatusNotFound {
		t.Fatalf("matching presigned request: got %d, want 404", got)
	}
	if got := presigned(b.AccessKeyID, a.SessionToken); got != http.StatusForbidden {
		t.Fatalf("presigned token with another session's key: got %d, want 403", got)
	}
	if got := presigned("ASIAnope", a.SessionToken); got != http.StatusForbidden {
		t.Fatalf("presigned token with unknown key: got %d, want 403", got)
	}
	if got := presigned(a.AccessKeyID, "bogus"); got != http.StatusForbidden {
		t.Fatalf("presigned unknown token: got %d, want 403", got)
	}
}

func TestSessions_OffByDefault(t *testing.T) {
	srv, c := sessionTestServer(t)
	if status, _ := headBucket(t, srv, c, "", ""); status != http.StatusNotFound {
		t.Fatalf("got %d, want 404 with no token", status)
	}
}

func TestSessions_RequireSessionsBeforeIssue(t *testing.T) {
	srv, c := sessionTestServer(t)
	srv.RequireSessions()
	if status, code := headBucket(t, srv, c, "", ""); status != http.StatusForbidden || code != "InvalidToken" {
		t.Fatalf("got %d %q, want 403 InvalidToken", status, code)
	}
}

func TestSessions_AdminEndpoint(t *testing.T) {
	srv, c := sessionTestServer(t)
	srv.RequireSessions()

	resp, err := c.Post(srv.Endpoint()+"/_session?ttl=1h", "", strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200 without a token", resp.StatusCode)
	}
	var got struct {
		Version                                                int
		AccessKeyId, SecretAccessKey, SessionToken, Expiration string
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 || got.AccessKeyId == "" || got.SecretAccessKey == "" || got.SessionToken == "" {
		t.Fatalf("incomplete credential: %+v", got)
	}
	exp, err := time.Parse(time.RFC3339, got.Expiration)
	if err != nil || !exp.After(time.Now()) {
		t.Fatalf("bad expiration %q: %v", got.Expiration, err)
	}
	if status, _ := headBucket(t, srv, c, got.AccessKeyId, got.SessionToken); status != http.StatusNotFound {
		t.Fatalf("issued token rejected: %d", status)
	}

	bad, err := c.Post(srv.Endpoint()+"/_session?ttl=soon", "", strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	bad.Body.Close()
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad ttl: got %d, want 400", bad.StatusCode)
	}
}
