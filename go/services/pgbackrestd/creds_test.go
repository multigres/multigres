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

package pgbackrestd

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var credsT0 = time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// fakeSource issues credentials that expire ttl after the fake clock's now.
type fakeSource struct {
	clock *fakeClock
	ttl   time.Duration // guarded by mu
	calls atomic.Int32
	// called receives one value per call.
	called chan struct{}

	mu  sync.Mutex
	err error
}

func newFakeSource(clock *fakeClock, ttl time.Duration) *fakeSource {
	return &fakeSource{clock: clock, ttl: ttl, called: make(chan struct{}, 64)}
}

func (f *fakeSource) setTTL(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ttl = d
}

func (f *fakeSource) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeSource) Get(context.Context) (Credentials, error) {
	n := f.calls.Add(1)
	f.called <- struct{}{}
	f.mu.Lock()
	err, ttl := f.err, f.ttl
	f.mu.Unlock()
	if err != nil {
		return Credentials{}, err
	}
	return Credentials{
		AccessKeyID:     "AKIA" + strconv.Itoa(int(n)),
		SecretAccessKey: "secret",
		SessionToken:    "token",
		Expiration:      f.clock.Now().Add(ttl),
	}, nil
}

func (f *fakeSource) waitCall(t *testing.T) {
	t.Helper()
	select {
	case <-f.called:
	case <-time.After(5 * time.Second):
		t.Fatal("source was not called")
	}
}

type credsEnv struct {
	srv    *Server
	clock  *fakeClock
	client *http.Client
}

func newCredsEnv(t *testing.T, src func(*fakeClock) credSource) *credsEnv {
	t.Helper()
	// Unix socket paths are short-limited; t.TempDir under a long test name
	// can exceed it.
	dir, err := os.MkdirTemp("", "creds")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	require.NoError(t, os.Mkdir(filepath.Join(dir, dirCreds), 0o700))

	clock := &fakeClock{t: credsT0}
	s := &Server{
		cfg:    Config{StateDir: dir},
		token:  "tok-1234",
		logger: slog.New(slog.DiscardHandler),
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, s.serveCreds(ctx, src(clock), clock.Now))

	sock := filepath.Join(dir, dirCreds, "creds.sock")
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		},
	}}
	t.Cleanup(client.CloseIdleConnections)
	return &credsEnv{srv: s, clock: clock, client: client}
}

func (e *credsEnv) get(t *testing.T, token string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://creds/creds", nil)
	require.NoError(t, err)
	if token != "" {
		req.Header.Set(credsHeader, token)
	}
	resp, err := e.client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, body
}

func (e *credsEnv) getOK(t *testing.T) credsResponse {
	t.Helper()
	status, body := e.get(t, e.srv.credsToken())
	require.Equal(t, http.StatusOK, status, string(body))
	var r credsResponse
	require.NoError(t, json.Unmarshal(body, &r))
	return r
}

func TestCreds_401WithoutToken(t *testing.T) {
	src := newFakeSource(nil, time.Hour)
	e := newCredsEnv(t, func(c *fakeClock) credSource { src.clock = c; return src })
	status, body := e.get(t, "")
	assert.Equal(t, http.StatusUnauthorized, status)
	assert.Empty(t, body)
	assert.Zero(t, src.calls.Load(), "an unauthenticated request must not reach the source")
}

func TestCreds_401WrongToken(t *testing.T) {
	src := newFakeSource(nil, time.Hour)
	e := newCredsEnv(t, func(c *fakeClock) credSource { src.clock = c; return src })
	for _, tok := range []string{"tok-1235", "tok-123", "tok-12345", "TOK-1234"} {
		status, body := e.get(t, tok)
		assert.Equal(t, http.StatusUnauthorized, status, tok)
		assert.Empty(t, body, tok)
	}
	assert.Zero(t, src.calls.Load())
}

func TestCreds_ServesDocument(t *testing.T) {
	var src *fakeSource
	e := newCredsEnv(t, func(c *fakeClock) credSource { src = newFakeSource(c, time.Hour); return src })
	r := e.getOK(t)
	assert.Equal(t, 1, r.Version)
	assert.Equal(t, "secret", r.SecretAccessKey)
	assert.Equal(t, "token", r.SessionToken)
	assert.Equal(t, credsT0.Add(time.Hour).Format(time.RFC3339), r.Expiration)

	info, err := os.Stat(filepath.Join(e.srv.cfg.StateDir, dirCreds, "creds.sock"))
	require.NoError(t, err)
	assert.Zero(t, info.Mode().Perm()&0o077, "socket must not be reachable by group or other")
}

func TestCreds_BurstSharesOneSourceCall(t *testing.T) {
	var src *fakeSource
	e := newCredsEnv(t, func(c *fakeClock) credSource { src = newFakeSource(c, time.Hour); return src })
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { e.getOK(t) })
	}
	wg.Wait()
	assert.EqualValues(t, 1, src.calls.Load())
}

func TestCreds_NeverReturnsUnder5Min(t *testing.T) {
	var src *fakeSource
	e := newCredsEnv(t, func(c *fakeClock) credSource { src = newFakeSource(c, time.Hour); return src })
	first := e.getOK(t)
	src.waitCall(t)

	// Jump past the 15 minute band so no background refresh ever ran: the
	// only thing standing between the helper and a 4 minute credential is
	// the floor, which must refresh synchronously.
	e.clock.Advance(56 * time.Minute)
	r := e.getOK(t)
	src.waitCall(t)
	assert.NotEqual(t, first.AccessKeyID, r.AccessKeyID, "the 4 minute credential was served")
	exp, err := time.Parse(time.RFC3339, r.Expiration)
	require.NoError(t, err)
	assert.Greater(t, exp.Sub(e.clock.Now()), 5*time.Minute)
	assert.EqualValues(t, 2, src.calls.Load())
}

func TestCreds_RefreshNeverShortensTheCredential(t *testing.T) {
	clock := &fakeClock{t: credsT0}
	src := newFakeSource(clock, time.Hour)
	cache := newCredCache(context.Background(), src, clock.Now, slog.New(slog.DiscardHandler))
	idle := func() {
		t.Helper()
		require.Eventually(t, func() bool {
			cache.mu.Lock()
			defer cache.mu.Unlock()
			return cache.flight == nil
		}, 5*time.Second, time.Millisecond)
	}

	first, err := cache.Get(context.Background())
	require.NoError(t, err)

	// 10 minutes left; the refresh yields one expiring 2 minutes sooner.
	clock.Advance(50 * time.Minute)
	src.setTTL(8 * time.Minute)
	_, err = cache.Get(context.Background())
	require.NoError(t, err)
	idle()
	require.EqualValues(t, 2, src.calls.Load())
	got, err := cache.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, first, got, "a shorter-lived refresh replaced the cached credential")

	// A longer-lived refresh does replace it.
	clock.Advance(credBackgroundGap)
	src.setTTL(time.Hour)
	_, err = cache.Get(context.Background())
	require.NoError(t, err)
	idle()
	got, err = cache.Get(context.Background())
	require.NoError(t, err)
	assert.NotEqual(t, first.AccessKeyID, got.AccessKeyID)
}

func TestCreds_SourceShorterThan5MinIsRejected(t *testing.T) {
	e := newCredsEnv(t, func(c *fakeClock) credSource { return newFakeSource(c, 4*time.Minute) })
	status, body := e.get(t, e.srv.credsToken())
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.NotContains(t, string(body), "secret")
}

func TestCreds_RefreshesAheadOf15Min(t *testing.T) {
	var src *fakeSource
	e := newCredsEnv(t, func(c *fakeClock) credSource { src = newFakeSource(c, time.Hour); return src })
	first := e.getOK(t)
	src.waitCall(t)

	// 20 minutes left: no refresh yet.
	e.clock.Advance(40 * time.Minute)
	assert.Equal(t, first.AccessKeyID, e.getOK(t).AccessKeyID)
	assert.EqualValues(t, 1, src.calls.Load())

	// 10 minutes left: the cached credential is served and a refresh starts.
	e.clock.Advance(10 * time.Minute)
	assert.Equal(t, first.AccessKeyID, e.getOK(t).AccessKeyID, "the request is not blocked on the refresh")
	src.waitCall(t)
	require.Eventually(t, func() bool { return e.getOK(t).AccessKeyID != first.AccessKeyID }, 5*time.Second, 5*time.Millisecond)
}

func TestCreds_ServesCachedOnSourceError(t *testing.T) {
	var src *fakeSource
	e := newCredsEnv(t, func(c *fakeClock) credSource { src = newFakeSource(c, time.Hour); return src })
	first := e.getOK(t)
	src.waitCall(t)

	src.setErr(errors.New("sts is down"))
	e.clock.Advance(50 * time.Minute) // 10 minutes left
	assert.Equal(t, first.AccessKeyID, e.getOK(t).AccessKeyID)
	src.waitCall(t)

	// The failed refresh does not poison the cached credential.
	e.clock.Advance(2 * time.Minute) // 8 minutes left
	assert.Equal(t, first.AccessKeyID, e.getOK(t).AccessKeyID)
}

func TestCreds_503WhenExpiredAndSourceDown(t *testing.T) {
	var src *fakeSource
	e := newCredsEnv(t, func(c *fakeClock) credSource { src = newFakeSource(c, time.Hour); return src })
	e.getOK(t)
	src.waitCall(t)

	src.setErr(errors.New("sts is down"))
	e.clock.Advance(56 * time.Minute) // 4 minutes left
	status, body := e.get(t, e.srv.credsToken())
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.NotContains(t, string(body), "secret")

	// Recovery: the source comes back and the next request, once the failure
	// gap passes, gets a fresh credential.
	src.setErr(nil)
	e.clock.Advance(credFailureGap)
	r := e.getOK(t)
	exp, err := time.Parse(time.RFC3339, r.Expiration)
	require.NoError(t, err)
	assert.Greater(t, exp.Sub(e.clock.Now()), 5*time.Minute)
}

func TestCreds_503WhenNeverFetched(t *testing.T) {
	e := newCredsEnv(t, func(*fakeClock) credSource { return unservedSource{CredStatic} })
	status, _ := e.get(t, e.srv.credsToken())
	assert.Equal(t, http.StatusServiceUnavailable, status)
}

func TestCreds_RejectsOtherPathsAndMethods(t *testing.T) {
	e := newCredsEnv(t, func(c *fakeClock) credSource { return newFakeSource(c, time.Hour) })
	for method, want := range map[string]int{http.MethodPost: http.StatusMethodNotAllowed, http.MethodDelete: http.StatusMethodNotAllowed} {
		req, err := http.NewRequest(method, "http://creds/creds", nil)
		require.NoError(t, err)
		req.Header.Set(credsHeader, e.srv.credsToken())
		resp, err := e.client.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, want, resp.StatusCode, method)
	}
	req, err := http.NewRequest(http.MethodGet, "http://creds/other", nil)
	require.NoError(t, err)
	req.Header.Set(credsHeader, e.srv.credsToken())
	resp, err := e.client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCredsEndpointSource_VerifiesCA(t *testing.T) {
	var gotMethod string
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		_, _ = io.WriteString(w, `{"Version":1,"AccessKeyId":"ASIAX","SecretAccessKey":"s","SessionToken":"t","Expiration":"2026-10-27T13:00:00Z"}`)
	}))
	defer ts.Close()

	// Without the certificate the client must refuse the server.
	bare, err := newEndpointSource(ts.URL, "")
	require.NoError(t, err)
	_, err = bare.Get(context.Background())
	require.Error(t, err)

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, pemOf(ts), 0o600))
	src, err := newEndpointSource(ts.URL, caFile)
	require.NoError(t, err)
	got, err := src.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "ASIAX", got.AccessKeyID)
	assert.Equal(t, time.Date(2026, 10, 27, 13, 0, 0, 0, time.UTC), got.Expiration)

	_, err = newEndpointSource(ts.URL, filepath.Join(t.TempDir(), "missing.pem"))
	require.Error(t, err)
}

func TestCredsPodIdentitySource(t *testing.T) {
	var gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, `{"AccessKeyId":"ASIAP","SecretAccessKey":"s","Token":"podtok","Expiration":"2026-10-27T13:00:00Z"}`)
	}))
	defer ts.Close()
	tokFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokFile, []byte("agent-token\n"), 0o600))

	src, err := newPodIdentitySource(ts.URL, tokFile)
	require.NoError(t, err)
	got, err := src.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "agent-token", gotAuth)
	assert.Equal(t, "podtok", got.SessionToken)

	_, err = newPodIdentitySource("", tokFile)
	require.Error(t, err)
}

func TestCredsWebIdentitySource(t *testing.T) {
	var gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "text/xml")
		_, _ = io.WriteString(w, `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>ASIAW</AccessKeyId><SecretAccessKey>s</SecretAccessKey><SessionToken>webtok</SessionToken><Expiration>2026-10-27T13:00:00Z</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`)
	}))
	defer ts.Close()
	tokFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokFile, []byte("jwt\n"), 0o600))

	src, err := newWebIdentitySource("us-east-1", tokFile, "arn:aws:iam::1:role/r", stsClientFor(ts.URL))
	require.NoError(t, err)
	got, err := src.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "ASIAW", got.AccessKeyID)
	assert.Equal(t, "webtok", got.SessionToken)
	assert.Contains(t, gotBody, "WebIdentityToken=jwt")
	assert.Contains(t, gotBody, "RoleArn=arn%3Aaws%3Aiam%3A%3A1%3Arole%2Fr")

	_, err = newWebIdentitySource("us-east-1", "", "arn", nil)
	require.Error(t, err)
}

func pemOf(ts *httptest.Server) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ts.Certificate().Raw})
}

func stsClientFor(url string) *sts.Client {
	return sts.New(sts.Options{Region: "us-east-1", BaseEndpoint: aws.String(url), Credentials: aws.AnonymousCredentials{}})
}

func TestCredsSTSRegion(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	assert.Equal(t, "eu-west-1", stsRegion("us-east-1", env(map[string]string{"AWS_REGION": "eu-west-1", "AWS_DEFAULT_REGION": "ap-south-1"})))
	assert.Equal(t, "ap-south-1", stsRegion("us-east-1", env(map[string]string{"AWS_DEFAULT_REGION": "ap-south-1"})))
	assert.Equal(t, "us-east-1", stsRegion("us-east-1", env(nil)))
}

func TestCredsURLValidation(t *testing.T) {
	for _, tc := range []struct {
		raw       string
		linkLocal bool
		ok        bool
	}{
		{"https://example.com/creds", false, true},
		{"https://example.com/creds", true, true},
		{"http://127.0.0.1:9/_session", false, true},
		{"http://localhost:9/_session", false, true},
		{"http://[::1]:9/x", false, true},
		{"http://example.com/x", false, false},
		{"http://example.com/x", true, false},
		{"http://169.254.170.23/v1/credentials", true, true},
		{"http://169.254.170.23/v1/credentials", false, false},
		{"http://[fd00:ec2::23]/v1/credentials", true, true},
		{"ftp://127.0.0.1/x", true, false},
		{"https:///nohost", false, false},
		{"/just/a/path", false, false},
	} {
		err := validateCredURL(tc.raw, "flag", tc.linkLocal)
		if tc.ok {
			assert.NoError(t, err, tc.raw)
		} else {
			assert.Error(t, err, tc.raw)
		}
	}
	_, err := newEndpointSource("http://example.com/_session", "")
	require.Error(t, err)
	_, err = newPodIdentitySource("http://example.com/creds", "/tok")
	require.Error(t, err)
}

func TestCredsReadsAreCapped(t *testing.T) {
	big := make([]byte, maxCredResponse+1)
	for i := range big {
		big[i] = 'a'
	}
	tokFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokFile, big, 0o600))

	web, err := newWebIdentitySource("us-east-1", tokFile, "arn", nil)
	require.NoError(t, err)
	_, err = web.Get(context.Background())
	require.ErrorContains(t, err, "exceeds")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(big) }))
	defer ts.Close()
	pod, err := newPodIdentitySource(ts.URL, filepath.Join(t.TempDir(), "absent"))
	require.NoError(t, err)
	small := filepath.Join(t.TempDir(), "small")
	require.NoError(t, os.WriteFile(small, []byte("t"), 0o600))
	pod.tokenFile = small
	_, err = pod.Get(context.Background())
	require.ErrorContains(t, err, "exceeds")
}
