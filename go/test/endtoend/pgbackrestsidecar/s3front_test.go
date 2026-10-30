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

package pgbackrestsidecar

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/s3mock"
)

// s3Front is a TLS proxy between the sidecar and an s3mock. s3mock cannot hold
// a GET, replay a credential or delay a request, and the shardsetup helper
// fixes the session TTL, so everything the tests need beyond s3mock lives here:
//
//   - it counts the sessions the sidecar mints and can answer every later mint
//     with the first session (a credential source whose renewal is broken);
//   - it records any request s3mock rejected for its credentials, which is how
//     an expired or unknown session shows up;
//   - rules hold or delay the requests they match.
//
// The sidecar is pointed at the proxy for both the credential endpoint and the
// repository, so the proxy sees every request it makes.
type s3Front struct {
	backend *s3mock.Server
	srv     *httptest.Server

	mu          sync.Mutex
	rules       []*s3Rule
	replayFirst bool
	first       []byte
	firstAt     time.Time
	sessions    int
	rejected    []string
}

// s3Rule holds or delays the requests it matches. A request waits for its own
// delay, and for hold to be closed when hold is set; a rule that is off passes
// everything.
type s3Rule struct {
	match func(*http.Request) bool
	delay time.Duration
	// until, when non-zero, ends the delay: later requests pass untouched.
	until atomic.Int64
	hold  chan struct{}
	// hit is closed by the first matching request.
	hit  chan struct{}
	once sync.Once
	hits atomic.Int64
	off  atomic.Bool
}

func newS3Rule(match func(*http.Request) bool) *s3Rule {
	return &s3Rule{match: match, hit: make(chan struct{})}
}

// holdRule blocks matching requests until release is called.
func holdRule(match func(*http.Request) bool) *s3Rule {
	r := newS3Rule(match)
	r.hold = make(chan struct{})
	return r
}

// delayRule delays each matching request by d.
func delayRule(match func(*http.Request) bool, d time.Duration) *s3Rule {
	r := newS3Rule(match)
	r.delay = d
	return r
}

func (r *s3Rule) release() {
	r.off.Store(true)
	if r.hold != nil {
		select {
		case <-r.hold:
		default:
			close(r.hold)
		}
	}
}

func newS3Front(t *testing.T, backend *s3mock.Server) *s3Front {
	t.Helper()
	target, err := url.Parse(backend.Endpoint())
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(backend.CertPEM()))
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
	rp.FlushInterval = -1

	f := &s3Front{backend: backend}
	f.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.serve(rp, w, r)
	}))
	f.srv.StartTLS()
	t.Cleanup(f.srv.Close)
	return f
}

func (f *s3Front) URL() string     { return f.srv.URL }
func (f *s3Front) CertPEM() []byte { return pemOf(f.srv) }

func (f *s3Front) addRule(r *s3Rule) *s3Rule {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = append(f.rules, r)
	return r
}

// replayFirstSession makes every later credential mint return the first
// session, expiry included.
func (f *s3Front) replayFirstSession() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replayFirst = true
}

func (f *s3Front) sessionCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sessions
}

func (f *s3Front) firstSessionAt() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.firstAt
}

// rejectedRequests are the requests s3mock refused for their credentials.
func (f *s3Front) rejectedRequests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.rejected...)
}

// statusWriter records the status the backend answered with.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (f *s3Front) serve(rp *httputil.ReverseProxy, w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/_session" {
		f.serveSession(rp, w, r)
		return
	}
	f.mu.Lock()
	rules := append([]*s3Rule(nil), f.rules...)
	f.mu.Unlock()
	for _, rule := range rules {
		if rule.off.Load() || !rule.match(r) {
			continue
		}
		rule.hits.Add(1)
		rule.once.Do(func() { close(rule.hit) })
		if rule.hold != nil {
			select {
			case <-rule.hold:
			case <-r.Context().Done():
				return
			}
		}
		if u := rule.until.Load(); rule.delay > 0 && (u == 0 || time.Now().UnixNano() < u) {
			select {
			case <-time.After(rule.delay):
			case <-r.Context().Done():
				return
			}
		}
	}
	sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
	rp.ServeHTTP(sw, r)
	// 404 is a normal answer for a missing key; a credential problem is 400 or 403.
	if sw.status == http.StatusBadRequest || sw.status == http.StatusForbidden {
		f.mu.Lock()
		f.rejected = append(f.rejected, r.Method+" "+r.URL.Path+" -> "+http.StatusText(sw.status))
		f.mu.Unlock()
	}
}

func (f *s3Front) serveSession(rp *httputil.ReverseProxy, w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.sessions++
	if f.replayFirst && f.first != nil {
		body := f.first
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		return
	}
	f.mu.Unlock()

	rec := httptest.NewRecorder()
	rp.ServeHTTP(rec, r)
	res := rec.Result()
	body, _ := io.ReadAll(res.Body)
	f.mu.Lock()
	if f.first == nil && res.StatusCode == http.StatusOK {
		f.first, f.firstAt = body, time.Now()
	}
	f.mu.Unlock()
	maps.Copy(w.Header(), res.Header)
	w.WriteHeader(res.StatusCode)
	_, _ = w.Write(body)
}
