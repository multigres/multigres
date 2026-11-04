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
	"encoding/json"
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
	sessions    int
	rejected    []string
	// expiries is the wall-clock expiry of every session s3mock issued, by
	// session token.
	expiries map[string]time.Time
	walPuts  []walPut
}

// s3Rule holds or delays the requests it matches. A request waits for its own
// delay, and for hold to be closed when hold is set; a rule that is off passes
// everything.
type s3Rule struct {
	match func(*http.Request) bool
	delay time.Duration
	// until, when set, ends the delay: later requests pass untouched. It is a
	// wall-clock instant (see wallNow), because the session it is meant to
	// outlast expires on the wall clock.
	until atomic.Pointer[time.Time]
	hold  chan struct{}
	// hit is closed by the first matching request.
	hit  chan struct{}
	once sync.Once
	hits atomic.Int64
	off  atomic.Bool
}

// wallNow is the current wall-clock time, without Go's monotonic reading, so
// that comparing or subtracting two of these uses the wall clock.
//
// A session's expiry is a wall-clock instant on both sides of the protocol:
// s3mock stores it without a monotonic reading and compares it with the wall
// clock, and the sidecar and pgBackRest only ever see it as an RFC 3339
// timestamp. Everything a test measures against an expiry (how long a command
// ran, when a delay ends) must therefore use the wall clock too. The monotonic
// clock stops while the host is suspended and the wall clock does not, so a
// duration from time.Since can be shorter than a session that expired inside it.
func wallNow() time.Time { return time.Now().Round(0) }

// walPut is one WAL segment PUT the front passed to s3mock.
type walPut struct {
	// conn is the client address of the TCP connection the request came on.
	conn string
	// token is the session token the request was signed for.
	token string
	// at is the wall-clock time s3mock answered.
	at     time.Time
	status int
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
	return newS3FrontWithCert(t, backend, nil)
}

// newS3FrontWithCert is newS3Front with a front that presents whatever
// certificate cert holds when a connection is made, so a test can change the
// endpoint's certificate while a cluster runs. A nil cert means the test
// server's own certificate.
func newS3FrontWithCert(t *testing.T, backend *s3mock.Server, cert *atomic.Pointer[tls.Certificate]) *s3Front {
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
	if cert != nil {
		f.srv.TLS = &tls.Config{
			MinVersion:     tls.VersionTLS12,
			GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return cert.Load(), nil },
		}
	}
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

// walPutLog returns the WAL segment PUTs seen so far, oldest first.
func (f *s3Front) walPutLog() []walPut {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]walPut(nil), f.walPuts...)
}

// sessionExpiry is the wall-clock expiry of the session with this token, as
// s3mock issued it.
func (f *s3Front) sessionExpiry(token string) (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	exp, ok := f.expiries[token]
	return exp, ok
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
		if u := rule.until.Load(); rule.delay > 0 && (u == nil || wallNow().Before(*u)) {
			select {
			case <-time.After(rule.delay):
			case <-r.Context().Done():
				return
			}
		}
	}
	sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
	rp.ServeHTTP(sw, r)
	if r.Method == http.MethodPut && walKey.MatchString(r.URL.Path) {
		f.mu.Lock()
		f.walPuts = append(f.walPuts, walPut{conn: r.RemoteAddr, token: r.Header.Get("X-Amz-Security-Token"), at: wallNow(), status: sw.status})
		f.mu.Unlock()
	}
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
	var issued struct{ SessionToken, Expiration string }
	expiry, expiryErr := time.Time{}, json.Unmarshal(body, &issued)
	if expiryErr == nil {
		expiry, expiryErr = time.Parse(time.RFC3339, issued.Expiration)
	}
	f.mu.Lock()
	if res.StatusCode == http.StatusOK {
		if f.first == nil {
			f.first = body
		}
		if expiryErr == nil {
			if f.expiries == nil {
				f.expiries = map[string]time.Time{}
			}
			f.expiries[issued.SessionToken] = expiry
		}
	}
	f.mu.Unlock()
	maps.Copy(w.Header(), res.Header)
	w.WriteHeader(res.StatusCode)
	_, _ = w.Write(body)
}
