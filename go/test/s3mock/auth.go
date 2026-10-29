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

// go/test/s3mock/auth.go
package s3mock

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	sessionTokenHeader = "X-Amz-Security-Token"
	sessionAdminPath   = "/_session"
)

// Session is a set of temporary credentials minted by IssueSession.
type Session struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Expiration      time.Time
}

// sessionAuth tracks issued sessions. Enforcement is opt-in so suites that
// never mint a session see no behavior change. Signatures are not verified:
// the point is to let a test prove credential renewal works, which needs token
// validity, expiry and that the token travels with the key it was issued for.
type sessionAuth struct {
	mu       sync.Mutex
	required bool
	sessions map[string]sessionEntry // session token -> issued key and expiration
}

type sessionEntry struct {
	accessKeyID string
	expiration  time.Time
}

// requestAccessKeyID extracts the AccessKeyId a SigV4 request claims, from the
// Authorization header's Credential field or a presigned X-Amz-Credential
// query parameter. It returns "" when the request carries neither.
func requestAccessKeyID(r *http.Request) string {
	cred := ""
	if _, rest, ok := strings.Cut(r.Header.Get("Authorization"), "Credential="); ok {
		cred, _, _ = strings.Cut(rest, ",")
	} else {
		cred = r.URL.Query().Get("X-Amz-Credential")
	}
	id, _, _ := strings.Cut(cred, "/")
	return id
}

// requestSessionToken returns the session token from the header, or from the
// X-Amz-Security-Token query parameter that a presigned request carries
// instead.
func requestSessionToken(r *http.Request) string {
	if t := r.Header.Get(sessionTokenHeader); t != "" {
		return t
	}
	return r.URL.Query().Get(sessionTokenHeader)
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("s3mock: crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b)
}

// IssueSession mints new random credentials valid for ttl and turns on session
// enforcement. A negative ttl yields an already-expired session, which tests
// use to exercise the ExpiredToken path.
func (s *Server) IssueSession(ttl time.Duration) Session {
	sess := Session{
		AccessKeyID:     "ASIA" + randomHex(8),
		SecretAccessKey: randomHex(20),
		SessionToken:    randomHex(32),
		Expiration:      time.Now().Add(ttl).UTC(),
	}
	s.auth.mu.Lock()
	defer s.auth.mu.Unlock()
	if s.auth.sessions == nil {
		s.auth.sessions = make(map[string]sessionEntry)
	}
	s.auth.sessions[sess.SessionToken] = sessionEntry{sess.AccessKeyID, sess.Expiration}
	s.auth.required = true
	return sess
}

// RequireSessions turns enforcement on before any session has been issued, so
// a request made before the first IssueSession is rejected too.
func (s *Server) RequireSessions() {
	s.auth.mu.Lock()
	defer s.auth.mu.Unlock()
	s.auth.required = true
}

// checkSession returns an S3 error code and status when the request must be
// rejected, or "" when it may proceed.
func (s *Server) checkSession(r *http.Request) (code, message string) {
	s.auth.mu.Lock()
	defer s.auth.mu.Unlock()
	if !s.auth.required {
		return "", ""
	}
	entry, ok := s.auth.sessions[requestSessionToken(r)]
	if !ok {
		return "InvalidToken", "The provided token is malformed or otherwise invalid."
	}
	// pgBackRest renews key, secret and token together, so a request pairing a
	// fresh token with a stale key means a partial credential update.
	if requestAccessKeyID(r) != entry.accessKeyID {
		return "InvalidToken", "The provided token does not belong to the signing access key."
	}
	if !time.Now().Before(entry.expiration) {
		return "ExpiredToken", "The provided token has expired."
	}
	return "", ""
}

// withSessionAuth rejects requests that lack a live session token and serves
// the session admin endpoint, which is exempt because it is how a client
// obtains its first token.
func (s *Server) withSessionAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == sessionAdminPath {
			s.handleIssueSession(w, r)
			return
		}
		if code, msg := s.checkSession(r); code != "" {
			writeS3Error(w, code, msg, http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// handleIssueSession serves POST /_session?ttl=<duration> in the shape of the
// credential protocol pgbackrestd's creds helper speaks.
func (s *Server) handleIssueSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeS3Error(w, "MethodNotAllowed", "Use POST", http.StatusMethodNotAllowed)
		return
	}
	ttl, err := time.ParseDuration(r.URL.Query().Get("ttl"))
	if err != nil {
		writeS3Error(w, "InvalidArgument", "ttl must be a Go duration: "+err.Error(), http.StatusBadRequest)
		return
	}
	sess := s.IssueSession(ttl)
	w.Header().Set("Content-Type", "application/json")
	// The mock hands out random throwaway credentials by design.
	_ = json.NewEncoder(w).Encode(struct { //nolint:gosec // G117
		Version         int
		AccessKeyId     string
		SecretAccessKey string
		SessionToken    string
		Expiration      string
	}{1, sess.AccessKeyID, sess.SecretAccessKey, sess.SessionToken, sess.Expiration.Format(time.RFC3339)})
}
