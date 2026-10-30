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
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

const (
	// credsHeader carries the token; pgBackRest runs the creds helper per
	// process, so the helper presents it on every call.
	credsHeader = "X-Pgbackrestd-Token" //nolint:gosec // G101: a header name, not a credential

	// credMinValid: pgBackRest renews a credential within five minutes of its
	// Expiration, so one that close to expiry is never handed out.
	credMinValid = 5 * time.Minute
	// credRefreshAhead starts a background refresh while the cached credential
	// still has this long left.
	credRefreshAhead = 15 * time.Minute
	// credBackgroundGap bounds how often a source is called ahead of need, so
	// a short-lived source cannot turn a burst of helper calls into a burst of
	// source calls. credFailureGap does the same after a failure.
	credBackgroundGap = 30 * time.Second
	credFailureGap    = 5 * time.Second
	// credRefreshTimeout bounds one source call; it does not follow a
	// requester's context, so one impatient helper cannot fail the others.
	credRefreshTimeout = 30 * time.Second
	credReadHeaderTime = 10 * time.Second
)

var errCredsUnavailable = errors.New("no usable credentials")

// credCache holds the last credential and refreshes it ahead of expiry. One
// source call is in flight at a time, however many helpers ask.
type credCache struct {
	src    credSource
	now    func() time.Time
	logger *slog.Logger
	// baseCtx parents every source call: it ends with the server, not with a
	// request.
	baseCtx context.Context

	mu          sync.Mutex
	cur         Credentials
	have        bool
	flight      *credFlight
	lastAttempt time.Time
	lastErr     error
}

type credFlight struct {
	done chan struct{}
	err  error
}

func newCredCache(ctx context.Context, src credSource, now func() time.Time, logger *slog.Logger) *credCache {
	return &credCache{src: src, now: now, logger: logger, baseCtx: context.WithoutCancel(ctx)}
}

// Get returns a credential with more than credMinValid left, or an error.
func (c *credCache) Get(ctx context.Context) (Credentials, error) {
	for {
		c.mu.Lock()
		now := c.now()
		left := c.cur.Expiration.Sub(now)
		if c.have && left > credRefreshAhead {
			cur := c.cur
			c.mu.Unlock()
			return cur, nil
		}
		if c.have && left > credMinValid {
			cur := c.cur
			if c.flight == nil && now.Sub(c.lastAttempt) >= credBackgroundGap {
				c.startLocked(now)
			}
			c.mu.Unlock()
			return cur, nil
		}

		// Nothing usable: wait for a refresh, starting one unless the source
		// failed moments ago.
		f := c.flight
		if f == nil {
			if c.lastErr != nil && now.Sub(c.lastAttempt) < credFailureGap {
				err := c.lastErr
				c.mu.Unlock()
				return Credentials{}, fmt.Errorf("%w: %w", errCredsUnavailable, err)
			}
			f = c.startLocked(now)
		}
		c.mu.Unlock()

		select {
		case <-f.done:
		case <-ctx.Done():
			return Credentials{}, ctx.Err()
		}
		if f.err != nil {
			return Credentials{}, fmt.Errorf("%w: %w", errCredsUnavailable, f.err)
		}
	}
}

// startLocked begins a source call. The caller holds c.mu.
func (c *credCache) startLocked(now time.Time) *credFlight {
	f := &credFlight{done: make(chan struct{})}
	c.flight = f
	c.lastAttempt = now
	go c.refresh(f)
	return f
}

func (c *credCache) refresh(f *credFlight) {
	ctx, cancel := context.WithTimeout(c.baseCtx, credRefreshTimeout)
	defer cancel()
	cred, err := c.src.Get(ctx)
	if err == nil && cred.Expiration.Sub(c.now()) <= credMinValid {
		err = fmt.Errorf("source returned a credential expiring at %s, within %s", cred.Expiration.Format(time.RFC3339), credMinValid)
	}

	c.mu.Lock()
	if err == nil {
		// A source that answers with a shorter-lived credential than the one
		// held (a role with a shorter session cap, a flapping agent) must not
		// shorten what pgBackRest is given.
		if !c.have || !cred.Expiration.Before(c.cur.Expiration) {
			c.cur, c.have = cred, true
		}
		c.lastErr = nil
	} else {
		c.lastErr = err
		if c.have && c.cur.Expiration.Sub(c.now()) > credMinValid {
			c.logger.Warn("credential refresh failed; serving the cached credential", "error", err)
		} else {
			c.logger.Error("credential refresh failed", "error", err)
		}
	}
	f.err = err
	c.flight = nil
	c.mu.Unlock()
	close(f.done)
}

// credsResponse is the creds protocol document. Field order is fixed by the
// struct, so the output is deterministic.
type credsResponse struct {
	Version         int
	AccessKeyID     string `json:"AccessKeyId"`
	SecretAccessKey string
	SessionToken    string
	Expiration      string
}

// credsHandler answers GET /creds. Token comparison happens before anything
// else, so an unauthenticated peer learns nothing about the path or method.
func credsHandler(token string, cache *credCache, logger *slog.Logger) http.Handler {
	want := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := sha256.Sum256([]byte(r.Header.Get(credsHeader)))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path != "/creds" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		cred, err := cache.Get(r.Context())
		if err != nil {
			logger.WarnContext(r.Context(), "credential request failed", "error", err)
			http.Error(w, "credentials unavailable", http.StatusServiceUnavailable)
			return
		}
		body, err := json.Marshal(credsResponse{ //nolint:gosec // G117: the credential document is the point of this endpoint
			Version:         1,
			AccessKeyID:     cred.AccessKeyID,
			SecretAccessKey: cred.SecretAccessKey,
			SessionToken:    cred.SessionToken,
			Expiration:      cred.Expiration.UTC().Format(time.RFC3339),
		})
		if err != nil {
			http.Error(w, "credentials unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(body)
	})
}

// credSourceFor picks the source for this configuration. Only S3 locations
// have credentials to serve.
func (s *Server) credSourceFor() (credSource, error) {
	if s.cfg.LocationType != LocationS3 {
		return unservedSource{"filesystem location"}, nil
	}
	return newCredSource(s.cfg)
}

// startCreds serves the credential socket until ctx ends. The socket lives
// in creds/, which sandboxed children may reach only through the unix-socket
// connect rule, never as a readable file.
func (s *Server) startCreds(ctx context.Context) error {
	src, err := s.credSourceFor()
	if err != nil {
		return err
	}
	return s.serveCreds(ctx, src, time.Now)
}

func (s *Server) serveCreds(ctx context.Context, src credSource, now func() time.Time) error {
	ln, err := listenUnix(s.cfg.stateSub(dirCreds + "/creds.sock"))
	if err != nil {
		return err
	}
	cache := newCredCache(ctx, src, now, s.logger)
	srv := &http.Server{
		Handler:           credsHandler(s.credsToken(), cache, s.logger),
		ReadHeaderTimeout: credReadHeaderTime,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.ErrorContext(ctx, "credential server stopped", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	return nil
}

// credsToken is the secret an S3-command child presents to the credential
// socket. It is generated once per process and never revoked per child:
// pgBackRest's async workers detach and outlive the process that started them.
func (s *Server) credsToken() string { return s.token }
