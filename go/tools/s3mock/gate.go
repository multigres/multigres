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

package s3mock

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
)

// Matcher reports whether an S3 PUT operation should trigger the gate.
type Matcher func(bucket, key string) bool

// Hit describes the operation that tripped the gate.
type Hit struct {
	Method, Bucket, Key string
}

// Gate pauses S3 operations matching its Matcher once Arm has been called,
// records the first match in a Hit, and blocks the request goroutine until
// Release is called or the request context is cancelled.
type Gate struct {
	matcher Matcher
	armed   atomic.Bool

	mu      sync.Mutex
	hitCh   chan Hit
	hitOnce sync.Once
	relCh   chan struct{}
}

// NewGate returns a disarmed Gate.
func NewGate(m Matcher) *Gate {
	return &Gate{
		matcher: m,
		hitCh:   make(chan Hit, 1),
		relCh:   make(chan struct{}),
	}
}

// Arm enables the gate. Before arming, all operations pass through.
func (g *Gate) Arm() { g.armed.Store(true) }

// Wait blocks until a matching operation arrives, or ctx is done.
// Returns the Hit on success, or ctx.Err() on cancellation.
func (g *Gate) Wait(ctx context.Context) (Hit, error) {
	g.mu.Lock()
	hitCh := g.hitCh
	g.mu.Unlock()
	select {
	case h := <-hitCh:
		return h, nil
	case <-ctx.Done():
		return Hit{}, ctx.Err()
	}
}

// Release unblocks the held operation. Safe to call multiple times; the
// first call wins, subsequent calls are no-ops.
func (g *Gate) Release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-g.relCh:
		// already closed
	default:
		close(g.relCh)
	}
}

// Rearm disarms, then re-enables the gate for the next match. Resets the
// release channel and the one-shot hit. Caller is responsible for not
// racing this with in-flight Wait/Release on the same Gate.
func (g *Gate) Rearm() {
	g.mu.Lock()
	g.relCh = make(chan struct{})
	g.hitCh = make(chan Hit, 1)
	g.hitOnce = sync.Once{}
	g.mu.Unlock()
	g.armed.Store(true)
}

// callback returns a PutCallback that consults the gate.
func (g *Gate) callback() PutCallback {
	return func(ctx context.Context, bucket, key string) error {
		if !g.armed.Load() {
			return nil
		}
		if !g.matcher(bucket, key) {
			return nil
		}
		g.mu.Lock()
		hitCh := g.hitCh
		relCh := g.relCh
		hitOnce := &g.hitOnce
		g.mu.Unlock()

		// Single-shot send so multiple matching PUTs do not stall behind a closed Wait.
		hitOnce.Do(func() {
			hitCh <- Hit{Method: "PUT", Bucket: bucket, Key: key}
		})
		select {
		case <-relCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// WithGate returns a ServerOption that installs g's callback. If a previous
// PutCallback was installed via WithPutCallback, it is replaced.
func WithGate(g *Gate) ServerOption {
	return func(s *Server) { s.putCallback = g.callback() }
}

// MatchPut returns a Matcher that fires when the key contains substr.
func MatchPut(substr string) Matcher {
	return func(_ string, key string) bool {
		return strings.Contains(key, substr)
	}
}

// MatchPutRegex returns a Matcher that fires when the key matches re.
func MatchPutRegex(re *regexp.Regexp) Matcher {
	return func(_, key string) bool { return re.MatchString(key) }
}

// Phase is a goroutine-safe string label that tests use to drive
// PhaseMatcher. Use descriptive labels ("bootstrap", "first-backup",
// "second-backup") so the s3mock log narrates the test as it runs.
type Phase struct {
	v atomic.Value
}

// Set updates the current phase.
func (p *Phase) Set(s string) { p.v.Store(s) }

// Get returns the current phase, or "" if Set has never been called.
func (p *Phase) Get() string {
	if v := p.v.Load(); v != nil {
		return v.(string)
	}
	return ""
}

// PhaseMatcher returns a Matcher that delegates to base only while
// phase.Get() == want. It is the standard way to use a Gate in tests
// that need to discriminate between multiple actors hitting the same
// s3mock — for example, holding the first concurrent backup while
// letting the second pass through. Switch the phase to a different
// value to stop matching new requests; goroutines already parked
// inside gate.callback stay parked until ctx cancellation.
func PhaseMatcher(phase *Phase, want string, base Matcher) Matcher {
	return func(bucket, key string) bool {
		if phase.Get() != want {
			return false
		}
		return base(bucket, key)
	}
}

// MatchDataUpload fires on data-file PUTs inside a backup set's pg_data/
// (loose files like backup_label.zst) or bundle/ (pgBackRest's bundled small
// files). Excludes manifest writes and WAL archive PUTs.
//
// This is the only useful pause point for "kill primary postgres" fault
// scenarios: pgBackRest's first S3 write during a backup is to a bundle file,
// and pausing here is early enough that pg_stop_backup has not yet been
// called. Other pause points were considered (manifest writes at the end of
// a backup) but they happen after pg_stop_backup returns, so killing postgres
// at that point is too late to fail the backup.
var MatchDataUpload Matcher = func(_ string, key string) bool {
	if !strings.Contains(key, "/backup/") {
		return false
	}
	return strings.Contains(key, "/pg_data/") || strings.Contains(key, "/bundle/")
}
