// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package latencydiag records bounded, opt-in latency evidence without SQL text,
// parameters, credentials, or per-connection metric labels. It is independent of
// the telemetry exporter: an inactive probe is an atomic load and a branch.
package latencydiag

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"
)

var (
	beginMu  sync.Mutex
	active   atomic.Pointer[Capture]
	instance = fmt.Sprintf("%x", time.Now().UnixNano())
)

type contextKey struct{}

// Options are intentionally bounded. SampleEvery samples frontend messages,
// not connections, so every worker can contribute. Use 1 for a complete window.
type Options struct {
	ID          string `json:"id"`
	SampleEvery uint64 `json:"sample_every"`
	Sockets     bool   `json:"sockets"`
	MaxEvents   int    `json:"max_events"`
}

type Event struct {
	ID               uint64 `json:"id"`
	Parent           uint64 `json:"parent,omitempty"`
	RemoteParent     string `json:"remote_parent,omitempty"`
	Root             string `json:"root"`
	Phase            string `json:"phase"`
	StartNS          int64  `json:"start_ns"`
	DurationNS       int64  `json:"duration_ns"`
	Failed           bool   `json:"failed,omitempty"`
	Cycle            uint64 `json:"cycle,omitempty"`
	TransactionAfter byte   `json:"transaction_after,omitempty"`
	SQLState         string `json:"sqlstate,omitempty"`
	Protocol         byte   `json:"protocol,omitempty"`
	Transaction      byte   `json:"transaction_status,omitempty"`
	Bytes            int    `json:"bytes,omitempty"`
}

type Report struct {
	PropagationDropped uint64    `json:"propagation_dropped"`
	Options            Options   `json:"options"`
	Instance           string    `json:"instance"`
	Started            time.Time `json:"started_utc"`
	DurationNS         int64     `json:"duration_ns"`
	MessagesSeen       uint64    `json:"messages_seen"`
	MessagesSelected   uint64    `json:"messages_selected"`
	SpansStarted       uint64    `json:"spans_started"`
	SpansFinished      uint64    `json:"spans_finished"`
	Dropped            uint64    `json:"events_dropped"`
	Unfinished         uint64    `json:"unfinished_at_stop"`
	Events             []Event   `json:"events"`
	// Even lossless server capture alone cannot rule out unsampled work or locate
	// time before the goroutine runs / after a socket write completes.
	Verdict string `json:"verdict"`
}

type Capture struct {
	sockets            map[socketKey]net.Conn
	socketOmitted      uint64
	propagationDropped atomic.Uint64
	opts               Options
	start              time.Time
	next               atomic.Uint64
	seen               atomic.Uint64
	selected           atomic.Uint64
	mu                 sync.Mutex
	closed             bool
	finished           uint64
	dropped            uint64
	events             []Event
}

func Active() bool { return active.Load() != nil }

// Begin permits only one capture in a process. IDs must match across processes
// for propagation. A peer cannot activate a capture by sending metadata.
func Begin(opts Options) (*Capture, error) {
	beginMu.Lock()
	defer beginMu.Unlock()
	if active.Load() != nil {
		return nil, errors.New("capture already active")
	}
	if !validID(opts.ID) || opts.SampleEvery < 1 || opts.SampleEvery > 1000000 || opts.MaxEvents < 1 || opts.MaxEvents > 200000 {
		return nil, errors.New("invalid capture options")
	}
	c := &Capture{opts: opts, start: time.Now(), events: make([]Event, 0, opts.MaxEvents)}
	if !active.CompareAndSwap(nil, c) {
		return nil, errors.New("capture already active")
	}
	return c, nil
}

func validID(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	for _, b := range s {
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '-' || b == '_') {
			return false
		}
	}
	return true
}

// Stop freezes the report. Work crossing the window is counted as unfinished;
// late completions cannot append to, or race with, the frozen events.
// The first call transfers event-buffer ownership to the caller. Later calls
// return an empty report, making deferred cleanup safe without retaining the
// buffer through contexts held by idle connections.
func (c *Capture) Stop() Report {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return Report{}
	}
	c.closed = true
	c.sockets = nil
	active.CompareAndSwap(c, nil)
	started := c.next.Load()
	events := c.events
	c.events = nil
	return Report{PropagationDropped: c.propagationDropped.Load(), Options: c.opts, Instance: instance, Started: c.start.UTC(), DurationNS: time.Since(c.start).Nanoseconds(), MessagesSeen: c.seen.Load(), MessagesSelected: c.selected.Load(), SpansStarted: started, SpansFinished: c.finished, Dropped: c.dropped, Unfinished: started - c.finished, Events: events, Verdict: "inconclusive_without_matched_client_and_transport_evidence"}
}

type Span struct {
	capture *Capture
	ctx     context.Context
	event   Event
	start   time.Time
	done    atomic.Bool
	region  *trace.Region
	task    *trace.Task
}

func (s *Span) Context() context.Context { return s.ctx }

// Message includes waiting for the type byte. A read already blocked when a
// capture starts is deliberately not backdated; the first captured sequence may
// start later. Sequence numbers must advance even while capture is inactive.
func Message(ctx context.Context, connection uint32, sequence uint64) *Span {
	c := active.Load()
	if c == nil {
		return nil
	}
	c.seen.Add(1)
	if sampleHash(sequence^(uint64(connection)<<32))%c.opts.SampleEvery != 0 {
		return nil
	}
	c.selected.Add(1)
	return c.startSpan(ctx, Event{Root: fmt.Sprintf("%s/%d/%d", instance, connection, sequence), Phase: "frontend.message"})
}

// Start creates a child only if ctx belongs to the current capture. Callers
// must pass the returned context downstream to preserve nesting.
func Start(ctx context.Context, phase string) (context.Context, *Span) {
	return start(ctx, phase, false)
}

// StartAsync uses a runtime task, not a goroutine-local region: a stream may
// finish on a different goroutine (for example on context cancellation).
func StartAsync(ctx context.Context, phase string) (context.Context, *Span) {
	return start(ctx, phase, true)
}

func start(ctx context.Context, phase string, async bool) (context.Context, *Span) {
	c := active.Load()
	if c == nil {
		return ctx, nil
	}
	parent, _ := ctx.Value(contextKey{}).(*Span)
	if parent == nil || parent.capture != c {
		return ctx, nil
	}
	s := c.startSpanMode(ctx, Event{Root: parent.event.Root, Parent: parent.event.ID, Phase: phase}, async)
	if s == nil {
		return ctx, nil
	}
	return s.ctx, s
}

func (c *Capture) startSpan(ctx context.Context, e Event) *Span {
	return c.startSpanMode(ctx, e, false)
}

func (c *Capture) startSpanMode(ctx context.Context, e Event, async bool) *Span {
	// Serialize span admission with Stop so unfinished accounting is exact.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	e.ID = c.next.Add(1)
	c.mu.Unlock()
	now := time.Now()
	e.StartNS = now.Sub(c.start).Nanoseconds()
	s := &Span{capture: c, event: e, start: now}
	if trace.IsEnabled() {
		if e.Parent == 0 || async {
			ctx, s.task = trace.NewTask(ctx, e.Phase)
			trace.Log(ctx, "latency.root", e.Root)
		}
		if !async {
			s.region = trace.StartRegion(ctx, e.Phase)
		}
		trace.Logf(ctx, "latency.span", "%s:%d", instance, e.ID)
	}
	s.ctx = context.WithValue(ctx, contextKey{}, s)
	return s
}

// End records only an error flag, never error text (which can contain SQL).
// bytes is a transfer size, or zero for non-I/O spans.
func (s *Span) End(err error, bytes int) {
	if s == nil || s.done.Swap(true) {
		return
	}
	if s.region != nil {
		s.region.End()
	}
	if s.task != nil {
		s.task.End()
	}
	s.event.DurationNS = time.Since(s.start).Nanoseconds()
	s.event.Failed = err != nil
	s.event.Bytes = bytes
	c := s.capture
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.finished++
	if len(c.events) == cap(c.events) {
		c.dropped++
		return
	}
	c.events = append(c.events, s.event)
}

// Protocol is called by the connection's serving goroutine before End.
func (s *Span) Protocol(kind, transaction byte) {
	if s != nil {
		s.event.Protocol = kind
		s.event.Transaction = transaction
	}
}

// Since records an already measured phase without introducing another clock
// read at its beginning. These intervals are children, never additive totals.
func Since(ctx context.Context, phase string, start time.Time, err error) {
	if !Selected(ctx) {
		return
	}
	parent := ctx.Value(contextKey{}).(*Span)
	c := parent.capture
	duration := time.Since(start)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	e := Event{ID: c.next.Add(1), Parent: parent.event.ID, Root: parent.event.Root, Phase: phase, StartNS: start.Sub(c.start).Nanoseconds(), DurationNS: duration.Nanoseconds(), Failed: err != nil}
	c.finished++
	if len(c.events) == cap(c.events) {
		c.dropped++
		return
	}
	c.events = append(c.events, e)
}

// Selected is a cheap guard before work such as constructing an RPC phase name.
func Selected(ctx context.Context) bool {
	c := active.Load()
	if c == nil {
		return false
	}
	s, _ := ctx.Value(contextKey{}).(*Span)
	return s != nil && s.capture == c
}

func sampleHash(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (s *Span) Boundary(cycle uint64, after byte) {
	if s != nil {
		s.event.Cycle = cycle
		s.event.TransactionAfter = after
	}
}

func (s *Span) ErrorCode(code string) {
	if s != nil && len(code) == 5 {
		s.event.SQLState = code
	}
}

// PropagationDropped marks a selected operation whose correlation could not be
// added without changing the application's existing propagation contract.
func PropagationDropped(ctx context.Context) {
	if Selected(ctx) {
		ctx.Value(contextKey{}).(*Span).capture.propagationDropped.Add(1)
	}
}
