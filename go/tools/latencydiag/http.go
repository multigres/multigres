// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package latencydiag

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"runtime/trace"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const maxTraceBytes = 32 << 20

var httpBusy atomic.Bool

type cappedTrace struct {
	mu        sync.Mutex
	data      bytes.Buffer
	truncated bool
}

func (b *cappedTrace) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	room := maxTraceBytes - b.data.Len()
	if len(p) > room {
		p = p[:room]
		b.truncated = true
	}
	_, _ = b.data.Write(p)
	return n, nil
}

// Handler must only be registered behind an explicit operator opt-in, on the
// existing diagnostic listener with its existing authentication middleware.
// No diagnostics may be exposed on the PostgreSQL/public API listener.
func Handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "GET required", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	number := func(key string, def, max int) (int, error) {
		if q.Get(key) == "" {
			return def, nil
		}
		n, err := strconv.Atoi(q.Get(key))
		if err != nil || n < 1 || n > max {
			return 0, fmt.Errorf("%s must be between 1 and %d", key, max)
		}
		return n, nil
	}
	seconds, err := number("seconds", 10, 60)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	every, err := number("sample_every", 100, 1000000)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	maxEvents, err := number("max_events", 100000, 200000)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id := q.Get("id")
	if id == "" {
		id = fmt.Sprintf("%x", time.Now().UnixNano())
	}
	if !validID(id) {
		http.Error(w, "id must contain 1-64 letters, digits, underscores or hyphens", http.StatusBadRequest)
		return
	}
	if q.Get("trace") != "" && q.Get("trace") != "0" && q.Get("trace") != "1" {
		http.Error(w, "trace must be 0 or 1", http.StatusBadRequest)
		return
	}
	if q.Get("sockets") != "" && q.Get("sockets") != "0" && q.Get("sockets") != "1" {
		http.Error(w, "sockets must be 0 or 1", http.StatusBadRequest)
		return
	}
	if !httpBusy.CompareAndSwap(false, true) {
		http.Error(w, "capture or download already active", http.StatusConflict)
		return
	}
	defer httpBusy.Store(false)
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(time.Duration(seconds+30) * time.Second))
	c, err := Begin(Options{ID: id, SampleEvery: uint64(every), MaxEvents: maxEvents, Sockets: q.Get("sockets") == "1"})
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	defer c.Stop()
	tracing := q.Get("trace") == "1"
	traceStarted := false
	defer func() {
		if traceStarted {
			trace.Stop()
		}
	}()
	var buffer cappedTrace
	if tracing {
		if err := trace.Start(&buffer); err != nil {
			c.Stop()
			http.Error(w, "runtime trace already active or unavailable", http.StatusConflict)
			return
		}
		traceStarted = true
	}
	before := snapshot()
	timer := time.NewTimer(time.Duration(seconds) * time.Second)
	defer timer.Stop()
	canceled := false
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var sockets []SocketSample
	var hostSamples []map[string]any
collect:
	for {
		select {
		case <-timer.C:
			break collect
		case <-ticker.C:
			if c.opts.Sockets {
				sockets = append(sockets, c.socketSamples()...)
			}
			hostSamples = append(hostSamples, snapshot())
		case <-r.Context().Done():
			canceled = true
			break collect
		}
	}
	ticker.Stop()
	if c.opts.Sockets {
		sockets = append(sockets, c.socketSamples()...)
	}

	timer.Stop()
	report := c.Stop()
	if tracing {
		trace.Stop()
		traceStarted = false
	}
	after := snapshot()
	if canceled {
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Disposition", `attachment; filename="latency.zip"`)
	archive := zip.NewWriter(w)
	defer archive.Close()
	entry, err := archive.Create("events.json")
	if err != nil {
		return
	}
	if json.NewEncoder(entry).Encode(report) != nil {
		return
	}
	entry, err = archive.Create("manifest.json")
	if err != nil {
		return
	}
	manifest := map[string]any{
		"socket_samples": sockets, "socket_observations_omitted": c.socketOmitted, "host_samples": hostSamples,
		"schema_version": 1, "before": before, "after": after, "trace_requested": tracing, "trace_truncated": buffer.truncated,
		"sampled": every != 1, "events_lost": report.Dropped != 0, "unfinished": report.Unfinished,
		"clock":           "monotonic nanoseconds since started_utc; UTC alignment across hosts is approximate",
		"coverage_limits": []string{"startup/auth and replication tunnels excluded", "reads already blocked at capture start excluded", "unselected messages excluded", "kernel packet arrival/delivery and client scheduling not measured", "PostgreSQL wait includes server execution, transport and result decoding; not pure database CPU", "host snapshots are periodic and cannot attribute sub-second host scheduling delays"},
	}
	if json.NewEncoder(entry).Encode(manifest) != nil {
		return
	}
	if tracing {
		entry, err = archive.Create("runtime.trace")
		if err == nil {
			_, _ = entry.Write(buffer.data.Bytes())
		}
	}
}

func snapshot() map[string]any {
	host, _ := os.Hostname()
	result := map[string]any{"utc": time.Now().UTC(), "hostname": host, "pid": os.Getpid(), "go_version": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0), "goos": runtime.GOOS, "goarch": runtime.GOARCH}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" || s.Key == "vcs.modified" {
				result[s.Key] = s.Value
			}
		}
	}
	samples := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/cpu/classes/total:cpu-seconds"}, {Name: "/sched/goroutines:goroutines"}, {Name: "/sched/latencies:seconds"}, {Name: "/gc/pauses:seconds"}}
	metrics.Read(samples)
	for _, s := range samples {
		switch s.Value.Kind() {
		case metrics.KindUint64:
			result[s.Name] = s.Value.Uint64()
		case metrics.KindFloat64:
			result[s.Name] = s.Value.Float64()
		case metrics.KindFloat64Histogram:
			h := s.Value.Float64Histogram()
			bounds := make([]string, len(h.Buckets))
			for i, b := range h.Buckets {
				bounds[i] = strconv.FormatFloat(b, 'g', -1, 64)
			}
			result[s.Name] = map[string]any{"buckets": bounds, "counts": h.Counts}
		}
	}
	if runtime.GOOS == "linux" {
		for _, path := range []string{"/proc/self/schedstat", "/proc/pressure/cpu", "/proc/pressure/io", "/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/memory.events"} {
			f, err := os.Open(path)
			if err != nil {
				result[path] = "unavailable"
				continue
			}
			b, err := io.ReadAll(io.LimitReader(f, 16384))
			f.Close()
			if err == nil {
				result[path] = string(b)
			} else {
				result[path] = "unavailable"
			}
		}
	}
	return result
}
