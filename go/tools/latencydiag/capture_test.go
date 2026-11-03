// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package latencydiag

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func capture(t *testing.T, every uint64, limit int) *Capture {
	t.Helper()
	c, err := Begin(Options{ID: "test", SampleEvery: every, MaxEvents: limit})
	require.NoError(t, err)
	t.Cleanup(func() {
		if active.Load() == c {
			c.Stop()
		}
	})
	return c
}

func TestInactiveNoAllocations(t *testing.T) {
	ctx := context.Background()
	n := testing.AllocsPerRun(1000, func() {
		next, s := Start(ctx, "phase")
		s.End(nil, 0)
		if next != ctx {
			panic("context changed")
		}
		Message(ctx, 1, 1).End(nil, 0)
	})
	require.Zero(t, n)
}

func TestCaptureNestingAndBounds(t *testing.T) {
	c := capture(t, 1, 2)
	root := Message(context.Background(), 7, 3)
	_, child := Start(root.Context(), "phase")
	child.End(errors.New("secret SQL must not appear"), 9)
	child.End(nil, 0)
	root.End(nil, 0)
	Message(context.Background(), 7, 4).End(nil, 0)
	report := c.Stop()
	require.Len(t, report.Events, 2)
	require.EqualValues(t, 3, report.SpansStarted)
	require.EqualValues(t, 3, report.SpansFinished)
	require.EqualValues(t, 1, report.Dropped)
	require.Zero(t, report.Unfinished)
	require.Equal(t, report.Events[1].ID, report.Events[0].Parent)
	require.True(t, report.Events[0].Failed)
	data, err := json.Marshal(report)
	require.NoError(t, err)
	require.NotContains(t, string(data), "secret SQL")
}

func TestSamplingAndWindowCrossing(t *testing.T) {
	c := capture(t, 10, 100)
	var pending *Span
	selected := 0
	for i := range uint64(100) {
		s := Message(context.Background(), 1, i)
		if s != nil {
			selected++
			if pending != nil {
				pending.End(nil, 0)
			}
			pending = s
		}
	}
	report := c.Stop()
	require.EqualValues(t, 100, report.MessagesSeen)
	require.EqualValues(t, selected, report.MessagesSelected)
	require.Greater(t, selected, 1)
	require.Less(t, selected, 30)
	require.EqualValues(t, 1, report.Unfinished)
	pending.End(nil, 0)
	require.Len(t, report.Events, selected-1)
	_, s := Start(pending.Context(), "late")
	require.Nil(t, s)
}

func TestConcurrentStop(t *testing.T) {
	c := capture(t, 1, 1000)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for j := range 1000 {
				s := Message(context.Background(), 1, uint64(j))
				if s != nil {
					_, child := Start(s.Context(), "child")
					child.End(nil, 0)
					s.End(nil, 0)
				}
			}
		})
	}
	time.Sleep(time.Millisecond)
	report := c.Stop()
	wg.Wait()
	require.Equal(t, report.SpansStarted, report.SpansFinished+report.Unfinished)
	require.Equal(t, report.SpansFinished, uint64(len(report.Events))+report.Dropped)
}

func TestCaptureValidationAndExclusion(t *testing.T) {
	_, err := Begin(Options{ID: "bad\n", SampleEvery: 1, MaxEvents: 1})
	require.Error(t, err)
	c := capture(t, 1, 10)
	_, err = Begin(c.opts)
	require.Error(t, err)
	c.Stop()
	d := capture(t, 1, 10)
	require.NotSame(t, c, d)
}

func TestHTTPValidationCancellationAndArchive(t *testing.T) {
	for _, path := range []string{"/?seconds=0", "/?seconds=61", "/?sample_every=-1", "/?max_events=999999", "/?trace=yes", "/?id=%0A"} {
		w := httptest.NewRecorder()
		Handler(w, httptest.NewRequest("GET", path, nil))
		require.Equal(t, 400, w.Code, path)
		require.False(t, Active())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	Handler(httptest.NewRecorder(), httptest.NewRequest("GET", "/?seconds=60", nil).WithContext(ctx))
	require.False(t, Active())
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		Handler(w, httptest.NewRequest("GET", "/?seconds=1&sample_every=1&trace=1&id=test", nil))
		close(done)
	}()
	require.Eventually(t, Active, time.Second, time.Millisecond)
	Message(context.Background(), 1, 1).End(nil, 0)
	<-done
	require.Equal(t, 200, w.Code)
	archive, err := zip.NewReader(bytes.NewReader(w.Body.Bytes()), int64(w.Body.Len()))
	require.NoError(t, err)
	require.Len(t, archive.File, 3)
	for _, f := range archive.File {
		r, err := f.Open()
		require.NoError(t, err)
		data, err := io.ReadAll(r)
		require.NoError(t, err)
		r.Close()
		require.NotEmpty(t, data)
		if f.Name == "events.json" {
			var result Report
			require.NoError(t, json.Unmarshal(data, &result))
			require.NotEmpty(t, result.Events)
		}
		if f.Name == "manifest.json" {
			var result map[string]any
			require.NoError(t, json.Unmarshal(data, &result))
			require.Equal(t, false, result["trace_truncated"])
		}
	}
	require.False(t, Active())
}

func TestTraceSizeBound(t *testing.T) {
	var b cappedTrace
	data := make([]byte, maxTraceBytes+10)
	n, err := b.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.True(t, b.truncated)
	require.Equal(t, maxTraceBytes, b.data.Len())
}

func BenchmarkProbe(b *testing.B) {
	for _, mode := range []string{"disabled", "sampled_100", "all"} {
		b.Run(mode, func(b *testing.B) {
			var c *Capture
			if mode != "disabled" {
				every := uint64(1)
				if mode == "sampled_100" {
					every = 100
				}
				c, _ = Begin(Options{ID: "bench", SampleEvery: every, MaxEvents: 1})
				defer c.Stop()
			}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				root := Message(ctx, 1, uint64(i))
				childCtx := ctx
				if root != nil {
					childCtx = root.Context()
				}
				_, s := Start(childCtx, "phase")
				s.End(nil, 0)
				root.End(nil, 0)
			}
		})
	}
}

func TestStopTransfersBufferAndDoesNotStopNextCapture(t *testing.T) {
	c := capture(t, 1, 100)
	root := Message(t.Context(), 1, 1)
	root.End(nil, 0)
	report := c.Stop()
	require.Len(t, report.Events, 1)
	require.Nil(t, c.events)
	next := capture(t, 1, 100)
	require.Empty(t, c.Stop().Events)
	require.True(t, Active(), "deferred cleanup must not stop a newer capture")
	require.Same(t, next, active.Load())
	require.Len(t, report.Events, 1, "the transferred buffer remains valid")
}
