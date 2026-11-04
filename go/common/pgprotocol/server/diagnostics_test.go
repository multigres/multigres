// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/tools/latencydiag"
)

type diagnosticHandler struct{ mockHandler }

func (*diagnosticHandler) HandleQuery(ctx context.Context, c *Conn, q string, callback func(context.Context, *sqltypes.Result) error) error {
	return callback(ctx, &sqltypes.Result{CommandTag: "SELECT 0"})
}

func TestDiagnosticTransportErrorAndBuffering(t *testing.T) {
	capture, err := latencydiag.Begin(latencydiag.Options{ID: "transport", SampleEvery: 1, MaxEvents: 100})
	require.NoError(t, err)
	defer capture.Stop()
	c := NewTestConn(bytes.NewBuffer(nil)).Conn
	root := latencydiag.Message(context.Background(), 1, 1)
	c.diagnostic.Store(root)
	// Partial frame must retain its error and produce a failed read span.
	_, err = c.ReadMessageType()
	require.ErrorIs(t, err, io.EOF)
	root.End(nil, 0)
	report := capture.Stop()
	require.Len(t, report.Events, 2)
	require.True(t, report.Events[0].Failed)
}

// Real loopback PGwire request/response without database execution. Reuse this
// benchmark in the base revision to measure the total disabled-path change.
func BenchmarkDiagnosticRoundTrip(b *testing.B) {
	for _, mode := range []string{"disabled", "sampled_100", "all"} {
		b.Run(mode, func(b *testing.B) {
			listener, err := NewListener(ListenerConfig{Address: "127.0.0.1:0", Handler: &diagnosticHandler{}, CredentialProvider: newMockCredentialProvider("local-test"), Logger: slog.New(slog.DiscardHandler)})
			if err != nil {
				b.Fatal(err)
			}
			go func() { _ = listener.Serve() }()
			defer listener.Close()
			ctx := context.Background()
			conn, err := client.Connect(ctx, ctx, &client.Config{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, User: "postgres", Password: "local-test", Database: "postgres"})
			if err != nil {
				b.Fatal(err)
			}
			defer conn.Close()
			var capture *latencydiag.Capture
			if mode != "disabled" {
				every := uint64(1)
				if mode == "sampled_100" {
					every = 100
				}
				capture, err = latencydiag.Begin(latencydiag.Options{ID: "bench", SampleEvery: every, MaxEvents: 200000})
				if err != nil {
					b.Fatal(err)
				}
				defer capture.Stop()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err = conn.Query(ctx, "SELECT 1"); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}

// A pipe supplies deterministic transport backpressure without relying on TCP
// buffer sizes. Delays are introduced only after the instrumented I/O starts.
type diagnosticBlockingConn struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (c *diagnosticBlockingConn) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Read(p)
}

func (c *diagnosticBlockingConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Write(p)
}

func TestDiagnosticTransportAttribution(t *testing.T) {
	for _, phase := range []string{"type", "body", "write"} {
		t.Run(phase, func(t *testing.T) {
			capture, err := latencydiag.Begin(latencydiag.Options{ID: "blocking", SampleEvery: 1, MaxEvents: 100})
			require.NoError(t, err)
			defer capture.Stop()
			local, peer := net.Pipe()
			defer local.Close()
			defer peer.Close()
			require.NoError(t, local.SetDeadline(time.Now().Add(3*time.Second)))
			require.NoError(t, peer.SetDeadline(time.Now().Add(3*time.Second)))
			blocked := &diagnosticBlockingConn{Conn: local, entered: make(chan struct{})}
			c := NewTestConn(bytes.NewBuffer(nil)).Conn
			c.conn = blocked
			c.diagnosticIO.owner = c
			c.bufferedReader = bufio.NewReader(&c.diagnosticIO)
			root := latencydiag.Message(t.Context(), 1, 1)
			c.diagnostic.Store(root)
			done := make(chan error, 1)
			go func() {
				var err error
				switch phase {
				case "type":
					_, err = c.ReadMessageType()
				case "body":
					_, err = c.readMessageBody(1)
				case "write":
					_, err = c.diagnosticIO.Write([]byte{1})
				}
				done <- err
			}()
			<-blocked.entered
			before := time.Now()
			time.Sleep(20 * time.Millisecond)
			injected := time.Since(before)
			if phase == "write" {
				_, err = io.ReadFull(peer, make([]byte, 1))
			} else {
				_, err = peer.Write([]byte{1})
			}
			require.NoError(t, err)
			require.NoError(t, <-done)
			root.End(nil, 0)
			report := capture.Stop()
			expected := "frontend.transport_read"
			if phase == "write" {
				expected = "frontend.transport_write"
			}
			found := false
			for _, event := range report.Events {
				if event.Phase == expected {
					found = true
					require.GreaterOrEqual(t, event.DurationNS, injected.Nanoseconds())
					require.False(t, event.Failed)
				}
			}
			require.True(t, found)
			require.Zero(t, report.Unfinished)
		})
	}
}

// Fixed-iteration runs (for example -benchtime=10000x) keep the full capture
// below its event limit. A run that fills the buffer fails rather than reporting
// misleading overhead for a mostly saturated recorder.
func BenchmarkDiagnosticConcurrentRoundTrip(b *testing.B) {
	for _, workers := range []int{1, 8, 32} {
		for _, mode := range []string{"disabled", "sampled_100", "all"} {
			b.Run(fmt.Sprintf("workers_%d/%s", workers, mode), func(b *testing.B) {
				listener, err := NewListener(ListenerConfig{Address: "127.0.0.1:0", Handler: &diagnosticHandler{}, CredentialProvider: newMockCredentialProvider("local-test"), Logger: slog.New(slog.DiscardHandler)})
				if err != nil {
					b.Fatal(err)
				}
				go func() { _ = listener.Serve() }()
				defer listener.Close()
				ctx := context.Background()
				conns := make([]*client.Conn, workers)
				// Sequential authentication avoids racing the mock provider's call log.
				for i := range conns {
					conns[i], err = client.Connect(ctx, ctx, &client.Config{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, User: "postgres", Password: "local-test", Database: "postgres"})
					if err != nil {
						b.Fatal(err)
					}
					defer conns[i].Close()
				}
				var capture *latencydiag.Capture
				if mode != "disabled" {
					every := uint64(1)
					if mode == "sampled_100" {
						every = 100
					}
					capture, err = latencydiag.Begin(latencydiag.Options{ID: "bench", SampleEvery: every, MaxEvents: 200000})
					if err != nil {
						b.Fatal(err)
					}
					defer capture.Stop()
				}
				var wg sync.WaitGroup
				failures := make(chan error, workers)
				b.ReportAllocs()
				b.ResetTimer()
				for worker, c := range conns {
					wg.Go(func() {
						for i := worker; i < b.N; i += workers {
							if _, err := c.Query(ctx, "SELECT 1"); err != nil {
								failures <- err
								return
							}
						}
					})
				}
				wg.Wait()
				b.StopTimer()
				close(failures)
				for err := range failures {
					b.Fatal(err)
				}
				if capture != nil {
					report := capture.Stop()
					if report.Dropped != 0 {
						b.Fatalf("capture overflow: %d dropped; use a smaller fixed iteration count", report.Dropped)
					}
					b.ReportMetric(float64(report.MessagesSelected)/float64(b.N), "selected/op")
				}
			})
		}
	}
}
