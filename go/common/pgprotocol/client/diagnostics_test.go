// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/tools/latencydiag"
)

func TestDiagnosticCallbackNestingAndNil(t *testing.T) {
	require.Nil(t, diagnosticCallback(nil), "a nil callback means discard results")
	capture, err := latencydiag.Begin(latencydiag.Options{ID: "callback", SampleEvery: 1, MaxEvents: 100})
	require.NoError(t, err)
	defer capture.Stop()
	root := latencydiag.Message(t.Context(), 1, 1)
	sentinel := errors.New("callback error")
	var blocked time.Duration
	cb := diagnosticCallback(func(ctx context.Context, _ *sqltypes.Result) error {
		_, child := latencydiag.Start(ctx, "delivery.child")
		before := time.Now()
		time.Sleep(20 * time.Millisecond)
		blocked = time.Since(before)
		child.End(sentinel, 0)
		return sentinel
	})
	require.ErrorIs(t, cb(root.Context(), &sqltypes.Result{}), sentinel)
	root.End(nil, 0)
	report := capture.Stop()
	require.Len(t, report.Events, 3)
	child, callback, parent := report.Events[0], report.Events[1], report.Events[2]
	require.Equal(t, callback.ID, child.Parent)
	require.Equal(t, parent.ID, callback.Parent)
	require.GreaterOrEqual(t, callback.DurationNS, blocked.Nanoseconds())
	require.True(t, callback.Failed)
}

// Exercise the real response loop with a delayed backend, both with a callback
// and in discard mode. Instrumentation must preserve the optional callback.
func TestDiagnosticBackendResponseDelay(t *testing.T) {
	for _, discard := range []bool{false, true} {
		t.Run(map[bool]string{false: "callback", true: "discard"}[discard], func(t *testing.T) {
			capture, err := latencydiag.Begin(latencydiag.Options{ID: "backend", SampleEvery: 1, MaxEvents: 100})
			require.NoError(t, err)
			defer capture.Stop()
			local, peer := net.Pipe()
			defer local.Close()
			defer peer.Close()
			require.NoError(t, local.SetDeadline(time.Now().Add(3*time.Second)))
			require.NoError(t, peer.SetDeadline(time.Now().Add(3*time.Second)))
			c := &Conn{}
			observed := &diagnosticReadConn{Conn: local, entered: make(chan struct{})}
			c.resetConn(observed)
			root := latencydiag.Message(t.Context(), 1, 1)
			var callback func(context.Context, *sqltypes.Result) error
			calls := 0
			if !discard {
				callback = func(context.Context, *sqltypes.Result) error { calls++; return nil }
			}
			done := make(chan error, 1)
			go func() { done <- c.processQueryResponses(root.Context(), callback) }()
			<-observed.entered
			before := time.Now()
			time.Sleep(20 * time.Millisecond)
			injected := time.Since(before)
			// CommandComplete("SELECT 0") and ReadyForQuery(idle).
			_, err = peer.Write([]byte{'C', 0, 0, 0, 13, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '0', 0, 'Z', 0, 0, 0, 5, 'I'})
			require.NoError(t, err)
			require.NoError(t, <-done)
			root.End(nil, 0)
			report := capture.Stop()
			response := false
			for _, event := range report.Events {
				if event.Phase == "postgres.response" {
					response = true
					require.GreaterOrEqual(t, event.DurationNS, injected.Nanoseconds())
				}
			}
			require.True(t, response)
			if discard {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 1, calls)
			}
		})
	}
}

type diagnosticReadConn struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (c *diagnosticReadConn) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Read(p)
}
