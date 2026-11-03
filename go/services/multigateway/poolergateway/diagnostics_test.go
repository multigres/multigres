// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package poolergateway

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/tools/latencydiag"
)

type diagnosticSendFailure struct {
	pb.MultipoolerService_ExecuteStreamClient
}

func (s diagnosticSendFailure) Send(*pb.ExecuteStreamRequest) error {
	return errors.New("injected send failure")
}

func TestDiagnosticSendFailure(t *testing.T) {
	server := &streamTestServer{execute: func(*pb.StreamExecuteRequest, pb.MultipoolerService_StreamExecuteServer) error { return nil }}
	_, pool := connectStreamPool(t, server)
	require.NoError(t, streamQuery(t, pool, t.Context(), "SELECT 1"))
	require.Len(t, pool.idleSnapshot(), 1)
	pool.mu.Lock()
	pool.idle[0].stream = diagnosticSendFailure{pool.idle[0].stream}
	pool.mu.Unlock()
	capture, err := latencydiag.Begin(latencydiag.Options{ID: "send-failure", SampleEvery: 1, MaxEvents: 100})
	require.NoError(t, err)
	defer capture.Stop()
	root := latencydiag.Message(t.Context(), 1, 1)
	_, used, err := pool.Open(root.Context(), &pb.StreamExecuteRequest{Query: "SELECT 1"})
	require.True(t, used)
	require.Error(t, err)
	root.End(err, 0)
	report := capture.Stop()
	var failedOperation, failedSend bool
	for _, event := range report.Events {
		if event.Phase == "rpc.operation.client" {
			failedOperation = event.Failed
		}
		if event.Phase == "rpc.operation.send" {
			failedSend = event.Failed
		}
	}
	require.True(t, failedOperation, "Release must not mark a failed submission successful")
	require.True(t, failedSend)
	require.Zero(t, report.Unfinished)
}
