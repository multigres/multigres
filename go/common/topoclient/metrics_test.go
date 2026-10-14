// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package topoclient

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/pb/mtrpc"
)

func TestClassifyResult(t *testing.T) {
	type testCase struct {
		name string
		err  error
		want OperationResult
	}
	tests := []testCase{
		{"nil", nil, OpResultSuccess},
		{"no node", NewError(NoNode, "x"), OpResultExpectedError},
		{"node exists", NewError(NodeExists, "x"), OpResultExpectedError},
		{"bad version", NewError(BadVersion, "x"), OpResultExpectedError},
		{"no update needed", NewError(NoUpdateNeeded, "x"), OpResultExpectedError},
		{"wrapped no node", fmt.Errorf("wrap: %w", NewError(NoNode, "x")), OpResultExpectedError},
		{"topo timeout", NewError(Timeout, "x"), OpResultTimeout},
		{"deadline exceeded", context.DeadlineExceeded, OpResultTimeout},
		{"wrapped deadline", fmt.Errorf("wrap: %w", context.DeadlineExceeded), OpResultTimeout},
		{"mterrors deadline", mterrors.New(mtrpc.Code_DEADLINE_EXCEEDED, "deadline"), OpResultTimeout},
		{"wrapped mterrors deadline", fmt.Errorf("wrap: %w", mterrors.New(mtrpc.Code_DEADLINE_EXCEEDED, "deadline")), OpResultTimeout},
		{"grpc deadline", status.Error(codes.DeadlineExceeded, "deadline"), OpResultTimeout},
		{"wrapped grpc deadline", fmt.Errorf("wrap: %w", status.Error(codes.DeadlineExceeded, "deadline")), OpResultTimeout},
		{"canceled", context.Canceled, OpResultExpectedError},
		{"wrapped canceled", fmt.Errorf("wrap: %w", context.Canceled), OpResultExpectedError},
		{"mterrors canceled", mterrors.New(mtrpc.Code_CANCELED, "canceled"), OpResultExpectedError},
		{"grpc canceled", status.Error(codes.Canceled, "canceled"), OpResultExpectedError},
		{"interrupted", NewError(Interrupted, "x"), OpResultExpectedError},
		{"unavailable", mterrors.Errorf(mtrpc.Code_UNAVAILABLE, "no connection available"), OpResultError},
		{"other topo error", NewError(NodeNotEmpty, "x"), OpResultError},
		{"generic", errors.New("boom"), OpResultError},
	}
	for _, code := range []ErrorCode{NoNode, NodeExists, BadVersion, NoUpdateNeeded, Interrupted, Timeout, NodeNotEmpty} {
		want := OpResultExpectedError
		switch code {
		case Timeout:
			want = OpResultTimeout
		case NodeNotEmpty:
			want = OpResultError
		}
		pointer := &TopoError{Code: code, Message: "x"}
		tests = append(tests,
			testCase{fmt.Sprintf("pointer topo %d", code), pointer, want},
			testCase{fmt.Sprintf("wrapped pointer topo %d", code), fmt.Errorf("wrap: %w", pointer), want},
			testCase{fmt.Sprintf("wrapped value topo %d", code), fmt.Errorf("wrap: %w", NewError(code, "x")), want},
		)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, classifyResult(tt.err))
		})
	}
}

// installTestMetrics swaps the package singleton for one backed by an
// in-memory reader and restores it on cleanup.
func installTestMetrics(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	orig := metrics()
	metricsPtr.Store(newMetricsWithMeter(provider.Meter("test")))
	t.Cleanup(func() {
		metricsPtr.Store(orig)
		_ = provider.Shutdown(context.Background())
	})
	return reader
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func histCount(m metricdata.Metrics, attrs ...attribute.KeyValue) uint64 {
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		return 0
	}
	want := attribute.NewSet(attrs...)
	for _, dp := range h.DataPoints {
		if dp.Attributes.Equals(&want) {
			return dp.Count
		}
	}
	return 0
}

func sumValue(m metricdata.Metrics, attrs ...attribute.KeyValue) int64 {
	s, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		return 0
	}
	want := attribute.NewSet(attrs...)
	for _, dp := range s.DataPoints {
		if dp.Attributes.Equals(&want) {
			return dp.Value
		}
	}
	return 0
}

func TestWrapperConn_RecordsOperationDuration(t *testing.T) {
	reader := installTestMetrics(t)
	factory := newMockFactory()
	wrapper := NewWrapperConn(factory.newConn, nil)
	ctx := context.Background()

	require.NoError(t, wrapper.PutEphemeral(ctx, "a/b", []byte("x")))
	_, _, err := wrapper.Get(ctx, "a/b")
	require.NoError(t, err)

	got := collect(t, reader)
	op := got["topoclient.operation.duration"]
	assert.Equal(t, uint64(1), histCount(op,
		attribute.String("operation", "put_ephemeral"), attribute.String("result", "success")))
	assert.Equal(t, uint64(1), histCount(op,
		attribute.String("operation", "get"), attribute.String("result", "success")))
}

func TestWrapperConn_RecordsNoConnectionAndConnectionFailure(t *testing.T) {
	reader := installTestMetrics(t)
	factory := newMockFactory()
	factory.setShouldFail(true)
	wrapper := NewWrapperConn(factory.newConn, nil)
	t.Cleanup(func() { _ = wrapper.Close() })

	_, _, err := wrapper.Get(context.Background(), "a/b")
	require.Error(t, err)

	got := collect(t, reader)
	assert.Equal(t, uint64(1), histCount(got["topoclient.operation.duration"],
		attribute.String("operation", "get"), attribute.String("result", "error")))
	// The failure is counted when the reconnect starts, which happens in a goroutine.
	assert.Eventually(t, func() bool {
		return sumValue(collect(t, reader)["topoclient.connection.failures"],
			attribute.String("reason", "no_connection")) >= 1
	}, 5*time.Second, 10*time.Millisecond)
}

func TestWrapperConn_ConnectionFailureCountedOncePerReconnect(t *testing.T) {
	reader := installTestMetrics(t)
	factory := newMockFactory()
	factory.setShouldFail(true)
	wrapper := NewWrapperConn(factory.newConn, nil)
	t.Cleanup(func() { _ = wrapper.Close() })

	for range 20 {
		_, _, err := wrapper.Get(context.Background(), "a/b")
		require.Error(t, err)
	}

	assert.Eventually(t, func() bool {
		return sumValue(collect(t, reader)["topoclient.connection.failures"],
			attribute.String("reason", "no_connection")) >= 1
	}, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, int64(1), sumValue(collect(t, reader)["topoclient.connection.failures"],
		attribute.String("reason", "no_connection")))
}

func TestWrapperConn_NotFoundIsExpectedError(t *testing.T) {
	reader := installTestMetrics(t)
	wrapper := NewWrapperConn(func() (Conn, error) {
		return &operationErrorConn{Conn: newMockConn(1), err: NewError(NoNode, "does/not/exist")}, nil
	}, nil)
	t.Cleanup(func() { _ = wrapper.Close() })
	ctx := context.Background()

	_, _, err := wrapper.Get(ctx, "does/not/exist")
	require.Error(t, err)

	got := collect(t, reader)
	op := got["topoclient.operation.duration"]
	assert.Equal(t, uint64(1), histCount(op,
		attribute.String("operation", "get"), attribute.String("result", "expected_error")))
}

func TestWrapperConn_BlockingLocksExcludedFromOperationDuration(t *testing.T) {
	reader := installTestMetrics(t)
	wrapper := NewWrapperConn(newMockFactory().newConn, nil)
	t.Cleanup(func() { _ = wrapper.Close() })
	ctx := t.Context()

	_, err := wrapper.Lock(ctx, "x", "owner")
	require.NoError(t, err)
	_, err = wrapper.LockWithTTL(ctx, "x", "owner", time.Second)
	require.NoError(t, err)
	_, err = wrapper.LockName(ctx, "x", "owner")
	require.NoError(t, err)
	_, err = wrapper.LockNameWithTTL(ctx, "x", "owner", time.Second)
	require.NoError(t, err)

	got := collect(t, reader)
	_, recorded := got["topoclient.operation.duration"]
	assert.False(t, recorded, "blocking lock acquisition must not contribute to backend operation duration")

	_, err = wrapper.TryLock(ctx, "x", "owner")
	require.NoError(t, err)
	_, err = wrapper.TryLockName(ctx, "x", "owner")
	require.NoError(t, err)
	_, err = wrapper.TryLockWithLease(ctx, "x", "owner", time.Second)
	require.NoError(t, err)
	op := collect(t, reader)["topoclient.operation.duration"]
	for _, operation := range []Operation{OpTryLock, OpTryLockName, OpTryLockWithLease} {
		assert.Equal(t, uint64(1), histCount(op,
			attribute.String("operation", string(operation)), attribute.String("result", "success")))
	}
}

func TestRecordCounters(t *testing.T) {
	reader := installTestMetrics(t)
	ctx := context.Background()

	RecordRegistrationFailure(ctx, RegistrationPhaseReassert)
	recordWatchReconnect(ctx, WatchReconnectStreamEnded)
	recordLeaseLost(ctx)

	got := collect(t, reader)
	assert.Equal(t, int64(1), sumValue(got["topoclient.registration.failures"], attribute.String("phase", "reassert")))
	assert.Equal(t, int64(1), sumValue(got["topoclient.watch.reconnects"], attribute.String("reason", "stream_ended")))
	assert.Equal(t, int64(1), sumValue(got["topoclient.lease.lost"]))
}

// operationErrorConn returns a fixed error from Get.
type operationErrorConn struct {
	Conn
	err error
}

func (c *operationErrorConn) Get(context.Context, string) ([]byte, Version, error) {
	return nil, nil, c.err
}
