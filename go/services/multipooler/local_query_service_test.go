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

package multipooler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// blockingQueryService answers ExecuteQuery once release is closed, reporting
// on started that the call is running. Other methods are not used.
type blockingQueryService struct {
	queryservice.QueryService
	started chan struct{}
	release chan struct{}
}

func (b *blockingQueryService) ExecuteQuery(context.Context, *query.Target, string, *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	close(b.started)
	<-b.release
	return &sqltypes.Result{}, nil, nil
}

func newBlockingQueryService() *blockingQueryService {
	return &blockingQueryService{started: make(chan struct{}), release: make(chan struct{})}
}

func requireNotServing(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	assert.True(t, mterrors.IsPreExecutionUnavailable(err), "refusal must be retryable as not executed: %v", err)
}

func TestLocalQueryService_RefusedBeforeRegistration(t *testing.T) {
	var l localQueryService
	_, _, err := l.ExecuteQuery(t.Context(), &query.Target{}, "select 1", nil)
	requireNotServing(t, err)
}

func TestLocalQueryService_RefusedAfterStop(t *testing.T) {
	l := localQueryService{inner: newBlockingQueryService()}
	l.Stop() // nothing in flight: returns at once
	_, _, err := l.ExecuteQuery(t.Context(), &query.Target{}, "select 1", nil)
	requireNotServing(t, err)
}

// TestLocalQueryService_StopWaitsForInFlight checks that Stop waits for a
// running call to return, as the gRPC server's graceful stop does, and that
// the running call completes normally.
func TestLocalQueryService_StopWaitsForInFlight(t *testing.T) {
	inner := newBlockingQueryService()
	l := localQueryService{inner: inner}

	callErr := make(chan error, 1)
	go func() {
		_, _, err := l.ExecuteQuery(t.Context(), &query.Target{}, "select 1", nil)
		callErr <- err
	}()
	<-inner.started

	stopped := make(chan struct{})
	go func() {
		l.Stop()
		close(stopped)
	}()

	// New calls are refused as soon as Stop begins, while the running one
	// holds Stop open.
	require.Eventually(t, func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.stopped
	}, 5*time.Second, time.Millisecond)
	_, _, err := l.ExecuteQuery(t.Context(), &query.Target{}, "select 1", nil)
	requireNotServing(t, err)
	select {
	case <-stopped:
		t.Fatal("Stop returned while a call was running")
	case <-time.After(50 * time.Millisecond):
	}

	close(inner.release)
	require.NoError(t, <-callErr)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the running call finished")
	}
}
