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

package poolerserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

func (m *drainMockPoolManager) AddAdmitted(reservation bool) {
	if reservation {
		m.reservedAdd(1)
	} else {
		m.regularAdd(1)
	}
}

func (m *drainMockPoolManager) RemoveAdmitted(reservation bool) {
	if reservation {
		m.reservedAdd(-1)
	} else {
		m.regularAdd(-1)
	}
}

// admit ends an admission StartRequest granted at once, and returns its error.
func admit(a Admission, err error) error {
	if err == nil {
		a.Done()
	}
	return err
}

// requirePending asserts that done has not received within a short wait.
func requirePending(t *testing.T, done <-chan error, msg string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("%s: returned early (%v)", msg, err)
	case <-time.After(50 * time.Millisecond):
	}
}

// requireReturns waits for done and returns what it received.
func requireReturns(t *testing.T, done <-chan error, msg string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: did not return", msg)
		return nil
	}
}

// A request on an existing reserved connection is not counted (the pool
// counts its connection), nor is one admitted past the fence gate.
func TestStartRequest_WhatIsNotCounted(t *testing.T) {
	mock := newDrainMockPoolManager()
	s := newFencedTestPooler(t, mock, servingstate.RoutingRolePrimary)
	applyFence(t, s, false)
	adm, err := s.StartRequest(nil, RequestExistingReserved)
	require.NoError(t, err)
	assert.Equal(t, Admission{}, adm)
	require.NoError(t, s.startUnfencedRequest(nil, RequestNewReservation))
	mock.mu.Lock()
	defer mock.mu.Unlock()
	assert.Zero(t, mock.regularCount+mock.reservedCount)
}

// StartRequest counts what it admits as the connection it will hold, until
// Done.
func TestStartRequest_CountsUntilDone(t *testing.T) {
	mock := newDrainMockPoolManager()
	s := newFencedTestPooler(t, mock, servingstate.RoutingRolePrimary)
	applyFence(t, s, false)
	counts := func() (regular, reserved int64) {
		mock.mu.Lock()
		defer mock.mu.Unlock()
		return mock.regularCount, mock.reservedCount
	}
	single, err := s.StartRequest(nil, RequestSingleQuery)
	require.NoError(t, err)
	reservation, err := s.StartRequest(nil, RequestNewReservation)
	require.NoError(t, err)
	regular, reserved := counts()
	assert.Equal(t, [2]int64{1, 1}, [2]int64{regular, reserved})
	single.Done()
	reservation.Done()
	regular, reserved = counts()
	assert.Equal(t, [2]int64{0, 0}, [2]int64{regular, reserved})
}

// The fence drain waits for requests admitted before the gate closed that do
// not hold a connection yet: a BEGIN admitted just before the fence and still
// waiting for a connection, then a single query admitted during stage 1.
func TestFenceDrain_WaitsForAdmittedRequests(t *testing.T) {
	mock := newDrainMockPoolManager()
	s := newFencedTestPooler(t, mock, servingstate.RoutingRolePrimary)
	applyFence(t, s, false)

	begin, err := s.StartRequest(nil, RequestNewReservation)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- s.ApplyFence(context.Background(), s.FenceEpoch(), true, 5*time.Second)
	}()
	waitForFenceDrainPhase(t, s, drainReserved)
	requirePending(t, done, "a drain with a BEGIN waiting for its connection")

	single, err := s.StartRequest(nil, RequestSingleQuery)
	require.NoError(t, err, "stage 1 serves single queries")

	// The BEGIN gets its connection, which the pool now counts, and returns.
	mock.reservedAdd(1)
	begin.Done()
	requirePending(t, done, "a drain with the BEGIN's transaction open")
	s.mu.Lock()
	assert.Equal(t, drainReserved, s.fence.drainPhase)
	s.mu.Unlock()

	mock.reservedAdd(-1)
	waitForFenceDrainPhase(t, s, drainRegular)
	requirePending(t, done, "a drain with a single query admitted in stage 1")

	single.Done()
	require.NoError(t, requireReturns(t, done, "the fence drain"))
	assert.Zero(t, mock.closeReservedCallCount())
}

// The not-serving drain waits for admitted requests the same way.
func TestNotServingDrain_WaitsForAdmittedRequests(t *testing.T) {
	mock := newDrainMockPoolManager()
	s := newTestPoolerWithDrain(mock)
	require.NoError(t, s.OnStateChange(t.Context(), servingState(servingstate.RoutingRolePrimary)))

	begin, err := s.StartRequest(nil, RequestNewReservation)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		done <- s.OnStateChange(context.Background(), disabledState(servingstate.RoutingRolePrimary))
	}()
	waitForDrainPhase(t, s, drainReserved)
	requirePending(t, done, "a drain with a BEGIN waiting for its connection")

	single, err := s.StartRequest(nil, RequestSingleQuery)
	require.NoError(t, err)
	begin.Done()
	waitForDrainPhase(t, s, drainRegular)
	requirePending(t, done, "a drain with a single query admitted in stage 1")

	single.Done()
	require.NoError(t, requireReturns(t, done, "the not-serving drain"))
	assert.False(t, s.IsServing())
}
