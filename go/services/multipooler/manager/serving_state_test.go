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

package manager

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// testComponent records state transitions for testing.
type testComponent struct {
	lastType   clustermetadatapb.PoolerType
	lastStatus clustermetadatapb.PoolerServingStatus
	callCount  int
	err        error // if set, OnStateChange returns this error
}

func (c *testComponent) OnStateChange(_ context.Context, poolerType clustermetadatapb.PoolerType, servingStatus clustermetadatapb.PoolerServingStatus) error {
	c.callCount++
	if c.err != nil {
		return c.err
	}
	c.lastType = poolerType
	c.lastStatus = servingStatus
	return nil
}

// slowComponent tracks concurrent execution via an atomic counter.
type slowComponent struct {
	called atomic.Bool
}

func (c *slowComponent) OnStateChange(_ context.Context, _ clustermetadatapb.PoolerType, _ clustermetadatapb.PoolerServingStatus) error {
	c.called.Store(true)
	return nil
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestMultiPooler(poolerType clustermetadatapb.PoolerType, status clustermetadatapb.PoolerServingStatus) *clustermetadatapb.MultiPooler {
	return &clustermetadatapb.MultiPooler{
		Type:          poolerType,
		ServingStatus: status,
	}
}

func TestServingStateManager_SetState_PrimaryServing(t *testing.T) {
	comp := &testComponent{}
	mp := newTestMultiPooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_NOT_SERVING)

	ssm := NewServingStateManager(newTestLogger(), mp, comp)

	err := ssm.SetState(context.Background(), clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
	require.NoError(t, err)

	// Component should receive the target state.
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, comp.lastType)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, comp.lastStatus)
	assert.Equal(t, 1, comp.callCount)

	// Multipooler record should be updated.
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, mp.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, mp.ServingStatus)
}

func TestServingStateManager_SetState_NotServing(t *testing.T) {
	comp := &testComponent{}
	mp := newTestMultiPooler(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)

	ssm := NewServingStateManager(newTestLogger(), mp, comp)

	err := ssm.SetState(context.Background(), clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_NOT_SERVING)
	require.NoError(t, err)

	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, comp.lastType)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_NOT_SERVING, comp.lastStatus)

	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, mp.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_NOT_SERVING, mp.ServingStatus)
}

func TestServingStateManager_SetState_ComponentError(t *testing.T) {
	comp := &testComponent{err: errors.New("transition failed")}
	mp := newTestMultiPooler(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)

	ssm := NewServingStateManager(newTestLogger(), mp, comp)

	err := ssm.SetState(context.Background(), clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transition failed")

	// Multipooler record should NOT be updated on error.
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, mp.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, mp.ServingStatus)
}

func TestServingStateManager_DemotionFlow(t *testing.T) {
	// Simulate the demotion flow:
	// Step 1: (PRIMARY, SERVING) -> (PRIMARY, NOT_SERVING)
	// Step 2: (PRIMARY, NOT_SERVING) -> (REPLICA, SERVING)
	comp := &testComponent{}
	mp := newTestMultiPooler(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)

	ssm := NewServingStateManager(newTestLogger(), mp, comp)

	// Step 1: Stop serving
	err := ssm.SetState(context.Background(), clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_NOT_SERVING)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, mp.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_NOT_SERVING, mp.ServingStatus)

	// Step 2: Transition to replica serving
	err = ssm.SetState(context.Background(), clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, mp.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, mp.ServingStatus)
	assert.Equal(t, 2, comp.callCount)
}

func TestServingStateManager_MultipleComponents(t *testing.T) {
	comp1 := &testComponent{}
	comp2 := &testComponent{}
	mp := newTestMultiPooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_NOT_SERVING)

	ssm := NewServingStateManager(newTestLogger(), mp, comp1, comp2)

	err := ssm.SetState(context.Background(), clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
	require.NoError(t, err)

	// Both components should have been called.
	assert.Equal(t, 1, comp1.callCount)
	assert.Equal(t, 1, comp2.callCount)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, comp1.lastType)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, comp2.lastType)
}

func TestServingStateManager_MultipleComponents_OneError(t *testing.T) {
	comp1 := &testComponent{}
	comp2 := &testComponent{err: errors.New("comp2 failed")}
	mp := newTestMultiPooler(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)

	ssm := NewServingStateManager(newTestLogger(), mp, comp1, comp2)

	err := ssm.SetState(context.Background(), clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "comp2 failed")

	// Multipooler record should NOT be updated when any component fails.
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, mp.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, mp.ServingStatus)
}

func TestServingStateManager_Register(t *testing.T) {
	comp1 := &testComponent{}
	comp2 := &testComponent{}
	mp := newTestMultiPooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_NOT_SERVING)

	ssm := NewServingStateManager(newTestLogger(), mp, comp1)

	// Register a second component after creation.
	ssm.Register(comp2)

	err := ssm.SetState(context.Background(), clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
	require.NoError(t, err)

	assert.Equal(t, 1, comp1.callCount)
	assert.Equal(t, 1, comp2.callCount)
}

func TestServingStateManager_NoComponents(t *testing.T) {
	mp := newTestMultiPooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_NOT_SERVING)

	ssm := NewServingStateManager(newTestLogger(), mp)

	err := ssm.SetState(context.Background(), clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
	require.NoError(t, err)

	// Multipooler record should still be updated.
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, mp.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, mp.ServingStatus)
}

func TestServingStateManager_ParallelExecution(t *testing.T) {
	// Verify both components are invoked (they run in parallel via errgroup).
	comp1 := &slowComponent{}
	comp2 := &slowComponent{}
	mp := newTestMultiPooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_NOT_SERVING)

	ssm := NewServingStateManager(newTestLogger(), mp, comp1, comp2)

	err := ssm.SetState(context.Background(), clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
	require.NoError(t, err)

	assert.True(t, comp1.called.Load())
	assert.True(t, comp2.called.Load())
}
