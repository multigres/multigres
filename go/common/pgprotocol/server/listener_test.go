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

package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/pid"
)

func testListenerWithGatewayID(t *testing.T, gatewayID uint32) *Listener {
	listener, err := NewListener(ListenerConfig{
		Address:      "localhost:0",
		Handler:      &mockHandler{},
		HashProvider: newMockHashProvider("postgres"),
		GatewayID:    gatewayID,
		Logger:       testLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		listener.Close()
	})
	return listener
}

func TestNextLocalID_WrapsToOne(t *testing.T) {
	l := testListenerWithGatewayID(t, 1)

	// Set counter just below the max so the next call wraps.
	l.nextConnectionID.Store(pid.MaxLocalConnID - 1)

	id1 := l.nextLocalID()
	assert.Equal(t, uint32(pid.MaxLocalConnID), id1)

	// Next call should wrap to 1, skipping 0.
	id2 := l.nextLocalID()
	assert.Equal(t, uint32(1), id2)
}

func TestNextLocalID_NeverReturnsZero(t *testing.T) {
	l := testListenerWithGatewayID(t, 1)

	// Set counter so the increment would land on MaxLocalConnID+1 which wraps.
	l.nextConnectionID.Store(pid.MaxLocalConnID)

	id := l.nextLocalID()
	assert.Equal(t, uint32(1), id)
}

func TestAssignConnectionID_EncodesGatewayPrefix(t *testing.T) {
	gatewayID := uint32(42)
	l := testListenerWithGatewayID(t, gatewayID)

	connID, ok := l.assignConnectionID()
	require.True(t, ok)

	prefix, localID := pid.DecodePID(connID)
	assert.Equal(t, gatewayID, prefix)
	assert.Greater(t, localID, uint32(0))
}

func TestAssignConnectionID_SkipsInUsePIDs(t *testing.T) {
	gatewayID := uint32(7)
	l := testListenerWithGatewayID(t, gatewayID)

	// Pre-register a connection at the first local ID (1) so it's occupied.
	occupiedPID := pid.EncodePID(gatewayID, 1)
	l.connsMu.Lock()
	l.conns[occupiedPID] = &Conn{} // placeholder
	l.connsMu.Unlock()

	connID, ok := l.assignConnectionID()
	require.True(t, ok)

	// The assigned PID must differ from the occupied one.
	assert.NotEqual(t, occupiedPID, connID)

	prefix, localID := pid.DecodePID(connID)
	assert.Equal(t, gatewayID, prefix)
	assert.Greater(t, localID, uint32(0))
}

func TestAssignConnectionID_FailsWhenAllIDsUsed(t *testing.T) {
	gatewayID := uint32(3)
	l := testListenerWithGatewayID(t, gatewayID)

	// Fill every possible local ID (1 through MaxLocalConnID).
	l.connsMu.Lock()
	for id := uint32(1); id <= pid.MaxLocalConnID; id++ {
		l.conns[pid.EncodePID(gatewayID, id)] = &Conn{}
	}
	l.connsMu.Unlock()

	_, ok := l.assignConnectionID()
	assert.False(t, ok, "should fail when all local IDs are in use")
}

func TestAssignConnectionID_WrapsAndFindsSlot(t *testing.T) {
	gatewayID := uint32(5)
	l := testListenerWithGatewayID(t, gatewayID)

	// Set counter near the max.
	l.nextConnectionID.Store(pid.MaxLocalConnID - 1)

	// Occupy MaxLocalConnID so the first attempt (at max) is taken.
	occupiedPID := pid.EncodePID(gatewayID, pid.MaxLocalConnID)
	l.connsMu.Lock()
	l.conns[occupiedPID] = &Conn{}
	l.connsMu.Unlock()

	connID, ok := l.assignConnectionID()
	require.True(t, ok)

	// After wrapping, it should land on local ID 1 (skipping the occupied max).
	prefix, localID := pid.DecodePID(connID)
	assert.Equal(t, gatewayID, prefix)
	assert.Equal(t, uint32(1), localID)
}
