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

package connstate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectionStateGetSetSettings(t *testing.T) {
	state := NewConnectionState()
	assert.Nil(t, state.GetSettings(), "fresh state has no settings")

	s := NewSettings(map[string]string{"timezone": "UTC"}, 1)
	state.SetSettings(s)
	assert.Same(t, s, state.GetSettings())
}

func TestConnectionStateSnapshotRestore(t *testing.T) {
	original := NewSettings(map[string]string{"timezone": "UTC"}, 1)
	state := NewConnectionStateWithSettings(original)

	// Snapshot the pre-transaction settings, then mutate as a transaction would.
	snap := state.SnapshotForTxn()
	assert.NotNil(t, snap)

	inTxn := NewSettings(map[string]string{"timezone": "America/New_York"}, 2)
	state.SetSettings(inTxn)
	assert.Same(t, inTxn, state.GetSettings())

	// A ROLLBACK restores the snapshot so the pool's view matches the backend.
	state.RestoreFromTxn(snap)
	assert.Same(t, original, state.GetSettings())
}

func TestConnectionStateRestoreNilSnapshotIsNoop(t *testing.T) {
	s := NewSettings(map[string]string{"timezone": "UTC"}, 1)
	state := NewConnectionStateWithSettings(s)

	state.RestoreFromTxn(nil)
	assert.Same(t, s, state.GetSettings(), "nil snapshot must not clobber settings")
}

func TestConnectionStateClose(t *testing.T) {
	state := NewConnectionStateWithSettings(NewSettings(map[string]string{"timezone": "UTC"}, 1))
	state.Close()
	assert.Nil(t, state.GetSettings())
	assert.Nil(t, state.PreparedStatements)
}

func TestConnectionStateNilReceiverSafe(t *testing.T) {
	var state *ConnectionState

	// Every method must tolerate a nil receiver without panicking.
	assert.Nil(t, state.SnapshotForTxn())
	assert.Nil(t, state.GetSettings())
	assert.NotPanics(t, func() {
		state.SetSettings(NewSettings(nil, 0))
		state.RestoreFromTxn(&TxnSnapshot{})
		state.Close()
	})
}
