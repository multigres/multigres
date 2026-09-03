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

	"github.com/multigres/multigres/go/pb/query"
)

func TestConnectionStateGetSetSettings(t *testing.T) {
	state := NewConnectionState()
	assert.Nil(t, state.GetSettings(), "fresh state has no settings")

	s := NewSettings(map[string]string{"timezone": "UTC"}, 1)
	state.SetSettings(s)
	assert.Same(t, s, state.GetSettings())
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
	assert.Nil(t, state.GetSettings())
	assert.NotPanics(t, func() {
		state.SetSettings(NewSettings(nil, 0))
		state.Close()
	})
}

func TestConnectionStatePreparedStatementNames(t *testing.T) {
	var nilState *ConnectionState
	assert.Nil(t, nilState.PreparedStatementNames())

	state := NewConnectionState()
	assert.Empty(t, state.PreparedStatementNames())

	state.StorePreparedStatement(&query.PreparedStatement{Name: "ppstmt1"})
	state.StorePreparedStatement(&query.PreparedStatement{Name: ""})
	state.DeletePreparedStatement("ppstmt1")
	state.StorePreparedStatement(&query.PreparedStatement{Name: "ppstmt2"})
	assert.ElementsMatch(t, []string{"", "ppstmt2"}, state.PreparedStatementNames())
}
