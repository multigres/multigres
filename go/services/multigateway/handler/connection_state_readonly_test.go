// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ReadOnlyOverlay rides the transaction frames like SessionSettings: a RESET
// ALL inside a transaction clears it, and ROLLBACK (or ROLLBACK TO) puts it
// back, exactly as PostgreSQL restores the backend's GUCs.
func TestConnectionState_ReadOnlyOverlayFollowsTransactionFrames(t *testing.T) {
	state := NewMultigatewayConnectionState()
	state.ReadOnlyOverlay = true

	state.BeginTransaction()
	state.ReadOnlyOverlay = false // RESET ALL ran on the backend
	state.RollbackTransaction()
	require.True(t, state.ReadOnlyOverlay, "ROLLBACK restores the pre-BEGIN record")

	state.BeginTransaction()
	state.PushSavepoint("sp")
	state.ReadOnlyOverlay = false
	state.RollbackToSavepoint("sp")
	require.True(t, state.ReadOnlyOverlay, "ROLLBACK TO restores the savepoint record")
	state.ReadOnlyOverlay = false
	state.CommitTransaction()
	require.False(t, state.ReadOnlyOverlay, "COMMIT keeps the current record")
}
