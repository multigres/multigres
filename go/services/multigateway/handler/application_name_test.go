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

package handler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConnectionState_ApplicationName(t *testing.T) {
	t.Run("returns default when not set", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.InitApplicationName("startup-val")
		require.Equal(t, "startup-val", s.GetApplicationName())
		require.Equal(t, "startup-val", s.ShowApplicationName())
	})

	t.Run("empty default mirrors PG default", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.InitApplicationName("")
		require.Equal(t, "", s.GetApplicationName())
		require.Equal(t, "", s.ShowApplicationName())
	})

	t.Run("set overrides default", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.InitApplicationName("startup-val")
		s.SetApplicationName("myapp")
		require.Equal(t, "myapp", s.GetApplicationName())
		require.Equal(t, "myapp", s.ShowApplicationName())
	})

	t.Run("set empty string is honoured", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.InitApplicationName("startup-val")
		s.SetApplicationName("")
		require.Equal(t, "", s.GetApplicationName(),
			"explicit SET to empty string overrides non-empty default")
	})

	t.Run("reset reverts to default", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.InitApplicationName("startup-val")
		s.SetApplicationName("myapp")
		s.ResetApplicationName()
		require.Equal(t, "startup-val", s.GetApplicationName())
	})

	t.Run("set local overrides session and shows local value", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.InitApplicationName("startup-val")
		s.SetApplicationName("session-val")
		s.SetLocalApplicationName("local-val")
		require.Equal(t, "local-val", s.GetApplicationName())
		require.Equal(t, "local-val", s.ShowApplicationName())
	})

	t.Run("ResetAllLocalGUCs clears local but keeps session", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.InitApplicationName("startup-val")
		s.SetApplicationName("session-val")
		s.SetLocalApplicationName("local-val")

		s.ResetAllLocalGUCs()
		require.Equal(t, "session-val", s.GetApplicationName())
	})

	t.Run("ResetAllLocalGUCs reverts to default when no session set", func(t *testing.T) {
		s := NewMultiGatewayConnectionState()
		s.InitApplicationName("startup-val")
		s.SetLocalApplicationName("local-val")

		s.ResetAllLocalGUCs()
		require.Equal(t, "startup-val", s.GetApplicationName())
	})

	t.Run("session set supersedes active local override", func(t *testing.T) {
		// Mirrors PG: SET inside a transaction with a prior SET LOCAL
		// supersedes the LOCAL — effective value is the new session value.
		s := NewMultiGatewayConnectionState()
		s.InitApplicationName("startup-val")
		s.SetLocalApplicationName("local-val")
		s.SetApplicationName("session-val")
		require.Equal(t, "session-val", s.GetApplicationName())
	})

	t.Run("session reset supersedes active local override", func(t *testing.T) {
		// Mirrors PG: RESET inside a transaction with a prior SET LOCAL
		// supersedes the LOCAL — effective value is the default.
		s := NewMultiGatewayConnectionState()
		s.InitApplicationName("startup-val")
		s.SetApplicationName("session-val")
		s.SetLocalApplicationName("local-val")
		s.ResetApplicationName()
		require.Equal(t, "startup-val", s.GetApplicationName())
	})

	t.Run("SetLocalApplicationNameToDefault masks session", func(t *testing.T) {
		// PG semantics: SET LOCAL var TO DEFAULT installs a transaction-scoped
		// override equal to the default, masking the session value during the
		// transaction. The session value must be preserved for restoration on
		// COMMIT/ROLLBACK (when ResetAllLocalGUCs fires).
		s := NewMultiGatewayConnectionState()
		s.InitApplicationName("startup-val")
		s.SetApplicationName("session-val")

		s.SetLocalApplicationNameToDefault()
		require.Equal(t, "startup-val", s.GetApplicationName(),
			"during txn: effective value is the default")

		s.ResetAllLocalGUCs()
		require.Equal(t, "session-val", s.GetApplicationName(),
			"after txn end: session value is restored, not lost")
	})
}

// --- Cross-GMV savepoint / transaction lifecycle tests ---
//
// These verify that the gmvs registry keeps every gateway-managed variable's
// snapshot stack in lockstep with the connection-state savepoint stack. If a
// new GMV is added but not registered, these tests catch the omission.

func TestConnectionState_ApplicationNameRollbackToSavepoint(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.InitApplicationName("startup-val")
	s.BeginTransaction()
	s.SetLocalApplicationName("outer")

	s.PushSavepoint("sp")
	s.SetLocalApplicationName("inner")
	require.Equal(t, "inner", s.GetApplicationName())

	s.RollbackToSavepoint("sp")
	require.Equal(t, "outer", s.GetApplicationName(),
		"ROLLBACK TO sp must restore the pre-savepoint LOCAL value")
}

func TestConnectionState_ApplicationNameCommitClearsLocal(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.InitApplicationName("startup-val")
	s.BeginTransaction()
	s.SetApplicationName("session-val")
	s.SetLocalApplicationName("local-val")
	require.Equal(t, "local-val", s.GetApplicationName())

	s.CommitTransaction()

	require.Equal(t, "session-val", s.GetApplicationName(),
		"after COMMIT, session-level SET persists; LOCAL is cleared")
}

func TestConnectionState_ApplicationNameRollbackRevertsNonLocal(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.InitApplicationName("startup-val")
	s.SetApplicationName("pre-txn")

	s.BeginTransaction()
	s.SetApplicationName("in-txn")
	require.Equal(t, "in-txn", s.GetApplicationName())

	s.RollbackTransaction()

	require.Equal(t, "pre-txn", s.GetApplicationName(),
		"ROLLBACK must revert non-LOCAL SET to pre-BEGIN session value")
}

func TestConnectionState_AllGMVsInLockstep(t *testing.T) {
	// Both statement_timeout and application_name are gateway-managed; the
	// registry must drive both through the same lifecycle in a single pass.
	s := NewMultiGatewayConnectionState()
	s.InitStatementTimeout(30 * time.Second)
	s.InitApplicationName("startup-val")

	s.SetStatementTimeout(5 * time.Second)
	s.SetApplicationName("session-val")

	s.BeginTransaction()
	s.SetStatementTimeout(10 * time.Second)
	s.SetApplicationName("in-txn")

	s.PushSavepoint("sp")
	s.SetStatementTimeout(20 * time.Second)
	s.SetApplicationName("after-sp")
	require.Equal(t, 20*time.Second, s.GetStatementTimeout())
	require.Equal(t, "after-sp", s.GetApplicationName())

	s.RollbackToSavepoint("sp")
	require.Equal(t, 10*time.Second, s.GetStatementTimeout(),
		"statement_timeout rolled back in lockstep")
	require.Equal(t, "in-txn", s.GetApplicationName(),
		"application_name rolled back in lockstep")

	s.RollbackTransaction()
	require.Equal(t, 5*time.Second, s.GetStatementTimeout(),
		"statement_timeout reverted to pre-BEGIN session value")
	require.Equal(t, "session-val", s.GetApplicationName(),
		"application_name reverted to pre-BEGIN session value")
}

func TestConnectionState_ResetAllLocalGUCs_ClearsAllVariables(t *testing.T) {
	// ResetAllLocalGUCs must clear LOCAL on every registered GMV, not just one.
	s := NewMultiGatewayConnectionState()
	s.InitStatementTimeout(30 * time.Second)
	s.InitApplicationName("startup-val")

	s.SetStatementTimeout(5 * time.Second)
	s.SetApplicationName("session-val")

	s.SetLocalStatementTimeout(100 * time.Millisecond)
	s.SetLocalApplicationName("local-val")

	s.ResetAllLocalGUCs()
	require.Equal(t, 5*time.Second, s.GetStatementTimeout())
	require.Equal(t, "session-val", s.GetApplicationName())
}
