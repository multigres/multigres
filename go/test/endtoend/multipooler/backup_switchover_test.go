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

package multipooler

import (
	"testing"
)

// TestBackup_FailureMatrix_PlannedSwitchover is the planned-switchover
// counterpart to TestBackup_FailureMatrix_PrimaryFailover: instead of killing
// the primary postgres mid-backup, it should issue a graceful primary
// shutdown and assert the bounded-time switchover contract.
//
// This depends on the multipooler Manager.Shutdown RPC (PR #902). Until that
// lands the test skips cleanly so it can be wired into CI now and filled in
// when the dependency is met.
//
// The eventual implementation will mirror runFailoverMatrixCase but call a
// new shardsetup.ShardSetup.PlannedSwitchover helper instead of KillPostgres.
// Empirically (see TestBackup_FailureMatrix_PrimaryFailover) only the
// "Upload" pause point yields a meaningful failure under the kill-postgres
// fault model; the switchover fault model may differ, and we'll re-evaluate
// pause points when the dependency unblocks.
func TestBackup_FailureMatrix_PlannedSwitchover(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	t.Skip("requires multipooler Manager.Shutdown RPC (PR #902)")
}
