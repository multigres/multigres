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

package manager

// PostgresMode is the physical recovery mode postgres reports via
// pg_is_in_recovery(): a primary (out of recovery, can accept writes) or a
// standby (in recovery). It is deliberately a dedicated type rather than a bool
// so the meaning travels with the value and cannot be confused with the routing
// role (servingstate.RoutingRole / writability) or consensus leadership — see
// deriveRoutingRole, which combines this with the consensus snapshot.
//
// It is distinct from writability: a pooler can be PostgresModePrimary (out of
// recovery) yet not the writable routing primary (e.g. the pg_promote -> WAL
// commit window, where postgres is primary but the new rule has not committed).
type PostgresMode int

const (
	// PostgresModeUnknown is the zero value: recovery mode not yet observed
	// (cold start). Treated as not-out-of-recovery, so it never derives a
	// writable routing role until postgres has actually been probed.
	PostgresModeUnknown PostgresMode = iota
	// PostgresModePrimary means postgres is out of recovery (pg_is_in_recovery()
	// is false) and can accept writes.
	PostgresModePrimary
	// PostgresModeInRecovery means postgres is in recovery (pg_is_in_recovery()
	// is true): a standby replaying WAL, read-only.
	PostgresModeInRecovery
)

// String returns a human-readable name for the postgres mode.
func (m PostgresMode) String() string {
	switch m {
	case PostgresModePrimary:
		return "primary"
	case PostgresModeInRecovery:
		return "in_recovery"
	case PostgresModeUnknown:
		return "unknown"
	default:
		return "invalid"
	}
}

// OutOfRecovery reports whether postgres is out of recovery (a primary that can
// accept writes). Only PostgresModePrimary qualifies; Unknown is treated as not
// out of recovery so an unobserved node is never mistaken for a writable primary.
func (m PostgresMode) OutOfRecovery() bool {
	return m == PostgresModePrimary
}
