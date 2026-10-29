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

package pgbackrestd

import (
	"github.com/multigres/multigres/go/common/sandbox"
)

// unconfinedDecision lets the runner, state and control tests run fake
// pgBackRest scripts on a host without Landlock, and outside the policy. It is
// the same unconfined path production takes under --sandbox=off, so the
// switch itself is exercised. Everything that asserts on confinement uses New
// and the real sandbox instead.
func unconfinedDecision() sandbox.Decision {
	return sandbox.Decision{Requested: sandbox.ModeOff, Effective: sandbox.EffectiveUnconfined}
}

// newForTest is New for tests that exercise behaviour other than confinement.
func newForTest(cfg Config) (*Server, error) {
	return newServer(cfg, unconfinedDecision())
}
