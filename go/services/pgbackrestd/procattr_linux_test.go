//go:build linux

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
	"testing"

	"github.com/stretchr/testify/require"
)

// Parent-death ownership is exercised through the actual supervisor by
// TestOwnedWorkerSurvivesParentOnlyUntilQuiescence, not a test-only Pdeathsig fork.
func TestSupervisedChildDoesNotUsePdeathsig(t *testing.T) {
	require.Zero(t, supervisedChildSysProcAttr().Pdeathsig)
	require.True(t, supervisorSysProcAttr().Setpgid)
	require.Zero(t, supervisorSysProcAttr().Pdeathsig)
}
