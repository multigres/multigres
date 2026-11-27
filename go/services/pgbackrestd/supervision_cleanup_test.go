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

	"github.com/multigres/multigres/go/common/pgbackreststate"
)

func TestCleanupGuardDaemonProtocol(t *testing.T) {
	s := newTestServer(t, nil)
	guard, err := pgbackreststate.AcquireCleanupGuard(s.cfg.StateDir)
	require.Nil(t, guard)
	require.ErrorIs(t, err, pgbackreststate.ErrStateInUse)
	require.NoError(t, s.Close())
	guard, err = pgbackreststate.AcquireCleanupGuard(s.cfg.StateDir)
	require.NoError(t, err)
	_, err = newForTest(s.cfg)
	require.ErrorContains(t, err, "still own", "cleanup lease must exclude startup until it is released")
	require.NoError(t, guard.Close())
	next, err := newForTest(s.cfg)
	require.NoError(t, err)
	require.NoError(t, next.Close())
}
