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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

func TestChildSysProcAttr(t *testing.T) {
	a := childSysProcAttr()
	assert.True(t, a.Setpgid)
}

func TestKillChildren_EndsRunningChild(t *testing.T) {
	fake := newFakeBackrest(t, nil, true)
	h := newControlHarness(t, nil)
	h.s.pauseFn = pauseRecorder(t, fake)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)

	done := make(chan error, 1)
	go func() {
		_, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
		done <- err
	}()
	fake.waitRestoreStarted(t)
	count := 0
	h.s.children.Range(func(_, _ any) bool { count++; return true })
	require.Equal(t, 1, count, "the restore child is tracked")

	h.s.killChildren()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("a restore child survived the group kill")
	}
	require.Eventually(t, func() bool {
		n := 0
		h.s.children.Range(func(_, _ any) bool { n++; return true })
		return n == 0
	}, 5*time.Second, 10*time.Millisecond, "finished children are forgotten")
}
