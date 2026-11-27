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

package pgbackrestd

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplaceOnce_RefusesAnEditThatDoesNotMatchExactlyOnce(t *testing.T) {
	out, err := replaceOnce("a=1\nb=2\n", "b=2", "b=3")
	require.NoError(t, err)
	assert.Equal(t, "a=1\nb=3\n", out)

	for name, body := range map[string]string{"absent": "a=1\n", "duplicated": "b=2\nb=2\n"} {
		t.Run(name, func(t *testing.T) {
			_, err := replaceOnce(body, "b=2", "b=3")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "exactly one")
		})
	}
}

func TestQueueMaxBytes_Units(t *testing.T) {
	for in, want := range map[string]uint64{
		"512": 512, "1k": 1 << 10, "1KiB": 1 << 10, "2m": 2 << 20, "256MiB": 256 << 20,
		"3g": 3 << 30, "1t": 1 << 40, "1TiB": 1 << 40,
		// Beyond what fits 64 bits is not a size.
		"99999999999999999999": 0,
	} {
		assert.Equal(t, want, queueMaxBytes(in), in)
	}
}

func TestArchiveGetQueueMax_IsRaisedToTwoSegments(t *testing.T) {
	assert.Equal(t, "256MiB", archiveGetQueueMax("256MiB", 16<<20))
	assert.Equal(t, "2147483648", archiveGetQueueMax("1MiB", 1<<30), "a queue smaller than two segments would be exceeded anyway")
}

func TestPgpassBody_RefusesAnUnusablePasswordFile(t *testing.T) {
	s := newTestServer(t, nil)

	s.cfg.PGPasswordFile = filepath.Join(t.TempDir(), "absent")
	_, _, err := s.pgpassBody()
	require.Error(t, err)

	empty := filepath.Join(t.TempDir(), "pw")
	require.NoError(t, os.WriteFile(empty, []byte("\n"), 0o600))
	s.cfg.PGPasswordFile = empty
	_, _, err = s.pgpassBody()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty")

	s.cfg.PGPasswordFile = ""
	_, ok, err := s.pgpassBody()
	require.NoError(t, err)
	assert.False(t, ok, "no password file means there is nothing to write")
}

// A pin that cannot be read is not a missing pin: the sidecar stops rather
// than choose a repository.
func TestRepositoryScope_UnreadablePinStopsTheSidecar(t *testing.T) {
	h := newControlHarness(t, scopedTo("commerce", "0-80", 7))
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	cfg := h.s.cfg
	require.NoError(t, h.s.Close())

	require.NoError(t, os.Remove(cfg.stateSub(fileRepositoryScope)))
	require.NoError(t, os.Mkdir(cfg.stateSub(fileRepositoryScope), 0o700))

	s, err := newForTest(cfg)
	if s != nil {
		s.Close()
	}
	require.Error(t, err)
	assert.Contains(t, err.Error(), fileRepositoryScope)
}

func TestValidateState_BoundsTheRepositoryCount(t *testing.T) {
	st := persistedState{}
	for i := range maxRepoNumber + 1 {
		st.Repos = append(st.Repos, repoGen{Generation: uint32(i + 1), RepoNumber: uint32(i + 1)})
	}
	err := validateState(st)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at most")
}

// A persisted state that cannot even be read does not crashloop the sidecar,
// which gates postgres: it starts unconfigured and says why.
func TestLoadState_StartsUnconfiguredWhenTheStateCannotBeRead(t *testing.T) {
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 4, repos(1, 1)).Accepted)
	cfg := h.s.cfg
	require.NoError(t, h.s.Close())

	require.NoError(t, os.Remove(cfg.stateSub(fileGenerations)))
	require.NoError(t, os.Mkdir(cfg.stateSub(fileGenerations), 0o700))

	s, err := newForTest(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	assert.False(t, s.configured())
	assert.Contains(t, s.stateError(), "set aside")
}

func TestGate_RefusesAnImpossibleClose(t *testing.T) {
	g := NewGate()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := g.Close(ctx, time.Second)
	require.ErrorIs(t, err, context.Canceled)

	reopen, err := g.Close(t.Context(), time.Second)
	require.NoError(t, err)
	_, err = g.Close(t.Context(), time.Second)
	require.Error(t, err, "a gate that is already closed cannot be closed again")
	assert.Contains(t, err.Error(), "already closed")
	reopen()

	require.Error(t, g.drainStopped(t.Context(), time.Second), "draining shutdown handlers needs a stopped gate")
}
