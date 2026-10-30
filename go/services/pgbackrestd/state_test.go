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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
)

func TestState_Validate(t *testing.T) {
	ok := persistedState{Repos: []repoGen{{1, 1}, {2, 256}}, WALSegmentSizeBytes: 16 << 20}
	require.NoError(t, validateState(ok))

	bad := map[string]persistedState{
		"none":              {WALSegmentSizeBytes: 16 << 20},
		"generation zero":   {Repos: []repoGen{{0, 1}}, WALSegmentSizeBytes: 16 << 20},
		"repo zero":         {Repos: []repoGen{{1, 0}}, WALSegmentSizeBytes: 16 << 20},
		"repo 257":          {Repos: []repoGen{{1, 257}}, WALSegmentSizeBytes: 16 << 20},
		"duplicate gen":     {Repos: []repoGen{{1, 1}, {1, 2}}, WALSegmentSizeBytes: 16 << 20},
		"duplicate number":  {Repos: []repoGen{{1, 1}, {2, 1}}, WALSegmentSizeBytes: 16 << 20},
		"segment zero":      {Repos: []repoGen{{1, 1}}},
		"segment not pow2":  {Repos: []repoGen{{1, 1}}, WALSegmentSizeBytes: 3 << 20},
		"segment too big":   {Repos: []repoGen{{1, 1}}, WALSegmentSizeBytes: 1 << 31},
		"segment too small": {Repos: []repoGen{{1, 1}}, WALSegmentSizeBytes: 1 << 19},
	}
	for name, st := range bad {
		assert.Error(t, validateState(st), name)
	}
}

// A persisted state that no longer loads or builds must not stop startup: the
// sidecar gates postgres, and multipooler cannot repair a sidecar that is
// down. It starts unconfigured, with the file set aside and the reason in
// Status, and a Configure repairs it.
func TestState_UnloadableStateStartsUnconfigured(t *testing.T) {
	good := `{"revision":4,"repos":[{"generation":1,"repo_number":1}],"wal_segment_size_bytes":16777216}`
	cases := map[string]struct {
		content   string
		wantFloor int64
		keys      string // replaces the key file before restart when non-empty
	}{
		"garbage":       {content: "{not json"},
		"unknown field": {content: `{"revision":4,"repos":[{"generation":1,"repo_number":1}],"wal_segment_size_bytes":16777216,"extra":1}`, wantFloor: 4},
		"invalid":       {wantFloor: 4, content: `{"revision":4,"repos":[{"generation":1,"repo_number":1},{"generation":1,"repo_number":2}],"wal_segment_size_bytes":16777216}`},
		"empty file":    {content: ""},
		"removed key":   {content: good, wantFloor: 4, keys: `{"2":"other"}`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			keys := writeKeyFile(t, `{"1":"one"}`)
			h := newControlHarness(t, func(c *Config) { c.CipherKeyFile = keys })
			path := h.s.cfg.stateSub(fileGenerations)
			require.NoError(t, os.WriteFile(path, []byte(tc.content), 0o600))
			stale := h.s.cfg.stateSub(fileClientConf)
			require.NoError(t, os.WriteFile(stale, []byte("[global]\nrepo1-path=/stale\n"), 0o600))
			if tc.keys != "" {
				require.NoError(t, os.WriteFile(keys, []byte(tc.keys), 0o600))
			}

			s2, err := newForTest(h.s.cfg)
			require.NoError(t, err, "a state that does not build must not crashloop the sidecar")
			t.Cleanup(func() { s2.Close() })
			assert.False(t, s2.configured())
			assert.NotContains(t, readFile(t, stale), "repo1-path", "the stale rendered conf is gone")
			assert.NoFileExists(t, path)
			assert.Equal(t, tc.content, readFile(t, path+".bad"))
			rev, got := s2.held()
			assert.Equal(t, tc.wantFloor, rev)
			assert.Empty(t, got)
			assert.Empty(t, s2.snapshot().Repos, "nothing of the old state is in force")
			assert.NotEmpty(t, s2.stateError())

			mux := http.NewServeMux()
			s2.registerControl(mux)
			srv := httptest.NewServer(mux)
			t.Cleanup(srv.Close)
			c2 := &controlHarness{s: s2, client: pgbackrestdserviceconnect.NewPgBackRestdClient(srv.Client(), srv.URL)}

			st, err := c2.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
			require.NoError(t, err, "Status never fails")
			assert.False(t, st.Msg.Configured)
			assert.NotEmpty(t, st.Msg.StateError)
			_, err = c2.run(t.Context(), pb.Command_COMMAND_INFO)
			assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

			// A list that still needs the removed key is refused and the floor
			// holds; one that does not repairs the sidecar and clears the reason.
			gen := uint32(1)
			if tc.keys != "" {
				gen = 2
				_, err := c2.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: 5, Repos: repos(1, 1)}))
				require.Error(t, err)
				assert.False(t, s2.configured())
				assert.False(t, c2.configure(t, tc.wantFloor-1, repos(2, 1)).Accepted, "the floor survived")
			}
			assert.True(t, c2.configure(t, 5, repos(gen, 1)).Accepted)
			assert.True(t, s2.configured())
			assert.Empty(t, s2.stateError())
			st, err = c2.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
			require.NoError(t, err)
			assert.Empty(t, st.Msg.StateError)
		})
	}
}

func TestState_NoFileMeansUnconfigured(t *testing.T) {
	s := newTestServer(t, nil)
	assert.False(t, s.configured())
	rev, got := s.held()
	assert.Zero(t, rev)
	assert.Empty(t, got)
	// The server conf does not wait for a Configure.
	assert.FileExists(t, s.cfg.stateSub(fileServerConf))
}

func TestState_WriteIsAtomicAndSorted(t *testing.T) {
	s := newTestServer(t, nil)
	require.NoError(t, s.writeState(persistedState{Revision: 3, Repos: []repoGen{{9, 2}, {4, 1}}, WALSegmentSizeBytes: 16 << 20}))
	assert.JSONEq(t, `{"revision":3,"repos":[{"generation":4,"repo_number":1},{"generation":9,"repo_number":2}],"wal_segment_size_bytes":16777216}`,
		readFile(t, s.cfg.stateSub(fileGenerations)))
	entries, err := os.ReadDir(s.cfg.stateSub(dirState))
	require.NoError(t, err)
	assert.Len(t, entries, 1, "no temp file left beside the state file")
}

// The floor and the reason survive any number of restarts, and a later
// set-aside state does not replace an earlier one.
func TestState_FloorAndReasonSurviveRepeatedRestarts(t *testing.T) {
	h := newControlHarness(t, nil)
	path := h.s.cfg.stateSub(fileGenerations)
	bad := `{"revision":9,"repos":[],"wal_segment_size_bytes":1,"from_the_future":true}`
	require.NoError(t, os.WriteFile(path, []byte(bad), 0o600))

	for restart := 1; restart <= 3; restart++ {
		s, err := newForTest(h.s.cfg)
		require.NoError(t, err, "restart %d", restart)
		rev, _ := s.held()
		assert.Equal(t, int64(9), rev, "restart %d keeps the floor", restart)
		assert.NotEmpty(t, s.stateError(), "restart %d keeps the reason", restart)
		assert.False(t, s.configured())
		s.Close()
	}
	assert.Equal(t, bad, readFile(t, path+".bad"))

	// A second bad state gets its own name, and the higher floor wins.
	require.NoError(t, os.WriteFile(path, []byte(`{"revision":12,"extra":1}`), 0o600))
	s, err := newForTest(h.s.cfg)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	assert.Equal(t, bad, readFile(t, path+".bad"))
	assert.Equal(t, `{"revision":12,"extra":1}`, readFile(t, path+".bad.1"))
	rev, _ := s.held()
	assert.Equal(t, int64(12), rev)

	// Bounded: more set-asides replace the last name instead of growing.
	for i := range 2 * maxBadStates {
		require.NoError(t, os.WriteFile(path, fmt.Appendf(nil, `{"revision":%d,"extra":1}`, 20+i), 0o600))
		s3, err := newForTest(h.s.cfg)
		require.NoError(t, err)
		s3.Close()
	}
	assert.NoFileExists(t, badStatePath(path, maxBadStates))

	// A Configure repairs it: the reason goes, and a restart now loads a good state.
	mux := http.NewServeMux()
	s.registerControl(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c := &controlHarness{s: s, client: pgbackrestdserviceconnect.NewPgBackRestdClient(srv.Client(), srv.URL)}
	require.True(t, c.configure(t, 50, repos(1, 1)).Accepted)
	s4, err := newForTest(h.s.cfg)
	require.NoError(t, err)
	t.Cleanup(func() { s4.Close() })
	assert.True(t, s4.configured())
	assert.Empty(t, s4.stateError())
}

// goBackground refuses once stopBackground has begun, so it can never race the
// WaitGroup's Wait.
func TestBackground_LateStartsAreRefusedAndRaceFree(t *testing.T) {
	s := newTestServer(t, nil)
	var wg sync.WaitGroup
	var ran atomic.Int32
	for range 50 {
		wg.Go(func() { s.goBackground(func() { ran.Add(1) }) })
	}
	wg.Go(s.stopBackground)
	wg.Wait()
	assert.False(t, s.goBackground(func() { t.Error("ran after stop") }))
	s.stopBackground()
	assert.LessOrEqual(t, ran.Load(), int32(50))
}
