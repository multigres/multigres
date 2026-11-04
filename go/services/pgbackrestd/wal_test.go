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
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testSegment = "000000010000000000000001"

// fakeBin puts a pgbackrest script first on PATH. In the script, $TMP is the
// last argument (archive-get's destination) and $FIRST is the first positional
// word after the command, so a script can act on either verb.
func fakeBin(t *testing.T, body string) {
	t.Helper()
	dir := t.TempDir()
	script := "#!/bin/sh\nfor a; do last=$a; done\nTMP=$last\n" + body + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbackrest"), []byte(script), 0o700))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func newWALServer(t *testing.T, mutate func(*Config)) (*Server, *httptest.Server) {
	t.Helper()
	s := newTestServer(t, mutate)
	s.configuredFlag.Store(true)
	mux := http.NewServeMux()
	s.registerWAL(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return s, ts
}

func do(t *testing.T, method, url string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body), resp.Header
}

func tmpEntries(t *testing.T, s *Server) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(s.cfg.stateSub(dirTmp))
	require.NoError(t, err)
	return entries
}

func TestWAL_RejectsBadNamesThroughHandler(t *testing.T) {
	s := newTestServer(t, nil)
	s.configuredFlag.Store(true)
	mux := http.NewServeMux()
	s.registerWAL(mux)
	fakeBin(t, "exit 0")

	for _, target := range []string{
		"/wal/..%2Fx",
		"/wal/" + testSegment + "%2Fx",
		"/wal/%2e%2e%2f" + testSegment,
		"/wal/" + "00000001000000000000000a",
		"/wal/archive.info",
	} {
		for _, method := range []string{http.MethodPost, http.MethodGet} {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
			assert.Equal(t, http.StatusBadRequest, rec.Code, "%s %s", method, target)
		}
	}
}

func TestWAL_OtherPathsAre404(t *testing.T) {
	_, ts := newWALServer(t, nil)
	for _, p := range []string{"/", "/wal", "/wal/", "/wal/" + testSegment + "/x", "/other"} {
		code, _, _ := do(t, http.MethodGet, ts.URL+p)
		assert.Equal(t, http.StatusNotFound, code, p)
	}
}

func TestWAL_503BeforeConfigure(t *testing.T) {
	s, ts := newWALServer(t, nil)
	s.configuredFlag.Store(false)
	fakeBin(t, "exit 0")

	for _, method := range []string{http.MethodPost, http.MethodGet} {
		code, body, _ := do(t, method, ts.URL+"/wal/"+testSegment)
		assert.Equal(t, http.StatusServiceUnavailable, code, method)
		assert.Equal(t, "not configured\n", body, method)
	}
}

func TestWAL_503WhenGateClosed(t *testing.T) {
	s, ts := newWALServer(t, nil)
	fakeBin(t, "exit 0")
	reopen, err := s.gate.Close(t.Context(), time.Second)
	require.NoError(t, err)

	for _, method := range []string{http.MethodPost, http.MethodGet} {
		code, body, _ := do(t, method, ts.URL+"/wal/"+testSegment)
		assert.Equal(t, http.StatusServiceUnavailable, code, method)
		assert.Equal(t, "restore in progress\n", body, method)
	}

	reopen()
	code, _, _ := do(t, http.MethodPost, ts.URL+"/wal/"+testSegment)
	assert.Equal(t, http.StatusOK, code)
}

func TestWAL_PushMapsExitCodes(t *testing.T) {
	t.Run("success passes the pg_wal path", func(t *testing.T) {
		s, ts := newWALServer(t, nil)
		argsFile := filepath.Join(t.TempDir(), "args")
		fakeBin(t, `echo "$@" > `+argsFile+"\nexit 0")

		code, _, _ := do(t, http.MethodPost, ts.URL+"/wal/"+testSegment)
		assert.Equal(t, http.StatusOK, code)
		args, err := os.ReadFile(argsFile)
		require.NoError(t, err)
		assert.Contains(t, string(args), "archive-push "+filepath.Join(s.cfg.PGData, "pg_wal", testSegment))
	})

	for _, exit := range []string{"1", "62", "103"} {
		t.Run("exit "+exit, func(t *testing.T) {
			_, ts := newWALServer(t, nil)
			fakeBin(t, "echo 'ERROR: [103]: boom' >&2\nexit "+exit)

			code, body, hdr := do(t, http.MethodPost, ts.URL+"/wal/"+testSegment)
			assert.Equal(t, http.StatusInternalServerError, code)
			assert.Contains(t, body, "ERROR: [103]: boom")
			assert.Contains(t, hdr.Get("Content-Type"), "text/plain")
		})
	}
}

func TestWAL_PushErrorBodyIsCapped(t *testing.T) {
	_, ts := newWALServer(t, nil)
	fakeBin(t, "head -c 200000 /dev/zero | tr '\\0' x >&2\nexit 103")

	code, body, _ := do(t, http.MethodPost, ts.URL+"/wal/"+testSegment)
	assert.Equal(t, http.StatusInternalServerError, code)
	assert.Len(t, body, maxErrBody)
}

func TestWAL_FetchServesFileWithContentLength(t *testing.T) {
	s, ts := newWALServer(t, nil)
	fakeBin(t, `printf 'segment-bytes' > "$TMP"`+"\nexit 0")

	code, body, hdr := do(t, http.MethodGet, ts.URL+"/wal/"+testSegment)
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "segment-bytes", body)
	assert.Equal(t, strconv.Itoa(len("segment-bytes")), hdr.Get("Content-Length"))
	assert.Empty(t, tmpEntries(t, s))
}

func TestWAL_FetchNotFoundIs404(t *testing.T) {
	t.Run("exit 1 is a missing segment", func(t *testing.T) {
		_, ts := newWALServer(t, nil)
		fakeBin(t, "echo 'ERROR: [001]: unable to find' >&2\nexit 1")

		code, _, _ := do(t, http.MethodGet, ts.URL+"/wal/"+testSegment)
		assert.Equal(t, http.StatusNotFound, code)
	})

	// A repository error must never read as "no more WAL".
	t.Run("exit 103 is a server error", func(t *testing.T) {
		_, ts := newWALServer(t, nil)
		fakeBin(t, "echo 'ERROR: [103]: no archive.info' >&2\nexit 103")

		code, body, _ := do(t, http.MethodGet, ts.URL+"/wal/"+testSegment)
		assert.Equal(t, http.StatusInternalServerError, code)
		assert.Contains(t, body, "[103]")
	})
}

func TestWAL_FetchRemovesTmpOnSuccessAndFailure(t *testing.T) {
	cases := map[string]struct {
		script string
		status int
	}{
		"success":        {`printf data > "$TMP"` + "\nexit 0", http.StatusOK},
		"not found":      {`printf partial > "$TMP"` + "\nexit 1", http.StatusNotFound},
		"error":          {`printf partial > "$TMP"` + "\nexit 103", http.StatusInternalServerError},
		"planted link":   {`ln -s /etc/passwd "$TMP"` + "\nexit 0", http.StatusInternalServerError},
		"no file at all": {"exit 0", http.StatusInternalServerError},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, ts := newWALServer(t, nil)
			fakeBin(t, tc.script)

			code, body, _ := do(t, http.MethodGet, ts.URL+"/wal/"+testSegment)
			assert.Equal(t, tc.status, code)
			assert.NotContains(t, body, "root:")
			assert.Empty(t, tmpEntries(t, s))
		})
	}
}

func TestWAL_FetchLimit(t *testing.T) {
	s, ts := newWALServer(t, func(c *Config) { c.MaxFetches = 1 })
	dir := t.TempDir()
	ready, release := filepath.Join(dir, "ready"), filepath.Join(dir, "release")
	// Only archive-get waits; archive-push returns at once.
	fakeBin(t, `case "$*" in *archive-get*) touch `+ready+"; while [ ! -e "+release+
		` ]; do sleep 0.05; done; printf ok > "$TMP";; esac`+"\nexit 0")

	var wg sync.WaitGroup
	wg.Add(1)
	var firstCode int
	go func() {
		defer wg.Done()
		firstCode, _, _ = do(t, http.MethodGet, ts.URL+"/wal/"+testSegment)
	}()
	require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, 10*time.Second, 20*time.Millisecond)

	code, body, _ := do(t, http.MethodGet, ts.URL+"/wal/"+testSegment)
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "fetch limit\n", body)

	// The slots are full, and a push must not be held up by them.
	code, _, _ = do(t, http.MethodPost, ts.URL+"/wal/"+testSegment)
	assert.Equal(t, http.StatusOK, code, "push blocked by full fetch slots")

	require.NoError(t, os.WriteFile(release, nil, 0o600))
	wg.Wait()
	assert.Equal(t, http.StatusOK, firstCode)
	assert.Empty(t, tmpEntries(t, s))

	// The slot is returned once the fetch ends.
	code, _, _ = do(t, http.MethodGet, ts.URL+"/wal/"+testSegment)
	assert.Equal(t, http.StatusOK, code)
}

func TestWAL_HeadIsRejectedAndRunsNothing(t *testing.T) {
	_, ts := newWALServer(t, nil)
	marker := filepath.Join(t.TempDir(), "ran")
	fakeBin(t, "touch "+marker+"\nexit 0")

	code, _, hdr := do(t, http.MethodHead, ts.URL+"/wal/"+testSegment)
	assert.Equal(t, http.StatusMethodNotAllowed, code)
	assert.Equal(t, "GET, POST", hdr.Get("Allow"))
	_, err := os.Stat(marker)
	assert.ErrorIs(t, err, os.ErrNotExist, "HEAD ran pgbackrest")
}

func TestWAL_FetchOfPlantedFIFODoesNotBlock(t *testing.T) {
	s, ts := newWALServer(t, func(c *Config) { c.MaxFetches = 1 })
	fakeBin(t, `mkfifo "$TMP"`+"\nexit 0")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/wal/"+testSegment, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "request blocked on the FIFO")
	resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Empty(t, tmpEntries(t, s))

	// Neither the fetch slot nor the gate was pinned.
	fakeBin(t, `printf ok > "$TMP"`+"\nexit 0")
	code, _, _ := do(t, http.MethodGet, ts.URL+"/wal/"+testSegment)
	assert.Equal(t, http.StatusOK, code)
	reopen, err := s.gate.Close(t.Context(), time.Second)
	require.NoError(t, err)
	reopen()
}

func TestWAL_DisconnectKillsChild(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		t.Run(method, func(t *testing.T) {
			s, ts := newWALServer(t, nil)
			dir := t.TempDir()
			pidFile := filepath.Join(dir, "pid")
			fakeBin(t, "echo $$ > "+pidFile+"\nsleep 300 &\nwait")

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, method, ts.URL+"/wal/"+testSegment, nil)
			require.NoError(t, err)
			errCh := make(chan error, 1)
			go func() {
				resp, err := http.DefaultClient.Do(req)
				if err == nil {
					resp.Body.Close()
				}
				errCh <- err
			}()

			var pid int
			require.Eventually(t, func() bool {
				raw, err := os.ReadFile(pidFile)
				if err != nil {
					return false
				}
				pid, err = strconv.Atoi(strings.TrimSpace(string(raw)))
				return err == nil
			}, 10*time.Second, 20*time.Millisecond)

			cancel()
			require.Error(t, <-errCh)
			require.Eventually(t, func() bool { return syscall.Kill(pid, 0) == syscall.ESRCH },
				10*time.Second, 20*time.Millisecond, "child survived the client disconnect")

			// The handler has released its gate slot, so a restore can proceed.
			require.Eventually(t, func() bool {
				reopen, err := s.gate.Close(t.Context(), 50*time.Millisecond)
				if err != nil {
					return false
				}
				reopen()
				return true
			}, 10*time.Second, 20*time.Millisecond)
			assert.Empty(t, tmpEntries(t, s))
		})
	}
}
