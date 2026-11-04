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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
)

const (
	// maxErrBody caps the pgBackRest output returned as an error body.
	maxErrBody = 64 << 10

	// exitNotFound is archive-get's exit code for a segment that is absent
	// from a healthy repository. Repository errors (403, unreachable, missing
	// archive.info) exit 103 and must not read as "not found", or a restore
	// would treat an outage as the end of the WAL.
	exitNotFound = 1
)

// limitedBuffer keeps the first maxErrBody bytes written and discards the
// rest, so a chatty child cannot grow the sidecar's memory.
type limitedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if room := maxErrBody - b.buf.Len(); room > 0 {
		b.buf.Write(p[:min(len(p), room)])
	}
	return len(p), nil
}

func (b *limitedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// registerWAL serves the WAL socket, which only postgres and the sidecar can
// reach. A request cannot name anything but a WAL file name.
func (s *Server) registerWAL(mux *http.ServeMux) {
	fetchSlots := make(chan struct{}, s.cfg.MaxFetches)
	mux.HandleFunc("POST /wal/{name}", s.handleWALPush)
	// Without this, the GET pattern also serves HEAD and would run archive-get.
	mux.HandleFunc("HEAD /wal/{name}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Allow", "GET, POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
	mux.HandleFunc("GET /wal/{name}", func(w http.ResponseWriter, r *http.Request) {
		s.handleWALFetch(w, r, fetchSlots)
	})
}

// admit applies the checks both verbs share. On success the caller owns the
// returned release and must call it when the child is done.
func (s *Server) admit(w http.ResponseWriter, r *http.Request) (name string, release func(), ok bool) {
	name = r.PathValue("name")
	if !validWALName(name) {
		http.Error(w, "invalid WAL name", http.StatusBadRequest)
		return "", nil, false
	}
	if !s.configured() {
		http.Error(w, "not configured", http.StatusServiceUnavailable)
		return "", nil, false
	}
	release, ok = s.gate.TryEnter()
	if !ok {
		http.Error(w, "restore in progress", http.StatusServiceUnavailable)
		return "", nil, false
	}
	return name, release, true
}

// handleWALPush answers only once the segment is in the repository: the async
// archive-push finds its work through archive_status/*.ready, and returning
// early would make postgres delete a segment that is not yet stored.
func (s *Server) handleWALPush(w http.ResponseWriter, r *http.Request) {
	name, release, ok := s.admit(w, r)
	if !ok {
		return
	}
	defer release()

	var out limitedBuffer
	code, err := s.runChild(r.Context(), KindS3Command, cmdArchivePush,
		[]string{filepath.Join(s.cfg.PGData, "pg_wal", name)}, &out, &out)
	if err != nil {
		s.childFailed(w, r.Context(), cmdArchivePush, err, &out)
		return
	}
	if code != 0 {
		writeChildError(w, code, &out)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleWALFetch(w http.ResponseWriter, r *http.Request, slots chan struct{}) {
	name, release, ok := s.admit(w, r)
	if !ok {
		return
	}
	defer release()

	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		http.Error(w, "fetch limit", http.StatusServiceUnavailable)
		return
	}

	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	tmpName := name + "." + hex.EncodeToString(suffix)
	tmpDir := s.cfg.stateSub(dirTmp)
	tmp := filepath.Join(tmpDir, tmpName)
	// The child writes here, so it can leave anything at the path, including
	// on a failure; it is removed whatever happened.
	defer os.Remove(tmp)

	var out limitedBuffer
	code, err := s.runChild(r.Context(), KindS3Command, cmdArchiveGet, []string{name, tmp}, &out, &out)
	if err != nil {
		s.childFailed(w, r.Context(), cmdArchiveGet, err, &out)
		return
	}
	switch code {
	case 0:
	case exitNotFound:
		http.Error(w, "not found", http.StatusNotFound)
		return
	default:
		writeChildError(w, code, &out)
		return
	}

	if err := s.streamTmp(w, tmpDir, tmpName); err != nil {
		s.logger.WarnContext(r.Context(), "serve fetched WAL", "name", name, "error", err)
	}
}

// streamTmp sends tmpDir/tmpName. The child could have replaced the file with
// a symlink, so it is opened through a root that refuses to leave tmpDir and
// must be a regular file.
func (s *Server) streamTmp(w http.ResponseWriter, tmpDir, tmpName string) error {
	root, err := os.OpenRoot(tmpDir)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return err
	}
	defer root.Close()
	// O_NONBLOCK so a FIFO left at the path fails the regular-file check below
	// instead of blocking open(2), which no request cancel could interrupt and
	// which would pin the gate and a fetch slot. It is a no-op on regular files.
	f, err := root.OpenFile(tmpName, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return fmt.Errorf("open fetched file: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err == nil && !fi.Mode().IsRegular() {
		err = errors.New("fetched file is not a regular file")
	}
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return err
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, f)
	return err
}

// childFailed handles a runChild error. A cancelled request has no client
// left to answer.
func (s *Server) childFailed(w http.ResponseWriter, ctx context.Context, command string, err error, out *limitedBuffer) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	s.logger.ErrorContext(ctx, "run pgbackrest", "command", command, "error", err)
	writeChildError(w, -1, out)
}

func writeChildError(w http.ResponseWriter, code int, out *limitedBuffer) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusInternalServerError)
	body := out.String()
	if body == "" {
		body = fmt.Sprintf("pgbackrest exited with code %d\n", code)
	}
	//nolint:gosec // text/plain, so nothing renders it as markup
	_, _ = io.WriteString(w, body)
}
