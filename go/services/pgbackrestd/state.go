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
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/tools/fileutil"
)

// defaultWALSegmentBytes is the postgres default; replaced once the real
// segment size is known.
const defaultWALSegmentBytes = 16 << 20

const (
	fileGenerations = dirState + "/generations.json"

	maxRepoNumber = 256
	// Postgres accepts WAL segment sizes from 1MiB to 1GiB, powers of two.
	minWALSegmentBytes = 1 << 20
	maxWALSegmentBytes = 1 << 30
)

// repoGen is one repository: a cipher generation on a pgBackRest repo number.
type repoGen struct {
	Generation uint32 `json:"generation"`
	RepoNumber uint32 `json:"repo_number"`
}

// persistedState is the content of generations.json. A sidecar restart reads
// it back, so a restart keeps the repository list and the revision it must
// not go below.
type persistedState struct {
	Revision            int64     `json:"revision"`
	Repos               []repoGen `json:"repos"`
	WALSegmentSizeBytes uint32    `json:"wal_segment_size_bytes"`
}

// serverState is the in-memory view of what Configure has established.
type serverState struct {
	configuredFlag atomic.Bool
	walSegBytes    atomic.Uint32
	pg             pgServer

	// configureMu serializes whole Configure calls, including their file
	// writes. mu guards the fields below and is held only to read or publish
	// them, never across I/O.
	configureMu sync.Mutex
	mu          sync.RWMutex
	revision    int64
	repos       []repoGen

	versionMu       sync.Mutex
	version         string
	versionBusy     bool
	versionFailedAt time.Time

	// serveCtx is Run's context, for work that must end with the process.
	serveCtx atomic.Pointer[context.Context]

	// bgMu and bgStopped make starting a background goroutine safe against
	// stopBackground's Wait: a late caller is refused instead of racing it.
	bgMu      sync.Mutex
	bgStopped bool
	// startPending counts post-restore start retries still outstanding.
	startPending atomic.Int32
	// children holds the pid (and process group id) of every running child.
	children sync.Map

	// serverPID is the running 8432 server child, 0 between runs.
	serverPID atomic.Int64

	// live holds the S3-command children that are running, for awaitCancelled.
	liveMu sync.Mutex
	live   map[*liveChild]struct{}
	// restoreActive counts restores between starting to quiesce and reopening
	// the gate. A count rather than a flag because a second restore is refused
	// at the gate and must not clear the first one's mark on its way out.
	restoreActive atomic.Int32
	// lastRestoreOK records that the latest restore child exited 0.
	lastRestoreOK atomic.Bool

	// stateErrMsg is why a persisted configuration was set aside at startup,
	// until a Configure replaces it.
	stateErrMu  sync.Mutex
	stateErrMsg string

	// pauseFn replaces pauseServer when set. It exists so the restore
	// sequence can be observed without a pgBackRest server child.
	pauseFn func() (resume func())
}

// configured reports whether the first Configure has completed.
func (s *Server) configured() bool { return s.configuredFlag.Load() }

// walSegmentSize returns the WAL segment size in bytes.
func (s *Server) walSegmentSize() uint32 { return s.walSegBytes.Load() }

// held returns the held revision and a copy of the repository list.
func (s *Server) held() (int64, []repoGen) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision, slices.Clone(s.repos)
}

// validateState checks the repository list the way Configure does, so a
// persisted file is held to the same rules as a request.
func validateState(st persistedState) error {
	if len(st.Repos) == 0 {
		return errors.New("at least one repository is required")
	}
	if len(st.Repos) > maxRepoNumber {
		return fmt.Errorf("at most %d repositories, got %d", maxRepoNumber, len(st.Repos))
	}
	gens := make(map[uint32]bool, len(st.Repos))
	nums := make(map[uint32]bool, len(st.Repos))
	for _, r := range st.Repos {
		if r.Generation < 1 {
			return fmt.Errorf("repository generation %d: generations start at 1", r.Generation)
		}
		if r.RepoNumber < 1 || r.RepoNumber > maxRepoNumber {
			return fmt.Errorf("repo number %d is outside 1..%d", r.RepoNumber, maxRepoNumber)
		}
		if gens[r.Generation] {
			return fmt.Errorf("duplicate repository generation %d", r.Generation)
		}
		if nums[r.RepoNumber] {
			return fmt.Errorf("duplicate repo number %d", r.RepoNumber)
		}
		gens[r.Generation], nums[r.RepoNumber] = true, true
	}
	sz := st.WALSegmentSizeBytes
	if sz < minWALSegmentBytes || sz > maxWALSegmentBytes || bits.OnesCount32(sz) != 1 {
		return fmt.Errorf("wal segment size %d is not a power of two between 1MiB and 1GiB", sz)
	}
	return nil
}

// sortRepos orders by repo number so equal states serialize equally.
func sortRepos(repos []repoGen) {
	slices.SortFunc(repos, func(a, b repoGen) int { return int(a.RepoNumber) - int(b.RepoNumber) })
}

func (st persistedState) repoMap() map[uint32]uint32 {
	m := make(map[uint32]uint32, len(st.Repos))
	for _, r := range st.Repos {
		m[r.RepoNumber] = r.Generation
	}
	return m
}

// writeState persists st with a temp file and a rename, so a crash leaves
// either the old or the new file.
func (s *Server) writeState(st persistedState) error {
	sortRepos(st.Repos)
	b, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("encode generations: %w", err)
	}
	if err := fileutil.AtomicWriteFile(s.cfg.stateSub(fileGenerations), append(b, '\n'), 0o600); err != nil {
		return fmt.Errorf("persist generations: %w", err)
	}
	return nil
}

// readState returns the persisted state, or ok=false when there is none.
func (s *Server) readState() (st persistedState, ok bool, err error) {
	path := s.cfg.stateSub(fileGenerations)
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return persistedState{}, false, nil
	}
	if err != nil {
		return persistedState{}, false, fmt.Errorf("read %s: %w", path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&st); err != nil {
		return persistedState{}, false, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := validateState(st); err != nil {
		return persistedState{}, false, fmt.Errorf("invalid %s: %w", path, err)
	}
	sortRepos(st.Repos)
	return st, true, nil
}

func (s *Server) setStateError(msg string) {
	s.stateErrMu.Lock()
	defer s.stateErrMu.Unlock()
	s.stateErrMsg = msg
}

func (s *Server) stateError() string {
	s.stateErrMu.Lock()
	defer s.stateErrMu.Unlock()
	return s.stateErrMsg
}

// loadState restores what the last Configure established, so a restart keeps
// the repository list and the revision floor. The conf is re-rendered from
// the current flags rather than trusted from disk.
//
// A persisted state that no longer loads or builds (a corrupt file, a field
// from a newer binary after a rollback, a generation whose key was removed)
// must not crashloop the sidecar: it gates postgres through its startup probe,
// and multipooler cannot send the Configure that repairs it to a sidecar that
// is down. The file is moved aside and the sidecar starts unconfigured, which
// answers WAL requests 503 and fails Run until a Configure arrives. It never
// falls back to running the repository unencrypted.
func (s *Server) loadState() error {
	if err := s.renderServerConf(); err != nil {
		return err
	}
	st, ok, err := s.readState()
	if err != nil {
		s.setAside(err)
		return s.startUnconfigured()
	}
	if !ok {
		return s.startUnconfigured()
	}
	snap, err := s.buildSnapshot(st.repoMap())
	if err != nil {
		// The file parsed, so its revision stays as the floor.
		s.setAside(fmt.Errorf("restore generations: %w", err))
		return s.startUnconfigured()
	}
	if err := s.renderClientConf(st, snap); err != nil {
		return err
	}
	if err := s.writePgpass(); err != nil {
		// Not a property of the state: the password file may not be mounted
		// yet. The next Configure writes it again.
		s.logger.Warn("write pgpass at startup", "error", err)
	}
	s.setRepoSnapshot(snap)
	s.mu.Lock()
	s.revision, s.repos = st.Revision, st.Repos
	s.mu.Unlock()
	s.walSegBytes.Store(st.WALSegmentSizeBytes)
	s.configuredFlag.Store(true)
	return nil
}

// maxBadStates bounds the files kept from set-aside states.
const maxBadStates = 5

func badStatePath(path string, n int) string {
	if n == 0 {
		return path + ".bad"
	}
	return fmt.Sprintf("%s.bad.%d", path, n)
}

// startUnconfigured is the state of a sidecar with no usable persisted
// configuration. A set-aside file means an earlier start (or this one) could
// not use its state, so the reason and the revision floor are recomputed from
// the files that were kept, on every start, until a Configure writes a good
// state. The stale rendered conf goes too: start and stop must read the
// minimal conf, not repositories nobody has configured.
func (s *Server) startUnconfigured() error {
	path := s.cfg.stateSub(fileGenerations)
	floor, found := int64(0), 0
	for n := range maxBadStates {
		raw, err := os.ReadFile(badStatePath(path, n))
		if err != nil {
			continue
		}
		found++
		floor = max(floor, lenientRevision(raw))
	}
	if found > 0 && s.stateError() == "" {
		s.setStateError(fmt.Sprintf("an earlier persisted state was set aside (%d kept as %s.bad*); waiting for a Configure", found, fileGenerations))
	}
	s.mu.Lock()
	s.revision, s.repos = floor, nil
	s.mu.Unlock()
	if err := os.Remove(s.cfg.stateSub(fileClientConf)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale pgbackrest.conf: %w", err)
	}
	return writeMinimalConf(s.cfg)
}

// lenientRevision reads just the revision from a state file the strict decode
// refused (an unknown field, a value that fails validation), so the floor
// survives. Anything it cannot read is 0.
func lenientRevision(raw []byte) int64 {
	var v struct {
		Revision int64 `json:"revision"`
	}
	if json.Unmarshal(raw, &v) != nil || v.Revision < 0 {
		return 0
	}
	return v.Revision
}

// setAside moves the persisted state file to the first free generations.json.bad
// name (the last one is replaced when all are taken), so an earlier set-aside
// state is not lost to a later one, and records why.
func (s *Server) setAside(cause error) {
	path := s.cfg.stateSub(fileGenerations)
	dest := badStatePath(path, maxBadStates-1)
	for n := range maxBadStates {
		if _, err := os.Lstat(badStatePath(path, n)); errors.Is(err, os.ErrNotExist) {
			dest = badStatePath(path, n)
			break
		}
	}
	msg := fmt.Sprintf("persisted state set aside: %v", cause)
	if err := os.Rename(path, dest); err != nil {
		msg += fmt.Sprintf(" (and it could not be moved aside: %v)", err)
	}
	s.logger.Error("persisted generations could not be loaded; starting unconfigured until a Configure arrives", "error", cause, "moved_to", dest)
	s.setStateError(msg)
}
