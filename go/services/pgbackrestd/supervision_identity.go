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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/multigres/multigres/go/common/pgbackreststate"
	"github.com/multigres/multigres/go/common/safefs"
)

// workerOwner identifies the process domain that could still contain writers.
// PID reuse, a fresh process token, or an available flock never proves exit.
// Linux namespace-init recovery has a narrower runtime contract documented in
// priorRuntimeDomainEnded; ordinary local owners always need an ACK.
type workerOwner struct {
	Platform        string `json:"platform"`
	Token           string `json:"token"`
	PID             int    `json:"pid"`
	StartTicks      uint64 `json:"start_ticks,omitempty"`
	BootID          string `json:"boot_id,omitempty"`
	NamespaceDevice uint64 `json:"pid_namespace_device,omitempty"`
	NamespaceInode  uint64 `json:"pid_namespace_inode,omitempty"`
	Hostname        string `json:"hostname,omitempty"`
}

type workerMarker struct {
	Version int         `json:"version"`
	Owner   workerOwner `json:"owner"`
}

func (s *Server) workerMarkerBytes() ([]byte, error) {
	return json.Marshal(workerMarker{Version: 1, Owner: s.workerOwner})
}

func decodeWorkerMarker(data []byte) (workerMarker, error) {
	var marker workerMarker
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&marker); err != nil {
		return marker, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return marker, errors.New("trailing worker marker data")
	}
	if marker.Version != 1 || marker.Owner.PID <= 0 {
		return marker, errors.New("unsupported worker marker identity")
	}
	token, err := uuid.Parse(marker.Owner.Token)
	if err != nil || token == uuid.Nil {
		return marker, errors.New("invalid worker owner token")
	}
	marker.Owner.Token = token.String()
	if marker.Owner.Platform == "" {
		return marker, errors.New("missing worker platform")
	}
	if marker.Owner.Platform == "linux" {
		if marker.Owner.StartTicks == 0 || marker.Owner.NamespaceDevice == 0 || marker.Owner.NamespaceInode == 0 || marker.Owner.Hostname == "" {
			return marker, errors.New("incomplete Linux worker domain")
		}
		boot, err := uuid.Parse(marker.Owner.BootID)
		if err != nil || boot == uuid.Nil {
			return marker, errors.New("invalid worker boot identity")
		}
		marker.Owner.BootID = boot.String()
	}
	return marker, nil
}

// recoverWorkerMarkers runs with the original instance-lock inode exclusively
// held. Validate EVERY marker before moving any. An unscoped, malformed, local
// or same-namespace marker remains an explicit fail-closed condition.
func (s *Server) recoverWorkerMarkers() error {
	entries, err := os.ReadDir(s.cfg.StateDir)
	if err != nil {
		return fmt.Errorf("inspect worker ownership markers: %w", err)
	}
	root, err := safefs.OpenRoot(s.cfg.StateDir)
	if err != nil {
		return err
	}
	defer root.Close()
	var retired []string
	for _, entry := range entries {
		name := entry.Name()
		if !pgbackreststate.IsPendingWorker(name) {
			continue
		}
		data, err := root.ReadFile(name)
		if err != nil {
			return fmt.Errorf("worker quiescence was not recorded; cannot read %s: %w", name, err)
		}
		marker, err := decodeWorkerMarker(data)
		if err != nil {
			return fmt.Errorf("worker quiescence was not recorded; invalid %s: %w", name, err)
		}
		if !priorRuntimeDomainEnded(marker.Owner, s.workerOwner) {
			return errors.New("worker quiescence was not recorded; prior process domain is not proven ended")
		}
		retired = append(retired, name)
	}
	if len(retired) == 0 {
		return nil
	}
	// Retain the evidence rather than erasing it. These names cannot be mistaken
	// for active worker markers by a later startup. The directory is private to
	// the sidecar and protected by the same instance lock.
	const archive = "recovered-workers"
	dir := s.cfg.stateSub(archive)
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := root.Stat(archive)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("worker recovery audit path is not a directory")
	}
	for _, name := range retired {
		target := filepath.Join(dir, name)
		if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("worker recovery audit entry already exists or cannot be checked: %s", name)
		}
		if err := os.Rename(s.cfg.stateSub(name), target); err != nil {
			return err
		}
	}
	for _, path := range []string{dir, s.cfg.StateDir} {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		err = f.Sync()
		f.Close()
		if err != nil {
			return err
		}
	}
	s.logger.Warn("retired worker markers after runtime PID-namespace teardown", "markers", len(retired), "pid_namespace", s.workerOwner.NamespaceInode, "owner_start_ticks", s.workerOwner.StartTicks, "boundary", "runtime-serialized PID-1 replacement")
	return nil
}
