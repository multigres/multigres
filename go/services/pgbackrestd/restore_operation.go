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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/safefs"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/tools/fileutil"
)

const restoreRecordFile = "state/restore.pb"

// privateWrite includes the directory sync AtomicWriteFile does not provide.
func privateWrite(path string, data []byte) error {
	if err := fileutil.AtomicWriteFile(path, data, 0o600); err != nil {
		return err
	}
	// #nosec G703 -- production callers use private StateDir plus a fixed journal name or generated UUID, never request-supplied paths
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (s *Server) loadRestoreRecord() error {
	s.incarnation = uuid.NewString()
	data, err := os.ReadFile(s.cfg.stateSub(restoreRecordFile))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	rec := new(pb.RestoreResult)
	if err := proto.Unmarshal(data, rec); err != nil {
		return fmt.Errorf("unreadable restore fence: %w", err)
	}
	if _, err := uuid.Parse(rec.GetOperation().GetId()); err != nil {
		return fmt.Errorf("invalid restore fence: %w", err)
	}
	if rec.GetOperation().GetSequence() == 0 || rec.GetOperation().GetIncarnation() == "" {
		return errors.New("invalid restore fence sequence or incarnation")
	}
	// Ownership lock and absence of dirty worker markers were established first.
	// An interrupted operation is never replayed, even if PG_VERSION is present.
	if !rec.Terminal {
		rec.Terminal = true
		rec.Succeeded = false
		rec.ExitCode = -1
	}
	s.restoreRecord = rec
	return s.saveRestoreRecord()
}

func (s *Server) saveRestoreRecord() error {
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(s.restoreRecord)
	if err == nil {
		err = privateWrite(s.cfg.stateSub(restoreRecordFile), data)
	}
	if err != nil {
		s.restorePoison = err
	}
	return err
}

// runRestoreOperation is the only wire entry for restore. CAS publication is
// durable before detaching or touching PGDATA. The mutex is not held during
// work: Status can read the admitted identity, but competing Runs cannot enter.
func (s *Server) runRestoreOperation(ctx context.Context, req *pb.RunRequest, out *streamSender) (code int, err error) {
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	if err := checkArgs(pb.Command_COMMAND_RESTORE, req.Args); err != nil {
		return -1, err
	}
	op := req.GetRestore()
	if _, err := uuid.Parse(op.GetId()); err != nil {
		return -1, connect.NewError(connect.CodeInvalidArgument, errors.New("restore requires an operation UUID"))
	}
	s.restoreMu.Lock()
	if s.restoreClosed {
		s.restoreMu.Unlock()
		return -1, errors.New("sidecar closing")
	}
	if s.restorePoison != nil {
		s.restoreMu.Unlock()
		return -1, connect.NewError(connect.CodeFailedPrecondition, errors.New("restore fence persistence failed"))
	}
	if s.restoreRecord.GetOperation().GetId() == op.Id {
		rec := s.restoreRecord
		if !proto.Equal(rec.Operation, op) || !slices.Equal(rec.Args, req.Args) {
			s.restoreMu.Unlock()
			return -1, connect.NewError(connect.CodeInvalidArgument, errors.New("restore identity reused with different parameters"))
		}
		if !rec.Terminal {
			s.restoreMu.Unlock()
			return -1, connect.NewError(connect.CodeUnavailable, errors.New("restore operation still running"))
		}
		code := int(rec.ExitCode)
		s.restoreMu.Unlock()
		return code, nil
	}
	if op.Incarnation != s.incarnation {
		s.restoreMu.Unlock()
		return -1, connect.NewError(connect.CodeFailedPrecondition, errors.New("obsolete sidecar incarnation"))
	}
	if s.restoreRunning {
		s.restoreMu.Unlock()
		return -1, connect.NewError(connect.CodeUnavailable, errors.New("restore operation active"))
	}
	if op.Sequence == 0 || op.Sequence != s.restoreRecord.GetOperation().GetSequence()+1 || op.Predecessor != s.restoreRecord.GetOperation().GetId() {
		s.restoreMu.Unlock()
		return -1, connect.NewError(connect.CodeFailedPrecondition, errors.New("restore predecessor superseded or active"))
	}
	if !s.configured() {
		s.restoreMu.Unlock()
		return -1, connect.NewError(connect.CodeFailedPrecondition, errors.New("not configured"))
	}
	s.restoreRecord = &pb.RestoreResult{Operation: proto.Clone(op).(*pb.RestoreOperation), Args: slices.Clone(req.Args), ExitCode: -1}
	if err := s.saveRestoreRecord(); err != nil {
		s.restoreMu.Unlock()
		return -1, err
	}
	s.restoreRunning = true
	s.restoreDone = make(chan struct{})
	s.restoreMu.Unlock()
	defer func() {
		s.restoreMu.Lock()
		defer s.restoreMu.Unlock()
		s.restoreRunning = false
		defer close(s.restoreDone)
		s.restoreRecord.Terminal = true
		s.restoreRecord.Succeeded = err == nil && code == 0
		s.restoreRecord.ExitCode = int32(code)
		if err != nil && code == 0 {
			s.restoreRecord.ExitCode = -1
		}
		if perr := s.saveRestoreRecord(); perr != nil {
			s.restoreRecord.Terminal = false
			s.restoreRecord.Succeeded = false
			code = -1
			err = perr
		}
	}()
	// s.run performs the gate/configuration checks, as for ordinary commands.
	return s.run(context.WithValue(ctx, restoreCleanupKey{}, op.Cleanup), pb.Command_COMMAND_RESTORE, req.Args, out)
}

type restoreCleanupKey struct{}

// cleanupRestoreData runs only while the restore gate is closed, all owned
// workers acknowledged quiescence, and the predecessor is durably fenced.
// The manager checks pgctld state and a complete backup before setting Cleanup;
// recheck postmaster and the durable marker immediately before deletion here.
func (s *Server) cleanupRestoreData(ctx context.Context) error {
	root, err := safefs.OpenRoot(s.cfg.DataVolume)
	if err != nil {
		return err
	}
	defer root.Close()
	rel, err := filepath.Rel(s.cfg.DataVolume, s.cfg.PGData)
	if err != nil {
		return err
	}
	if rel == "." || rel == ".." || filepath.IsAbs(rel) {
		return errors.New("invalid PGDATA cleanup path")
	}
	if _, err := root.Stat(filepath.Join(rel, constants.PostmasterPIDFile)); err == nil {
		return errors.New("postmaster.pid exists; refusing restore")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	cleanup, _ := ctx.Value(restoreCleanupKey{}).(bool)
	if !cleanup {
		return nil
	}
	marker := filepath.Join(filepath.Dir(rel), constants.RestoreSentinelFile)
	if _, err := root.ReadFile(marker); err != nil {
		return fmt.Errorf("interrupted cleanup requires restore marker: %w", err)
	}
	return root.RemoveAll(rel)
}
